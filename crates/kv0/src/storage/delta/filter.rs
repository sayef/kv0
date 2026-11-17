//! [`Pred`] without a query engine: whether a file or row group may hold a matching row (from
//! its statistics), and which rows of a record batch match.
//!
//! Statistics only ever rule rows out: anything unknown (a missing statistic, a column a file
//! lacks, a literal of another type) counts as "may match". Rows are evaluated with SQL's null
//! rule: a comparison with a null never matches.

use std::collections::HashSet;
use std::sync::Arc;

use deltalake_core::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use deltalake_core::arrow::buffer::BooleanBuffer;
use deltalake_core::arrow::compute::kernels::cmp;
use deltalake_core::arrow::compute::{cast, is_not_null, is_null, prep_null_mask_filter};
use deltalake_core::arrow::datatypes::{DataType, Int64Type};
use deltalake_core::parquet::file::metadata::RowGroupMetaData;
use deltalake_core::parquet::file::statistics::Statistics;

use crate::error::{Error, Result};
use crate::storage::pred::{Cmp, Lit, Pred};

/// A bound of a column's values, in the order kv0 compares them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Bound {
    Int(i64),
    Bytes(Vec<u8>),
}

/// What statistics say about a column in a file or row group.
#[derive(Clone, Debug, Default)]
pub(crate) struct ColStats {
    pub min: Option<Bound>,
    pub max: Option<Bound>,
    pub nulls: Option<u64>,
    pub rows: u64,
}

fn bound(v: &Lit) -> Option<Bound> {
    match v {
        Lit::Int(i) | Lit::TimestampMicros(i) => Some(Bound::Int(*i)),
        Lit::Str(s) => Some(Bound::Bytes(s.as_bytes().to_vec())),
        // Delta has no unsigned columns; such a literal never reaches it (see `eval`).
        Lit::UInt(_) => None,
    }
}

/// Whether two bounds can be compared (same kind).
fn comparable(a: &Bound, b: &Bound) -> bool {
    matches!(
        (a, b),
        (Bound::Int(_), Bound::Int(_)) | (Bound::Bytes(_), Bound::Bytes(_))
    )
}

/// Whether rows described by `stats` (per column, `None` when unknown) may match `pred`.
pub(crate) fn may_match(pred: &Pred, stats: &dyn Fn(&str) -> Option<ColStats>) -> bool {
    match pred {
        Pred::And(ps) => ps.iter().all(|p| may_match(p, stats)),
        Pred::Or(ps) => ps.iter().any(|p| may_match(p, stats)),
        Pred::In(c, vs) => {
            if vs.is_empty() {
                return false;
            }
            let Some(s) = stats(c) else { return true };
            vs.iter().any(|v| cmp_may_match(&s, Cmp::Eq, v))
        }
        Pred::Cmp(c, op, v) => stats(c).is_none_or(|s| cmp_may_match(&s, *op, v)),
        Pred::IsNull(c) => stats(c).and_then(|s| s.nulls).is_none_or(|n| n > 0),
        Pred::NotNull(c) => stats(c).is_none_or(|s| s.nulls.is_none_or(|n| n < s.rows)),
        Pred::StartsWith(c, p) => {
            let Some(s) = stats(c) else { return true };
            if s.nulls.is_some_and(|n| n >= s.rows) && s.rows > 0 {
                return false;
            }
            let p = p.as_bytes();
            let trunc = |b: &[u8]| b[..b.len().min(p.len())].to_vec();
            let lo = match &s.min {
                Some(Bound::Bytes(b)) => trunc(b).as_slice() <= p,
                _ => true,
            };
            let hi = match &s.max {
                Some(Bound::Bytes(b)) => p <= trunc(b).as_slice(),
                _ => true,
            };
            lo && hi
        }
    }
}

fn cmp_may_match(s: &ColStats, op: Cmp, v: &Lit) -> bool {
    if s.rows > 0 && s.nulls.is_some_and(|n| n >= s.rows) {
        return false; // only nulls, which never match a comparison
    }
    let Some(v) = bound(v) else { return true };
    let min = s.min.as_ref().filter(|m| comparable(m, &v));
    let max = s.max.as_ref().filter(|m| comparable(m, &v));
    match op {
        Cmp::Eq => min.is_none_or(|m| *m <= v) && max.is_none_or(|m| *m >= v),
        Cmp::Ne => !(min == Some(&v) && max == Some(&v)),
        Cmp::Lt => min.is_none_or(|m| *m < v),
        Cmp::Le => min.is_none_or(|m| *m <= v),
        Cmp::Ge => max.is_none_or(|m| *m >= v),
    }
}

/// Statistics of a column from a Delta `Add.stats` JSON document (`numRecords`, `minValues`,
/// `maxValues`, `nullCount`). Only integer bounds are taken: string bounds there may be
/// truncated in ways that are not bounds.
pub(crate) fn file_stats(doc: &serde_json::Value, column: &str) -> Option<ColStats> {
    let rows = doc.get("numRecords")?.as_u64()?;
    let int = |k: &str| {
        doc.get(k)
            .and_then(|m| m.get(column))
            .and_then(serde_json::Value::as_i64)
            .map(Bound::Int)
    };
    let nulls = doc
        .get("nullCount")
        .and_then(|m| m.get(column))
        .and_then(serde_json::Value::as_u64);
    let s = ColStats {
        min: int("minValues"),
        max: int("maxValues"),
        nulls,
        rows,
    };
    (s.min.is_some() || s.max.is_some() || s.nulls.is_some()).then_some(s)
}

/// Statistics of a top-level column of a Parquet row group.
pub(crate) fn row_group_stats(rg: &RowGroupMetaData, column: &str) -> Option<ColStats> {
    let c = rg
        .columns()
        .iter()
        .find(|c| c.column_path().parts() == [column])?;
    let st = c.statistics()?;
    let (min, max) = match st {
        Statistics::Int32(s) => (
            s.min_opt().map(|v| Bound::Int((*v).into())),
            s.max_opt().map(|v| Bound::Int((*v).into())),
        ),
        Statistics::Int64(s) => (
            s.min_opt().map(|v| Bound::Int(*v)),
            s.max_opt().map(|v| Bound::Int(*v)),
        ),
        // Parquet truncates long byte-array bounds so that they still bound the values.
        Statistics::ByteArray(s) => (
            s.min_opt().map(|v| Bound::Bytes(v.data().to_vec())),
            s.max_opt().map(|v| Bound::Bytes(v.data().to_vec())),
        ),
        _ => (None, None),
    };
    Some(ColStats {
        min,
        max,
        nulls: st.null_count_opt(),
        rows: rg.num_rows().max(0) as u64,
    })
}

/// The values of `pred` that a Bloom filter can rule out: `column = value` or `column IN
/// (values)` at the top level (or under `AND`). `None` when the predicate has no such part.
pub(crate) fn bloom_probe(pred: &Pred, column: &str) -> Option<Vec<String>> {
    match pred {
        Pred::Cmp(c, Cmp::Eq, Lit::Str(s)) if *c == column => Some(vec![s.clone()]),
        Pred::In(c, vs) if *c == column => vs
            .iter()
            .map(|v| match v {
                Lit::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        Pred::And(ps) => ps.iter().find_map(|p| bloom_probe(p, column)),
        _ => None,
    }
}

/// The columns `pred` reads.
pub(crate) fn columns(pred: &Pred, out: &mut Vec<String>) {
    let mut add = |c: &str| {
        if !out.iter().any(|o| o == c) {
            out.push(c.to_string());
        }
    };
    match pred {
        Pred::Cmp(c, _, _)
        | Pred::In(c, _)
        | Pred::IsNull(c)
        | Pred::NotNull(c)
        | Pred::StartsWith(c, _) => add(c),
        Pred::And(ps) | Pred::Or(ps) => {
            for p in ps {
                columns(p, out);
            }
        }
    }
}

/// Which rows of `batch` match `pred` (never null). A column the batch lacks is all null.
pub(crate) fn eval(pred: &Pred, batch: &RecordBatch) -> Result<BooleanArray> {
    let n = batch.num_rows();
    let all = |v: bool| BooleanArray::new(BooleanBuffer::collect_bool(n, |_| v), None);
    Ok(match pred {
        Pred::And(ps) => {
            let mut acc = all(true);
            for p in ps {
                acc = deltalake_core::arrow::compute::and(&acc, &eval(p, batch)?)?;
            }
            acc
        }
        Pred::Or(ps) => {
            let mut acc = all(false);
            for p in ps {
                acc = deltalake_core::arrow::compute::or(&acc, &eval(p, batch)?)?;
            }
            acc
        }
        Pred::IsNull(c) => match batch.column_by_name(c) {
            Some(a) => is_null(a)?,
            None => all(true),
        },
        Pred::NotNull(c) => match batch.column_by_name(c) {
            Some(a) => is_not_null(a)?,
            None => all(false),
        },
        Pred::Cmp(c, op, v) => match batch.column_by_name(c) {
            Some(a) => {
                let r = compare(a, *op, v)?;
                if r.null_count() > 0 {
                    prep_null_mask_filter(&r)
                } else {
                    r
                }
            }
            None => all(false),
        },
        Pred::In(c, vs) => match batch.column_by_name(c) {
            Some(a) => is_in(a, vs)?,
            None => all(false),
        },
        Pred::StartsWith(c, p) => match batch.column_by_name(c) {
            Some(a) => {
                let s = strings(a)?;
                BooleanArray::new(
                    BooleanBuffer::collect_bool(n, |i| {
                        s.is_valid(i) && s.value(i).starts_with(p.as_str())
                    }),
                    None,
                )
            }
            None => all(false),
        },
    })
}

fn unsigned() -> Error {
    Error::InvalidArgument("an unsigned literal reached a Delta table".into())
}

/// An integer or timestamp column as `Int64`.
fn ints(a: &ArrayRef) -> Result<Arc<Int64Array>> {
    if let Some(a) = a.as_primitive_opt::<Int64Type>() {
        return Ok(Arc::new(a.clone()));
    }
    let to = match a.data_type() {
        DataType::Timestamp(_, _) => {
            // The raw value (microseconds for kv0's columns), not a conversion.
            let c = cast(a, &DataType::Int64)?;
            return Ok(Arc::new(c.as_primitive::<Int64Type>().clone()));
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 => cast(a, &DataType::Int64)?,
        t => {
            return Err(Error::InvalidArgument(format!(
                "cannot compare a {t} column with an integer"
            )))
        }
    };
    Ok(Arc::new(to.as_primitive::<Int64Type>().clone()))
}

fn strings(a: &ArrayRef) -> Result<StringArray> {
    match a.data_type() {
        DataType::Utf8 => Ok(a.as_string::<i32>().clone()),
        DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(cast(a, &DataType::Utf8)?.as_string::<i32>().clone())
        }
        t => Err(Error::InvalidArgument(format!(
            "cannot compare a {t} column with a string"
        ))),
    }
}

fn compare(a: &ArrayRef, op: Cmp, v: &Lit) -> Result<BooleanArray> {
    let apply = |l: &dyn deltalake_core::arrow::array::Datum,
                 r: &dyn deltalake_core::arrow::array::Datum| match op {
        Cmp::Eq => cmp::eq(l, r),
        Cmp::Ne => cmp::neq(l, r),
        Cmp::Lt => cmp::lt(l, r),
        Cmp::Le => cmp::lt_eq(l, r),
        Cmp::Ge => cmp::gt_eq(l, r),
    };
    Ok(match v {
        Lit::Int(i) | Lit::TimestampMicros(i) => {
            let l = ints(a)?;
            apply(l.as_ref(), &Int64Array::new_scalar(*i))?
        }
        Lit::Str(s) => {
            let l = strings(a)?;
            apply(&l, &StringArray::new_scalar(s.as_str()))?
        }
        Lit::UInt(_) => return Err(unsigned()),
    })
}

fn is_in(a: &ArrayRef, vs: &[Lit]) -> Result<BooleanArray> {
    let n = a.len();
    if vs.is_empty() {
        return Ok(BooleanArray::new(BooleanBuffer::new_unset(n), None));
    }
    if matches!(vs[0], Lit::Str(_)) {
        let set: HashSet<&str> = vs
            .iter()
            .map(|v| match v {
                Lit::Str(s) => Ok(s.as_str()),
                _ => Err(Error::InvalidArgument("mixed IN literals".into())),
            })
            .collect::<Result<_>>()?;
        let s = strings(a)?;
        return Ok(BooleanArray::new(
            BooleanBuffer::collect_bool(n, |i| s.is_valid(i) && set.contains(s.value(i))),
            None,
        ));
    }
    let set: HashSet<i64> = vs
        .iter()
        .map(|v| match v {
            Lit::Int(i) | Lit::TimestampMicros(i) => Ok(*i),
            Lit::UInt(_) => Err(unsigned()),
            Lit::Str(_) => Err(Error::InvalidArgument("mixed IN literals".into())),
        })
        .collect::<Result<_>>()?;
    let l = ints(a)?;
    Ok(BooleanArray::new(
        BooleanBuffer::collect_bool(n, |i| l.is_valid(i) && set.contains(&l.value(i))),
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints_stats(min: i64, max: i64, nulls: u64, rows: u64) -> ColStats {
        ColStats {
            min: Some(Bound::Int(min)),
            max: Some(Bound::Int(max)),
            nulls: Some(nulls),
            rows,
        }
    }

    #[test]
    fn statistics_rule_out_only_what_cannot_match() {
        let s = |c: &str| (c == "x").then(|| ints_stats(10, 20, 0, 5));
        assert!(may_match(&Pred::eq("x", 10i64), &s));
        assert!(!may_match(&Pred::eq("x", 21i64), &s));
        assert!(!may_match(&Pred::lt("x", 10i64), &s));
        assert!(may_match(&Pred::le("x", 10i64), &s));
        assert!(!may_match(&Pred::ge("x", 21i64), &s));
        assert!(!may_match(&Pred::is_in("x", [1i64, 30]), &s));
        assert!(may_match(&Pred::is_in("x", [1i64, 15]), &s));
        assert!(!may_match(&Pred::IsNull("x"), &s));
        // Unknown columns may match anything.
        assert!(may_match(&Pred::eq("y", 1i64), &s));
        assert!(!may_match(
            &Pred::eq("x", 1i64).and(Pred::eq("y", 1i64)),
            &s
        ));
        assert!(may_match(&Pred::eq("x", 1i64).or(Pred::eq("y", 1i64)), &s));
        let one = |_: &str| Some(ints_stats(7, 7, 0, 3));
        assert!(!may_match(&Pred::ne("x", 7i64), &one));
        let strs = |_: &str| {
            Some(ColStats {
                min: Some(Bound::Bytes(b"abc".to_vec())),
                max: Some(Bound::Bytes(b"abz".to_vec())),
                nulls: Some(0),
                rows: 2,
            })
        };
        assert!(may_match(&Pred::StartsWith("s", "ab".into()), &strs));
        assert!(may_match(&Pred::StartsWith("s", "abe".into()), &strs));
        assert!(!may_match(&Pred::StartsWith("s", "ac".into()), &strs));
        assert!(!may_match(&Pred::StartsWith("s", "aa".into()), &strs));
        assert!(!may_match(&Pred::eq("s", "b"), &strs));
    }

    #[test]
    fn rows_follow_the_null_rule() {
        use deltalake_core::arrow::array::Int32Array;
        use deltalake_core::arrow::datatypes::{Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("ab"), Some("b"), None])),
            ],
        )
        .unwrap();
        let rows = |p: Pred| -> Vec<bool> { eval(&p, &b).unwrap().iter().flatten().collect() };
        assert_eq!(rows(Pred::ne("i", 1i32)), [false, false, true]);
        assert_eq!(
            rows(Pred::IsNull("i").or(Pred::ne("i", 1i32))),
            [false, true, true]
        );
        assert_eq!(rows(Pred::is_in("s", ["b", "x"])), [false, true, false]);
        assert_eq!(
            rows(Pred::StartsWith("s", "a".into())),
            [true, false, false]
        );
        assert_eq!(rows(Pred::eq("missing", 1i64)), [false, false, false]);
        assert_eq!(rows(Pred::IsNull("missing")), [true, true, true]);
        assert_eq!(rows(Pred::And(vec![])), [true, true, true]);
        assert_eq!(rows(Pred::Or(vec![])), [false, false, false]);
        assert!(eval(&Pred::Cmp("i", Cmp::Eq, Lit::UInt(1)), &b).is_err());
    }
}
