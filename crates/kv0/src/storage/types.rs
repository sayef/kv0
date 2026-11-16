//! The logical column types kv0 reads and writes, and their storage at the edge of an engine.
//!
//! kv0's tables are described with signed integers only (`seq: Int64`, `op: Int8`,
//! `series_id: Int64`, `fields: Int32`), so that every engine can store them: Delta Lake has
//! no unsigned types. The Rust and Python APIs keep their unsigned values; a value crosses
//! with a bit-cast (`as`), so every `u64` round-trips, and ordering follows the signed value.
//!
//! Lance datasets written so far store these columns unsigned. The Lance engine reinterprets
//! them at its boundary ([`to_stored`], [`to_logical`], [`stored_pred`]); no data is
//! rewritten.

use std::sync::Arc;

use arrow_array::{make_array, Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::pred::{Cmp, Lit, Pred};
use crate::error::{Error, Result};

/// A sequence number as stored. Sequence numbers are hybrid-clock nanoseconds, below
/// `i64::MAX` until the year 2262.
pub(crate) fn seq_to_i64(seq: u64) -> i64 {
    debug_assert!(seq <= i64::MAX as u64, "sequence number {seq} out of range");
    seq as i64
}

pub(crate) fn seq_from_i64(seq: i64) -> u64 {
    seq as u64
}

/// A series id as stored: the xxh3 hash, bit-cast.
pub(crate) fn series_to_i64(id: u64) -> i64 {
    id as i64
}

pub(crate) fn series_from_i64(id: i64) -> u64 {
    id as u64
}

/// The signed type that stores `t`, or `None` when `t` is stored as it is.
fn signed(t: &DataType) -> Option<DataType> {
    Some(match t {
        DataType::UInt8 => DataType::Int8,
        DataType::UInt16 => DataType::Int16,
        DataType::UInt32 => DataType::Int32,
        DataType::UInt64 => DataType::Int64,
        _ => return None,
    })
}

/// `schema` with every unsigned integer column signed: kv0's logical schema of a table an
/// engine stores as `schema`.
pub(crate) fn logical_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| match signed(f.data_type()) {
                Some(t) => Arc::new(Field::new(f.name(), t, f.is_nullable())),
                None => f.clone(),
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

/// `a` reinterpreted as `to` (same width, so the buffers are shared, not copied).
fn reinterpret(a: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    if a.data_type() == to {
        return Ok(a.clone());
    }
    let data = a
        .to_data()
        .into_builder()
        .data_type(to.clone())
        .build()
        .map_err(|e| Error::Corruption(format!("cannot store {} as {to}: {e}", a.data_type())))?;
    Ok(make_array(data))
}

/// The columns of `batch` typed as `schema` types the column of the same name. Only integer
/// columns of the same width differing in sign change; others must match.
fn retype(
    batch: &RecordBatch,
    schema: &Schema,
    want: impl Fn(&DataType) -> DataType,
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut cols = Vec::with_capacity(batch.num_columns());
    for (f, c) in batch.schema().fields().iter().zip(batch.columns()) {
        let to = match schema.field_with_name(f.name()) {
            Ok(target) => want(target.data_type()),
            Err(_) => f.data_type().clone(),
        };
        if &to == f.data_type() {
            fields.push(f.clone());
            cols.push(c.clone());
        } else {
            let same_width = signed(&to).as_ref() == Some(f.data_type())
                || signed(f.data_type()).as_ref() == Some(&to);
            if !same_width {
                return Err(Error::Corruption(format!(
                    "column {} is {} where {to} is expected",
                    f.name(),
                    f.data_type()
                )));
            }
            fields.push(Arc::new(Field::new(f.name(), to.clone(), f.is_nullable())));
            cols.push(reinterpret(c, &to)?);
        }
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    Ok(RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

/// A logical batch as an engine stores it in a table of schema `stored`.
pub(crate) fn to_stored(batch: &RecordBatch, stored: &Schema) -> Result<RecordBatch> {
    retype(batch, stored, |t| t.clone())
}

/// A batch read from a table of schema `stored` with kv0's logical types.
pub(crate) fn to_logical(batch: &RecordBatch, stored: &Schema) -> Result<RecordBatch> {
    retype(batch, stored, |t| signed(t).unwrap_or_else(|| t.clone()))
}

/// `pred` over logical (signed) values, for a table whose schema `stored` keeps some of
/// those columns unsigned. Equality maps value to value; a range in signed order becomes
/// one or two ranges in unsigned order.
pub(crate) fn stored_pred(pred: &Pred, stored: &Schema) -> Pred {
    let unsigned = |c: &str| {
        stored
            .field_with_name(c)
            .is_ok_and(|f| signed(f.data_type()).is_some())
    };
    let lit = |c: &str, v: &Lit| match v {
        Lit::Int(i) if unsigned(c) => Lit::UInt(bits(c, *i, stored)),
        other => other.clone(),
    };
    match pred {
        Pred::Cmp(c, op, Lit::Int(v)) if unsigned(c) => signed_cmp(c, *op, *v, stored),
        Pred::Cmp(c, op, v) => Pred::Cmp(c, *op, lit(c, v)),
        Pred::In(c, vs) => Pred::In(c, vs.iter().map(|v| lit(c, v)).collect()),
        Pred::And(ps) => Pred::And(ps.iter().map(|p| stored_pred(p, stored)).collect()),
        Pred::Or(ps) => Pred::Or(ps.iter().map(|p| stored_pred(p, stored)).collect()),
        p => p.clone(),
    }
}

/// The unsigned bits of `v` in column `c`'s width.
fn bits(c: &str, v: i64, stored: &Schema) -> u64 {
    match stored.field_with_name(c).map(|f| f.data_type().clone()) {
        Ok(DataType::UInt8) => v as i8 as u8 as u64,
        Ok(DataType::UInt16) => v as i16 as u16 as u64,
        Ok(DataType::UInt32) => v as i32 as u32 as u64,
        _ => v as u64,
    }
}

/// `c <op> v` in signed order, for an unsigned column `c` holding the bits.
fn signed_cmp(c: &'static str, op: Cmp, v: i64, stored: &Schema) -> Pred {
    let u = bits(c, v, stored);
    // The smallest stored value of a negative number: the sign bit of the column's width.
    let neg = match stored.field_with_name(c).map(|f| f.data_type().clone()) {
        Ok(DataType::UInt8) => 1u64 << 7,
        Ok(DataType::UInt16) => 1u64 << 15,
        Ok(DataType::UInt32) => 1u64 << 31,
        _ => 1u64 << 63,
    };
    let ge = |x: u64| Pred::Cmp(c, Cmp::Ge, Lit::UInt(x));
    let lt = |x: u64| Pred::Cmp(c, Cmp::Lt, Lit::UInt(x));
    let le = |x: u64| Pred::Cmp(c, Cmp::Le, Lit::UInt(x));
    let eq_ne = |op| Pred::Cmp(c, op, Lit::UInt(u));
    // Non-negative values are `[0, neg)`, negative ones `[neg, max]`, each in order.
    match (op, v >= 0) {
        (Cmp::Eq, _) => eq_ne(Cmp::Eq),
        (Cmp::Ne, _) => eq_ne(Cmp::Ne),
        (Cmp::Ge, true) => ge(u).and(lt(neg)),
        (Cmp::Ge, false) => ge(u).or(lt(neg)),
        (Cmp::Lt, true) => lt(u).or(ge(neg)),
        (Cmp::Lt, false) => ge(neg).and(lt(u)),
        (Cmp::Le, true) => le(u).or(ge(neg)),
        (Cmp::Le, false) => ge(neg).and(le(u)),
    }
}
