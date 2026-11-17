//! Typed row filters over named columns.
//!
//! Callers build a [`Pred`] instead of an SQL string; each storage engine turns it into what it
//! evaluates. Lance takes SQL ([`Pred::to_sql`]); moraine a DataFusion expression ([`to_expr`],
//! with the `moraine` feature).

use std::fmt::Write as _;

/// A literal compared with a column.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Lit {
    Str(String),
    Int(i64),
    /// An unsigned value, for engines that store unsigned columns (see `storage::types`).
    UInt(u64),
    /// Microseconds since the Unix epoch (UTC), compared with a timestamp column.
    TimestampMicros(i64),
}

impl From<&str> for Lit {
    fn from(s: &str) -> Self {
        Lit::Str(s.to_string())
    }
}

impl From<String> for Lit {
    fn from(s: String) -> Self {
        Lit::Str(s)
    }
}

impl From<&String> for Lit {
    fn from(s: &String) -> Self {
        Lit::Str(s.clone())
    }
}

impl From<i64> for Lit {
    fn from(v: i64) -> Self {
        Lit::Int(v)
    }
}

impl From<i32> for Lit {
    fn from(v: i32) -> Self {
        Lit::Int(v.into())
    }
}

/// How a column compares with a literal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Ge,
}

impl Cmp {
    fn sql(self) -> &'static str {
        match self {
            Cmp::Eq => "=",
            Cmp::Ne => "<>",
            Cmp::Lt => "<",
            Cmp::Le => "<=",
            Cmp::Ge => ">=",
        }
    }
}

/// A filter on the rows of a table.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Pred {
    /// `column <cmp> literal`; a null column never matches.
    Cmp(&'static str, Cmp, Lit),
    /// The column equals one of the literals; an empty list matches nothing.
    In(&'static str, Vec<Lit>),
    IsNull(&'static str),
    NotNull(&'static str),
    /// A string column starts with the prefix.
    StartsWith(&'static str, String),
    /// Every one matches; an empty list matches everything.
    And(Vec<Pred>),
    /// Any one matches; an empty list matches nothing.
    Or(Vec<Pred>),
}

impl Pred {
    pub fn eq(col: &'static str, v: impl Into<Lit>) -> Pred {
        Pred::Cmp(col, Cmp::Eq, v.into())
    }

    pub fn ne(col: &'static str, v: impl Into<Lit>) -> Pred {
        Pred::Cmp(col, Cmp::Ne, v.into())
    }

    pub fn lt(col: &'static str, v: impl Into<Lit>) -> Pred {
        Pred::Cmp(col, Cmp::Lt, v.into())
    }

    pub fn le(col: &'static str, v: impl Into<Lit>) -> Pred {
        Pred::Cmp(col, Cmp::Le, v.into())
    }

    pub fn ge(col: &'static str, v: impl Into<Lit>) -> Pred {
        Pred::Cmp(col, Cmp::Ge, v.into())
    }

    pub fn is_in<L: Into<Lit>>(col: &'static str, vs: impl IntoIterator<Item = L>) -> Pred {
        Pred::In(col, vs.into_iter().map(Into::into).collect())
    }

    pub fn and(self, other: Pred) -> Pred {
        match self {
            Pred::And(mut v) => {
                v.push(other);
                Pred::And(v)
            }
            p => Pred::And(vec![p, other]),
        }
    }

    pub fn or(self, other: Pred) -> Pred {
        match self {
            Pred::Or(mut v) => {
                v.push(other);
                Pred::Or(v)
            }
            p => Pred::Or(vec![p, other]),
        }
    }

    /// The filter as an SQL expression (DataFusion's dialect, which Lance evaluates).
    pub fn to_sql(&self) -> String {
        let mut s = String::new();
        self.write_sql(&mut s);
        s
    }

    fn write_sql(&self, out: &mut String) {
        match self {
            Pred::Cmp(c, op, v) => {
                let _ = write!(out, "{c} {} ", op.sql());
                lit_sql(v, out);
            }
            Pred::In(_, vs) if vs.is_empty() => out.push_str("false"),
            Pred::In(c, vs) => {
                let _ = write!(out, "{c} IN (");
                for (i, v) in vs.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    lit_sql(v, out);
                }
                out.push(')');
            }
            Pred::IsNull(c) => {
                let _ = write!(out, "{c} IS NULL");
            }
            Pred::NotNull(c) => {
                let _ = write!(out, "{c} IS NOT NULL");
            }
            Pred::StartsWith(c, prefix) => {
                // LIKE with `\` escaping `%`, `_` and itself, so the prefix matches literally.
                let p = like_prefix(prefix);
                let _ = write!(out, "{c} LIKE {}", quote(&p));
            }
            Pred::And(ps) if ps.is_empty() => out.push_str("true"),
            Pred::Or(ps) if ps.is_empty() => out.push_str("false"),
            Pred::And(ps) | Pred::Or(ps) => {
                let sep = if matches!(self, Pred::And(_)) {
                    " AND "
                } else {
                    " OR "
                };
                for (i, p) in ps.iter().enumerate() {
                    if i > 0 {
                        out.push_str(sep);
                    }
                    out.push('(');
                    p.write_sql(out);
                    out.push(')');
                }
            }
        }
    }
}

/// An SQL string literal.
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn lit_sql(v: &Lit, out: &mut String) {
    match v {
        Lit::Str(s) => out.push_str(&quote(s)),
        Lit::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Lit::UInt(u) => {
            let _ = write!(out, "{u}");
        }
        Lit::TimestampMicros(t) => {
            let _ = write!(out, "to_timestamp_micros({t})");
        }
    }
}

/// `pred` as a moraine (DataFusion) expression over a table of `schema`. Integer and timestamp
/// literals take the type of the column they are compared with, so neither the filter nor file
/// pruning depends on type coercion. A prefix match is `LIKE` with `\\` escaping `%`, `_` and
/// itself.
#[cfg(feature = "moraine")]
pub(crate) fn to_expr(pred: &Pred, schema: &arrow_schema_59::Schema) -> moraine::expr::Expr {
    use moraine::expr::{col, lit, Expr};
    match pred {
        Pred::Cmp(c, op, v) => {
            let (l, r) = (col(*c), lit(scalar(c, v, schema)));
            match op {
                Cmp::Eq => l.eq(r),
                Cmp::Ne => l.not_eq(r),
                Cmp::Lt => l.lt(r),
                Cmp::Le => l.lt_eq(r),
                Cmp::Ge => l.gt_eq(r),
            }
        }
        Pred::In(_, vs) if vs.is_empty() => lit(false),
        Pred::In(c, vs) => col(*c).in_list(
            vs.iter().map(|v| lit(scalar(c, v, schema))).collect(),
            false,
        ),
        Pred::IsNull(c) => col(*c).is_null(),
        Pred::NotNull(c) => col(*c).is_not_null(),
        Pred::StartsWith(c, prefix) => {
            let mut e = col(*c).like(lit(like_prefix(prefix)));
            if let Expr::Like(l) = &mut e {
                l.escape_char = Some('\\');
            }
            e
        }
        Pred::And(ps) => ps
            .iter()
            .map(|p| to_expr(p, schema))
            .reduce(Expr::and)
            .unwrap_or_else(|| lit(true)),
        Pred::Or(ps) => ps
            .iter()
            .map(|p| to_expr(p, schema))
            .reduce(Expr::or)
            .unwrap_or_else(|| lit(false)),
    }
}

/// The `LIKE` pattern matching strings that start with `prefix`.
fn like_prefix(prefix: &str) -> String {
    let mut p = String::with_capacity(prefix.len() + 1);
    for ch in prefix.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            p.push('\\');
        }
        p.push(ch);
    }
    p.push('%');
    p
}

/// `v` as a value of column `c`'s type.
#[cfg(feature = "moraine")]
fn scalar(c: &str, v: &Lit, schema: &arrow_schema_59::Schema) -> moraine::expr::ScalarValue {
    use arrow_schema_59::{DataType, TimeUnit};
    use moraine::expr::ScalarValue as S;
    let t = schema
        .field_with_name(c)
        .map(|f| f.data_type().clone())
        .ok();
    match (v, t) {
        (Lit::Str(s), _) => S::Utf8(Some(s.clone())),
        (Lit::Int(i), Some(DataType::Int8)) => S::Int8(Some(*i as i8)),
        (Lit::Int(i), Some(DataType::Int16)) => S::Int16(Some(*i as i16)),
        (Lit::Int(i), Some(DataType::Int32)) => S::Int32(Some(*i as i32)),
        (Lit::Int(i), _) => S::Int64(Some(*i)),
        (Lit::UInt(u), _) => S::Int64(Some(*u as i64)),
        (Lit::TimestampMicros(t), Some(DataType::Timestamp(TimeUnit::Microsecond, tz))) => {
            S::TimestampMicrosecond(Some(*t), tz)
        }
        (Lit::TimestampMicros(t), _) => S::TimestampMicrosecond(Some(*t), Some("UTC".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_quotes_strings_and_groups_clauses() {
        let p = Pred::eq("run_id", "it's")
            .and(Pred::is_in("step", [1i64, -2]))
            .and(Pred::IsNull("fields").or(Pred::ne("fields", 2i32)));
        assert_eq!(
            p.to_sql(),
            "(run_id = 'it''s') AND (step IN (1, -2)) AND ((fields IS NULL) OR (fields <> 2))"
        );
        assert_eq!(
            Pred::Cmp("timestamp", Cmp::Ge, Lit::TimestampMicros(5)).to_sql(),
            "timestamp >= to_timestamp_micros(5)"
        );
        assert_eq!(
            Pred::StartsWith("run_id", "a_b%".into()).to_sql(),
            r"run_id LIKE 'a\_b\%%'"
        );
        assert_eq!(Pred::is_in("x", Vec::<i64>::new()).to_sql(), "false");
    }
}

#[cfg(all(test, feature = "moraine"))]
mod moraine_tests {
    use arrow_schema_59::{DataType, Field, Schema, TimeUnit};
    use moraine::expr::{col, lit, ScalarValue as S};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("run_id", DataType::Utf8, false),
            Field::new("fields", DataType::Int32, true),
            Field::new("op", DataType::Int8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ])
    }

    #[test]
    fn literals_take_their_column_type() {
        let s = schema();
        assert_eq!(
            to_expr(&Pred::eq("fields", 2i32), &s),
            col("fields").eq(lit(S::Int32(Some(2))))
        );
        // An `Int` literal compared with a narrower column is narrowed.
        assert_eq!(
            to_expr(&Pred::Cmp("op", Cmp::Ge, Lit::Int(3)), &s),
            col("op").gt_eq(lit(S::Int8(Some(3))))
        );
        assert_eq!(
            to_expr(&Pred::lt("seq", -1i64), &s),
            col("seq").lt(lit(S::Int64(Some(-1))))
        );
        assert_eq!(
            to_expr(
                &Pred::Cmp("timestamp", Cmp::Le, Lit::TimestampMicros(7)),
                &s
            ),
            col("timestamp").lt_eq(lit(S::TimestampMicrosecond(Some(7), Some("UTC".into()))))
        );
        assert_eq!(
            to_expr(&Pred::ne("run_id", "it's"), &s),
            col("run_id").not_eq(lit(S::Utf8(Some("it's".into()))))
        );
    }

    #[test]
    fn lists_and_groups_map_with_their_empty_cases() {
        let s = schema();
        assert_eq!(
            to_expr(&Pred::is_in("seq", Vec::<i64>::new()), &s),
            lit(false)
        );
        assert_eq!(to_expr(&Pred::And(vec![]), &s), lit(true));
        assert_eq!(to_expr(&Pred::Or(vec![]), &s), lit(false));
        assert_eq!(
            to_expr(&Pred::is_in("fields", [1i32, 2]), &s),
            col("fields").in_list(vec![lit(S::Int32(Some(1))), lit(S::Int32(Some(2)))], false)
        );
        let p = Pred::eq("run_id", "a").and(Pred::IsNull("fields").or(Pred::ne("fields", 2i32)));
        assert_eq!(
            to_expr(&p, &s),
            col("run_id").eq(lit(S::Utf8(Some("a".into())))).and(
                col("fields")
                    .is_null()
                    .or(col("fields").not_eq(lit(S::Int32(Some(2)))))
            )
        );
    }

    #[test]
    fn prefixes_escape_like_wildcards() {
        assert_eq!(like_prefix("a_b%c"), r"a\_b\%c%");
        assert_eq!(like_prefix(r"x\y"), r"x\\y%");
        assert_eq!(like_prefix(""), "%");
        let e = to_expr(
            &Pred::StartsWith("run_id", "__kv0_wal__/".into()),
            &schema(),
        );
        match e {
            moraine::expr::Expr::Like(l) => {
                assert_eq!(l.escape_char, Some('\\'));
                assert!(!l.negated && !l.case_insensitive);
                assert_eq!(*l.pattern, lit(S::Utf8(Some(r"\_\_kv0\_wal\_\_/%".into()))));
            }
            other => panic!("not a LIKE: {other}"),
        }
    }
}
