//! Typed row filters over named columns.
//!
//! Callers build a [`Pred`] instead of an SQL string; each storage engine turns it into what it
//! evaluates. Lance takes SQL ([`Pred::to_sql`]).

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
                let mut p = String::with_capacity(prefix.len() + 1);
                for ch in prefix.chars() {
                    if matches!(ch, '%' | '_' | '\\') {
                        p.push('\\');
                    }
                    p.push(ch);
                }
                p.push('%');
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
