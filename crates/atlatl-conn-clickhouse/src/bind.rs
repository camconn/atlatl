//! `bind` lists (04 §12): the plan renders `{{ bind.<name> }}` as a marker; once the lists are
//! known, each marker becomes a comma-separated list of ClickHouse literals (a tuple per row when
//! the list has several columns), and a batched list fans the query out into one query per batch.

use std::num::NonZeroUsize;

use atlatl_core::template::bind_marker;

#[derive(Clone, Debug)]
pub struct Bound {
    pub name: String,
    /// Each row's column values, in column order.
    pub rows: Vec<Vec<Lit>>,
}

/// A bind value. Numbers keep their text, so wide integers and long decimals stay exact.
#[derive(Clone, Debug, PartialEq)]
pub enum Lit {
    Null,
    Bool(bool),
    /// A JSON number's text.
    Number(String),
    String(String),
}

/// A ClickHouse literal for a value.
pub fn literal(v: &Lit) -> String {
    match v {
        Lit::Null => "NULL".into(),
        Lit::Bool(b) => b.to_string(),
        Lit::Number(n) => number(n),
        Lit::String(s) => quoted(s),
    }
}

/// ClickHouse reads an integer literal past 64 bits, and any fractional one, as `Float64`: `1.1`
/// then misses a `Decimal(38,20)` 1.1 and a `Float32` 1.1. Wider integers go in as a typed cast,
/// and fractions quoted, which ClickHouse converts exactly to the type they're compared with or
/// inserted into (`IN`, `values()`, comparisons).
fn number(n: &str) -> String {
    if n.contains(['.', 'e', 'E']) {
        return quoted(n);
    }
    if n.parse::<i64>().is_ok() || n.parse::<u64>().is_ok() {
        return n.to_owned();
    }
    let ty = if n.parse::<i128>().is_ok() {
        "Int128"
    } else if n.parse::<u128>().is_ok() {
        "UInt128"
    } else if n.starts_with('-') {
        "Int256"
    } else {
        "UInt256"
    };
    format!("to{ty}({})", quoted(n))
}

/// A single-quoted ClickHouse string.
pub fn quoted(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// A row's literal: its one value, or a tuple of several.
pub fn row(vals: &[Lit]) -> String {
    match vals {
        [v] => literal(v),
        vs => format!(
            "({})",
            vs.iter().map(literal).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// The queries `query` expands to: `whole` lists spliced into each, and one query per `n` items
/// of `batched`. An empty list renders as `NULL`, so the query still runs once and returns its
/// schema; `IN (NULL)` then matches nothing, but `NOT IN (NULL)` matches every row. Which lists
/// exist, are used and are batched is checked at plan time (04 §12).
pub fn render(
    query: &str,
    whole: &[Bound],
    batched: Option<(&Bound, NonZeroUsize)>,
) -> Vec<String> {
    let mut base = query.to_owned();
    for b in whole {
        base = base.replace(&bind_marker(&b.name), &joined(&literals(b)));
    }
    let Some((b, n)) = batched else {
        return vec![base];
    };
    let marker = bind_marker(&b.name);
    let lits = literals(b);
    if lits.is_empty() {
        return vec![base.replace(&marker, "NULL")];
    }
    lits.chunks(n.get())
        .map(|c| base.replace(&marker, &c.join(", ")))
        .collect()
}

fn literals(b: &Bound) -> Vec<String> {
    b.rows.iter().map(|r| row(r)).collect()
}

fn joined(lits: &[String]) -> String {
    if lits.is_empty() {
        "NULL".into()
    } else {
        lits.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(names: &[&str]) -> String {
        let m: Vec<String> = names.iter().map(|n| bind_marker(n)).collect();
        match m.as_slice() {
            [a] => format!("SELECT * FROM t WHERE id IN ({a})"),
            [a, b] => format!("SELECT * FROM t WHERE id IN ({a}) AND c IN ({b})"),
            _ => unreachable!(),
        }
    }

    fn num(n: impl ToString) -> Lit {
        Lit::Number(n.to_string())
    }

    fn s(v: &str) -> Lit {
        Lit::String(v.into())
    }

    fn bound(name: &str, items: Vec<Lit>) -> Bound {
        Bound {
            name: name.into(),
            rows: items.into_iter().map(|v| vec![v]).collect(),
        }
    }

    fn n(v: usize) -> NonZeroUsize {
        NonZeroUsize::new(v).expect("non-zero")
    }

    #[test]
    fn literals() {
        assert_eq!(literal(&num(42)), "42");
        assert_eq!(literal(&num("-1.5")), "'-1.5'");
        assert_eq!(
            literal(&num("18446744073709551615")),
            "18446744073709551615"
        );
        assert_eq!(
            literal(&num("-9223372036854775808")),
            "-9223372036854775808"
        );
        assert_eq!(
            literal(&num("-170141183460469231731687303715884105728")),
            "toInt128('-170141183460469231731687303715884105728')"
        );
        assert_eq!(
            literal(&num("340282366920938463463374607431768211455")),
            "toUInt128('340282366920938463463374607431768211455')"
        );
        assert_eq!(
            literal(&num("-170141183460469231731687303715884105729")),
            "toInt256('-170141183460469231731687303715884105729')"
        );
        assert_eq!(
            literal(&num("340282366920938463463374607431768211456")),
            "toUInt256('340282366920938463463374607431768211456')"
        );
        assert_eq!(literal(&num("1.1")), "'1.1'");
        assert_eq!(literal(&num("-0.5")), "'-0.5'");
        assert_eq!(literal(&num("1.5e-7")), "'1.5e-7'");
        assert_eq!(
            literal(&num("1.12345678901234567890")),
            "'1.12345678901234567890'"
        );
        assert_eq!(literal(&Lit::Null), "NULL");
        assert_eq!(literal(&Lit::Bool(true)), "true");
        assert_eq!(literal(&s("o'k\\")), r"'o\'k\\'");
    }

    #[test]
    fn splices_whole_lists() {
        let got = render(
            &q(&["ids", "cs"]),
            &[
                bound("ids", vec![num(1), num(2)]),
                bound("cs", vec![s("USD")]),
            ],
            None,
        );
        assert_eq!(got, ["SELECT * FROM t WHERE id IN (1, 2) AND c IN ('USD')"]);
    }

    #[test]
    fn batches_one_list() {
        let ids = bound("ids", (1..=5).map(num).collect());
        let got = render(
            &q(&["ids", "cs"]),
            &[bound("cs", vec![s("X")])],
            Some((&ids, n(2))),
        );
        assert_eq!(
            got,
            [
                "SELECT * FROM t WHERE id IN (1, 2) AND c IN ('X')",
                "SELECT * FROM t WHERE id IN (3, 4) AND c IN ('X')",
                "SELECT * FROM t WHERE id IN (5) AND c IN ('X')",
            ]
        );
    }

    #[test]
    fn empty_lists_still_run_once() {
        let empty = bound("ids", vec![]);
        for got in [
            render(&q(&["ids"]), std::slice::from_ref(&empty), None),
            render(&q(&["ids"]), &[], Some((&empty, n(10)))),
        ] {
            assert_eq!(got, ["SELECT * FROM t WHERE id IN (NULL)"]);
        }
    }

    #[test]
    fn multi_column_rows_render_as_tuples() {
        let windows = Bound {
            name: "w".into(),
            rows: vec![
                vec![num(1), s("2024-01-01"), Lit::Null],
                vec![num(2), s("2024-02-01"), s("2024-03-01")],
                vec![num(3), s("2024-03-01"), Lit::Null],
            ],
        };
        let got = render(
            &format!(
                "SELECT * FROM t JOIN values('id Int64, f Date, u Nullable(Date)', {}) USING id",
                bind_marker("w")
            ),
            &[],
            Some((&windows, n(2))),
        );
        assert_eq!(
            got,
            [
                "SELECT * FROM t JOIN values('id Int64, f Date, u Nullable(Date)', (1, '2024-01-01', NULL), (2, '2024-02-01', '2024-03-01')) USING id",
                "SELECT * FROM t JOIN values('id Int64, f Date, u Nullable(Date)', (3, '2024-03-01', NULL)) USING id",
            ]
        );
    }
}
