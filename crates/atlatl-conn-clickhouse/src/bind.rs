//! `bind` lists (04 §12): the plan renders `{{ bind.<name> }}` as a marker; once the lists are
//! known, each marker becomes a comma-separated list of ClickHouse literals (a tuple per row when
//! the list has several columns), and a batched list fans the query out into one query per batch.

use std::num::NonZeroUsize;

use atlatl_core::template::bind_marker;

#[derive(Clone, Debug)]
pub struct Bound {
    pub name: String,
    /// The bind query's columns; numbers in a column missing here are written as `Num::Other`.
    pub cols: Vec<Col>,
    /// Each row's column values, in column order.
    pub rows: Vec<Vec<Lit>>,
}

/// A bind query's column: its name, its type there, and how its numbers are written.
#[derive(Clone, Debug, PartialEq)]
pub struct Col {
    pub name: String,
    pub ty: String,
    pub num: Num,
}

/// How a bind column's numbers are written, from its type in the bind query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Num {
    /// `DECIMAL(p,s)`: a typed literal at that precision and scale. ClickHouse compares it with a
    /// narrower `Decimal` exactly (`1.239` matches no `Decimal(5,2)`) and refuses it against a
    /// float or integer column, where a quoted value would be truncated to the column's type.
    Decimal(u32, u32),
    /// `DOUBLE`/`FLOAT`: quoted, so it's parsed as the compared column's type (`0.1` matches a
    /// `Float32` 0.1, which no `Float64` literal does). Against a `Decimal` it's rounded to the
    /// column's scale: float precision loss, accepted as elsewhere.
    Float,
    /// Anything else. Integers stay exact (`number`); the caller refuses fractions.
    Other,
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
    typed(v, Num::Other)
}

/// A ClickHouse literal for a value of a column whose numbers are `num`.
pub fn typed(v: &Lit, num: Num) -> String {
    match (v, num) {
        (Lit::Null, _) => "NULL".into(),
        (Lit::Bool(b), _) => b.to_string(),
        (Lit::Number(n), Num::Decimal(p, s)) => format!("CAST({} AS Decimal({p}, {s}))", quoted(n)),
        (Lit::Number(n), Num::Float) => quoted(n),
        (Lit::Number(n), Num::Other) => number(n),
        (Lit::String(s), _) => quoted(s),
    }
}

/// ClickHouse reads an integer literal past 64 bits, and any fractional one, as `Float64`, so wider
/// integers go in as a typed cast and fractions quoted (callers type fractions by `Num` instead).
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
pub fn row(vals: &[Lit], cols: &[Col]) -> String {
    let lit = |(i, v): (usize, &Lit)| typed(v, cols.get(i).map_or(Num::Other, |c| c.num));
    match vals {
        [v] => lit((0, v)),
        vs => format!(
            "({})",
            vs.iter()
                .enumerate()
                .map(lit)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Refuses `b` where it's spliced into `values('<structure>', …)` and a decimal or float column
/// would be converted to the structure's type silently: ClickHouse rounds a `Decimal` to the
/// structure's scale whatever the literal's form, and no setting makes that an error. The marker
/// must be a whole argument, so each row lines up with the structure's columns.
pub fn check_values(query: &str, b: &Bound) -> std::result::Result<(), String> {
    if !b.cols.iter().any(|c| c.num != Num::Other) {
        return Ok(());
    }
    let marker = bind_marker(&b.name);
    let calls = values_calls(query);
    for (at, _) in query.match_indices(&marker) {
        let Some(&(open, close)) = calls
            .iter()
            .filter(|(o, c)| *o < at && at < *c)
            .max_by_key(|(o, _)| *o)
        else {
            continue;
        };
        let refuse = |why: String| {
            format!(
                "`bind.{}` is spliced into `values()` {why}; widen the structure's scale, or round in the bind query (`CAST(round(x, 2) AS DECIMAL(18, 2))`)",
                b.name
            )
        };
        let call = format!("v{}", &query[open..=close]);
        let (_, args) = crate::types::split(&call);
        let structure = args.first().and_then(|a| unquote(a));
        let (Some(structure), true) = (structure, args.iter().skip(1).any(|a| *a == marker)) else {
            return Err(refuse(
                "without a structure atlatl can read (`values('name Type, …', <bind>)`, the bind a whole argument), so its decimal and float columns can't be checked against it".into(),
            ));
        };
        let wrapped = format!("s({structure})");
        let (_, fields) = crate::types::split(&wrapped);
        if fields.len() != b.cols.len() {
            return Err(refuse(format!(
                "with {} columns, but the bind query returns {}",
                fields.len(),
                b.cols.len()
            )));
        }
        for (c, f) in b.cols.iter().zip(&fields) {
            let ty = crate::types::element_type(f);
            let scale = decimal_scale(ty);
            let lost = match (c.num, scale) {
                (Num::Decimal(_, s), Some(t)) if s > t => {
                    Some(format!("would round it to {t} digits"))
                }
                (Num::Decimal(..), None) => Some("isn't a Decimal".into()),
                (Num::Float, Some(_)) => Some("would round it to the structure's scale".into()),
                _ => None,
            };
            if let Some(lost) = lost {
                return Err(refuse(format!(
                    "where its column `{}` ({}) meets structure column `{}`, which {lost}",
                    c.name,
                    c.ty,
                    f.trim()
                )));
            }
        }
    }
    Ok(())
}

/// A `Decimal` type's scale, through `Nullable` and `LowCardinality`.
fn decimal_scale(ty: &str) -> Option<u32> {
    let (name, args) = crate::types::split(ty.trim());
    let arg = |i: usize| args.get(i).and_then(|a| a.trim().parse().ok());
    match name {
        "Nullable" | "LowCardinality" => args.first().and_then(|a| decimal_scale(a)),
        "Decimal" => Some(arg(1).unwrap_or(0)),
        "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => arg(0),
        _ => None,
    }
}

/// A single-quoted ClickHouse string's text.
fn unquote(lit: &str) -> Option<String> {
    let body = lit.trim().strip_prefix('\'')?.strip_suffix('\'')?;
    let mut out = String::new();
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.push(chars.next()?),
            '\'' => {
                if chars.next()? != '\'' {
                    return None;
                }
                out.push('\'');
            }
            c => out.push(c),
        }
    }
    Some(out)
}

/// Each `values(…)` call's parentheses (byte offsets), quotes skipped over.
fn values_calls(q: &str) -> Vec<(usize, usize)> {
    let mut open: Vec<(usize, bool)> = Vec::new();
    let mut calls = Vec::new();
    let mut quote = None::<char>;
    let mut chars = q.char_indices();
    while let Some((i, c)) = chars.next() {
        match (quote, c) {
            (Some(_), '\\') => {
                chars.next();
            }
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '`' | '"') => quote = Some(c),
            (None, '(') => {
                let head = q[..i].trim_end();
                let word = head
                    .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .map_or(head, |j| &head[j + 1..]);
                open.push((i, word.eq_ignore_ascii_case("values")));
            }
            (None, ')') => {
                if let Some((o, true)) = open.pop() {
                    calls.push((o, i));
                }
            }
            _ => {}
        }
    }
    calls
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
    b.rows.iter().map(|r| row(r, &b.cols)).collect()
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
            cols: Vec::new(),
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
            cols: Vec::new(),
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
    fn col(name: &str, ty: &str, num: Num) -> Col {
        Col {
            name: name.into(),
            ty: ty.into(),
            num,
        }
    }

    fn typed_bound(cols: Vec<Col>, rows: Vec<Vec<Lit>>) -> Bound {
        Bound {
            name: "w".into(),
            cols,
            rows,
        }
    }

    #[test]
    fn numbers_follow_their_source_type() {
        assert_eq!(
            typed(&num("1.239"), Num::Decimal(4, 3)),
            "CAST('1.239' AS Decimal(4, 3))"
        );
        assert_eq!(typed(&num("0.1"), Num::Float), "'0.1'");
        assert_eq!(typed(&Lit::Null, Num::Decimal(4, 3)), "NULL");
        let b = typed_bound(
            vec![
                col("id", "BIGINT", Num::Other),
                col("d", "DECIMAL(5,2)", Num::Decimal(5, 2)),
            ],
            vec![vec![num(1), num("1.23")]],
        );
        let got = render(&q(&["w"]), &[b], None);
        assert_eq!(
            got,
            ["SELECT * FROM t WHERE id IN ((1, CAST('1.23' AS Decimal(5, 2))))"]
        );
    }

    #[test]
    fn values_structures_must_hold_decimals_and_floats() {
        let m = bind_marker("w");
        let dec = |p, s| col("d", "DECIMAL", Num::Decimal(p, s));
        let check = |structure: &str, cols: Vec<Col>| {
            check_values(
                &format!("SELECT * FROM t JOIN values({structure}, {m}) USING id"),
                &typed_bound(cols, vec![]),
            )
        };
        let id = || col("id", "BIGINT", Num::Other);
        assert_eq!(
            check("'id Int64, d Decimal(5, 2)'", vec![id(), dec(5, 2)]),
            Ok(())
        );
        assert_eq!(
            check(
                "'id Int64, d Nullable(Decimal64(3))'",
                vec![id(), dec(4, 3)]
            ),
            Ok(())
        );
        assert_eq!(
            check("'d Float32'", vec![col("d", "DOUBLE", Num::Float)]),
            Ok(())
        );
        // Strings and integers alone are spliced as before, structure or not.
        assert_eq!(check("'id Int64, x String'", vec![id()]), Ok(()));
        assert_eq!(check("'x' || 'y'", vec![id()]), Ok(()));
        for (structure, cols, want) in [
            (
                "'id Int64, d Decimal(5,2)'",
                vec![id(), dec(4, 3)],
                "would round it to 2 digits",
            ),
            (
                "'d Decimal32(2)'",
                vec![dec(4, 3)],
                "would round it to 2 digits",
            ),
            (
                "'d Decimal(5,2)'",
                vec![col("d", "DOUBLE", Num::Float)],
                "would round it to the structure's scale",
            ),
            ("'d String'", vec![dec(4, 3)], "isn't a Decimal"),
            (
                "'d Decimal(5,3)'",
                vec![id(), dec(4, 3)],
                "with 1 columns, but the bind query returns 2",
            ),
            (
                "concat('d ', 'Decimal(5,3)')",
                vec![dec(4, 3)],
                "without a structure atlatl can read",
            ),
        ] {
            let e = check(structure, cols).expect_err(structure);
            assert!(
                e.starts_with("`bind.w` is spliced into `values()`") && e.contains(want),
                "{structure}: {e}"
            );
            assert!(
                e.contains("widen the structure's scale, or round in the bind query"),
                "{e}"
            );
        }
        // The bind must be a whole row to line up with the structure.
        let e = check_values(
            &format!("SELECT * FROM values('a UInt8, d Decimal(5,3)', (1, {m}))"),
            &typed_bound(vec![dec(4, 3)], vec![]),
        )
        .expect_err("nested");
        assert!(e.contains("without a structure atlatl can read"), "{e}");
        // Outside `values()`, nothing to check.
        assert_eq!(
            check_values(
                &format!("SELECT * FROM t WHERE d IN ({m})"),
                &typed_bound(vec![dec(4, 3)], vec![])
            ),
            Ok(())
        );
        assert_eq!(unquote(r"'it\'s ''x'''"), Some("it's 'x'".into()));
    }
}
