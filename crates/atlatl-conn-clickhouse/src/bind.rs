//! `bind` lists (04 §12): the plan renders `{{ bind.<name> }}` as a marker; once the lists are
//! known, each marker becomes a comma-separated list of ClickHouse literals, and a batched list
//! fans the query out into one query per batch.

use std::num::NonZeroUsize;

use atlatl_core::AtlatlError;
use atlatl_core::error::Result;
use atlatl_core::template::bind_marker;
use serde_json::Value as J;

#[derive(Clone, Debug)]
pub struct Bound {
    pub name: String,
    pub items: Vec<J>,
}

/// A ClickHouse literal for a scalar.
pub fn literal(v: &J) -> Result<String> {
    Ok(match v {
        J::Null => "NULL".into(),
        J::Bool(b) => b.to_string(),
        J::Number(n) => n.to_string(),
        J::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")),
        J::Array(_) | J::Object(_) => {
            return Err(AtlatlError::config(format!(
                "a bind value must be a scalar, got {v}"
            )));
        }
    })
}

/// The queries `query` expands to: `whole` lists spliced into each, and one query per `n` items
/// of `batched`. An empty list renders as `NULL`, so the query still runs once and returns its
/// schema; `IN (NULL)` then matches nothing, but `NOT IN (NULL)` matches every row. Which lists
/// exist, are used and are batched is checked at plan time (04 §12).
pub fn render(
    query: &str,
    whole: &[Bound],
    batched: Option<(&Bound, NonZeroUsize)>,
) -> Result<Vec<String>> {
    let mut base = query.to_owned();
    for b in whole {
        let lits = literals(b)?;
        base = base.replace(&bind_marker(&b.name), &joined(&lits));
    }
    let Some((b, n)) = batched else {
        return Ok(vec![base]);
    };
    let marker = bind_marker(&b.name);
    let lits = literals(b)?;
    if lits.is_empty() {
        return Ok(vec![base.replace(&marker, "NULL")]);
    }
    Ok(lits
        .chunks(n.get())
        .map(|c| base.replace(&marker, &c.join(", ")))
        .collect())
}

fn literals(b: &Bound) -> Result<Vec<String>> {
    b.items.iter().map(literal).collect()
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
    use serde_json::json;

    use super::*;

    fn q(names: &[&str]) -> String {
        let m: Vec<String> = names.iter().map(|n| bind_marker(n)).collect();
        match m.as_slice() {
            [a] => format!("SELECT * FROM t WHERE id IN ({a})"),
            [a, b] => format!("SELECT * FROM t WHERE id IN ({a}) AND c IN ({b})"),
            _ => unreachable!(),
        }
    }

    fn bound(name: &str, items: Vec<J>) -> Bound {
        Bound {
            name: name.into(),
            items,
        }
    }

    fn n(v: usize) -> NonZeroUsize {
        NonZeroUsize::new(v).expect("non-zero")
    }

    #[test]
    fn literals() {
        assert_eq!(literal(&json!(42)).expect("int"), "42");
        assert_eq!(literal(&json!(-1.5)).expect("float"), "-1.5");
        assert_eq!(literal(&json!(null)).expect("null"), "NULL");
        assert_eq!(literal(&json!(true)).expect("bool"), "true");
        assert_eq!(literal(&json!("o'k\\")).expect("string"), r"'o\'k\\'");
        assert!(literal(&json!([1])).is_err());
    }

    #[test]
    fn splices_whole_lists() {
        let got = render(
            &q(&["ids", "cs"]),
            &[
                bound("ids", vec![json!(1), json!(2)]),
                bound("cs", vec![json!("USD")]),
            ],
            None,
        )
        .expect("render");
        assert_eq!(got, ["SELECT * FROM t WHERE id IN (1, 2) AND c IN ('USD')"]);
    }

    #[test]
    fn batches_one_list() {
        let ids = bound("ids", (1..=5).map(|i| json!(i)).collect());
        let got = render(
            &q(&["ids", "cs"]),
            &[bound("cs", vec![json!("X")])],
            Some((&ids, n(2))),
        )
        .expect("render");
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
            assert_eq!(got.expect("render"), ["SELECT * FROM t WHERE id IN (NULL)"]);
        }
    }

    #[test]
    fn rejects_non_scalars() {
        let e =
            render(&q(&["ids"]), &[bound("ids", vec![json!({"a": 1})])], None).expect_err("map");
        assert!(e.message.contains("scalar"), "{}", e.message);
    }
}
