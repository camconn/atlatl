//! Invariants checked on staging before anything touches the target (04 §9).

use atlatl_core::config::model::{Duplicates, Guard, NullKeys, OnEmpty};
use atlatl_core::error::{AtlatlError, Result};
use duckdb::Connection;

use crate::sql::{db, ident};
use crate::staging::{Column, quoted_relation};

pub const MAX_SAMPLES: usize = 20;

fn samples(c: &Connection, sql: &str) -> Result<Vec<serde_json::Value>> {
    let mut s = c.prepare(sql).map_err(db)?;
    let rows = s.query_map([], |r| r.get::<_, String>(0)).map_err(db)?;
    rows.map(|r| {
        let t = r.map_err(db)?;
        Ok(serde_json::from_str(&t).unwrap_or(serde_json::Value::String(t)))
    })
    .collect()
}

/// Every named column must be present in the source; `role` names the setting (04 §9.4).
pub fn require_columns(staged: &[Column], required: &[(&str, &str)]) -> Result<()> {
    let names: Vec<&str> = staged.iter().map(|c| c.name.as_str()).collect();
    for (role, col) in required {
        if !names
            .iter()
            .any(|n| n == col || n.eq_ignore_ascii_case(col))
        {
            return Err(AtlatlError::config(format!(
                "{role} column `{col}` is not in the source (columns: {})",
                names.join(", ")
            ))
            .code("missing_column"));
        }
    }
    Ok(())
}

/// Staged rows must equal extracted rows; rejects are counted separately (04 §9.1).
pub fn check_row_count(extracted: u64, rejected: u64, staged: u64) -> Result<()> {
    if extracted != staged + rejected {
        return Err(AtlatlError::data(format!(
            "staging holds {staged} rows but extract produced {extracted} ({rejected} rejected): rows were lost"
        ))
        .code("row_count_mismatch"));
    }
    Ok(())
}

fn pk_list(pk: &[String]) -> String {
    pk.iter().map(|k| ident(k)).collect::<Vec<_>>().join(", ")
}

/// Applies `null_keys`; returns rows dropped (`skip`).
pub fn null_keys(c: &Connection, stg: &str, pk: &[String], policy: NullKeys) -> Result<u64> {
    if pk.is_empty() || policy == NullKeys::NullSafe {
        return Ok(0);
    }
    let rel = quoted_relation(stg);
    let any_null = pk
        .iter()
        .map(|k| format!("{} IS NULL", ident(k)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let n: i64 = c
        .query_row(
            &format!("SELECT count(*) FROM {rel} WHERE {any_null}"),
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    if n == 0 {
        return Ok(0);
    }
    match policy {
        NullKeys::Skip => {
            c.execute_batch(&format!("DELETE FROM {rel} WHERE {any_null}"))
                .map_err(db)?;
            Ok(n as u64)
        }
        _ => {
            let s = samples(
                c,
                &format!(
                    "SELECT to_json(t)::VARCHAR FROM (SELECT * FROM {rel} WHERE {any_null} LIMIT {MAX_SAMPLES}) t"
                ),
            )?;
            Err(AtlatlError::data(format!(
                "{n} rows have a NULL in primary key ({})",
                pk.join(", ")
            ))
            .code("null_primary_key")
            .samples(s))
        }
    }
}

/// Applies `duplicates`; returns rows removed (`first` / `last`).
pub fn duplicates(
    c: &Connection,
    stg: &str,
    pk: &[String],
    policy: Duplicates,
    order_by: &[String],
) -> Result<u64> {
    if pk.is_empty() {
        return Ok(0);
    }
    let rel = quoted_relation(stg);
    let keys = pk_list(pk);
    let dup: i64 = c
        .query_row(
            &format!(
                "SELECT count(*) FROM (SELECT 1 FROM {rel} GROUP BY {keys} HAVING count(*) > 1)"
            ),
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    if dup == 0 {
        return Ok(0);
    }
    match policy {
        Duplicates::Fail => {
            let s = samples(
                c,
                &format!(
                    "SELECT to_json(t)::VARCHAR FROM (SELECT {keys}, count(*) AS copies FROM {rel} GROUP BY {keys} HAVING count(*) > 1 LIMIT {MAX_SAMPLES}) t"
                ),
            )?;
            Err(AtlatlError::data(format!(
                "{dup} primary keys ({}) appear more than once",
                pk.join(", ")
            ))
            .code("duplicate_primary_key")
            .samples(s))
        }
        Duplicates::First | Duplicates::Last => {
            let dir = if policy == Duplicates::First {
                "ASC"
            } else {
                "DESC"
            };
            let order = order_by
                .iter()
                .map(|o| format!("{} {dir}", ident(o)))
                .collect::<Vec<_>>()
                .join(", ");
            let before: i64 = c
                .query_row(&format!("SELECT count(*) FROM {rel}"), [], |r| r.get(0))
                .map_err(db)?;
            c.execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE {rel} AS SELECT * FROM {rel} QUALIFY row_number() OVER (PARTITION BY {keys} ORDER BY {order}) = 1"
            ))
            .map_err(db)?;
            let after: i64 = c
                .query_row(&format!("SELECT count(*) FROM {rel}"), [], |r| r.get(0))
                .map_err(db)?;
            Ok((before - after) as u64)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmptyOutcome {
    Proceed,
    /// `on_empty: skip`: leave the target untouched.
    Skip,
}

/// `on_empty` and `guard` for full-refresh (04 §4.1).
pub fn empty_and_guard(
    staged: u64,
    target_rows: Option<u64>,
    on_empty: OnEmpty,
    guard: Option<&Guard>,
) -> Result<EmptyOutcome> {
    if staged == 0 {
        match on_empty {
            OnEmpty::Skip => return Ok(EmptyOutcome::Skip),
            OnEmpty::Fail => {
                return Err(
                    AtlatlError::data("source is empty (`on_empty: fail`)").code("empty_source")
                );
            }
            OnEmpty::Mirror => {}
        }
    }
    if let Some(g) = guard {
        if let Some(min) = g.min_rows
            && staged < min
        {
            return Err(AtlatlError::data(format!(
                "guard: {staged} rows staged, `min_rows` is {min}"
            ))
            .code("guard_min_rows"));
        }
        if let (Some(max), Some(t)) = (g.max_shrink, target_rows)
            && t > 0
            && (staged as f64) < (t as f64) * (1.0 - max)
        {
            return Err(AtlatlError::data(format!(
                "guard: {staged} rows staged vs {t} in the target, a shrink beyond `max_shrink: {max}`"
            ))
            .code("guard_max_shrink"));
        }
    }
    Ok(EmptyOutcome::Proceed)
}

#[cfg(test)]
mod tests {
    use atlatl_core::error::ErrorClass;

    use super::*;
    use crate::staging::{describe, stage_query};

    fn conn() -> Connection {
        let c = Connection::open_in_memory().expect("db");
        stage_query(
            &c,
            "stg",
            "SELECT * FROM (VALUES (1, 'a', 1), (2, 'b', 1), (2, 'b2', 2), (NULL, 'n', 1), (3, 'c', 1)) t(id, v, ver)",
        )
        .expect("stage");
        c
    }

    #[test]
    fn columns_and_counts() {
        let c = conn();
        let cols = describe(&c, "stg").expect("describe");
        assert!(require_columns(&cols, &[("primary_key", "id"), ("update_key", "VER")]).is_ok());
        let e = require_columns(&cols, &[("update_key", "updated_at")]).expect_err("missing");
        assert!(
            e.message.contains("update_key column `updated_at`") && e.class == ErrorClass::Config,
            "{e}"
        );
        assert!(check_row_count(5, 0, 5).is_ok());
        assert!(check_row_count(6, 1, 5).is_ok());
        assert_eq!(
            check_row_count(6, 0, 5).expect_err("lost").code.as_deref(),
            Some("row_count_mismatch")
        );
    }

    #[test]
    fn null_key_policies() {
        let c = conn();
        let pk = vec!["id".to_owned()];
        let e = null_keys(&c, "stg", &pk, NullKeys::Fail).expect_err("nulls");
        assert_eq!(
            (e.class, e.code.as_deref(), e.samples.len()),
            (ErrorClass::Data, Some("null_primary_key"), 1)
        );
        assert_eq!(e.samples[0]["v"], "n");
        assert_eq!(
            null_keys(&c, "stg", &pk, NullKeys::NullSafe).expect("safe"),
            0
        );
        assert_eq!(null_keys(&c, "stg", &pk, NullKeys::Skip).expect("skip"), 1);
        assert_eq!(crate::staging::row_count(&c, "stg").expect("n"), 4);
    }

    #[test]
    fn duplicate_policies() {
        let c = conn();
        let pk = vec!["id".to_owned()];
        let e = duplicates(&c, "stg", &pk, Duplicates::Fail, &[]).expect_err("dups");
        assert_eq!(e.code.as_deref(), Some("duplicate_primary_key"));
        assert_eq!(e.samples[0]["copies"], 2);
        assert_eq!(
            duplicates(&c, "stg", &pk, Duplicates::Last, &["ver".into()]).expect("last"),
            1
        );
        let v: String = c
            .query_row("SELECT v FROM stg WHERE id = 2", [], |r| r.get(0))
            .expect("kept");
        assert_eq!(v, "b2", "last by ver");
        assert_eq!(
            duplicates(&c, "stg", &pk, Duplicates::Fail, &[]).expect("clean"),
            0
        );
    }

    #[test]
    fn empty_and_guards() {
        let g = Guard {
            min_rows: Some(10),
            max_shrink: Some(0.5),
        };
        assert_eq!(
            empty_and_guard(0, Some(5), OnEmpty::Mirror, None).expect("mirror"),
            EmptyOutcome::Proceed
        );
        assert_eq!(
            empty_and_guard(0, Some(5), OnEmpty::Skip, None).expect("skip"),
            EmptyOutcome::Skip
        );
        assert!(empty_and_guard(0, None, OnEmpty::Fail, None).is_err());
        assert_eq!(
            empty_and_guard(5, None, OnEmpty::Mirror, Some(&g))
                .expect_err("min")
                .code
                .as_deref(),
            Some("guard_min_rows")
        );
        assert_eq!(
            empty_and_guard(40, Some(100), OnEmpty::Mirror, Some(&g))
                .expect_err("shrink")
                .code
                .as_deref(),
            Some("guard_max_shrink")
        );
        assert!(empty_and_guard(60, Some(100), OnEmpty::Mirror, Some(&g)).is_ok());
        assert!(
            empty_and_guard(
                60,
                Some(0),
                OnEmpty::Mirror,
                Some(&Guard {
                    min_rows: None,
                    max_shrink: Some(0.5)
                })
            )
            .is_ok()
        );
    }
}
