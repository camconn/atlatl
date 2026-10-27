//! Run-scoped staging (04 §3). Staging is created before the commit transaction begins, so it
//! can spill; TEMP tables made inside an open transaction are held until it ends (spike S3 §4).

use atlatl_core::error::Result;
use duckdb::Connection;

use crate::sql::{db, ident};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// DuckDB type as `DESCRIBE` prints it.
    pub kind: String,
}

/// `stg_<run_id8>_<stream_seq>_<member_seq>`.
pub fn staging_name(run_id: &str, stream_seq: usize, member_seq: usize) -> String {
    let short: String = run_id
        .chars()
        .rev()
        .take(8)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("stg_{}_{stream_seq}_{member_seq}", short.to_lowercase())
}

/// `CREATE TEMP TABLE <name> AS <select>`; returns the staged row count.
pub fn stage_query(c: &Connection, name: &str, select: &str) -> Result<u64> {
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {} AS {select}",
        ident(name)
    ))
    .map_err(db)?;
    row_count(c, name)
}

pub fn row_count(c: &Connection, relation: &str) -> Result<u64> {
    let n: i64 = c
        .query_row(
            &format!("SELECT count(*) FROM {}", quoted_relation(relation)),
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    Ok(n as u64)
}

pub fn drop(c: &Connection, name: &str) -> Result<()> {
    c.execute_batch(&format!("DROP TABLE IF EXISTS temp.{}", ident(name)))
        .map_err(db)
}

/// A bare name is quoted; a dotted or already-quoted relation is used as written.
pub fn quoted_relation(r: &str) -> String {
    if r.contains('.') || r.contains('"') || r.contains('(') {
        r.to_owned()
    } else {
        ident(r)
    }
}

/// Columns of a relation (table name or `(SELECT …)`), in order.
pub fn describe(c: &Connection, relation: &str) -> Result<Vec<Column>> {
    let mut s = c
        .prepare(&format!(
            "DESCRIBE SELECT * FROM {}",
            quoted_relation(relation)
        ))
        .map_err(db)?;
    let rows = s
        .query_map([], |r| {
            Ok(Column {
                name: r.get(0)?,
                kind: r.get(1)?,
            })
        })
        .map_err(db)?;
    rows.map(|r| r.map_err(db)).collect()
}

/// Columns of an existing table, or `None` if it doesn't exist. `table` is `catalog.schema.table`.
pub fn table_columns(
    c: &Connection,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<Vec<Column>>> {
    let mut s = c
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns
             WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        )
        .map_err(db)?;
    let cols: Vec<Column> = s
        .query_map([catalog, schema, table], |r| {
            Ok(Column {
                name: r.get(0)?,
                kind: r.get(1)?,
            })
        })
        .map_err(db)?
        .collect::<Result<_, _>>()
        .map_err(db)?;
    Ok((!cols.is_empty()).then_some(cols))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(staging_name("01J9ZZABCDEFGHJK", 2, 0), "stg_cdefghjk_2_0");
        assert_eq!(staging_name("run", 0, 1), "stg_run_0_1");
    }

    #[test]
    fn stage_and_describe() {
        let c = Connection::open_in_memory().expect("db");
        let n = stage_query(
            &c,
            "stg_x_0_0",
            "SELECT range AS id, 'v' || range AS \"Name\" FROM range(5)",
        )
        .expect("stage");
        assert_eq!(n, 5);
        let cols = describe(&c, "stg_x_0_0").expect("describe");
        assert_eq!(
            cols,
            [
                Column {
                    name: "id".into(),
                    kind: "BIGINT".into()
                },
                Column {
                    name: "Name".into(),
                    kind: "VARCHAR".into()
                }
            ]
        );
        c.execute_batch("CREATE TABLE t (a INTEGER, b DECIMAL(24,6))")
            .expect("t");
        let t = table_columns(&c, "memory", "main", "t")
            .expect("cols")
            .expect("exists");
        assert_eq!(t[1].kind, "DECIMAL(24,6)");
        assert!(
            table_columns(&c, "memory", "main", "nope")
                .expect("cols")
                .is_none()
        );
        drop(&c, "stg_x_0_0").expect("drop");
        assert!(describe(&c, "stg_x_0_0").is_err());
    }
}
