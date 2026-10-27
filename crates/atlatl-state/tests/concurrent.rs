//! Concurrent replications committing their state rows (spike S3 §3). DuckLake conflicts are per
//! table, so two transactions that each advance an existing cursor in `_atlatl.stream_state`
//! conflict even when their data tables are disjoint; the loser is retried as a whole (03 §6).

use atlatl_core::ErrorClass;
use atlatl_engine::error::{COMMIT_CONFLICT, classify};
use atlatl_state::{LakeState, StateKind, StateRow};
use atlatl_testkit::{LocalLake, pg16};
use duckdb::Connection;

fn row(stream: &str, v: i64) -> StateRow {
    StateRow {
        stream_id: stream.into(),
        member: String::new(),
        kind: StateKind::UpdateKey,
        value: serde_json::json!({"column": "u", "type": "BIGINT", "value": v}),
        config_hash: "h".into(),
        run_id: "r".into(),
        lake_snapshot: None,
        updated_at: None,
    }
}

fn value(c: &Connection, s: &LakeState, stream: &str) -> Option<serde_json::Value> {
    s.get(c, stream, "", StateKind::UpdateKey)
        .expect("get")
        .map(|r| r.value["value"].clone())
}

/// A replication's commit: its data table, then its state row.
fn apply(c: &Connection, s: &LakeState, table: &str, stream: &str, v: i64) {
    c.execute_batch(&format!("BEGIN; INSERT INTO lake.{table} VALUES ({v})"))
        .expect("begin");
    s.put(c, &row(stream, v)).expect("put");
}

fn commit(c: &Connection) -> Result<(), atlatl_core::AtlatlError> {
    c.execute_batch("COMMIT").map_err(|e| {
        let _ = c.execute_batch("ROLLBACK");
        classify(&e)
    })
}

/// A and B on separate connections: `(A, B)` from two instances, or one instance twice.
fn race(a: &Connection, b: &Connection) {
    let s = LakeState::new("lake");
    s.bootstrap(a).expect("bootstrap");
    a.execute_batch("CREATE TABLE lake.ta (v BIGINT); CREATE TABLE lake.tb (v BIGINT);")
        .expect("tables");

    // First cursors: both MERGEs only insert, and appends don't conflict.
    apply(a, &s, "ta", "s/a", 1);
    apply(b, &s, "tb", "s/b", 1);
    commit(a).expect("A");
    commit(b).expect("B: appends to stream_state commit side by side");

    // Advancing existing cursors rewrites rows of the same table.
    apply(a, &s, "ta", "s/a", 2);
    apply(b, &s, "tb", "s/b", 2);
    commit(a).expect("A");
    let e = commit(b).expect_err("B conflicts on stream_state");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Conflict, Some(COMMIT_CONFLICT)),
        "{e}"
    );
    assert_eq!(
        value(b, &s, "s/b"),
        Some(serde_json::json!(1)),
        "B rolled back"
    );
    apply(b, &s, "tb", "s/b", 2);
    commit(b).expect("B's retry");

    for c in [a, b] {
        assert_eq!(value(c, &s, "s/a"), Some(serde_json::json!(2)));
        assert_eq!(value(c, &s, "s/b"), Some(serde_json::json!(2)));
        let n: i64 = c
            .query_row(
                "SELECT (SELECT count(*) FROM lake.ta) + (SELECT count(*) FROM lake.tb)",
                [],
                |r| r.get(0),
            )
            .expect("rows");
        assert_eq!(n, 4);
    }
}

#[test]
fn state_rows_across_processes() {
    let lake = LocalLake::on_postgres(pg16()).expect("lake");
    race(
        &lake.open("lake").expect("A"),
        &lake.open("lake").expect("B"),
    );
}

#[test]
fn state_rows_across_connections_of_one_engine() {
    let lake = LocalLake::on_postgres(pg16()).expect("lake");
    let a = lake.open("lake").expect("A");
    let b = a.try_clone().expect("B");
    race(&a, &b);
}
