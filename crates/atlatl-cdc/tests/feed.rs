//! The 08 §4 diff on parquet-backed tables: with `DATA_INLINING_ROW_LIMIT 0` every
//! write is a data or delete file, so rewrites, folded delete files and compaction are exercised.

use atlatl_cdc::{
    NetChanges, Source, Window, feed_rows, guard_identity, guard_retention, head, ident,
    net_changes, stamp_deletes,
};
use atlatl_core::ErrorClass;
use atlatl_testkit::LocalLake;
use duckdb::Connection;

const PARQUET: &str = "DATA_INLINING_ROW_LIMIT 0";

fn src(table: &str) -> Source {
    Source {
        catalog: "lake".into(),
        schema: "au".into(),
        table: table.into(),
        primary_key: vec!["k".into()],
        columns: vec![
            ("k".into(), "\"k\"".into()),
            ("v".into(), "\"v\"".into()),
            ("u".into(), "\"u\"".into()),
        ],
        ignore: vec![],
        filter: None,
    }
}

fn setup() -> (LocalLake, Connection) {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open_with("lake", PARQUET).expect("open");
    c.execute_batch(
        "CREATE SCHEMA lake.au;
         CREATE TABLE lake.au.x (k BIGINT, v VARCHAR, u INTEGER);
         INSERT INTO lake.au.x SELECT i, 'v' || i, 0 FROM range(1, 11) t(i);",
    )
    .expect("setup");
    assert!(inlined_tables(&c) == 0 && data_files(&c, "x") > 0);
    (lake, c)
}

fn exec(c: &Connection, sql: &str) -> i64 {
    c.execute_batch(sql).expect(sql);
    head(c, "lake").expect("head")
}

fn scalar(c: &Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |r| r.get(0)).expect(sql)
}

fn data_files(c: &Connection, table: &str) -> i64 {
    scalar(
        c,
        &format!("SELECT count(*) FROM ducklake_list_files('lake', '{table}', schema => 'au')"),
    )
}

fn inlined_tables(c: &Connection) -> i64 {
    scalar(
        c,
        "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_inlined_data_tables",
    )
}

fn diff(c: &Connection, s: &Source, from: i64, to: i64, p: &str) -> (Vec<(i64, String)>, Vec<i64>) {
    let nc = net_changes(c, s, Window { from, to }, p).expect("diff");
    (ups(c, &nc), dels(c, &nc))
}

fn ups(c: &Connection, nc: &NetChanges) -> Vec<(i64, String)> {
    let mut s = c
        .prepare(&format!(
            "SELECT k, v FROM {} ORDER BY k",
            ident(&nc.upserts)
        ))
        .expect("q");
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows")
}

fn dels(c: &Connection, nc: &NetChanges) -> Vec<i64> {
    let mut s = c
        .prepare(&format!("SELECT k FROM {} ORDER BY k", ident(&nc.deletes)))
        .expect("q");
    s.query_map([], |r| r.get(0))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows")
}

fn stamps(c: &Connection, s: &Source, w: Window, p: &str) -> Vec<(i64, i64)> {
    let tid = guard_identity(c, s, None).expect("id");
    let nc = net_changes(c, s, w, p).expect("diff");
    stamp_deletes(c, s, tid, w, &nc).expect("stamp");
    let mut st = c
        .prepare(&format!(
            "SELECT k, _snapshot FROM {} WHERE _ts IS NOT NULL ORDER BY k",
            ident(&nc.deletes)
        ))
        .expect("q");
    st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows")
}

#[test]
fn dbt_delete_insert_keeps_identity_and_nets_to_the_real_change() {
    let (_l, c) = setup();
    let s = src("x");
    let tid = guard_identity(&c, &s, None).expect("id");
    let c0 = head(&c, "lake").expect("head");
    let e = exec(
        &c,
        "BEGIN; CREATE TEMP TABLE keep AS SELECT * FROM lake.au.x; DELETE FROM lake.au.x;
         INSERT INTO lake.au.x SELECT * FROM keep; COMMIT; DROP TABLE keep;",
    );
    assert_eq!(
        guard_identity(&c, &s, Some(tid)).expect("same table"),
        tid,
        "delete-all + insert keeps the table id"
    );
    assert_eq!(feed_rows(&c, &s, c0, e).expect("feed"), 20);
    assert_eq!(diff(&c, &s, c0, e, "a"), (vec![], vec![]));

    let e2 = exec(
        &c,
        "BEGIN; CREATE TEMP TABLE keep AS SELECT k, CASE WHEN k = 5 THEN 'five' ELSE v END AS v, u FROM lake.au.x;
         DELETE FROM lake.au.x; INSERT INTO lake.au.x SELECT * FROM keep; COMMIT; DROP TABLE keep;",
    );
    assert_eq!(diff(&c, &s, e, e2, "b"), (vec![(5, "five".into())], vec![]));
    assert_eq!(guard_identity(&c, &s, Some(tid)).expect("same table"), tid);
    assert_eq!(inlined_tables(&c), 0);
}

#[test]
fn delete_then_reinsert_in_one_window() {
    let (_l, c) = setup();
    let s = src("x");
    let c0 = head(&c, "lake").expect("head");
    let s1 = exec(&c, "DELETE FROM lake.au.x WHERE k IN (6, 7)");
    let e = exec(&c, "INSERT INTO lake.au.x VALUES (6, 'v6', 0)");
    assert_eq!(diff(&c, &s, c0, e, "a"), (vec![], vec![7]));
    assert_eq!(stamps(&c, &s, Window { from: c0, to: e }, "b"), [(7, s1)]);
}

#[test]
fn compaction_is_not_a_change() {
    let (_l, c) = setup();
    let s = src("x");
    exec(&c, "INSERT INTO lake.au.x VALUES (11, 'v11', 0)");
    let c0 = head(&c, "lake").expect("head");
    let s1 = exec(&c, "DELETE FROM lake.au.x WHERE k = 8");
    let with_deletes = "SELECT count(*) FROM ducklake_list_files('lake', 'x', schema => 'au') WHERE delete_file IS NOT NULL";
    assert!(scalar(&c, with_deletes) > 0);
    let e = exec(
        &c,
        "CALL ducklake_rewrite_data_files('lake', 'x', schema => 'au', delete_threshold => 0.0)",
    );
    assert!(e > s1, "compaction commits a snapshot");
    assert_eq!(scalar(&c, with_deletes), 0, "deletes were rewritten away");
    assert_eq!(diff(&c, &s, s1, e, "a"), (vec![], vec![]));
    assert_eq!(diff(&c, &s, c0, e, "b"), (vec![], vec![8]));
    assert_eq!(stamps(&c, &s, Window { from: c0, to: e }, "c"), [(8, s1)]);
}

/// An UPDATE and a DELETE on one data file share a delete file whose metadata keeps
/// the UPDATE's snapshot. Through DuckDB v1.5.3's ducklake a fresh attach then labelled the DELETE
/// with the UPDATE's snapshot; v1.5.6's labels it right. Stamps are still probed, not read off the feed.
#[test]
fn delete_stamps_from_a_fresh_attach_when_deletes_share_a_file() {
    let lake = LocalLake::file().expect("lake");
    let s = src("y");
    let (c0, s1, s2) = {
        let c = lake.open_with("lake", PARQUET).expect("open");
        exec(
            &c,
            "CREATE SCHEMA lake.au; CREATE TABLE lake.au.y (k BIGINT, v VARCHAR, u INTEGER)",
        );
        let c0 = exec(
            &c,
            "INSERT INTO lake.au.y VALUES (1, 'a', 0), (2, 'b', 0), (3, 'c', 0)",
        );
        let s1 = exec(&c, "UPDATE lake.au.y SET v = 'B' WHERE k = 2");
        let s2 = exec(&c, "DELETE FROM lake.au.y WHERE k = 3");
        (c0, s1, s2)
    };
    let fresh = || lake.open_with("lake", PARQUET).expect("reopen");
    let w = Window { from: c0, to: s2 };

    let c = fresh();
    let folded = scalar(
        &c,
        &format!(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file WHERE begin_snapshot = {s1}"
        ),
    );
    let at_s2 = scalar(
        &c,
        &format!(
            "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_delete_file WHERE begin_snapshot = {s2}"
        ),
    );
    assert!(
        folded >= 1 && at_s2 == 0,
        "the delete at {s2} was folded into {s1}'s delete file"
    );
    let label = scalar(
        &c,
        &format!(
            "SELECT snapshot_id FROM ducklake_table_changes('lake', 'au', 'y', {}, {s2}) WHERE change_type = 'delete'",
            c0 + 1
        ),
    );
    assert_eq!(
        label, s2,
        "fixed in the ducklake bundled with DuckDB v1.5.6"
    );
    assert_eq!(diff(&c, &s, c0, s2, "a"), (vec![(2, "B".into())], vec![3]));
    assert_eq!(stamps(&c, &s, w, "b"), [(3, s2)]);
    drop(c);

    // A later delete+insert over the rest of that file relabelled the feed again before v1.5.6.
    let s3 = exec(
        &fresh(),
        "BEGIN; DELETE FROM lake.au.y WHERE k IN (1, 2); INSERT INTO lake.au.y VALUES (1, 'a', 0), (2, 'B', 0); COMMIT;",
    );
    let c = fresh();
    let relabelled = scalar(
        &c,
        &format!(
            "SELECT snapshot_id FROM ducklake_table_changes('lake', 'au', 'y', {}, {s3}) WHERE change_type = 'delete' AND k = 3",
            c0 + 1
        ),
    );
    assert_eq!(relabelled, s2);
    assert_eq!(stamps(&c, &s, w, "c"), [(3, s2)]);
    assert_eq!(stamps(&c, &s, Window { from: c0, to: s3 }, "d"), [(3, s2)]);
}

#[test]
fn a_cursor_expiring_after_the_guard_is_cursor_expired() {
    let (_l, c) = setup();
    let s = src("x");
    let c0 = head(&c, "lake").expect("head");
    exec(&c, "INSERT INTO lake.au.x VALUES (11, 'v11', 0)");
    let e = exec(&c, "UPDATE lake.au.x SET v = 'x' WHERE k = 1");
    guard_retention(&c, "lake", c0).expect("retained");
    c.execute_batch(&format!(
        "CALL ducklake_expire_snapshots('lake', versions => [{c0}])"
    ))
    .expect("expire");
    let err = net_changes(&c, &s, Window { from: c0, to: e }, "a").expect_err("expired");
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Data, Some("cursor_expired")),
        "{err}"
    );
    assert!(
        err.message.contains("No snapshot found at version"),
        "{err}"
    );
    assert_eq!(
        diff(&c, &s, c0 + 1, e, "b"),
        (vec![(1, "x".into())], vec![]),
        "later windows still read"
    );
}
