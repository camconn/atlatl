use atlatl_testkit::LocalLake;

use super::*;

fn src() -> Source {
    Source {
        catalog: "lake".into(),
        schema: "au".into(),
        table: "x".into(),
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
    let c = lake.open("lake").expect("open");
    c.execute_batch(
        "CREATE SCHEMA lake.au;
         CREATE TABLE lake.au.x (k BIGINT, v VARCHAR, u INTEGER);
         INSERT INTO lake.au.x SELECT i, 'v' || i, 0 FROM range(1, 6) t(i);",
    )
    .expect("setup");
    (lake, c)
}

fn exec(c: &Connection, sql: &str) -> i64 {
    c.execute_batch(sql).expect(sql);
    head(c, "lake").expect("head")
}

fn ups(c: &Connection, nc: &NetChanges) -> Vec<(i64, Option<String>)> {
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

#[test]
fn net_changes_are_exact() {
    let (_l, c) = setup();
    let s = src();
    let c0 = head(&c, "lake").expect("head");
    exec(&c, "UPDATE lake.au.x SET v = 'two' WHERE k = 2");
    exec(&c, "DELETE FROM lake.au.x WHERE k = 3");
    let e = exec(&c, "INSERT INTO lake.au.x VALUES (9, 'nine', 0)");
    let nc = net_changes(&c, &s, Window { from: c0, to: e }, "t1").expect("diff");
    assert_eq!(
        ups(&c, &nc),
        [(2, Some("two".into())), (9, Some("nine".into()))]
    );
    assert_eq!(dels(&c, &nc), [3]);
    assert_eq!((nc.touched, nc.upsert_rows, nc.delete_rows), (3, 2, 1));
}

#[test]
fn rewrites_and_ignored_churn_produce_nothing() {
    let (_l, c) = setup();
    let mut s = src();
    let c0 = head(&c, "lake").expect("head");
    let e = exec(
        &c,
        "BEGIN; CREATE TEMP TABLE keep AS SELECT * FROM lake.au.x; DELETE FROM lake.au.x;
         INSERT INTO lake.au.x SELECT * FROM keep; COMMIT; DROP TABLE keep;",
    );
    let nc = net_changes(&c, &s, Window { from: c0, to: e }, "t2").expect("diff");
    assert_eq!(
        (nc.touched, nc.upsert_rows, nc.delete_rows),
        (5, 0, 0),
        "dbt delete+insert of unchanged rows"
    );
    let e2 = exec(&c, "UPDATE lake.au.x SET u = 1");
    s.ignore = vec!["u".into()];
    let nc = net_changes(&c, &s, Window { from: e, to: e2 }, "t3").expect("diff");
    assert_eq!(nc.upsert_rows, 0, "ignore_columns");
    s.ignore.clear();
    let nc = net_changes(&c, &s, Window { from: e, to: e2 }, "t4").expect("diff");
    assert_eq!(nc.upsert_rows, 5);
}

#[test]
fn filters_turn_exits_into_deletes() {
    let (_l, c) = setup();
    let mut s = src();
    s.filter = Some("\"v\" <> 'hidden'".into());
    let c0 = head(&c, "lake").expect("head");
    let e = exec(&c, "UPDATE lake.au.x SET v = 'hidden' WHERE k = 4");
    let nc = net_changes(&c, &s, Window { from: c0, to: e }, "t5").expect("diff");
    assert_eq!((ups(&c, &nc), dels(&c, &nc)), (vec![], vec![4]));
}

/// A row leaving the filter is a delete with no DELETE behind it: the feed only has the UPDATE,
/// so its stamp can only come from probing (why `stamp_deletes` doesn't read the feed's labels).
#[test]
fn filter_exits_are_stamped_with_the_update_that_hid_them() {
    let (_l, c) = setup();
    let mut s = src();
    s.filter = Some("\"v\" <> 'hidden'".into());
    let tid = guard_identity(&c, &s, None).expect("id");
    let c0 = head(&c, "lake").expect("head");
    let s1 = exec(&c, "UPDATE lake.au.x SET v = 'hidden' WHERE k = 4");
    let e = exec(&c, "UPDATE lake.au.x SET v = 'x' WHERE k = 2");
    let w = Window { from: c0, to: e };
    let nc = net_changes(&c, &s, w, "t9").expect("diff");
    stamp_deletes(&c, &s, tid, w, &nc).expect("stamp");
    let got: (i64, i64) = c
        .query_row(
            &format!("SELECT k, _snapshot FROM {}", ident(&nc.deletes)),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("row");
    assert_eq!(got, (4, s1));
    let feed_deletes: i64 = c
        .query_row(
            &format!("SELECT count(*) FROM ducklake_table_changes('lake', 'au', 'x', {}, {e}) WHERE change_type = 'delete'", c0 + 1),
            [],
            |r| r.get(0),
        )
        .expect("feed");
    assert_eq!(feed_deletes, 0, "the feed has no delete for a filter exit");
}

#[test]
fn columns_added_after_c_and_transforms() {
    let (_l, c) = setup();
    let c0 = head(&c, "lake").expect("head");
    exec(&c, "ALTER TABLE lake.au.x ADD COLUMN rating VARCHAR");
    let e = exec(&c, "UPDATE lake.au.x SET rating = 'strong buy' WHERE k = 1");
    let mut s = src();
    s.columns.push((
        "rating".into(),
        "upper(replace(\"rating\", ' ', '_'))".into(),
    ));
    let nc = net_changes(&c, &s, Window { from: c0, to: e }, "t6").expect("diff");
    let r: String = c
        .query_row(
            &format!("SELECT rating FROM {} WHERE k = 1", ident(&nc.upserts)),
            [],
            |r| r.get(0),
        )
        .expect("rating");
    assert_eq!((nc.upsert_rows, r.as_str()), (1, "STRONG_BUY"));
}

#[test]
fn soft_delete_stamps_come_from_the_deleting_snapshot() {
    let (_l, c) = setup();
    let s = src();
    let tid = guard_identity(&c, &s, None).expect("id");
    let c0 = head(&c, "lake").expect("head");
    exec(&c, "UPDATE lake.au.x SET v = 'x' WHERE k = 2");
    let s1 = exec(&c, "DELETE FROM lake.au.x WHERE k = 5");
    let e = exec(&c, "DELETE FROM lake.au.x WHERE k = 1");
    let w = Window { from: c0, to: e };
    let nc = net_changes(&c, &s, w, "t7").expect("diff");
    stamp_deletes(&c, &s, tid, w, &nc).expect("stamp");
    let mut st = c
        .prepare(&format!(
            "SELECT k, _snapshot, _ts IS NOT NULL FROM {} ORDER BY k",
            ident(&nc.deletes)
        ))
        .expect("q");
    let got: Vec<(i64, i64, bool)> = st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows");
    assert_eq!(got, [(1, e, true), (5, s1, true)]);
}

#[test]
fn guards() {
    let (_l, c) = setup();
    let s = src();
    let tid = guard_identity(&c, &s, None).expect("id");
    assert_eq!(guard_identity(&c, &s, Some(tid)).expect("same"), tid);
    let c0 = head(&c, "lake").expect("head");
    exec(&c, "INSERT INTO lake.au.x VALUES (6, 'six', 0)");
    exec(&c, "INSERT INTO lake.au.x VALUES (7, 'seven', 0)");
    guard_retention(&c, "lake", c0).expect("retained");
    c.execute_batch(&format!(
        "CALL ducklake_expire_snapshots('lake', versions => [{c0}])"
    ))
    .expect("expire");
    let e = guard_retention(&c, "lake", c0).expect_err("expired");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Data, Some("cursor_expired"))
    );
    assert!(e.message.contains("oldest retained"), "{e}");
    exec(
        &c,
        "CREATE OR REPLACE TABLE lake.au.x AS SELECT 1::BIGINT AS k, 'a' AS v, 0 AS u",
    );
    let e = guard_identity(&c, &s, Some(tid)).expect_err("recreated");
    assert_eq!(e.code.as_deref(), Some("resync_required"));
}

#[test]
fn bounds_and_lag() {
    let (_l, c) = setup();
    let s = src();
    let tid = guard_identity(&c, &s, None).expect("id");
    let c0 = head(&c, "lake").expect("head");
    let s1 = exec(
        &c,
        "INSERT INTO lake.au.x SELECT i, 'a', 0 FROM range(100, 110) t(i)",
    );
    exec(&c, "CREATE TABLE lake.au.other (a INT)");
    let s3 = exec(
        &c,
        "INSERT INTO lake.au.x SELECT i, 'b', 0 FROM range(200, 210) t(i)",
    );
    let s4 = exec(
        &c,
        "INSERT INTO lake.au.x SELECT i, 'c', 0 FROM range(300, 310) t(i)",
    );
    assert_eq!(
        touching_snapshots(&c, "lake", tid, c0, s4).expect("snaps"),
        [s1, s3, s4]
    );
    assert_eq!(
        bound(&c, &s, tid, c0, s4, None, None).expect("unbounded"),
        s4
    );
    assert_eq!(
        bound(&c, &s, tid, c0, s4, Some(2), None).expect("snapshots"),
        s3
    );
    assert_eq!(
        bound(&c, &s, tid, c0, s4, None, Some(20)).expect("changes"),
        s3
    );
    assert_eq!(
        bound(&c, &s, tid, c0, s4, None, Some(5)).expect("progress"),
        s1,
        "always advances"
    );
    let l = lag(&c, "lake", c0, s4).expect("lag");
    assert_eq!(l.snapshots_behind, 4);
    assert!(l.seconds_behind >= 0.0);
}

#[test]
fn null_and_duplicate_keys_fail() {
    let (_l, c) = setup();
    let s = src();
    let c0 = head(&c, "lake").expect("head");
    let e = exec(
        &c,
        "INSERT INTO lake.au.x VALUES (1, 'dup', 0), (NULL, 'n', 0)",
    );
    let err = net_changes(&c, &s, Window { from: c0, to: e }, "t8").expect_err("bad keys");
    assert_eq!(err.class, ErrorClass::Data);
}
