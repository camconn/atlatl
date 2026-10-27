use atlatl_testkit::LocalLake;

use super::*;

fn setup() -> (LocalLake, Connection) {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch(
        "CREATE TABLE lake.t (id INTEGER, v VARCHAR, keep VARCHAR);
         INSERT INTO lake.t VALUES (1, 'a', 'k1'), (2, 'b', 'k2'), (3, 'c', 'k3');",
    )
    .expect("setup");
    (lake, c)
}

fn stage(c: &Connection, rows: &str) {
    c.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE stg AS SELECT * FROM (VALUES {rows}) x(id, v)"
    ))
    .expect("stage");
}

fn cols() -> Vec<(String, String)> {
    vec![
        ("id".into(), "CAST(\"id\" AS INTEGER)".into()),
        ("v".into(), "\"v\"".into()),
    ]
}

fn rows(c: &Connection) -> Vec<(i32, String, Option<String>)> {
    let mut s = c
        .prepare("SELECT id, v, keep FROM lake.t ORDER BY id")
        .expect("q");
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows")
}

fn snaps(c: &Connection) -> i64 {
    c.query_row(
        "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')",
        [],
        |r| r.get(0),
    )
    .expect("snap")
}

fn apply<'a>(kind: Kind, pk: &'a [String], cols: &'a [(String, String)]) -> Apply<'a> {
    Apply {
        kind,
        target: "lake.t",
        source: "stg",
        columns: cols,
        primary_key: pk,
        null_safe: false,
    }
}

#[test]
fn incremental_upsert_touches_only_changes() {
    let (_l, c) = setup();
    stage(&c, "(1, 'a'), (2, 'B'), (4, 'd')");
    let pk = vec!["id".to_owned()];
    let cols = cols();
    let before = snaps(&c);
    c.execute_batch("BEGIN").expect("begin");
    let n = apply(Kind::Incremental, &pk, &cols).run(&c).expect("apply");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(
        n,
        Counts {
            inserted: 1,
            updated: 1,
            deleted: 0,
            unchanged: 1
        }
    );
    assert_eq!(
        rows(&c),
        [
            (1, "a".into(), Some("k1".into())),
            (2, "B".into(), Some("k2".into())),
            (3, "c".into(), Some("k3".into())),
            (4, "d".into(), None)
        ],
        "columns absent from the source are untouched on update and NULL on insert"
    );
    assert_eq!(snaps(&c), before + 1);

    let before = snaps(&c);
    c.execute_batch("BEGIN").expect("begin");
    let n = apply(Kind::Incremental, &pk, &cols).run(&c).expect("again");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(
        n,
        Counts {
            inserted: 0,
            updated: 0,
            deleted: 0,
            unchanged: 3
        }
    );
    assert_eq!(snaps(&c), before, "no change, no snapshot");
}

#[test]
fn full_refresh_with_pk_is_a_diff_replace() {
    let (_l, c) = setup();
    let id0: i64 = c
        .query_row(
            "SELECT table_id FROM ducklake_table_info('lake') WHERE table_name = 't'",
            [],
            |r| r.get(0),
        )
        .expect("id");
    stage(&c, "(1, 'a'), (3, 'C')");
    let pk = vec!["id".to_owned()];
    let cols = cols();
    c.execute_batch("BEGIN").expect("begin");
    let n = apply(Kind::FullRefresh, &pk, &cols).run(&c).expect("apply");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(
        n,
        Counts {
            inserted: 0,
            updated: 1,
            deleted: 1,
            unchanged: 1
        }
    );
    assert_eq!(
        rows(&c)
            .iter()
            .map(|r| (r.0, r.1.clone()))
            .collect::<Vec<_>>(),
        [(1, "a".into()), (3, "C".into())]
    );
    let id1: i64 = c
        .query_row(
            "SELECT table_id FROM ducklake_table_info('lake') WHERE table_name = 't'",
            [],
            |r| r.get(0),
        )
        .expect("id");
    assert_eq!(id0, id1, "table identity kept");
}

#[test]
fn full_refresh_without_pk_and_empty_mirror() {
    let (_l, c) = setup();
    stage(&c, "(9, 'z')");
    let cols = cols();
    c.execute_batch("BEGIN").expect("begin");
    let n = apply(Kind::FullRefresh, &[], &cols).run(&c).expect("apply");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(
        n,
        Counts {
            inserted: 1,
            deleted: 3,
            ..Default::default()
        }
    );
    c.execute_batch(
        "CREATE OR REPLACE TEMP TABLE stg AS SELECT 1::INTEGER AS id, 'x' AS v WHERE false",
    )
    .expect("empty");
    c.execute_batch("BEGIN").expect("begin");
    let n = apply(Kind::FullRefresh, &[], &cols)
        .run(&c)
        .expect("mirror");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(n.deleted, 1);
    assert!(rows(&c).is_empty(), "on_empty: mirror empties the target");
}

#[test]
fn append_modes() {
    let (_l, c) = setup();
    stage(&c, "(1, 'a')");
    let cols = cols();
    c.execute_batch("BEGIN").expect("begin");
    assert_eq!(
        apply(Kind::Incremental, &[], &cols)
            .run(&c)
            .expect("append")
            .inserted,
        1
    );
    assert_eq!(
        apply(Kind::Snapshot, &[], &cols)
            .run(&c)
            .expect("snapshot")
            .inserted,
        1
    );
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(rows(&c).len(), 5);
}

#[test]
fn null_safe_join() {
    let (_l, c) = setup();
    c.execute_batch("INSERT INTO lake.t VALUES (NULL, 'n', 'kn')")
        .expect("null row");
    stage(&c, "(NULL, 'N')");
    let pk = vec!["id".to_owned()];
    let cols = cols();
    let mut a = apply(Kind::Incremental, &pk, &cols);
    a.null_safe = true;
    c.execute_batch("BEGIN").expect("begin");
    let n = a.run(&c).expect("apply");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!((n.updated, n.inserted), (1, 0));
}

#[test]
fn chunked_full_refresh_in_one_transaction() {
    let (_l, c) = setup();
    stage(&c, "(1, 'A'), (2, 'b'), (5, 'e')");
    c.execute_batch("CREATE OR REPLACE TEMP TABLE seen (id INTEGER)")
        .expect("seen");
    let pk = vec!["id".to_owned()];
    let cols = cols();
    let before = snaps(&c);
    c.execute_batch("BEGIN").expect("begin");
    let mut total = Counts::default();
    for k in 0..2 {
        let rel = format!("(SELECT * FROM stg WHERE id % 2 = {k})");
        let a = Apply {
            source: &rel,
            ..apply(Kind::FullRefresh, &pk, &cols)
        };
        total.add(a.run_chunk(&c, "seen").expect("chunk"));
    }
    total.deleted = apply(Kind::FullRefresh, &pk, &cols)
        .finish_chunked(&c, "seen")
        .expect("finish");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(
        total,
        Counts {
            inserted: 1,
            updated: 1,
            deleted: 1,
            unchanged: 1
        }
    );
    assert_eq!(rows(&c).iter().map(|r| r.0).collect::<Vec<_>>(), [1, 2, 5]);
    assert_eq!(snaps(&c), before + 1, "one snapshot for every chunk");
}

#[test]
fn update_key_filters_and_max() {
    assert_eq!(
        update_key_filter("d", Some("2025-05-01"), None, None, true, false).as_deref(),
        Some("\"d\" >= '2025-05-01'")
    );
    assert_eq!(
        update_key_filter("d", Some("2025-05-01"), None, None, false, false).as_deref(),
        Some("\"d\" > '2025-05-01'")
    );
    assert_eq!(
        update_key_filter(
            "ts",
            Some("2025-05-01 00:00:00"),
            None,
            Some("2 hours"),
            true,
            true
        )
        .as_deref(),
        Some("(\"ts\" >= ('2025-05-01 00:00:00' - INTERVAL '2 hours') OR \"ts\" IS NULL)")
    );
    assert_eq!(
        update_key_filter("d", None, None, None, true, false).as_deref(),
        Some("\"d\" IS NOT NULL")
    );
    assert_eq!(update_key_filter("d", None, None, None, true, true), None);
    let c = Connection::open_in_memory().expect("db");
    c.execute_batch("CREATE TEMP TABLE s AS SELECT * FROM (VALUES (DATE '2025-01-02'), (NULL), (DATE '2025-03-04')) x(d)").expect("s");
    assert_eq!(
        staged_max(&c, "s", "d").expect("max"),
        Some(("2025-03-04".into(), "DATE".into()))
    );
    c.execute_batch("CREATE TEMP TABLE e AS SELECT DATE '2025-01-01' AS d WHERE false")
        .expect("e");
    assert_eq!(staged_max(&c, "e", "d").expect("max"), None);
    // The filter binds against real data.
    let n: i64 = c
        .query_row(
            &format!(
                "SELECT count(*) FROM s WHERE {}",
                update_key_filter("d", Some("2025-02-01"), None, None, true, false).expect("f")
            ),
            [],
            |r| r.get(0),
        )
        .expect("n");
    assert_eq!(n, 1);
    let f = update_key_filter(
        "d",
        Some("2025-03-04"),
        Some("DATE"),
        Some("62 days"),
        true,
        false,
    )
    .expect("f");
    assert_eq!(
        f,
        "\"d\" >= (CAST('2025-03-04' AS DATE) - INTERVAL '62 days')"
    );
    let n: i64 = c
        .query_row(&format!("SELECT count(*) FROM s WHERE {f}"), [], |r| {
            r.get(0)
        })
        .expect("lookback binds");
    assert_eq!(n, 2);
}

fn stamps(c: &Connection) -> Vec<(i32, i32)> {
    let mut s = c
        .prepare("SELECT id, stamp FROM lake.s ORDER BY id")
        .expect("q");
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("rows")
}

#[test]
fn unguarded_stamps_move_only_with_a_change() {
    let (_l, c) = setup();
    c.execute_batch(
        "CREATE TABLE lake.s (id INTEGER, v VARCHAR, stamp INTEGER);
         INSERT INTO lake.s VALUES (1, 'a', 100), (2, 'b', 100);
         CREATE OR REPLACE TEMP TABLE stg AS SELECT * FROM (VALUES (1, 'a', 200), (2, 'B', 200), (3, 'c', 200)) x(id, v, stamp);",
    )
    .expect("setup");
    let pk = vec!["id".to_owned()];
    let cols = vec![
        ("id".to_owned(), "\"id\"".to_owned()),
        ("v".to_owned(), "\"v\"".to_owned()),
        ("stamp".to_owned(), "\"stamp\"".to_owned()),
    ];
    let unguarded = vec!["stamp".to_owned()];
    for kind in [Kind::Incremental, Kind::FullRefresh] {
        let a = Apply {
            target: "lake.s",
            ..apply(kind, &pk, &cols)
        };
        let d = a.diff(&c, &[], &unguarded, 5).expect("diff");
        assert_eq!(
            d.counts,
            Counts {
                inserted: 1,
                updated: 1,
                deleted: 0,
                unchanged: 1
            },
            "{kind:?}: a new stamp alone isn't a change"
        );
    }
    let a = Apply {
        target: "lake.s",
        ..apply(Kind::Incremental, &pk, &cols)
    };
    c.execute_batch("BEGIN").expect("begin");
    let n = a.run_guarded(&c, &unguarded).expect("apply");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!((n.inserted, n.updated, n.unchanged), (1, 1, 1));
    assert_eq!(
        stamps(&c),
        [(1, 100), (2, 200), (3, 200)],
        "the unchanged row keeps its stamp; changed and new rows get the run's"
    );

    // Everything the same but the stamp: nothing to write, no snapshot.
    c.execute_batch("UPDATE stg SET stamp = 300")
        .expect("restamp");
    let before = snaps(&c);
    c.execute_batch("BEGIN").expect("begin");
    let n = a.run_guarded(&c, &unguarded).expect("again");
    c.execute_batch("COMMIT").expect("commit");
    assert!(!n.changed(), "{n:?}");
    assert_eq!(snaps(&c), before);
    let n = a.run(&c).expect("guarded on every column");
    assert_eq!(n.updated, 3, "without the exclusion every row restamps");
}

#[test]
fn diff_counts_and_samples_what_run_would_do() {
    let (_l, c) = setup();
    stage(&c, "(1, 'a'), (3, 'C'), (4, 'd'), (5, 'e')");
    let pk = vec!["id".to_owned()];
    let cols = cols();
    let before = snaps(&c);
    let a = apply(Kind::FullRefresh, &pk, &cols);
    let d = a.diff(&c, &[], &[], 1).expect("diff");
    assert!(d.keyed);
    assert_eq!(
        d.counts,
        Counts {
            inserted: 2,
            updated: 1,
            deleted: 1,
            unchanged: 1
        }
    );
    assert_eq!(
        d.samples,
        [
            serde_json::json!({"op": "insert", "key": {"id": 4}}),
            serde_json::json!({"op": "update", "key": {"id": 3}, "changed": ["v"], "staged": {"v": "C"}, "target": {"v": "c"}}),
            serde_json::json!({"op": "delete", "key": {"id": 2}}),
        ],
        "one per op at limit 1, ordered by key"
    );
    assert_eq!(d.samples.len(), 3);
    assert_eq!(
        apply(Kind::Incremental, &pk, &cols)
            .diff(&c, &[], &[], 5)
            .expect("inc")
            .counts,
        Counts {
            inserted: 2,
            updated: 1,
            deleted: 0,
            unchanged: 1
        },
        "an upsert deletes nothing"
    );
    for (kind, pk, deleted) in [
        (Kind::FullRefresh, &[][..], 3),
        (Kind::Incremental, &[][..], 0),
        (Kind::Snapshot, &pk[..], 0),
    ] {
        let d = apply(kind, pk, &cols)
            .diff(&c, &[], &[], 5)
            .expect("keyless");
        assert!(!d.keyed && d.samples.is_empty(), "{kind:?}");
        assert_eq!(
            (d.counts.inserted, d.counts.deleted),
            (4, deleted),
            "{kind:?}"
        );
    }
    // Chunks are compared one by one, deletes against all of their keys.
    let rels: Vec<String> = (0..2)
        .map(|k| format!("(SELECT * FROM stg WHERE id % 2 = {k})"))
        .collect();
    let d = a.diff(&c, &rels, &[], 5).expect("chunked");
    assert_eq!(
        d.counts,
        Counts {
            inserted: 2,
            updated: 1,
            deleted: 1,
            unchanged: 1
        }
    );
    assert_eq!(
        d.samples
            .iter()
            .filter(|s| s["op"] == "insert")
            .map(|s| s["key"]["id"].clone())
            .collect::<Vec<_>>(),
        [serde_json::json!(4), serde_json::json!(5)]
    );
    assert_eq!(snaps(&c), before, "read-only");
    assert_eq!(rows(&c).len(), 3);

    c.execute_batch("BEGIN").expect("begin");
    let n = a.run(&c).expect("apply");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(n, d.counts, "the diff is what the apply does");
}
