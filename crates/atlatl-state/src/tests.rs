use atlatl_testkit::{LocalLake, pg16};

use super::*;

fn row(stream: &str, member: &str, v: serde_json::Value, hash: &str) -> StateRow {
    StateRow {
        stream_id: stream.into(),
        member: member.into(),
        kind: StateKind::UpdateKey,
        value: v,
        config_hash: hash.into(),
        run_id: "r1".into(),
        lake_snapshot: Some(3),
        updated_at: None,
    }
}

fn snapshots(c: &Connection) -> i64 {
    c.query_row("SELECT count(*) FROM ducklake_snapshots('lake')", [], |r| {
        r.get(0)
    })
    .expect("snaps")
}

fn check_store(lake: &LocalLake) {
    let c = lake.open("lake").expect("open");
    let s = LakeState::new("lake");
    s.bootstrap(&c).expect("bootstrap");
    s.bootstrap(&c).expect("idempotent");

    let v =
        serde_json::json!({"column": "publication_date", "type": "DATE", "value": "2025-05-01"});
    s.put(&c, &row("a/b", "", v.clone(), "h1")).expect("put");
    let got = s
        .get(&c, "a/b", "", StateKind::UpdateKey)
        .expect("get")
        .expect("row");
    assert_eq!(
        (
            got.value.clone(),
            got.config_hash.as_str(),
            got.lake_snapshot
        ),
        (v, "h1", Some(3))
    );
    assert!(got.updated_at.as_deref().is_some_and(|t| t.ends_with('Z')));

    let v2 =
        serde_json::json!({"column": "publication_date", "type": "DATE", "value": "2025-06-01"});
    s.put(&c, &row("a/b", "", v2.clone(), "h1"))
        .expect("upsert");
    s.put(&c, &row("a/b", "est_v2.x", serde_json::json!(1), "h1"))
        .expect("member");
    assert_eq!(s.list(&c, Some("a/b")).expect("list").len(), 2);
    assert_eq!(
        s.get(&c, "a/b", "", StateKind::UpdateKey)
            .expect("get")
            .map(|r| r.value),
        Some(v2)
    );

    let e = s
        .get_checked(&c, "a/b", "", StateKind::UpdateKey, "h2")
        .expect_err("drift");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("config_drift"))
    );
    assert_eq!(s.accept(&c, "a/b", "h2").expect("accept"), 2);
    assert!(
        s.get_checked(&c, "a/b", "", StateKind::UpdateKey, "h2")
            .expect("ok")
            .is_some()
    );

    assert_eq!(s.rename(&c, "a/b", "a/c").expect("move"), 2);
    assert!(s.list(&c, Some("a/b")).expect("list").is_empty());
    s.put(&c, &row("a/b", "", serde_json::json!(1), "h"))
        .expect("put");
    assert!(s.rename(&c, "a/b", "a/c").is_err(), "target exists");
    assert_eq!(
        s.reset(&c, "a/c", Some("est_v2.x")).expect("reset member"),
        1
    );
    assert_eq!(s.reset(&c, "a/c", None).expect("reset"), 1);

    // State written in a replication transaction lands in the same snapshot as the data, or not at all.
    c.execute_batch("CREATE TABLE lake.t (d DATE)").expect("t");
    let before = snapshots(&c);
    c.execute_batch("BEGIN; INSERT INTO lake.t VALUES (DATE '2025-07-01');")
        .expect("begin");
    s.put(&c, &row("tx", "", serde_json::json!("x"), "h"))
        .expect("put in txn");
    c.execute_batch("ROLLBACK").expect("rollback");
    assert!(
        s.get(&c, "tx", "", StateKind::UpdateKey)
            .expect("get")
            .is_none()
    );
    c.execute_batch("BEGIN; INSERT INTO lake.t VALUES (DATE '2025-07-02');")
        .expect("begin");
    s.put(&c, &row("tx", "", serde_json::json!("y"), "h"))
        .expect("put in txn");
    c.execute_batch("COMMIT").expect("commit");
    assert_eq!(snapshots(&c), before + 1, "data + state = one snapshot");

    let seeded = s
        .seed_from_target(&c, "lake.t", "d")
        .expect("seed")
        .expect("value");
    assert_eq!(
        seeded,
        serde_json::json!({"column": "d", "type": "DATE", "value": "2025-07-02"})
    );
    c.execute_batch("CREATE TABLE lake.empty (d DATE)")
        .expect("empty");
    assert!(
        s.seed_from_target(&c, "lake.empty", "d")
            .expect("seed")
            .is_none()
    );

    for (rep, st) in [("x", "succeeded"), ("y", "failed"), ("z", "up_to_date")] {
        s.record_replication(
            &c,
            &RunReplicationRecord {
                run_id: "r1".into(),
                external_run_id: Some("dag-1".into()),
                replication: rep.into(),
                status: st.into(),
                attempts: 1,
                lake_snapshot: None,
                error_class: None,
                error: None,
            },
        )
        .expect("record");
    }
    s.record_run(
        &c,
        &RunRecord {
            run_id: "r1".into(),
            external_run_id: Some("dag-1".into()),
            env: Some("dev".into()),
            kind: "run".into(),
            status: "partial_failure".into(),
            plan_hash: Some("p".into()),
            atlatl_version: "0.1.0".into(),
            detail: serde_json::json!({}),
        },
    )
    .expect("run");
    let ok = s.succeeded_under(&c, "dag-1").expect("succeeded");
    assert_eq!(ok, ["x".to_owned(), "z".to_owned()].into_iter().collect());
    assert!(s.succeeded_under(&c, "dag-2").expect("none").is_empty());

    let rej = Reject {
        stream_id: "monthly/e".into(),
        member: "".into(),
        reject_key: "k1".into(),
        request: serde_json::json!({"url": "https://x/y"}),
        raw: "{bad".into(),
        error_class: "decode".into(),
        error: "eof".into(),
        run_id: "r1".into(),
        attempts: 1,
    };
    s.upsert_reject(&c, &rej).expect("reject");
    s.upsert_reject(
        &c,
        &Reject {
            run_id: "r2".into(),
            ..rej.clone()
        },
    )
    .expect("again");
    let open = s.open_rejects(&c, "monthly/e").expect("open");
    assert_eq!(
        (open.len(), open[0].attempts, open[0].run_id.as_str()),
        (1, 2, "r2")
    );
    assert_eq!(
        s.resolve_rejects(&c, "monthly/e", "", &["k1".into()])
            .expect("resolve"),
        1
    );
    assert!(s.open_rejects(&c, "monthly/e").expect("open").is_empty());

    for (m, k) in [("", "k1"), ("", "k2"), ("", "k'3"), ("est.a", "k1")] {
        s.upsert_reject(
            &c,
            &Reject {
                member: m.into(),
                reject_key: k.into(),
                ..rej.clone()
            },
        )
        .expect("reject");
    }
    let listed = s.list_rejects(&c).expect("list");
    assert_eq!(listed.len(), 4);
    let r = &listed[0];
    assert_eq!(
        (r.member.as_str(), r.reject_key.as_str(), r.attempts),
        ("", "k'3", 1)
    );
    assert!(r.first_seen_at.as_deref().is_some_and(|t| t.ends_with('Z')));
    assert_eq!(r.request["url"], "https://x/y");

    let day = std::time::Duration::from_secs(86_400);
    assert!(
        s.expired_rejects(&c, "monthly/e", "", day)
            .expect("ttl")
            .is_empty()
    );
    c.execute_batch(
        "UPDATE lake._atlatl.rejects SET first_seen_at = now() - INTERVAL 2 DAY WHERE reject_key = 'k2'",
    )
    .expect("age");
    assert_eq!(
        s.expired_rejects(&c, "monthly/e", "", day).expect("ttl"),
        ["k2"]
    );

    assert_eq!(
        s.drop_rejects(&c, "monthly/e", Some(""), &["k'3".into(), "nope".into()])
            .expect("drop"),
        1
    );
    assert_eq!(
        s.drop_rejects(&c, "monthly/e", Some("est.a"), &[])
            .expect("drop"),
        1
    );
    assert_eq!(s.drop_rejects(&c, "monthly/e", None, &[]).expect("drop"), 2);
    assert!(s.list_rejects(&c).expect("list").is_empty());
}

#[test]
fn file_catalog() {
    check_store(&LocalLake::file().expect("lake"));
}

#[test]
fn postgres_catalog() {
    check_store(&LocalLake::on_postgres(pg16()).expect("lake"));
}
