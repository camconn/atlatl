use atlatl_conn_pgrepl::StopReason;
use atlatl_core::config::model::DeleteMode;

use crate::{Env, stream, versions};

pub const TYPES_DDL: &str = r#"
CREATE TABLE t (id int PRIMARY KEY, n numeric(12,3), t text, ts timestamptz, d date, j jsonb,
                arr int[], b bool, f float8, u uuid, by bytea, i8 bigint);
CREATE TABLE other (k text PRIMARY KEY, v int);
"#;

pub const T_INSERT: &str = r#"
INSERT INTO t VALUES
  (1, 12.345, 'x', '2024-03-10 12:00:00.123456+00', '2024-01-31', '{"k": [1, 2]}', '{1,2,NULL}',
   true, 1.5, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\x0001', 9007199254740993),
  (2, -0.5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
"#;

pub const T_ROWS: &str = "SELECT id::VARCHAR || '|' || coalesce(n::VARCHAR, '∅') || '|' || coalesce(t, '∅') || '|' ||
    coalesce(ts::VARCHAR, '∅') || '|' || coalesce(d::VARCHAR, '∅') || '|' || coalesce(j::VARCHAR, '∅') || '|' ||
    coalesce(arr::VARCHAR, '∅') || '|' || coalesce(b::VARCHAR, '∅') || '|' || coalesce(f::VARCHAR, '∅') || '|' ||
    coalesce(u::VARCHAR, '∅') || '|' || coalesce(\"by\"::VARCHAR, '∅') || '|' || coalesce(i8::VARCHAR, '∅') || '|' || _atlatl_op
    FROM lake.br.t ORDER BY id";

pub const T_WANT: [&str; 2] = [
    "1|12.345|x|2024-03-10 12:00:00.123456+00|2024-01-31|{\"k\": [1, 2]}|[1, 2, NULL]|true|1.5|a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|\\x00\\x01|9007199254740993|insert",
    "2|-0.500|∅|∅|∅|∅|∅|∅|∅|∅|∅|∅|insert",
];

async fn stream_and_apply(v: u32) {
    let env = Env::new(v).await;
    env.exec(TYPES_DDL).await;
    env.exec(T_INSERT).await;
    env.exec("INSERT INTO other VALUES ('a', 1)").await;
    let spec = env.spec(vec![stream("t"), stream("other")]);

    let r = env.run(spec.clone()).await;
    let w = &r.applied.window;
    assert!(w.slot_created && r.applied.committed, "{r:?}");
    assert_eq!(w.backfilled, vec!["pg/t", "pg/other"]);
    assert_eq!(w.stop, StopReason::CaughtUp);
    assert_eq!(r.applied.streams[0].counts.inserted, 2);
    assert_eq!(
        env.rows(
            "SELECT column_name || ' ' || data_type FROM information_schema.columns
             WHERE table_catalog = 'lake' AND table_schema = 'br' AND table_name = 't' ORDER BY ordinal_position"
        ),
        [
            "id INTEGER",
            "n DECIMAL(12,3)",
            "t VARCHAR",
            "ts TIMESTAMP WITH TIME ZONE",
            "d DATE",
            "j JSON",
            "arr INTEGER[]",
            "b BOOLEAN",
            "f DOUBLE",
            "u UUID",
            "by BLOB",
            "i8 BIGINT",
            "_atlatl_op VARCHAR",
            "_atlatl_synced_at TIMESTAMP WITH TIME ZONE",
            "_atlatl_lsn BIGINT",
        ]
    );
    assert_eq!(env.rows(T_ROWS), T_WANT);
    let slot = atlatl_conn_pgrepl::slot_name(&env.name);
    assert_eq!(
        env.pg_rows(&format!(
            "SELECT tablename::text FROM pg_publication_tables WHERE pubname = '{slot}' ORDER BY 1"
        ))
        .await,
        ["other", "t"]
    );
    let c1 = env.cursor("pg/t").expect("cursor");
    assert_eq!(r.confirmed.map(|l| l.to_string()), Some(c1.clone()));
    assert_eq!(env.cursor("pg/other"), Some(c1.clone()));

    env.each(&[
        "INSERT INTO t (id, t) VALUES (3, 'three'), (4, 'four')",
        "UPDATE t SET t = 'y', ts = '2025-01-01 00:00:00+00' WHERE id = 1",
        "UPDATE t SET id = 20 WHERE id = 2",
        "DELETE FROM t WHERE id = 4",
        "BEGIN; DELETE FROM t WHERE id = 3; INSERT INTO t (id, t) VALUES (3, 'again'); COMMIT",
        "INSERT INTO other VALUES ('b', 2)",
        "UPDATE other SET v = 10 WHERE k = 'a'",
        "BEGIN; INSERT INTO t (id) VALUES (99); ROLLBACK",
    ])
    .await;
    let r = env.run(spec.clone()).await;
    assert!(!r.applied.window.slot_created && r.applied.window.backfilled.is_empty());
    assert_eq!(r.applied.window.skipped_transactions, 0);
    assert_eq!(
        env.rows("SELECT id::VARCHAR || '|' || coalesce(t, '∅') || '|' || coalesce(ts::VARCHAR, '∅') || '|' || coalesce(n::VARCHAR, '∅') || '|' || _atlatl_op FROM lake.br.t ORDER BY id"),
        [
            "1|y|2025-01-01 00:00:00+00|12.345|update",
            "3|again|∅|∅|insert",
            "20|∅|∅|-0.500|update",
        ]
    );
    let c = r.applied.streams[0].counts;
    assert_eq!((c.inserted, c.updated, c.deleted), (2, 1, 1), "{c:?}");
    assert_eq!(
        env.rows("SELECT k || '=' || v::VARCHAR FROM lake.br.other ORDER BY k"),
        ["a=10", "b=2"]
    );
    let c2 = env.cursor("pg/t").expect("cursor");
    assert!(c2.parse::<atlatl_conn_pgrepl::Lsn>().expect("lsn") > c1.parse().expect("lsn"));
    assert!(env.confirmed_reaches(&c2).await);

    let r = env.run(spec).await;
    assert_eq!(r.applied.streams[0].counts, Default::default());
    assert_eq!(env.rows("SELECT count(*)::VARCHAR FROM lake.br.t"), ["3"]);
}
versions!(stream_and_apply);

/// The same rows as `stream_and_apply`, but decoded from the walsender's text output instead of backfilled.
async fn values_arrive_typed_through_pgoutput(v: u32) {
    let env = Env::new(v).await;
    env.exec(TYPES_DDL).await;
    let spec = env.spec(vec![stream("t")]);
    let r = env.run(spec.clone()).await;
    assert_eq!(r.applied.window.backfilled, vec!["pg/t"]);
    assert_eq!(r.applied.streams[0].counts.inserted, 0);

    env.exec(T_INSERT).await;
    let r = env.run(spec).await;
    assert!(r.applied.window.backfilled.is_empty());
    assert_eq!(r.applied.window.binary, v >= 14, "{:?}", r.applied.window);
    assert_eq!(r.applied.streams[0].counts.inserted, 2);
    assert_eq!(env.rows(T_ROWS), T_WANT);
}
versions!(values_arrive_typed_through_pgoutput);

async fn schema_change_adds_column(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE a (id int PRIMARY KEY, v text); INSERT INTO a VALUES (1, 'x'), (3, 'z');",
    )
    .await;
    let spec = env.spec(vec![stream("a")]);
    env.run(spec.clone()).await;

    env.each(&[
        "INSERT INTO a VALUES (4, 'w')",
        "ALTER TABLE a ADD COLUMN extra int",
        "INSERT INTO a VALUES (2, 'y', 99)",
        "UPDATE a SET extra = 7 WHERE id = 1",
    ])
    .await;
    env.run(spec).await;
    assert_eq!(
        env.rows(
            "SELECT column_name || ' ' || data_type FROM information_schema.columns
             WHERE table_catalog = 'lake' AND table_schema = 'br' AND table_name = 'a' AND column_name = 'extra'"
        ),
        ["extra INTEGER"]
    );
    assert_eq!(
        env.rows(
            "SELECT id::VARCHAR || '|' || v || '|' || coalesce(extra::VARCHAR, '∅') || '|' || _atlatl_op
             FROM lake.br.a ORDER BY id"
        ),
        ["1|x|7|update", "2|y|99|insert", "3|z|∅|insert", "4|w|∅|insert"]
    );
}
versions!(schema_change_adds_column);

async fn replica_identity_full_and_nothing(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE f (k text NOT NULL, v int);
         ALTER TABLE f REPLICA IDENTITY FULL;
         INSERT INTO f VALUES ('a', 1), ('b', 2), ('c', 3);",
    )
    .await;
    let mut s = stream("f");
    s.primary_key = Some(vec!["k".into()]);
    let spec = env.spec(vec![s]);
    env.run(spec.clone()).await;

    env.each(&[
        "UPDATE f SET v = 10 WHERE k = 'a'",
        "UPDATE f SET k = 'bb' WHERE k = 'b'",
        "DELETE FROM f WHERE k = 'c'",
    ])
    .await;
    env.run(spec.clone()).await;
    assert_eq!(
        env.rows("SELECT k || '=' || v::VARCHAR || ':' || _atlatl_op FROM lake.br.f ORDER BY k"),
        ["a=10:update", "bb=2:update"]
    );

    // Postgres refuses UPDATE/DELETE on a published table without an identity, so only an insert gets through.
    env.each(&[
        "ALTER TABLE f REPLICA IDENTITY NOTHING",
        "INSERT INTO f VALUES ('d', 4)",
    ])
    .await;
    let e = env
        .try_run(spec, crate::policy(true, false))
        .await
        .expect_err("NOTHING has no key");
    assert_eq!(e.class, atlatl_core::ErrorClass::Config, "{e}");
    assert!(e.to_string().contains("REPLICA IDENTITY NOTHING"), "{e}");
}
versions!(replica_identity_full_and_nothing);

async fn toast_unchanged_keeps_values(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE doc (id int PRIMARY KEY, title text, body text);
         ALTER TABLE doc ALTER COLUMN body SET STORAGE EXTERNAL;
         INSERT INTO doc SELECT i, 't' || i, repeat(md5(i::text), 300) FROM generate_series(1, 4) i;",
    )
    .await;
    let spec = env.spec(vec![stream("doc")]);
    env.run(spec.clone()).await;

    env.each(&[
        "UPDATE doc SET title = 'u1' WHERE id = 1",
        "UPDATE doc SET body = repeat('z', 5000) WHERE id = 2",
        "UPDATE doc SET title = 'u2' WHERE id = 2",
        "UPDATE doc SET id = 30 WHERE id = 3",
        "UPDATE doc SET title = 'u4' WHERE id = 4",
        "UPDATE doc SET id = 40 WHERE id = 4",
        "UPDATE doc SET title = 'u40' WHERE id = 40",
    ])
    .await;
    let r = env.run(spec.clone()).await;
    assert!(
        r.applied.window.unchanged_toast >= 5,
        "{:?}",
        r.applied.window
    );
    let want = env
        .pg_rows("SELECT id || '|' || title || '|' || md5(body) FROM doc ORDER BY id")
        .await;
    assert_eq!(
        env.rows(
            "SELECT id::VARCHAR || '|' || title || '|' || md5(body) FROM lake.br.doc ORDER BY id"
        ),
        want
    );
    assert_eq!(want.len(), 4);

    env.exec("UPDATE doc SET title = 'again' WHERE id = 30")
        .await;
    env.run(spec).await;
    assert_eq!(
        env.rows("SELECT length(body)::VARCHAR FROM lake.br.doc WHERE id = 30"),
        ["9600"]
    );
}
versions!(toast_unchanged_keeps_values);

async fn truncate_empties_the_lake_table(v: u32) {
    let env = Env::new(v).await;
    env.exec(
        "CREATE TABLE tr (id int PRIMARY KEY, v text);
         INSERT INTO tr SELECT i, 'v' || i FROM generate_series(1, 3) i;
         CREATE TABLE sd (id int PRIMARY KEY, v text);
         INSERT INTO sd SELECT i, 'v' || i FROM generate_series(1, 3) i;",
    )
    .await;
    let mut soft = stream("sd");
    soft.deletes = DeleteMode::Soft;
    let spec = env.spec(vec![stream("tr"), soft]);
    env.run(spec.clone()).await;

    env.each(&[
        "INSERT INTO tr VALUES (4, 'lost')",
        "TRUNCATE tr",
        "INSERT INTO tr VALUES (10, 'x')",
        "DELETE FROM sd WHERE id = 1",
    ])
    .await;
    let r = env.run(spec.clone()).await;
    assert!(r.applied.streams[0].truncated);
    assert_eq!(r.applied.streams[0].counts.deleted, 3);
    assert_eq!(
        env.rows("SELECT id::VARCHAR || v FROM lake.br.tr ORDER BY id"),
        ["10x"]
    );
    assert_eq!(
        env.rows("SELECT id::VARCHAR || ':' || _atlatl_op FROM lake.br.sd ORDER BY id"),
        ["1:delete", "2:insert", "3:insert"]
    );

    env.each(&["TRUNCATE sd", "INSERT INTO sd VALUES (2, 'back')"])
        .await;
    let r = env.run(spec).await;
    assert_eq!(r.applied.streams[1].counts.deleted, 2);
    assert_eq!(
        env.rows("SELECT id::VARCHAR || ':' || _atlatl_op || ':' || v FROM lake.br.sd ORDER BY id"),
        ["1:delete:v1", "2:insert:back", "3:delete:v3"]
    );
}
versions!(truncate_empties_the_lake_table);

async fn history_appends_changes(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE h (id int PRIMARY KEY, v text); INSERT INTO h VALUES (1, 'a');")
        .await;
    let mut s = stream("h");
    s.history = true;
    let spec = env.spec(vec![s]);
    env.run(spec.clone()).await;
    env.each(&[
        "UPDATE h SET v = 'b' WHERE id = 1",
        "INSERT INTO h VALUES (2, 'x')",
    ])
    .await;
    env.run(spec.clone()).await;
    env.each(&[
        "DELETE FROM h WHERE id = 2",
        "UPDATE h SET v = 'c' WHERE id = 1",
    ])
    .await;
    env.run(spec.clone()).await;
    env.exec("TRUNCATE h").await;
    env.run(spec).await;
    assert_eq!(
        env.rows(
            "SELECT id::VARCHAR || ':' || coalesce(v, '∅') || ':' || _atlatl_op FROM lake.br.h
             ORDER BY _atlatl_lsn, id"
        ),
        [
            "1:a:insert",
            "1:b:update",
            "2:x:insert",
            "2:∅:delete",
            "1:c:update",
            "1:∅:delete",
        ]
    );
}
versions!(history_appends_changes);

async fn limits_bound_the_window(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE l (id int PRIMARY KEY)").await;
    let mut spec = env.spec(vec![stream("l")]);
    env.run(spec.clone()).await;
    env.each(&[
        "INSERT INTO l VALUES (1)",
        "INSERT INTO l VALUES (2)",
        "INSERT INTO l VALUES (3)",
    ])
    .await;
    spec.limits.max_changes = Some(2);
    let r = env.run(spec.clone()).await;
    assert_eq!(r.applied.window.stop, StopReason::MaxChanges);
    assert_eq!(
        env.rows("SELECT id::VARCHAR FROM lake.br.l ORDER BY id"),
        ["1", "2"]
    );
    spec.limits.max_changes = None;
    let r = env.run(spec).await;
    assert_eq!(r.applied.window.stop, StopReason::CaughtUp);
    assert_eq!(r.applied.window.skipped_transactions, 0);
    assert_eq!(
        env.rows("SELECT id::VARCHAR FROM lake.br.l ORDER BY id"),
        ["1", "2", "3"]
    );
}
versions!(limits_bound_the_window);
