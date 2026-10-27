use atlatl_conn_postgres::source::{self, PgSource, Read};
use atlatl_conn_postgres::{ConnectOptions, connect};
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_testkit::Postgres;

use crate::fresh;

on_both!(
    discovery_is_sorted_and_skips_views_and_partitions,
    types_read_exactly,
    attach_is_read_only,
    errors_are_classified,
    primary_keys_come_from_the_constraint,
    exported_snapshots_are_shared_and_released,
    cursor_predicates_run_in_postgres,
);

async fn seed(o: &ConnectOptions, sql: &str) {
    connect(o)
        .await
        .expect("connect")
        .client()
        .batch_execute(sql)
        .await
        .expect("seed");
}

fn engine(alias: &str, o: &ConnectOptions) -> duckdb::Connection {
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.execute_batch(&source::attach_sql(alias, o))
        .expect("attach");
    c
}

fn engine_err(e: duckdb::Error) -> AtlatlError {
    source::reclassify(AtlatlError::internal(e.to_string()))
}

async fn discovery_is_sorted_and_skips_views_and_partitions(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE SCHEMA appdb; CREATE SCHEMA other;
           CREATE TABLE appdb.b (id int); CREATE TABLE appdb."B" (id int); CREATE TABLE appdb.a_b (id int);
           CREATE TABLE appdb._x (id int); CREATE TABLE appdb."aB" (id int); CREATE TABLE appdb.old_users (id int);
           CREATE TABLE appdb.parted (id int, k int) PARTITION BY RANGE (k);
           CREATE TABLE appdb.parted_1 PARTITION OF appdb.parted FOR VALUES FROM (0) TO (10);
           CREATE VIEW appdb.v AS SELECT 1 AS id; CREATE MATERIALIZED VIEW appdb.mv AS SELECT 1 AS id;
           CREATE TABLE other.a (id int); CREATE TABLE public.users_1 (id int);"#,
    )
    .await;
    let s = PgSource::new(o).expect("source");
    let got = s
        .discover("appdb.*", &["appdb.old_*".into()], false)
        .await
        .expect("discover");
    assert_eq!(
        got,
        [
            "appdb.B",
            "appdb._x",
            "appdb.aB",
            "appdb.a_b",
            "appdb.b",
            "appdb.parted"
        ]
    );
    let got = s.discover("appdb.*", &[], true).await.expect("views");
    assert!(
        got.contains(&"appdb.v".to_owned()) && got.contains(&"appdb.mv".to_owned()),
        "{got:?}"
    );
    assert_eq!(
        s.discover("*.a", &[], false).await.expect("any schema"),
        ["other.a"]
    );
    assert_eq!(
        s.discover("users_*", &[], false).await.expect("public"),
        ["public.users_1"]
    );
    assert!(
        s.discover("nope.*", &[], false)
            .await
            .expect("none")
            .is_empty()
    );
    s.close().await;
}

async fn types_read_exactly(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE TYPE mood AS ENUM ('happy', 'sad');
           CREATE TABLE t (id int PRIMARY KEY, m mood, tags text[], doc jsonb, n numeric(12,4),
             nu numeric, nbig numeric(50,10), ts timestamptz, "Mixed" text, skip_me bytea);
           INSERT INTO t VALUES
             (1, 'happy', ARRAY['a', 'b c', 'q"x', NULL], '{"z": 1, "a": [1, 2]}', 12345678.1234,
              1234567890123456789012345.123456789, 1234567890123456789012345678901234567890.1234567890,
              '2024-01-02 03:04:05.123456+05', 'x', '\xdead'),
             (2, NULL, '{}', '[]', NULL, NULL, NULL, NULL, NULL, NULL);
           CREATE TABLE plain (id int, m mood, ts timestamptz);
           INSERT INTO plain VALUES (1, 'sad', '2024-01-01T00:00:00Z');"#,
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = s
        .columns("public", "t", None, &["skip_me".into()])
        .await
        .expect("columns");
    let reads: Vec<(&str, Read)> = cols.iter().map(|c| (c.name.as_str(), c.read)).collect();
    assert_eq!(
        reads,
        [
            ("id", Read::Native),
            ("m", Read::Enum),
            ("tags", Read::ServerJson),
            ("doc", Read::Native),
            ("n", Read::Native),
            ("nu", Read::ServerText),
            ("nbig", Read::ServerText),
            ("ts", Read::Native),
            ("Mixed", Read::Native),
        ]
    );
    let only = s
        .columns("public", "t", Some(&["ID".into(), "ts".into()]), &[])
        .await
        .expect("include");
    assert_eq!(
        only.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["id", "ts"]
    );

    let c = engine("src", &o);
    let rel = source::relation("src", "public", "t", &cols).expect("rel");
    assert!(rel.contains("postgres_query"), "{rel}");
    let types: String = c
        .query_row(
            &format!(
                "SELECT concat_ws(',', typeof(id), typeof(m), typeof(tags), typeof(doc), typeof(n), typeof(nu), typeof(nbig), typeof(ts), typeof(\"Mixed\")) FROM {rel} LIMIT 1"
            ),
            [],
            |r| r.get(0),
        )
        .expect("types");
    assert_eq!(
        types,
        "INTEGER,VARCHAR,VARCHAR,VARCHAR,DECIMAL(12,4),VARCHAR,VARCHAR,TIMESTAMP WITH TIME ZONE,VARCHAR"
    );
    let row: (String, String, String, String, String, String, String) = c
        .query_row(
            &format!("SELECT m, tags, doc, n::VARCHAR, nu, nbig, strftime(ts AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') FROM {rel} WHERE id = 1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
        )
        .expect("read");
    assert_eq!(
        row,
        (
            "happy".into(),
            r#"["a","b c","q\"x",null]"#.into(),
            r#"{"a": [1, 2], "z": 1}"#.into(),
            "12345678.1234".into(),
            "1234567890123456789012345.123456789".into(),
            "1234567890123456789012345678901234567890.1234567890".into(),
            "2024-01-01 22:04:05.123456".into(),
        )
    );
    let empty: (String, Option<String>) = c
        .query_row(
            &format!("SELECT tags, m FROM {rel} WHERE id = 2"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("row 2");
    assert_eq!(empty, ("[]".into(), None));

    let pcols = s
        .columns("public", "plain", None, &[])
        .await
        .expect("plain");
    let prel = source::relation("src", "public", "plain", &pcols).expect("rel");
    assert!(!prel.contains("postgres_query"), "scans natively: {prel}");
    let (ty, m): (String, String) = c
        .query_row(
            &format!(
                "SELECT typeof(m), m FROM {prel} WHERE ts >= TIMESTAMPTZ '2024-01-01 00:00:00+00'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("plain read");
    assert_eq!((ty.as_str(), m.as_str()), ("VARCHAR", "sad"));
    let q: i64 = c
        .query_row(
            &format!(
                "SELECT count(*) FROM {}",
                source::query_relation("src", "SELECT id FROM t WHERE doc ? 'z'")
            ),
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(q, 1);
    c.execute_batch(&source::detach_sql("src")).expect("detach");
    s.close().await;
}

async fn attach_is_read_only(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE t (id int); INSERT INTO t VALUES (1); CREATE SEQUENCE s;",
    )
    .await;
    let c = engine("src", &o);
    let e = c
        .execute_batch("INSERT INTO src.public.t VALUES (2)")
        .expect_err("insert");
    assert!(e.to_string().contains("read-only"), "{e}");
    let e = c
        .execute_batch("CALL postgres_execute('src', 'DELETE FROM t')")
        .expect_err("execute");
    assert_eq!(engine_err(e).class, ErrorClass::Config);
    let e = c
        .query_row(
            "SELECT * FROM postgres_query('src', 'SELECT nextval(''s'')')",
            [],
            |r| r.get::<_, i64>(0),
        )
        .expect_err("writing function");
    assert_eq!(engine_err(e).code.as_deref(), Some("25006"));
    let n: i64 = connect(&o)
        .await
        .expect("c")
        .client()
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 1);
}

async fn errors_are_classified(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE secret (id int); CREATE TABLE t (id int);
         DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'atlatl_ro') THEN CREATE ROLE atlatl_ro LOGIN PASSWORD 'ro'; END IF; END $$;
         GRANT SELECT ON t TO atlatl_ro;",
    )
    .await;
    let mut bad = o.clone();
    bad.password = Some("wrong".into());
    let e = atlatl_testkit::lake::open_engine()
        .expect("engine")
        .execute_batch(&source::attach_sql("src", &bad))
        .expect_err("bad password");
    let e = engine_err(e);
    assert_eq!(e.class, ErrorClass::Auth, "{e}");
    assert!(!e.redacted_message().contains("wrong"), "{e}");
    let e = PgSource::new(bad)
        .expect("s")
        .columns("public", "t", None, &[])
        .await
        .expect_err("catalog auth");
    assert_eq!(e.class, ErrorClass::Auth, "{e}");

    let s = PgSource::new(o.clone()).expect("s");
    let e = s
        .columns("public", "missing", None, &[])
        .await
        .expect_err("missing");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Config, Some("42P01")),
        "{e}"
    );

    let mut ro = o.clone();
    ro.user = "atlatl_ro".into();
    ro.password = Some("ro".into());
    ro.statement_timeout = Some(std::time::Duration::from_millis(200));
    let c = engine("ro", &ro);
    let e = c
        .query_row("SELECT count(*) FROM ro.public.secret", [], |r| {
            r.get::<_, i64>(0)
        })
        .expect_err("denied");
    assert_eq!(engine_err(e).class, ErrorClass::Config);
    let e = c
        .query_row(
            "SELECT * FROM postgres_query('ro', 'SELECT pg_sleep(2)::text')",
            [],
            |r| r.get::<_, String>(0),
        )
        .expect_err("timeout");
    assert_eq!(engine_err(e).class, ErrorClass::Timeout);

    let mut down = o.clone();
    down.port = 1;
    let e = atlatl_testkit::lake::open_engine()
        .expect("engine")
        .execute_batch(&source::attach_sql("down", &down))
        .expect_err("refused");
    assert_eq!(engine_err(e).class, ErrorClass::Transient);
}

async fn primary_keys_come_from_the_constraint(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE SCHEMA appdb;
           CREATE TABLE appdb.one (id int PRIMARY KEY, v text);
           CREATE TABLE appdb.two (v text, b int, a text, PRIMARY KEY (a, b));
           CREATE TABLE appdb.keyless (id int UNIQUE, v text);
           CREATE TABLE appdb."Odd" ("Key Col" int PRIMARY KEY);
           CREATE TABLE appdb.parted (d date, id int, PRIMARY KEY (id, d)) PARTITION BY RANGE (d);
           CREATE TABLE appdb.parted_2024 PARTITION OF appdb.parted FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
           CREATE VIEW appdb.v AS SELECT * FROM appdb.one;"#,
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let got = |t: &'static str| {
        let s = &s;
        async move { s.primary_key("appdb", t).await.expect(t) }
    };
    let names = |v: &[&str]| Some(v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>());
    assert_eq!(got("one").await, names(&["id"]));
    assert_eq!(
        got("two").await,
        names(&["a", "b"]),
        "key order, not column order"
    );
    assert_eq!(
        got("keyless").await,
        None,
        "a unique constraint isn't the key"
    );
    assert_eq!(got("Odd").await, names(&["Key Col"]));
    assert_eq!(got("parted").await, names(&["id", "d"]));
    assert_eq!(got("v").await, None);
    let e = s.primary_key("appdb", "nope").await.expect_err("missing");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    s.close().await;
}

async fn exported_snapshots_are_shared_and_released(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        "CREATE TABLE a (v int); CREATE TABLE b (v int); INSERT INTO a VALUES (1); INSERT INTO b VALUES (1);",
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let id = s.export_snapshot().await.expect("export");
    assert_eq!(
        s.export_snapshot().await.expect("again"),
        id,
        "one per source"
    );
    seed(&o, "UPDATE a SET v = 2; UPDATE b SET v = 2;").await;
    let c = engine("src", &o);
    let read = |sql: &str| -> i32 { c.query_row(sql, [], |r| r.get(0)).expect(sql) };
    let in_snapshot = |sql: &str| -> i32 {
        c.execute_batch(&format!("BEGIN; {}", source::set_snapshot_sql("src", &id)))
            .expect("set snapshot");
        let v = read(sql);
        c.execute_batch("COMMIT").expect("commit");
        v
    };
    assert_eq!(
        read("SELECT v FROM src.public.a"),
        2,
        "autocommit: a fresh snapshot"
    );
    assert_eq!(in_snapshot("SELECT v FROM src.public.a"), 1);
    assert_eq!(
        in_snapshot("SELECT v::INTEGER FROM postgres_query('src', 'SELECT v FROM b')"),
        1,
        "postgres_query reads the same snapshot"
    );
    let holders: i64 = connect(&o)
        .await
        .expect("c")
        .client()
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction'",
            &[],
        )
        .await
        .expect("activity")
        .get(0);
    assert_eq!(holders, 1, "the exporting transaction stays open");
    s.release_snapshot().await;
    let e = c
        .execute_batch(&format!("BEGIN; {}", source::set_snapshot_sql("src", &id)))
        .expect_err("a released snapshot can't be imported");
    let _ = c.execute_batch("ROLLBACK");
    let e = engine_err(e);
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Transient, Some("snapshot_lost")),
        "{e}"
    );
    s.close().await;
}

async fn cursor_predicates_run_in_postgres(pg: &'static Postgres) {
    let o = fresh(pg);
    seed(
        &o,
        r#"CREATE TABLE t (id int PRIMARY KEY, at timestamptz, name text, tags text[]);
           INSERT INTO t SELECT i, '2024-01-01'::timestamptz + i * interval '1 day', 'n' || i, ARRAY['t' || i]
             FROM generate_series(1, 5) i;
           INSERT INTO t VALUES (6, NULL, 'Z', '{}');"#,
    )
    .await;
    let s = PgSource::new(o.clone()).expect("source");
    let cols = s.columns("public", "t", None, &[]).await.expect("cols");
    let c = engine("src", &o);
    let count = |cur: &source::Cursor| -> (i64, bool) {
        let pred = source::cursor_predicate(cur, &cols);
        let (rel, pushed) =
            source::relation_where("src", "public", "t", &cols, pred.as_deref()).expect("rel");
        let n = c
            .query_row(&format!("SELECT count(*) FROM {rel}"), [], |r| r.get(0))
            .expect("count");
        (n, pushed)
    };
    let cur = |col: &str, v: Option<&str>| source::Cursor {
        column: col.into(),
        stored: v.map(Into::into),
        lookback_s: None,
        inclusive: true,
        include_nulls: false,
    };
    assert_eq!(count(&cur("at", Some("2024-01-04 00:00:00+00"))), (3, true));
    assert_eq!(
        count(&source::Cursor {
            lookback_s: Some(86_400),
            include_nulls: true,
            inclusive: false,
            ..cur("at", Some("2024-01-04 00:00:00+00"))
        }),
        (4, true),
        "lookback and nulls"
    );
    assert_eq!(count(&cur("at", None)), (5, true));
    assert_eq!(
        count(&cur("name", Some("n3"))),
        (3, true),
        "byte-wise, as the engine compares: 'Z' < 'n3'"
    );
    assert_eq!(count(&cur("tags", Some("x"))), (6, false));
    s.close().await;
}
