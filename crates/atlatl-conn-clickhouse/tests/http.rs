use std::time::Duration;

use atlatl_conn_clickhouse::{ClickHouse, Endpoint, Options};
use atlatl_core::ErrorClass;
use atlatl_core::retry::RetryPolicy;
use tokio_util::sync::CancellationToken;

fn once() -> Options {
    Options {
        retry: RetryPolicy {
            max_attempts: 1,
            ..RetryPolicy::default()
        },
        connect_timeout: Duration::from_secs(5),
        ..Options::default()
    }
}

fn client(db: &str, password: Option<&str>) -> ClickHouse {
    let ch = atlatl_testkit::clickhouse();
    ClickHouse::with_endpoint(
        Endpoint {
            base: ch.url.clone(),
            user: ch.user.clone(),
            password: password.unwrap_or(&ch.password).to_owned(),
            database: db.to_owned(),
        },
        once(),
    )
    .expect("client")
}

fn seeded() -> String {
    let ch = atlatl_testkit::clickhouse();
    let db = ch.fresh_db().expect("db");
    ch.exec(&format!(
        "CREATE TABLE {db}.eod (series_id UInt32, timestamp DateTime('UTC'), close Float64, \
         name LowCardinality(String), n Nullable(Int64)) ENGINE = MergeTree ORDER BY series_id"
    ))
    .expect("table");
    ch.exec(&format!(
        "INSERT INTO {db}.eod VALUES (1, '2026-10-01 20:00:00', 1.5, 'a', NULL), \
         (2, '2026-10-01 20:00:00', 2.5, 'b''c', 7)"
    ))
    .expect("rows");
    db
}

fn read(path: &std::path::Path) -> (Vec<(String, String)>, Vec<String>) {
    let c = duckdb::Connection::open_in_memory().expect("duckdb");
    let rel = format!("read_parquet('{}')", path.display());
    let cols = c
        .prepare(&format!("DESCRIBE SELECT * FROM {rel}"))
        .expect("clickhouse")
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("clickhouse")
        .collect::<Result<_, _>>()
        .expect("clickhouse");
    let rows = c
        .prepare(&format!(
            "SELECT concat_ws('|', series_id, strftime(timestamp AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M'), close, name, coalesce(n::VARCHAR, '-')) FROM {rel} ORDER BY series_id"
        ))
        .expect("clickhouse")
        .query_map([], |r| r.get(0))
        .expect("clickhouse")
        .collect::<Result<_, _>>()
        .expect("clickhouse");
    (cols, rows)
}

#[tokio::test]
async fn query_lands_a_parquet_file() {
    let db = seeded();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.parquet");
    let got = client(&db, None)
        .query_to_file("SELECT * FROM eod", &path, &CancellationToken::new())
        .await
        .expect("query");
    assert!(got.bytes > 0);
    let (cols, rows) = read(&path);
    let types: Vec<&str> = cols.iter().map(|(_, t)| t.as_str()).collect();
    assert_eq!(
        types,
        [
            "UINTEGER",
            "TIMESTAMP WITH TIME ZONE",
            "DOUBLE",
            "VARCHAR",
            "BIGINT"
        ]
    );
    assert_eq!(
        rows,
        ["1|2026-10-01 20:00|1.5|a|-", "2|2026-10-01 20:00|2.5|b'c|7"]
    );
}

#[tokio::test]
async fn an_empty_result_keeps_its_schema() {
    let db = seeded();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.parquet");
    client(&db, None)
        .query_to_file(
            "SELECT * FROM eod WHERE series_id IN (NULL)",
            &path,
            &CancellationToken::new(),
        )
        .await
        .expect("query");
    let (cols, rows) = read(&path);
    assert_eq!(cols.len(), 5);
    assert!(rows.is_empty());
}

#[tokio::test]
async fn writes_are_refused() {
    let db = seeded();
    let dir = tempfile::tempdir().expect("tempdir");
    let e = client(&db, None)
        .query_to_file(
            "INSERT INTO eod VALUES (3, now(), 1, 'x', NULL)",
            &dir.path().join("x"),
            &CancellationToken::new(),
        )
        .await
        .expect_err("readonly");
    assert_eq!(e.class, ErrorClass::Config, "{e:?}");
    assert_eq!(e.code.as_deref(), Some("clickhouse_164"));
    let n = atlatl_testkit::clickhouse()
        .exec(&format!("SELECT count() FROM {db}.eod"))
        .expect("count");
    assert_eq!(n, "2");
}

#[tokio::test]
async fn errors_are_classified() {
    let db = seeded();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("x");
    let cancel = CancellationToken::new();
    let e = client(&db, None)
        .query_to_file("SELEC 1", &path, &cancel)
        .await
        .expect_err("syntax");
    assert_eq!(e.class, ErrorClass::Config, "{e:?}");
    let e = client(&db, None)
        .query_to_file("SELECT * FROM missing", &path, &cancel)
        .await
        .expect_err("table");
    assert_eq!(e.class, ErrorClass::Config, "{e:?}");
    let e = client(&db, Some("wrong"))
        .query_to_file("SELECT 1", &path, &cancel)
        .await
        .expect_err("auth");
    assert_eq!(e.class, ErrorClass::Auth, "{e:?}");
    assert!(!e.message.contains("wrong"), "{}", e.message);
    let e = client(&db, None)
        .query_to_file("SELECT 1 FORMAT JSON", &path, &cancel)
        .await
        .expect_err("not parquet");
    assert_eq!(e.class, ErrorClass::Decode, "{e:?}");
}

#[tokio::test]
async fn an_unreachable_server_is_transient() {
    let c = ClickHouse::with_endpoint(
        Endpoint {
            base: "http://127.0.0.1:1".into(),
            user: "u".into(),
            password: "p".into(),
            database: "default".into(),
        },
        once(),
    )
    .expect("client");
    let dir = tempfile::tempdir().expect("tempdir");
    let e = c
        .query_to_file("SELECT 1", &dir.path().join("x"), &CancellationToken::new())
        .await
        .expect_err("refused");
    assert_eq!(e.class, ErrorClass::Transient, "{e:?}");
}
