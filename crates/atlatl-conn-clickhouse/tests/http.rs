use std::time::Duration;

use atlatl_conn_clickhouse::{ClickHouse, Endpoint, Options, types};
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

/// What each type lands as through Parquet, before `types::check`: what it lets through must land
/// as its DuckDB counterpart, and the `update_key` watermark is rendered from these.
#[tokio::test]
async fn types_land_as() {
    let db = atlatl_testkit::clickhouse().fresh_db().expect("db");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("t.parquet");
    let cases = [
        ("toInt8(-1)", "TINYINT"),
        ("toInt16(-1)", "SMALLINT"),
        ("toInt32(-1)", "INTEGER"),
        ("toInt64(-1)", "BIGINT"),
        ("toInt128(-1)", "BLOB"),
        ("toInt256(-1)", "BLOB"),
        ("toUInt8(1)", "UTINYINT"),
        ("toUInt16(1)", "USMALLINT"),
        ("toUInt32(1)", "UINTEGER"),
        ("toUInt64(1)", "UBIGINT"),
        ("toUInt128(1)", "BLOB"),
        ("toUInt256(1)", "BLOB"),
        ("toFloat32(1.5)", "FLOAT"),
        ("toFloat64(1.5)", "DOUBLE"),
        ("toDecimal32(1.5, 2)", "DECIMAL(9,2)"),
        ("toDecimal64(1.5, 4)", "DECIMAL(18,4)"),
        ("toDecimal128(1.5, 10)", "DECIMAL(38,10)"),
        ("toDecimal256(1.5, 20)", "DOUBLE"),
        ("true", "BOOLEAN"),
        ("toDate('2026-10-01')", "DATE"),
        ("toDate32('1900-10-01')", "DATE"),
        (
            "toDateTime('2026-10-01 20:00:00')",
            "TIMESTAMP WITH TIME ZONE",
        ),
        (
            "toDateTime64('2026-10-01 20:00:00.123456789', 9, 'UTC')",
            "TIMESTAMP WITH TIME ZONE",
        ),
        ("'x'", "VARCHAR"),
        ("toLowCardinality('x')", "VARCHAR"),
        ("toUUID('00000000-0000-0000-0000-000000000001')", "UUID"),
        ("CAST('{\"a\": 1}' AS JSON)", "JSON"),
        ("[toInt64(1), 2]", "BIGINT[]"),
        ("map('k', toInt64(1))", "MAP(VARCHAR, BIGINT)"),
        (
            "CAST((1, 'a') AS Tuple(x UInt8, y String))",
            "STRUCT(x UTINYINT, y VARCHAR)",
        ),
        (
            "tuple(toInt8(1), 'a')",
            "STRUCT(\"1\" TINYINT, \"2\" VARCHAR)",
        ),
    ];
    let sel: Vec<String> = cases
        .iter()
        .enumerate()
        .map(|(i, (e, _))| format!("{e} AS c{i}"))
        .collect();
    client(&db, None)
        .query_to_file(
            &format!("SELECT {}", sel.join(", ")),
            &path,
            &CancellationToken::new(),
        )
        .await
        .expect("query");
    let cols = read_types(&path);
    let got: Vec<(&str, &str)> = cases
        .iter()
        .zip(&cols)
        .map(|((e, _), t)| (*e, t.as_str()))
        .collect();
    let want: Vec<(&str, &str)> = cases.to_vec();
    assert_eq!(got, want);
    // No Parquet form at all, so never a staged column.
    for t in [
        "toBFloat16(1.5)",
        "CAST('20:00:00' AS Time)",
        "CAST('20:00:00' AS Time64(3))",
    ] {
        let e = client(&db, None)
            .query_to_file(
                &format!("SELECT {t} AS x SETTINGS enable_time_time64_type = 1"),
                &path,
                &CancellationToken::new(),
            )
            .await
            .expect_err(t);
        assert!(e.message.contains("Parquet"), "{t}: {}", e.message);
    }
}

fn read_types(path: &std::path::Path) -> Vec<String> {
    let c = duckdb::Connection::open_in_memory().expect("duckdb");
    c.prepare(&format!(
        "SELECT column_type FROM (DESCRIBE SELECT * FROM read_parquet('{}'))",
        path.display()
    ))
    .expect("describe")
    .query_map([], |r| r.get(0))
    .expect("describe")
    .collect::<Result<_, _>>()
    .expect("describe")
}

#[tokio::test]
async fn describe_lists_columns_without_running() {
    let db = seeded();
    let cols = client(&db, None)
        .describe(
            "SELECT series_id, toInt128(n) AS big FROM eod WHERE throwIf(1) -- trailing\n;",
            &CancellationToken::new(),
        )
        .await
        .expect("describe")
        .columns;
    assert_eq!(
        cols,
        [
            ("series_id".to_owned(), "UInt32".to_owned()),
            ("big".to_owned(), "Nullable(Int128)".to_owned())
        ]
    );
}

/// The type gate on `DESCRIBE`'s own spelling of each type.
#[tokio::test]
async fn the_type_gate_reads_describe() {
    let db = atlatl_testkit::clickhouse().fresh_db().expect("db");
    let c = client(&db, None);
    let landed = [
        "toInt64(1)",
        "toInt128(1)",
        "toNullable(toUInt128(1))",
        "toDecimal256('1.5', 2)",
        "toDateTime64('2026-10-01 20:00:00.123456', 6, 'UTC')",
        "toDate32('1900-01-01')",
        "toUUID('00000000-0000-0000-0000-000000000001')",
        "toLowCardinality('x')",
        "map('k', [1, 2])",
        "CAST((1, 'a') AS Tuple(IPv4 UInt8, `Enum8` String))",
        "CAST('{\"a\": 1}' AS JSON)",
    ];
    let refused = [
        ("toInt256(1)", "no 256-bit integer"),
        ("toUInt256(1)", "no 256-bit integer"),
        (
            "toDateTime64('2026-10-01 20:00:00.1234567', 7, 'UTC')",
            "to the microsecond",
        ),
        ("toIPv4('1.2.3.4')", "no IP address type"),
        ("toIPv6('::1')", "no IP address type"),
        ("CAST('a' AS Enum8('a' = 1, 'b,)' = 2))", "no enum type"),
        ("toFixedString('abc', 3)", "no fixed-length string"),
        ("[toInt128(1)]", "inside an Array"),
        (
            "CAST((1, '::1') AS Tuple(a UInt8, b IPv6))",
            "no IP address type",
        ),
        ("uniqState(toUInt64(1))", "doesn't land this type"),
        ("(1.0, 2.0)::Point", "doesn't land this type"),
        ("NULL", "an untyped `NULL`"),
        ("[]", "empty array"),
    ];
    for e in landed {
        let cols = c
            .describe(&format!("SELECT {e} AS x"), &CancellationToken::new())
            .await
            .expect(e)
            .columns;
        types::check(&cols).unwrap_or_else(|err| panic!("{e}: {err:?}"));
    }
    for (e, want) in refused {
        let cols = c
            .describe(&format!("SELECT {e} AS x"), &CancellationToken::new())
            .await
            .expect(e)
            .columns;
        let err = types::check(&cols).expect_err(e);
        assert!(err[0].contains(want), "{e}: {err:?}");
    }
}

/// Float32 values whose shortest text the default parser rounds to a neighbour: the watermark
/// `CAST('…' AS Float32)` must land on the stored value.
#[tokio::test]
async fn float32_text_parses_to_the_same_value() {
    let db = atlatl_testkit::clickhouse().fresh_db().expect("db");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("t.parquet");
    let cases = [
        (4151590082u32, "-4.9537217e+33"),
        (5056982, "7.086341e-39"),
        (3868883, "5.42146e-39"),
        (5084783, "7.125299e-39"),
        (106025646, "3.9463314e-35"),
    ];
    let exprs: Vec<String> = cases
        .iter()
        .map(|(bits, text)| {
            assert_eq!(
                f32::from_bits(*bits).to_string().parse::<f32>().ok(),
                text.parse::<f32>().ok()
            );
            format!("reinterpretAsFloat32(toUInt32({bits})) = CAST('{text}' AS Float32)")
        })
        .collect();
    client(&db, None)
        .query_to_file(
            &format!("SELECT arrayAll(x -> x, [{}]) AS ok", exprs.join(", ")),
            &path,
            &CancellationToken::new(),
        )
        .await
        .expect("query");
    let c = duckdb::Connection::open_in_memory().expect("duckdb");
    let ok: bool = c
        .query_row(
            &format!("SELECT ok::BOOLEAN FROM read_parquet('{}')", path.display()),
            [],
            |r| r.get(0),
        )
        .expect("read");
    assert!(ok);
}
