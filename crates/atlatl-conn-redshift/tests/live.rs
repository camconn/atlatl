//! Live Redshift over the wire (`--features network,redshift`, 12 §5). Connection settings come
//! from `REDSHIFT_{HOST,PORT,DATABASE,USER,PASSWORD}` (environment, else the workspace `.env`).
//! Tables are created in a fresh `atlatl_test_*` schema of the connected database and dropped
//! afterwards. `REDSHIFT_SAMPLE_TABLE` (`database.schema.table`, optional) names an existing
//! table, read cross-database; its name stays out of the repository.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use atlatl_conn_redshift::export::ExportRequest;
use atlatl_conn_redshift::{ExportOptions, Object, Options, RawTable, Redshift, Sink};
use atlatl_core::config::model::RedshiftConn;
use atlatl_core::error::{ErrorClass, Result};
use atlatl_core::retry::RetryPolicy;
use duckdb::Connection;
use duckdb::arrow::array::RecordBatch;
use serde_json::json;
use tokio_util::sync::CancellationToken;

fn env(k: &str) -> Option<String> {
    let file = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env"))
        .unwrap_or_default();
    std::env::var(k).ok().or_else(|| {
        file.lines()
            .find_map(|l| l.strip_prefix(&format!("{k}=")).map(str::to_owned))
    })
}

fn conn() -> RedshiftConn {
    let need = |k: &str| env(k).unwrap_or_else(|| panic!("{k} is not set"));
    serde_json::from_value(json!({
        "access": "wire",
        "wire": {
            "host": need("REDSHIFT_HOST"),
            "port": env("REDSHIFT_PORT").map_or(5439, |p| p.parse::<u16>().unwrap()),
            "database": need("REDSHIFT_DATABASE"),
            "user": need("REDSHIFT_USER"),
            "password": need("REDSHIFT_PASSWORD"),
            "sslmode": "require"
        }
    }))
    .unwrap()
}

fn redshift(batch_rows: usize) -> Redshift {
    Redshift::new(
        &conn(),
        Options {
            query_retry: RetryPolicy {
                max_attempts: 3,
                base: Duration::from_millis(200),
                max: Duration::from_secs(2),
                ..RetryPolicy::default()
            },
            // A terminated Serverless backend can leave the socket open and silent.
            export: ExportOptions {
                batch_rows,
                stall: Duration::from_secs(20),
            },
            statement_timeout: Duration::from_secs(600),
            ..Options::default()
        },
    )
    .unwrap()
}

struct DuckSink {
    c: Connection,
    raw: RawTable,
    appends: u64,
    resets: u64,
    /// Terminate our own export backend (found by `marker`) after these appends.
    terminate_at: Vec<u64>,
    cancel_at: Option<(u64, CancellationToken)>,
    marker: String,
    terminated: Vec<i64>,
}

impl DuckSink {
    fn new(columns: Vec<atlatl_conn_redshift::SourceColumn>) -> Self {
        let c = Connection::open_in_memory().unwrap();
        let raw = RawTable::new("stg_raw", columns);
        raw.create(&c).unwrap();
        Self {
            c,
            raw,
            appends: 0,
            resets: 0,
            terminate_at: vec![],
            cancel_at: None,
            marker: String::new(),
            terminated: vec![],
        }
    }

    fn one<T: duckdb::types::FromSql>(&self, sql: &str) -> T {
        self.c
            .query_row(sql, [], |r| r.get(0))
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }
}

impl Sink for DuckSink {
    async fn append(&mut self, batch: RecordBatch) -> Result<()> {
        self.raw.append(&self.c, batch)?;
        self.appends += 1;
        if self.terminate_at.contains(&self.appends) {
            // Only the session whose statement carries this test's marker.
            let rs = redshift(1);
            let c = CancellationToken::new();
            let found = rs
                .query(
                    &format!(
                        "SELECT session_id AS pid FROM sys_query_history WHERE query_text LIKE '%{}%' AND query_text NOT LIKE '%sys_query_history%' AND session_id <> pg_backend_pid() ORDER BY start_time DESC LIMIT 1",
                        self.marker
                    ),
                    "find",
                    &c,
                )
                .await?;
            let pid = found.int(0, "pid").expect("the export's session");
            rs.query(&format!("SELECT pg_terminate_backend({pid})"), "kill", &c)
                .await?;
            self.terminated.push(pid);
        }
        if let Some((n, t)) = &self.cancel_at
            && *n == self.appends
        {
            t.cancel();
        }
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        self.resets += 1;
        self.raw.reset(&self.c)
    }
}

/// Runs `body` with a fresh `atlatl_test_*` schema, dropping it afterwards even on failure.
async fn with_schema<F, Fut>(body: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let schema = atlatl_testkit::unique("atlatl_test");
    let rs = redshift(1);
    let c = CancellationToken::new();
    rs.execute(&format!("CREATE SCHEMA {schema}"), "create", &c)
        .await
        .unwrap();
    let r = tokio::spawn(body(schema.clone())).await;
    rs.execute(
        &format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        "drop",
        &c,
    )
    .await
    .unwrap();
    if let Err(e) = r {
        std::panic::resume_unwind(e.into_panic());
    }
}

async fn request(rs: &Redshift, object: &str, key: &[&str], filters: Vec<String>) -> ExportRequest {
    ExportRequest {
        object: Object::parse(object),
        columns: rs.columns(object, &CancellationToken::new()).await.unwrap(),
        filters,
        key: key.iter().map(|k| (*k).to_owned()).collect(),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(all(feature = "network", feature = "redshift")),
    ignore = "needs live Redshift (--features network,redshift)"
)]
async fn live_types_round_trip_through_the_text_protocol() {
    with_schema(|s| async move {
        let rs = redshift(1_000);
        let c = CancellationToken::new();
        rs.execute(
            &format!(
                "CREATE TABLE {s}.types (id bigint NOT NULL, i2 smallint, i4 integer, n numeric(18,4), big numeric(38,10),
                                         r real, f float8, b boolean, d date, ts timestamp, tz timestamptz, t time,
                                         c char(3), v varchar(64))"
            ),
            "q",
            &c,
        )
        .await
        .unwrap();
        rs.execute(
            &format!(
                "SET extra_float_digits TO 2;
                 INSERT INTO {s}.types VALUES
                   (1, -32768, 2147483647, 1.2345, 12345678901234567890.1234567890, 1.5, 1e-300, true, '2024-02-29',
                    '2024-01-31 12:34:56.123456', '2024-01-31 12:34:56.123456+05', '23:59:59', 'ab', 'it''s ünïcode'),
                   (2, NULL, NULL, NULL, NULL, NULL, 0.1, false, NULL, NULL, NULL, NULL, NULL, ''),
                   (3, NULL, NULL, -0.0001, NULL, NULL, 2.2250738585072014e-308, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
                 INSERT INTO {s}.types (id, f) SELECT 4, f / 3 FROM {s}.types WHERE id = 2"
            ),
            "q",
            &c,
        )
        .await
        .unwrap();
        let req = request(&rs, &format!("{s}.types"), &["id"], vec![]).await;
        assert_eq!(
            req.columns.iter().map(|c| c.duck_type.as_str()).collect::<Vec<_>>(),
            [
                "BIGINT", "SMALLINT", "INTEGER", "DECIMAL(18,4)", "DECIMAL(38,10)", "FLOAT", "DOUBLE", "BOOLEAN",
                "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "VARCHAR", "VARCHAR"
            ]
        );
        let mut sink = DuckSink::new(req.columns.clone());
        let out = rs.export(&req, &mut sink, &c).await.unwrap();
        assert_eq!(out.rows, 4);
        let row: Vec<String> = sink
            .c
            .query_row(
                "SELECT [i2::VARCHAR, i4::VARCHAR, n::VARCHAR, big::VARCHAR, r::VARCHAR, b::VARCHAR, d::VARCHAR,
                         ts::VARCHAR, (tz AT TIME ZONE 'UTC')::VARCHAR, t::VARCHAR, c, v] FROM temp.stg_raw WHERE id = 1",
                [],
                |r| r.get::<_, duckdb::types::Value>(0),
            )
            .map(|v| match v {
                duckdb::types::Value::List(l) => l
                    .into_iter()
                    .map(|x| match x {
                        duckdb::types::Value::Text(t) => t,
                        o => format!("{o:?}"),
                    })
                    .collect(),
                o => panic!("{o:?}"),
            })
            .unwrap();
        assert_eq!(
            row,
            [
                "-32768",
                "2147483647",
                "1.2345",
                "12345678901234567890.1234567890",
                "1.5",
                "true",
                "2024-02-29",
                "2024-01-31 12:34:56.123456",
                "2024-01-31 07:34:56.123456",
                "23:59:59",
                "ab ",
                "it's ünïcode"
            ]
        );
        // float8 at full precision: `extra_float_digits` is raised for the export.
        assert_eq!(sink.one::<f64>("SELECT f FROM temp.stg_raw WHERE id = 1"), 1e-300);
        assert_eq!(sink.one::<f64>("SELECT f FROM temp.stg_raw WHERE id = 2"), 0.1);
        assert_eq!(
            sink.one::<f64>("SELECT f FROM temp.stg_raw WHERE id = 3"),
            2.2250738585072014e-308
        );
        // Computed on the cluster, not parsed from a literal.
        assert_eq!(sink.one::<f64>("SELECT f FROM temp.stg_raw WHERE id = 4"), 0.1 / 3.0);
        assert_eq!(sink.one::<String>("SELECT v FROM temp.stg_raw WHERE id = 2"), "");
        assert_eq!(sink.one::<i64>("SELECT count(*) FROM temp.stg_raw WHERE v IS NULL"), 2);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(all(feature = "network", feature = "redshift")),
    ignore = "needs live Redshift (--features network,redshift)"
)]
async fn live_a_terminated_export_resumes_and_a_kill_cancels() {
    with_schema(|s| async move {
        let rs = redshift(2_000);
        let c = CancellationToken::new();
        rs.execute(
            &format!(
                "CREATE TABLE {s}.big (k bigint NOT NULL, j integer NOT NULL, pad varchar(128));
                 INSERT INTO {s}.big VALUES (0, 0, md5('0') || md5('0') || md5('0'))"
            ),
            "q",
            &c,
        )
        .await
        .unwrap();
        // 2^17 rows; `k / 3` repeats so key groups span batch edges.
        for _ in 0..17 {
            rs.execute(
                &format!(
                    "INSERT INTO {s}.big SELECT b.k + n.n, 0, md5((b.k + n.n)::varchar) || md5('x') || md5('y')
                     FROM {s}.big b, (SELECT count(*) AS n FROM {s}.big) n"
                ),
                "q",
                &c,
            )
            .await
            .unwrap();
        }
        rs.execute(&format!("UPDATE {s}.big SET j = k % 3, k = k / 3"), "q", &c)
            .await
            .unwrap();
        let total: i64 = rs
            .query(&format!("SELECT count(DISTINCT k::varchar || '/' || j::varchar) AS n FROM {s}.big"), "q", &c)
            .await
            .unwrap()
            .int(0, "n")
            .unwrap();
        assert_eq!(total, 1 << 17);

        let marker = atlatl_testkit::unique("m");
        let req = request(&rs, &format!("{s}.big"), &["k", "j"], vec![format!("'{marker}' = '{marker}'")]).await;
        let mut sink = DuckSink::new(req.columns.clone());
        sink.marker = marker.clone();
        sink.terminate_at = vec![3, 30];
        let out = rs.export(&req, &mut sink, &c).await.unwrap();
        assert_eq!(sink.terminated.len(), 2);
        // A backend terminated after it sent its last row doesn't interrupt the read.
        assert!(
            out.resumes >= 1 && out.resumes as usize <= sink.terminated.len(),
            "{:?}",
            out.retries
        );
        assert!(out.retries.iter().all(|a| a.class == ErrorClass::Transient));
        assert_eq!((out.restarts, sink.resets), (0, 0));
        assert_eq!(out.rows as i64, total);
        assert_eq!(sink.one::<i64>("SELECT count(*) FROM temp.stg_raw"), total);
        assert_eq!(
            sink.one::<i64>("SELECT count(*) FROM (SELECT DISTINCT k, j FROM temp.stg_raw)"),
            total
        );

        // A kill cancels the backend and isn't retried.
        let cancel = CancellationToken::new();
        let mut sink = DuckSink::new(req.columns.clone());
        sink.cancel_at = Some((2, cancel.clone()));
        let e = rs.export(&req, &mut sink, &cancel).await.unwrap_err();
        assert_eq!(e.class, ErrorClass::Killed, "{e}");
        assert!(e.message.contains("pg_cancel_backend"), "{e}");
        assert_eq!(sink.appends, 2);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(all(feature = "network", feature = "redshift")),
    ignore = "needs live Redshift (--features network,redshift)"
)]
async fn live_cross_database_sample_table() {
    let Some(table) = env("REDSHIFT_SAMPLE_TABLE") else {
        eprintln!("REDSHIFT_SAMPLE_TABLE not set; skipping");
        return;
    };
    let o = Object::parse(&table);
    assert!(
        o.database.is_some(),
        "REDSHIFT_SAMPLE_TABLE is database.schema.table"
    );
    let rs = redshift(20_000);
    let c = CancellationToken::new();
    assert_eq!(
        rs.discover(&table, &[], false, &c).await.unwrap(),
        std::slice::from_ref(&table)
    );
    let pattern = format!(
        "{}.{}.{}*",
        o.database.as_deref().unwrap(),
        o.schema,
        &o.table[..o.table.len() - 1]
    );
    assert!(
        rs.discover(&pattern, &[], true, &c)
            .await
            .unwrap()
            .contains(&table)
    );
    let req = request(&rs, &table, &[], vec![]).await;
    assert!(!req.columns.is_empty());
    let want: i64 = rs
        .query(
            &format!("SELECT count(*) AS n FROM {}", o.sql()),
            "count",
            &c,
        )
        .await
        .unwrap()
        .int(0, "n")
        .unwrap();
    let mut sink = DuckSink::new(req.columns.clone());
    let out = rs.export(&req, &mut sink, &c).await.unwrap();
    assert_eq!(out.rows as i64, want);
    assert_eq!(sink.one::<i64>("SELECT count(*) FROM temp.stg_raw"), want);
    eprintln!(
        "{} rows, {} columns, {:.1} MB of text",
        out.rows,
        req.columns.len(),
        out.bytes as f64 / 1e6
    );
}
