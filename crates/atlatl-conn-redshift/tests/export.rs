//! Wire export (`access: wire` without `unload`) against Postgres 16 standing in for Redshift's
//! SQL layer: typed staging, cross-database names, and reads dropped mid-stream (backend
//! terminated, connection reset) resuming after the last key group or starting over.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use atlatl_conn_redshift::export::ExportRequest;
use atlatl_conn_redshift::{ExportOptions, Object, Options, RawTable, Redshift, Sink};
use atlatl_core::config::model::RedshiftConn;
use atlatl_core::error::{ErrorClass, Result};
use atlatl_core::retry::RetryPolicy;
use atlatl_testkit::Postgres;
use duckdb::Connection;
use duckdb::arrow::array::RecordBatch;
use serde_json::json;
use tokio_util::sync::CancellationToken;

const ROWS: i64 = 200_000;

/// What a sink does after its n-th append (1-based).
type Hook = Box<dyn FnMut(u64) -> Option<Fault> + Send>;

enum Fault {
    /// Close every proxied connection, then accept new ones again.
    Bounce,
    /// Stop forwarding on every open relay connection, without closing it.
    Freeze,
    Cancel(CancellationToken),
}

struct DuckSink {
    c: Connection,
    raw: RawTable,
    appends: u64,
    resets: u64,
    hook: Option<Hook>,
    proxy: Option<(String, String)>,
    relay: Option<Relay>,
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
            hook: None,
            proxy: None,
            relay: None,
        }
    }

    fn one<T: duckdb::types::FromSql>(&self, sql: &str) -> T {
        self.c.query_row(sql, [], |r| r.get(0)).unwrap()
    }
}

impl Sink for DuckSink {
    async fn append(&mut self, batch: RecordBatch) -> Result<()> {
        self.raw.append(&self.c, batch)?;
        self.appends += 1;
        let fault = self.hook.as_mut().and_then(|h| h(self.appends));
        match fault {
            Some(Fault::Bounce) => {
                let (api, name) = self.proxy.clone().unwrap();
                tokio::task::spawn_blocking(move || {
                    for on in ["false", "true"] {
                        toxiproxy_post(
                            &api,
                            &format!("/proxies/{name}"),
                            &format!(r#"{{"enabled":{on}}}"#),
                        );
                    }
                })
                .await
                .unwrap();
            }
            Some(Fault::Freeze) => self.relay.as_ref().unwrap().freeze(),
            Some(Fault::Cancel(t)) => t.cancel(),
            None => {}
        }
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        self.resets += 1;
        self.raw.reset(&self.c)
    }
}

/// A TCP relay whose open connections can be frozen: kept open, forwarding nothing, like a
/// terminated backend behind a proxy that never closes the client side.
#[derive(Clone)]
struct Relay {
    port: u16,
    /// Connections numbered below this forward nothing.
    frozen_below: std::sync::Arc<std::sync::atomic::AtomicU64>,
    next: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Relay {
    async fn start(upstream: String) -> Self {
        use std::sync::atomic::Ordering::SeqCst;
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let r = Relay {
            port: l.local_addr().unwrap().port(),
            frozen_below: Default::default(),
            next: Default::default(),
        };
        let r2 = r.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = l.accept().await else {
                    return;
                };
                let id = r2.next.fetch_add(1, SeqCst);
                let (frozen, up) = (r2.frozen_below.clone(), upstream.clone());
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(up).await else {
                        return;
                    };
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    let pump =
                        |mut from: tokio::net::tcp::OwnedReadHalf,
                         mut to: tokio::net::tcp::OwnedWriteHalf| {
                            let frozen = frozen.clone();
                            async move {
                                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                let mut buf = vec![0u8; 16 * 1024];
                                loop {
                                    if id < frozen.load(SeqCst) {
                                        std::future::pending::<()>().await;
                                    }
                                    match from.read(&mut buf).await {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => {
                                            if id < frozen.load(SeqCst) {
                                                std::future::pending::<()>().await;
                                            }
                                            if to.write_all(&buf[..n]).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        };
                    let _ = tokio::join!(pump(sr, cw), pump(cr, sw));
                });
            }
        });
        r
    }

    fn freeze(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        self.frozen_below.store(self.next.load(SeqCst), SeqCst);
    }
}

fn toxiproxy_post(api: &str, path: &str, body: &str) {
    toxiproxy_request(api, "POST", path, body);
}

fn toxiproxy_request(api: &str, method: &str, path: &str, body: &str) {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(api).unwrap();
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: {api}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut r = String::new();
    s.read_to_string(&mut r).unwrap();
    assert!(r.starts_with("HTTP/1.1 20"), "{r}");
}

fn opts(attempts: u32) -> Options {
    Options {
        query_retry: RetryPolicy {
            max_attempts: attempts,
            base: Duration::from_millis(5),
            max: Duration::from_millis(20),
            ..RetryPolicy::default()
        },
        export: ExportOptions {
            batch_rows: 1_000,
            stall: Duration::from_secs(2),
        },
        ..Options::default()
    }
}

struct Db {
    db: String,
    client: tokio_postgres::Client,
}

/// A fresh database with `svv_*` stand-ins and `src.wide` (`ROWS` rows; `k` repeats so key
/// groups span batches).
async fn seed(pg: &Postgres) -> Db {
    let db = pg.fresh_db().unwrap();
    let (client, conn) = tokio_postgres::connect(&pg.dsn(&db), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    client
        .batch_execute(&format!(
            "CREATE VIEW svv_columns AS SELECT * FROM information_schema.columns;
             CREATE VIEW svv_all_columns AS SELECT table_catalog AS database_name, table_schema AS schema_name, * FROM information_schema.columns;
             CREATE VIEW svv_tables AS SELECT * FROM information_schema.tables;
             CREATE VIEW svv_all_tables AS SELECT table_catalog AS database_name, table_schema AS schema_name, table_name,
                    CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS table_type FROM information_schema.tables;
             CREATE SCHEMA src;
             CREATE TABLE src.wide (k int NOT NULL, j int NOT NULL, n numeric(18,4), b boolean, d date, ts timestamp,
                                    tz timestamptz, f float8, s varchar(200));
             INSERT INTO src.wide
               SELECT g / 3, g % 3, g * 1.2345, g % 2 = 0, DATE '2024-01-01' + (g % 400),
                      TIMESTAMP '2024-01-31 12:34:56.123456' + g * INTERVAL '1 second',
                      TIMESTAMPTZ '2024-01-31 12:34:56.123456+05' + g * INTERVAL '1 second',
                      CASE WHEN g = 7 THEN 1e-300 ELSE g / 7.0 END,
                      CASE WHEN g % 10 = 0 THEN NULL ELSE repeat(md5(g::text), 3) END
               FROM generate_series(0, {}) g;
             ANALYZE src.wide;",
            ROWS - 1
        ))
        .await
        .unwrap();
    Db { db, client }
}

fn wire_conn(pg: &Postgres, db: &str) -> RedshiftConn {
    serde_json::from_value(json!({
        "access": "wire",
        "wire": {"host": pg.host, "port": pg.port, "database": db, "user": pg.user, "password": pg.password, "sslmode": "disable"}
    }))
    .unwrap()
}

async fn request(rs: &Redshift, object: &str, key: &[&str], filters: Vec<String>) -> ExportRequest {
    ExportRequest {
        object: Object::parse(object),
        columns: rs.columns(object, &CancellationToken::new()).await.unwrap(),
        filters,
        key: key.iter().map(|k| (*k).to_owned()).collect(),
    }
}

/// Every source row exactly once, with its values.
fn assert_complete(sink: &DuckSink, rows: i64) {
    assert_eq!(sink.one::<i64>("SELECT count(*) FROM temp.stg_raw"), rows);
    assert_eq!(
        sink.one::<i64>("SELECT count(*) FROM (SELECT DISTINCT k, j FROM temp.stg_raw)"),
        rows
    );
    assert_eq!(
        sink.one::<String>("SELECT sum(n)::VARCHAR FROM temp.stg_raw"),
        format!("{:.4}", (0..rows).map(|g| g as f64).sum::<f64>() * 1.2345)
    );
}

/// Row gates that stop the export's backend mid-result so it can be terminated with rows still
/// unproduced: the rows with `k = at[i]` take advisory lock `i + 1`, held by the side connection
/// until that gate opens. A kill elsewhere can land after the server has written its whole
/// result, which the client then reads to the end and never sees dropped.
struct Gates {
    stop: CancellationToken,
    task: tokio::task::JoinHandle<(tokio_postgres::Client, i64)>,
}

impl Gates {
    fn filter(at: &[i32]) -> String {
        let arms: String = at
            .iter()
            .enumerate()
            .map(|(i, k)| {
                format!(
                    " WHEN \"k\" = {k} THEN pg_advisory_xact_lock_shared({})::text = ''",
                    i + 1
                )
            })
            .collect();
        format!("CASE{arms} ELSE TRUE END")
    }

    /// Terminates each backend waiting at a gate (the export, by `marker`), opening the next gate
    /// after each kill when `open`.
    async fn start(side: tokio_postgres::Client, marker: &str, gates: usize, open: bool) -> Self {
        for i in 1..=gates as i64 {
            side.execute("SELECT pg_advisory_lock($1)", &[&i])
                .await
                .unwrap();
        }
        let stop = CancellationToken::new();
        let (s, marker) = (stop.clone(), format!("%{marker}%"));
        let task = tokio::spawn(async move {
            let mut kills = 0i64;
            while !s.is_cancelled() {
                let waiting = side
                    .query_opt(
                        "SELECT pid FROM pg_stat_activity WHERE query LIKE $1 AND wait_event_type = 'Lock' AND wait_event = 'advisory' AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
                        &[&marker],
                    )
                    .await
                    .unwrap();
                let Some(pid) = waiting.map(|r| r.get::<_, i32>(0)) else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                let gone: bool = side
                    .query_one("SELECT pg_terminate_backend($1, 10000)", &[&pid])
                    .await
                    .unwrap()
                    .get(0);
                assert!(gone, "backend {pid} still running");
                kills += 1;
                if open {
                    side.execute("SELECT pg_advisory_unlock($1)", &[&kills])
                        .await
                        .unwrap();
                }
            }
            (side, kills)
        });
        Self { stop, task }
    }

    async fn kills(self) -> (tokio_postgres::Client, i64) {
        self.stop.cancel();
        self.task.await.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn exports_typed_rows_by_key_with_filters_pushed_down() {
    let pg = atlatl_testkit::pg16();
    let d = seed(pg).await;
    let rs = Redshift::new(&wire_conn(pg, &d.db), opts(1)).unwrap();
    assert!(!rs.unloads());
    // Cross-database form: the catalog comes from `svv_all_columns`.
    let object = format!("{}.src.wide", d.db);
    let req = request(&rs, &object, &["k", "j"], vec!["\"k\" < 1000".into()]).await;
    assert_eq!(
        req.columns
            .iter()
            .map(|c| c.duck_type.as_str())
            .collect::<Vec<_>>(),
        [
            "INTEGER",
            "INTEGER",
            "DECIMAL(18,4)",
            "BOOLEAN",
            "DATE",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "DOUBLE",
            "VARCHAR"
        ]
    );
    let mut sink = DuckSink::new(req.columns.clone());
    let out = rs
        .export(&req, &mut sink, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(out.rows, 3_000);
    assert_eq!((out.resumes, out.restarts), (0, 0));
    assert!(out.bytes > 0);
    // Batch edges are moved back to key boundaries.
    assert!((3..=4).contains(&sink.appends), "{}", sink.appends);
    let types: Vec<(String, String)> = {
        let mut st = sink
            .c
            .prepare("SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = 'stg_raw' ORDER BY column_index")
            .unwrap();
        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(types[2], ("n".into(), "DECIMAL(18,4)".into()));
    assert_eq!(types[6], ("tz".into(), "TIMESTAMP WITH TIME ZONE".into()));
    let row: (String, bool, String, String, String) = sink
        .c
        .query_row(
            "SELECT n::VARCHAR, b, d::VARCHAR, ts::VARCHAR, (tz AT TIME ZONE 'UTC')::VARCHAR FROM temp.stg_raw WHERE k = 2 AND j = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (
            "8.6415".into(),
            false,
            "2024-01-08".into(),
            "2024-01-31 12:35:03.123456".into(),
            "2024-01-31 07:35:03.123456".into()
        )
    );
    // float8 round-trips at full precision (`extra_float_digits`), NULLs stay NULL.
    assert_eq!(
        sink.one::<f64>("SELECT f FROM temp.stg_raw WHERE k = 2 AND j = 1"),
        1e-300
    );
    assert_eq!(
        sink.one::<f64>("SELECT f FROM temp.stg_raw WHERE k = 3 AND j = 0"),
        9.0 / 7.0
    );
    assert_eq!(
        sink.one::<i64>("SELECT count(*) FROM temp.stg_raw WHERE s IS NULL"),
        300
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_terminated_backend_resumes_after_the_last_key_group() {
    let pg = atlatl_testkit::pg16();
    let d = seed(pg).await;
    // Rows stream in key order, so each gate is reached after the groups before it are out.
    d.client
        .batch_execute(&format!(
            "CREATE INDEX ON src.wide (k); ALTER DATABASE {} SET enable_sort = off",
            d.db
        ))
        .await
        .unwrap();
    let rs = Redshift::new(&wire_conn(pg, &d.db), opts(3)).unwrap();
    let marker = atlatl_testkit::unique("m");
    // Key `k` alone repeats 3× per value: groups are held back at batch edges.
    let req = request(
        &rs,
        "src.wide",
        &["k"],
        vec![
            format!("'{marker}' = '{marker}'"),
            Gates::filter(&[20_000, 40_000]),
        ],
    )
    .await;
    let gates = Gates::start(d.client, &marker, 2, true).await;
    let mut sink = DuckSink::new(req.columns.clone());
    let out = rs
        .export(&req, &mut sink, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(gates.kills().await.1, 2);
    assert_eq!(out.resumes, 2, "{:?}", out.retries);
    // Both reads made progress first: each resume continued rather than starting over.
    assert!(
        out.retries.iter().all(|r| r.attempt == 1),
        "{:?}",
        out.retries
    );
    assert_eq!(out.restarts, 0);
    assert_eq!(sink.resets, 0);
    assert_eq!(out.rows as i64, ROWS);
    assert!(
        out.retries
            .iter()
            .all(|r| r.class == ErrorClass::Transient
                && r.message.contains("terminating connection")),
        "{:?}",
        out.retries
    );
    assert_complete(&sink, ROWS);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_connection_resumes_through_a_proxy() {
    let pg = atlatl_testkit::pg16();
    let d = seed(pg).await;
    let tp = atlatl_testkit::toxiproxy();
    let proxy = tp.postgres(pg).unwrap();
    let via = pg.via(&proxy.host, proxy.port);
    let rs = Redshift::new(&wire_conn(&via, &d.db), opts(3)).unwrap();
    let req = request(&rs, "src.wide", &["k", "j"], vec![]).await;
    let mut sink = DuckSink::new(req.columns.clone());
    sink.proxy = Some((tp.api.clone(), proxy.name.clone()));
    sink.hook = Some(Box::new(|n| (n == 5).then_some(Fault::Bounce)));
    let out = rs
        .export(&req, &mut sink, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(out.resumes, 1, "{:?}", out.retries);
    assert_eq!(out.rows as i64, ROWS);
    assert_complete(&sink, ROWS);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_silent_connection_is_treated_as_dropped_and_resumed() {
    let pg = atlatl_testkit::pg16();
    let d = seed(pg).await;
    let relay = Relay::start(format!("{}:{}", pg.host, pg.port)).await;
    let via = pg.via("127.0.0.1", relay.port);
    let rs = Redshift::new(&wire_conn(&via, &d.db), opts(3)).unwrap();
    let req = request(&rs, "src.wide", &["k", "j"], vec![]).await;
    let mut sink = DuckSink::new(req.columns.clone());
    sink.relay = Some(relay);
    sink.hook = Some(Box::new(|n| (n == 5).then_some(Fault::Freeze)));
    let t0 = std::time::Instant::now();
    let out = rs
        .export(&req, &mut sink, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(out.resumes, 1, "{:?}", out.retries);
    assert!(
        out.retries[0].message.contains("no data for 2"),
        "{:?}",
        out.retries
    );
    assert!(t0.elapsed() < Duration::from_secs(60));
    assert_complete(&sink, ROWS);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyless_read_starts_over_and_gives_up_after_its_attempts() {
    let pg = atlatl_testkit::pg16();
    let d = seed(pg).await;
    let rs = Redshift::new(&wire_conn(pg, &d.db), opts(2)).unwrap();
    let marker = atlatl_testkit::unique("m");
    let req = request(
        &rs,
        "src.wide",
        &[],
        vec![format!("'{marker}' = '{marker}'"), Gates::filter(&[20_000])],
    )
    .await;
    let gates = Gates::start(d.client, &marker, 1, true).await;
    let mut sink = DuckSink::new(req.columns.clone());
    let out = rs
        .export(&req, &mut sink, &CancellationToken::new())
        .await
        .unwrap();
    let (side, kills) = gates.kills().await;
    assert_eq!(kills, 1);
    assert_eq!((out.resumes, out.restarts, sink.resets), (0, 1, 1));
    assert_eq!(out.rows as i64, ROWS);
    assert_complete(&sink, ROWS);

    // Every attempt dropped: the last error, after `max_attempts`.
    let gates = Gates::start(side, &marker, 1, false).await;
    let mut sink2 = DuckSink::new(req.columns.clone());
    let e = rs
        .export(&req, &mut sink2, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(gates.kills().await.1, 2);
    assert_eq!(e.class, ErrorClass::Transient, "{e}");
    assert!(e.message.contains("after 2 attempts"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_cancels_the_backend_and_is_not_retried() {
    let pg = atlatl_testkit::pg16();
    let d = seed(pg).await;
    let rs = Redshift::new(&wire_conn(pg, &d.db), opts(3)).unwrap();
    let marker = atlatl_testkit::unique("m");
    let req = request(
        &rs,
        "src.wide",
        &["k", "j"],
        vec![format!("'{marker}' = '{marker}'")],
    )
    .await;
    let cancel = CancellationToken::new();
    let mut sink = DuckSink::new(req.columns.clone());
    let c2 = cancel.clone();
    sink.hook = Some(Box::new(move |n| {
        (n == 2).then(|| Fault::Cancel(c2.clone()))
    }));
    let e = rs.export(&req, &mut sink, &cancel).await.unwrap_err();
    assert_eq!(e.class, ErrorClass::Killed, "{e}");
    assert!(e.message.contains("pg_cancel_backend: true"), "{e}");
    assert_eq!(sink.appends, 2);
    let mut active = -1i64;
    for _ in 0..50 {
        active = d
            .client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE query LIKE $1 AND state = 'active' AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
                &[&format!("%{marker}%")],
            )
            .await
            .unwrap()
            .get(0);
        if active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(active, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn data_api_access_needs_unload() {
    let cfg: RedshiftConn = serde_json::from_value(json!({
        "access": "data_api",
        "data_api": {"workgroup": "wg", "database": "db", "region": "us-east-1", "key_id": "k", "secret": "s"}
    }))
    .unwrap();
    let rs = Redshift::new(&cfg, opts(1)).unwrap();
    let req = ExportRequest {
        object: Object::parse("s.t"),
        columns: vec![],
        filters: vec![],
        key: vec![],
    };
    let mut sink = DuckSink {
        c: Connection::open_in_memory().unwrap(),
        raw: RawTable::new("x", vec![]),
        appends: 0,
        resets: 0,
        hook: None,
        proxy: None,
        relay: None,
    };
    let e = rs
        .export(&req, &mut sink, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Config);
}
