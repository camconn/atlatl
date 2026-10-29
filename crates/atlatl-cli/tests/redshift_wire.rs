//! Redshift over the wire without `unload` (06 §4): members are streamed SELECTs, ordered by
//! key so dropped reads resume. End to end through the `atlatl` binary: a daily PK merge and an
//! hourly `update_key` + `window` delta writing the same tables, the hourly cursor adopted from a
//! target loaded by the previous loader (`state init --from-target`), unchanged reruns, and a
//! connection that keeps dropping. Runs on Postgres 16 standing in for Redshift's SQL layer, and
//! against live Redshift with `--features network,redshift` (12 §5). Every name is synthetic.

use std::process::Output;

use atlatl_conn_postgres::{ConnectOptions, Session, connect_with};
use atlatl_core::config::model::SslMode;
use atlatl_testkit::LocalLake;
use serde_json::Value;

const PK: &str = "[sec_id, fp_end, metric]";

/// Where the source tables live, and how to reach it.
struct Source {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    sslmode: &'static str,
    /// Created (and dropped) by the test.
    schema: String,
    session: Session,
}

impl Source {
    async fn connect(
        (host, port, database, user, password): (&str, u16, &str, &str, &str),
        sslmode: &'static str,
        schema: String,
    ) -> Self {
        let mut o = ConnectOptions::new(host, port, database, user, Some(password.to_owned()));
        o.sslmode = if sslmode == "disable" {
            SslMode::Disable
        } else {
            SslMode::Require
        };
        let tls = o.tls().expect("tls");
        let session = connect_with(&o, tls.as_ref()).await.expect("connect");
        Self {
            host: host.into(),
            port,
            database: database.into(),
            user: user.into(),
            password: password.into(),
            sslmode,
            schema,
            session,
        }
    }

    async fn pg16() -> Self {
        let pg = atlatl_testkit::pg16();
        let db = pg.fresh_db().expect("db");
        let s = Self::connect(
            (&pg.host, pg.port, &db, &pg.user, &pg.password),
            "disable",
            "src".into(),
        )
        .await;
        s.sql(
            "CREATE VIEW svv_columns AS SELECT * FROM information_schema.columns;
             CREATE VIEW svv_tables AS SELECT * FROM information_schema.tables;",
        )
        .await;
        s
    }

    /// One statement at a time: Redshift checks a whole multi-statement batch before running
    /// any of it, so a `CREATE SCHEMA` can't be followed by its tables in one batch.
    async fn sql(&self, sql: &str) {
        for stmt in sql.split(";\n").map(str::trim).filter(|s| !s.is_empty()) {
            self.session
                .client()
                .batch_execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("{stmt}: {}", atlatl_conn_postgres::classify(&e)));
        }
    }

    async fn texts(&self, sql: &str) -> Vec<String> {
        self.session
            .client()
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {}", atlatl_conn_postgres::classify(&e)))
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    Some(r.get(0).unwrap_or("NULL").to_owned())
                }
                _ => None,
            })
            .collect()
    }

    /// Two members of one family; NULL `published_on` rows never move hourly.
    async fn seed_family(&self) {
        let s = &self.schema;
        self.sql(&format!(
            "CREATE SCHEMA {s};
             CREATE TABLE {s}.est_act_af (sec_id varchar(16) NOT NULL, fp_end date NOT NULL, metric varchar(8) NOT NULL,
                                          val numeric(18,4), val_f float8, published_on timestamp, note varchar(64));
             CREATE TABLE {s}.est_act_qf (sec_id varchar(16) NOT NULL, fp_end date NOT NULL, metric varchar(8) NOT NULL,
                                          val numeric(18,4), val_f float8, published_on timestamp, note varchar(64));
             CREATE TABLE {s}.est_other (id int);
             INSERT INTO {s}.est_act_af VALUES
               ('S0001-A', '2024-12-31', 'EPS', 1.2345, 0.1, '2025-01-10 08:00:00', 'it''s'),
               ('S0001-A', '2024-12-31', 'SAL', 98765.4321, 1e-300, '2025-01-10 08:00:00', NULL),
               ('S0002-B', '2024-12-31', 'EPS', -0.5, 2.5, NULL, 'no publish date'),
               ('S0003-C', '2023-12-31', 'EPS', 3, 3.0, '2025-01-11 09:30:00.123456', NULL);
             INSERT INTO {s}.est_act_qf VALUES
               ('S0001-A', '2024-09-30', 'EPS', 0.3, 0.3, '2025-01-09 00:00:00', NULL),
               ('S0002-B', '2024-09-30', 'EPS', 0.4, 0.4, '2025-01-12 00:00:00', NULL);"
        ))
        .await;
    }

    fn connection_yaml(&self) -> String {
        format!(
            "  warehouse:
    type: redshift
    read_only: true
    access: wire
    wire:
      host: {}
      port: {}
      database: {}
      user: {}
      password: {{secret: env:RW_PASSWORD}}
      sslmode: {}
",
            self.host, self.port, self.database, self.user, self.sslmode
        )
    }

    /// Every row of `<schema>.<table>`, as text, in key order.
    async fn rows(&self, table: &str) -> Vec<String> {
        self.texts(&format!(
            "SELECT sec_id || '|' || fp_end::varchar || '|' || metric || '|' || coalesce(val::varchar, 'NULL') || '|' || coalesce(published_on::varchar, 'NULL')
             FROM {}.{table} ORDER BY sec_id, fp_end, metric",
            self.schema
        ))
        .await
    }
}

struct Project {
    dir: tempfile::TempDir,
    lake: LocalLake,
    password: String,
}

struct Run {
    code: i32,
    stderr: String,
    report: Value,
}

impl Run {
    fn ok(&self) -> &Self {
        assert_eq!(
            self.code, 0,
            "{}\n{:#}",
            self.stderr, self.report["replications"]
        );
        self
    }

    fn status(&self, rep: &str) -> &str {
        self.report["replications"]
            .as_array()
            .and_then(|a| a.iter().find(|r| r["replication"] == rep))
            .and_then(|r| r["status"].as_str())
            .unwrap_or_else(|| panic!("no {rep} in {:#}", self.report))
    }

    fn member(&self, stream: &str, member: &str) -> &Value {
        self.report["replications"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|r| r["streams"].as_array().into_iter().flatten())
            .filter(|s| s["stream_id"] == stream)
            .flat_map(|s| s["members"].as_array().into_iter().flatten())
            .find(|m| m["member"] == member)
            .unwrap_or_else(|| panic!("no member {member} of {stream}: {:#}", self.report))
    }

    fn rows(&self, stream: &str, member: &str, what: &str) -> u64 {
        self.member(stream, member)["rows"][what]
            .as_u64()
            .expect("count")
    }

    /// `what` summed over a stream's members (a serving stream has one, named by its table).
    fn stream_rows(&self, stream: &str, what: &str) -> u64 {
        self.report["replications"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|r| r["streams"].as_array().into_iter().flatten())
            .filter(|s| s["stream_id"] == stream)
            .flat_map(|s| s["members"].as_array().into_iter().flatten())
            .map(|m| m["rows"][what].as_u64().unwrap_or(0))
            .sum()
    }
}

impl Project {
    fn new(src: &Source, replications: &str) -> Self {
        Self::with_connections(src, replications, "")
    }

    fn with_connections(src: &Source, replications: &str, extra: &str) -> Self {
        let dir = tempfile::tempdir().expect("tmp");
        let lake = LocalLake::file().expect("lake");
        let yaml = format!(
            "version: 1
engine: {{memory: 1GB, threads: 2}}
state: {{lake: lake}}
connections:
  lake:
    type: ducklake
    alias: lake
    catalog: {{type: duckdb, path: \"{}\"}}
    data_path: \"{}\"
    storage: {{type: local}}
{}{extra}",
            lake.dir.path().join("meta.ducklake").display(),
            lake.data_path(),
            src.connection_yaml()
        );
        std::fs::write(dir.path().join("atlatl.yaml"), yaml).expect("write");
        std::fs::create_dir_all(dir.path().join("replications")).expect("mkdir");
        std::fs::write(dir.path().join("replications/est.yaml"), replications).expect("write");
        Self {
            dir,
            lake,
            password: src.password.clone(),
        }
    }

    async fn atlatl_raw(&self, args: &[&str]) -> Output {
        tokio::process::Command::new(env!("CARGO_BIN_EXE_atlatl"))
            .arg("--project")
            .arg(self.dir.path())
            .args(args)
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env("RW_PASSWORD", &self.password)
            .env_remove("ATLATL_PROJECT")
            .output()
            .await
            .expect("run atlatl")
    }

    async fn run(&self, replications: &[&str]) -> Run {
        let report = self.dir.path().join("report.json");
        let _ = std::fs::remove_file(&report);
        let mut args = vec![
            "--report".to_owned(),
            report.display().to_string(),
            "run".into(),
        ];
        for r in replications {
            args.extend(["--replication".into(), (*r).to_owned()]);
        }
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        let o = self.atlatl_raw(&a).await;
        Run {
            code: o.status.code().expect("exit code"),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
            report: std::fs::read(&report)
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(Value::Null),
        }
    }

    async fn ok(&self, args: &[&str]) -> Value {
        let o = self.atlatl_raw(args).await;
        assert!(
            o.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        serde_json::from_slice(&o.stdout).unwrap_or(Value::Null)
    }

    fn lake_q(&self, sql: &str) -> Vec<String> {
        let c = self.lake.open("lake").expect("open lake");
        let mut s = c.prepare(sql).expect(sql);
        s.query_map([], |r| r.get::<_, Option<String>>(0))
            .expect(sql)
            .map(|r| r.expect("row").unwrap_or_else(|| "NULL".into()))
            .collect()
    }

    fn lake_one(&self, sql: &str) -> String {
        self.lake_q(sql)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("no row: {sql}"))
    }

    fn rows(&self, table: &str) -> Vec<String> {
        self.lake_q(&format!(
            "SELECT sec_id || '|' || fp_end || '|' || metric || '|' || coalesce(val::VARCHAR, 'NULL') || '|' || coalesce(strftime(published_on, '%Y-%m-%d %H:%M:%S.%f'), 'NULL')
             FROM lake.br.{table} ORDER BY sec_id, fp_end, metric"
        ))
    }

    fn type_of(&self, table: &str, col: &str) -> String {
        self.lake_one(&format!(
            "SELECT data_type FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = 'br' AND table_name = '{table}' AND column_name = '{col}'"
        ))
    }
}

fn family(schema: &str) -> String {
    format!(
        "replications:
  est_1d:
    source: warehouse
    target: lake
    op: est
    lock_group: est
    retry: {{max_attempts: 3, base: 10ms, max: 50ms}}
    defaults: {{mode: incremental, object: \"br.{{{{ member.table }}}}\"}}
    streams:
      - {{id: est/act, from: {{tables: \"{schema}.est_act_*\"}}, primary_key: {PK}}}
  est_1h:
    source: warehouse
    target: lake
    op: est
    lock_group: est
    retry: {{max_attempts: 3, base: 10ms, max: 50ms}}
    defaults:
      mode: incremental
      object: \"br.{{{{ member.table }}}}\"
      update_key: published_on
      window: \"fp_end >= DATE '2024-01-01'\"
    streams:
      - {{id: est_1h/act, from: {{tables: \"{schema}.est_act_*\"}}, primary_key: {PK}}}
"
    )
}

/// Source rows rendered like [`Project::rows`] (Postgres and Redshift print timestamps
/// without trailing zeros; DuckDB's `%f` always prints six digits).
fn normalise(rows: Vec<String>) -> Vec<String> {
    rows.into_iter()
        .map(|r| {
            let mut parts: Vec<String> = r.split('|').map(str::to_owned).collect();
            if let Some(ts) = parts.last_mut()
                && ts != "NULL"
            {
                match ts.split_once('.') {
                    Some((a, f)) => *ts = format!("{a}.{f:0<6}"),
                    None => ts.push_str(".000000"),
                }
            }
            parts.join("|")
        })
        .collect()
}

async fn daily_hourly_and_cut_over(src: &Source) {
    src.seed_family().await;
    let s = &src.schema;
    let af = format!("{s}.est_act_af");
    let qf = format!("{s}.est_act_qf");
    let p = Project::new(src, &family(s));

    // The previous loader's daily load: the hourly cursor is adopted from these tables.
    let r = p.run(&["est_1d"]).await;
    r.ok();
    assert_eq!(r.rows("est/act", &af, "inserted"), 4);
    assert_eq!(r.rows("est/act", &qf, "inserted"), 2);
    assert_eq!(
        p.rows("est_act_af"),
        normalise(src.rows("est_act_af").await)
    );
    assert_eq!(
        p.rows("est_act_qf"),
        normalise(src.rows("est_act_qf").await)
    );
    assert_eq!(p.type_of("est_act_af", "val"), "DECIMAL(18,4)");
    assert_eq!(p.type_of("est_act_af", "val_f"), "DOUBLE");
    assert_eq!(
        p.lake_one("SELECT val_f::VARCHAR FROM lake.br.est_act_af WHERE metric = 'SAL'"),
        "1e-300"
    );
    assert_eq!(p.run(&["est_1d"]).await.ok().status("est_1d"), "up_to_date");

    let seeded = p
        .ok(&[
            "state",
            "init",
            "est_1h/act",
            "--from-target",
            "--member",
            &af,
            "--member",
            &qf,
        ])
        .await;
    assert_eq!(
        seeded["after"][af.as_str()]["value"]
            .as_str()
            .map(|v| v.trim_end_matches('0')),
        Some("2025-01-11 09:30:00.123456".trim_end_matches('0')),
        "{seeded:#}"
    );

    // A republication, a new row, a backfill with an old publish date (below the cursor), and a
    // new row outside the hourly window.
    src.sql(&format!(
        "UPDATE {af} SET val = 1.5, published_on = '2025-02-01 10:00:00' WHERE sec_id = 'S0001-A' AND metric = 'EPS';
         INSERT INTO {af} VALUES ('S0004-D', '2024-12-31', 'EPS', 4, 4.0, '2025-02-01 11:00:00', NULL),
                                 ('S0005-E', '2024-12-31', 'EPS', 5, 5.0, '2024-06-01 00:00:00', 'backfill'),
                                 ('S0006-F', '2023-06-30', 'EPS', 6, 6.0, '2025-02-02 00:00:00', 'out of window');"
    ))
    .await;
    let r = p.run(&["est_1h"]).await;
    r.ok();
    assert_eq!(
        r.rows("est_1h/act", &af, "extracted"),
        2,
        "{:#}",
        r.member("est_1h/act", &af)
    );
    assert_eq!(r.rows("est_1h/act", &af, "updated"), 1);
    assert_eq!(r.rows("est_1h/act", &af, "inserted"), 1);
    // The cursor is inclusive: the boundary row is read again and left as it is.
    assert_eq!(r.rows("est_1h/act", &qf, "extracted"), 1);
    assert_eq!(r.rows("est_1h/act", &qf, "unchanged"), 1);
    let lake_af = p.rows("est_act_af");
    assert!(
        lake_af.contains(&"S0001-A|2024-12-31|EPS|1.5000|2025-02-01 10:00:00.000000".to_owned())
    );
    assert!(lake_af.iter().any(|r| r.starts_with("S0004-D|")));
    assert!(
        !lake_af
            .iter()
            .any(|r| r.starts_with("S0005-E|") || r.starts_with("S0006-F|"))
    );
    let cursor = p.lake_one(&format!(
        "SELECT value->>'value' FROM lake._atlatl.stream_state WHERE stream_id = 'est_1h/act' AND member = '{af}'"
    ));
    assert!(cursor.starts_with("2025-02-01 11:00:00"), "{cursor}");
    assert_eq!(p.run(&["est_1h"]).await.ok().status("est_1h"), "up_to_date");

    // The daily merge picks up what the hourly delta can't see.
    p.run(&["est_1d"]).await.ok();
    assert_eq!(
        p.rows("est_act_af"),
        normalise(src.rows("est_act_af").await)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daily_and_hourly_families_with_an_adopted_cursor() {
    let src = Source::pg16().await;
    daily_hourly_and_cut_over(&src).await;
}

/// `max_concurrency` caps the connection's sessions across the run, not per unit: two
/// replications that overlap on one connection share one pool. The role takes one connection, so
/// a second session would fail with "too many connections for role".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_concurrency_caps_sessions_across_replications() {
    let src = Source::pg16().await;
    let role = format!("rw_one_{}", src.database);
    src.sql(&format!(
        "CREATE SCHEMA src;
         CREATE TABLE src.base (id int NOT NULL);
         INSERT INTO src.base VALUES (1), (2);
         CREATE VIEW src.slow_a AS SELECT b.id FROM src.base b, pg_sleep(1);
         CREATE VIEW src.slow_b AS SELECT b.id FROM src.base b, pg_sleep(1);
         CREATE ROLE {role} LOGIN PASSWORD 'pw' CONNECTION LIMIT 1;
         GRANT USAGE ON SCHEMA src TO {role};
         GRANT SELECT ON ALL TABLES IN SCHEMA src TO {role};
         GRANT SELECT ON svv_columns, svv_tables TO {role}"
    ))
    .await;
    let one = format!(
        "  one:
    type: redshift
    read_only: true
    access: wire
    max_concurrency: 1
    wire: {{host: {}, port: {}, database: {}, user: {role}, password: pw, sslmode: disable}}
",
        src.host, src.port, src.database
    );
    let p = Project::with_connections(
        &src,
        "replications:
  a:
    source: one
    target: lake
    streams:
      - {id: one/a, mode: full-refresh, from: {table: src.slow_a}, object: br.slow_a, primary_key: [id]}
  b:
    source: one
    target: lake
    streams:
      - {id: one/b, mode: full-refresh, from: {table: src.slow_b}, object: br.slow_b, primary_key: [id]}
",
        &one,
    );
    let r = p.run(&["a", "b"]).await;
    r.ok();
    // A per-unit pool opens a second session, which the role refuses; the query retry would then
    // hide it, so no attempt may have met the limit.
    assert!(!r.stderr.contains("too many connections"), "{}", r.stderr);
    assert_eq!(
        p.lake_one(
            "SELECT ((SELECT count(*) FROM lake.br.slow_a) + (SELECT count(*) FROM lake.br.slow_b))::VARCHAR"
        ),
        "4"
    );
    src.sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_keeps_dropping_still_lands_every_row_once() {
    let pg = atlatl_testkit::pg16();
    let db = pg.fresh_db().expect("db");
    let direct = Source::connect(
        (&pg.host, pg.port, &db, &pg.user, &pg.password),
        "disable",
        "src".into(),
    )
    .await;
    direct
        .sql(
            "CREATE VIEW svv_columns AS SELECT * FROM information_schema.columns;
             CREATE VIEW svv_tables AS SELECT * FROM information_schema.tables;
             CREATE SCHEMA src;
             CREATE TABLE src.big (sec_id varchar(16) NOT NULL, fp_end date NOT NULL, metric varchar(8) NOT NULL, val numeric(18,4), note varchar(64));
             INSERT INTO src.big SELECT 'S' || lpad((g / 4)::text, 7, '0'), DATE '2024-01-01' + (g % 4) * 90, 'EPS', g * 0.5, md5(g::text)
               FROM generate_series(0, 59999) g;",
        )
        .await;
    let tp = atlatl_testkit::toxiproxy();
    let proxy = tp.postgres(pg).expect("proxy");
    // Each connection is cut after ~1 MB from the server: the ~5 MB read needs several resumes.
    proxy
        .toxic("cut", "limit_data", "downstream", r#"{"bytes":1000000}"#)
        .expect("toxic");
    let via = Source {
        host: proxy.host.clone(),
        port: proxy.port,
        ..direct
    };
    let p = Project::new(
        &via,
        &format!(
            "replications:
  big:
    source: warehouse
    target: lake
    retry: {{max_attempts: 3, base: 10ms, max: 50ms}}
    streams:
      - {{id: big, from: {{table: src.big}}, object: br.big, mode: full-refresh, primary_key: {PK}}}
"
        ),
    );
    let r = p.run(&["big"]).await;
    r.ok();
    let m = r.member("big", "src.big");
    let retries = m["retries"].as_array().expect("retries");
    assert!(retries.len() >= 3, "{m:#}");
    assert!(retries.iter().all(|a| a["class"] == "transient"), "{m:#}");
    assert_eq!(r.rows("big", "src.big", "inserted"), 60_000);
    assert_eq!(
        p.lake_one("SELECT count(DISTINCT (sec_id, fp_end))::VARCHAR FROM lake.br.big"),
        "60000"
    );
    assert_eq!(
        p.lake_one("SELECT sum(val)::VARCHAR FROM lake.br.big"),
        format!("{:.4}", (0..60_000u64).sum::<u64>() as f64 * 0.5)
    );
}

// ------------------------------------------------------------- serving targets

const COLS: &str = "[sec_id, fp_end, metric, val, val_f, published_on, note]";

/// Postgres 16, SQLite and Turso (local sqld) fed from the lake by `mode: cdc`.
struct Serving {
    pg: Source,
    sqlite: tempfile::TempDir,
    turso: atlatl_testkit::Libsql,
}

impl Serving {
    async fn start() -> Self {
        let pg = atlatl_testkit::pg16();
        let db = pg.fresh_db().expect("db");
        let app = Source::connect(
            (&pg.host, pg.port, &db, &pg.user, &pg.password),
            "disable",
            "app".into(),
        )
        .await;
        app.sql(
            "CREATE SCHEMA app;
             CREATE TABLE app.est_act_af (sec_id text NOT NULL, fp_end date NOT NULL, metric text NOT NULL, val numeric(18,4),
                                          val_f double precision, published_on timestamp, note text, PRIMARY KEY (sec_id, fp_end, metric));
             CREATE TABLE app.est_act_qf (LIKE app.est_act_af INCLUDING ALL);",
        )
        .await;
        Self {
            pg: app,
            sqlite: tempfile::tempdir().expect("dir"),
            turso: tokio::task::spawn_blocking(atlatl_testkit::libsql)
                .await
                .expect("libsql"),
        }
    }

    fn connections_yaml(&self) -> String {
        let p = &self.pg;
        format!(
            "  app_pg: {{type: postgres, host: {}, port: {}, database: {}, user: {}, password: \"{}\", sslmode: disable}}
  app_sqlite: {{type: sqlite, path: \"{}\"}}
  app_turso: {{type: turso, url: \"{}\"}}
",
            p.host,
            p.port,
            p.database,
            p.user,
            p.password,
            self.sqlite.path().join("app.db").display(),
            self.turso.url
        )
    }

    async fn lite(&self, target: &str) -> atlatl_conn_sqlite::Client {
        use atlatl_core::config::model::{ConnectionCommon, ConnectionKind, SqliteConn, TursoConn};
        let kind = match target {
            "sqlite" => ConnectionKind::Sqlite(SqliteConn {
                path: self.sqlite.path().join("app.db").display().to_string(),
                busy_timeout: None,
                create_tables: None,
            }),
            _ => ConnectionKind::Turso(TursoConn {
                url: self.turso.url.clone(),
                auth_token: None,
                transaction_rows: None,
                create_tables: None,
            }),
        };
        let c = atlatl_core::config::connection::Connection {
            name: target.into(),
            span: atlatl_core::config::node::Span::new(
                std::sync::Arc::from(std::path::Path::new("t.yaml")),
                1,
                1,
            ),
            common: ConnectionCommon::default(),
            kind,
            resolved_from: "base".into(),
        };
        atlatl_conn_sqlite::Client::open(&c).await.expect("open")
    }

    /// `sec_id|fp_end|metric|val` per row, key order, from one target.
    async fn rows(&self, target: &str, table: &str) -> Vec<String> {
        if target == "pg" {
            return self
                .pg
                .texts(&format!(
                    "SELECT sec_id || '|' || fp_end || '|' || metric || '|' || coalesce(val::text, 'NULL') FROM app.{table} ORDER BY sec_id, fp_end, metric"
                ))
                .await;
        }
        let mut c = self.lite(target).await;
        c.query(
            &format!("SELECT sec_id || '|' || fp_end || '|' || metric || '|' || coalesce(val, 'NULL') FROM {table} ORDER BY sec_id, fp_end, metric"),
            vec![],
        )
        .await
        .expect("query")
        .into_iter()
        .map(|r| match &r[0] {
            atlatl_conn_sqlite::Value::Text(t) => t.clone(),
            v => format!("{v:?}"),
        })
        .collect()
    }

    /// `val_f` and `published_on` of one row, as each target stores them.
    async fn detail(&self, target: &str, key: (&str, &str, &str)) -> (f64, String) {
        let w = format!(
            "sec_id = '{}' AND fp_end = '{}' AND metric = '{}'",
            key.0, key.1, key.2
        );
        if target == "pg" {
            let r = self
                .pg
                .session
                .client()
                .query_one(
                    &format!("SELECT val_f, published_on::text FROM app.est_act_af WHERE {w}"),
                    &[],
                )
                .await
                .expect("row");
            return (r.get(0), r.get(1));
        }
        let mut c = self.lite(target).await;
        let r = c
            .query(
                &format!("SELECT val_f, published_on FROM est_act_af WHERE {w}"),
                vec![],
            )
            .await
            .expect("row");
        match r.as_slice() {
            [row] => match (&row[0], &row[1]) {
                (atlatl_conn_sqlite::Value::Real(f), atlatl_conn_sqlite::Value::Text(t)) => {
                    (*f, t.clone())
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }
}

fn serving_replications(schema: &str) -> String {
    let mut y = family(schema);
    for (t, prefix) in [("app_pg", "app."), ("app_sqlite", ""), ("app_turso", "")] {
        y.push_str(&format!(
            "  serve_{t}:
    source: lake
    target: {t}
    defaults: {{mode: cdc, deletes: {{mode: hard}}, primary_key: {PK}, columns: {{include: {COLS}}}}}
    streams:
      - {{id: {t}/est_act_af, from: {{table: br.est_act_af}}, object: {prefix}est_act_af}}
      - {{id: {t}/est_act_qf, from: {{table: br.est_act_qf}}, object: {prefix}est_act_qf}}
"
        ));
    }
    y
}

const SERVE: [&str; 3] = ["serve_app_pg", "serve_app_sqlite", "serve_app_turso"];
const TARGETS: [(&str, &str); 3] = [
    ("pg", "app_pg"),
    ("sqlite", "app_sqlite"),
    ("turso", "app_turso"),
];

/// Every target holds exactly the lake's rows.
async fn assert_served(p: &Project, s: &Serving) {
    for table in ["est_act_af", "est_act_qf"] {
        let lake = p.lake_q(&format!(
            "SELECT sec_id || '|' || fp_end || '|' || metric || '|' || coalesce(val::VARCHAR, 'NULL') FROM lake.br.{table} ORDER BY sec_id, fp_end, metric"
        ));
        for (t, _) in TARGETS {
            assert_eq!(s.rows(t, table).await, lake, "{t}.{table}");
        }
    }
}

/// `(inserted, updated, deleted)` of one served stream, the same on every target.
fn served(r: &Run, table: &str) -> (u64, u64, u64) {
    let per: Vec<(u64, u64, u64)> = TARGETS
        .iter()
        .map(|(_, c)| {
            let id = format!("{c}/{table}");
            (
                r.stream_rows(&id, "inserted"),
                r.stream_rows(&id, "updated"),
                r.stream_rows(&id, "deleted"),
            )
        })
        .collect();
    assert!(
        per.windows(2).all(|w| w[0] == w[1]),
        "{table}: {per:?}\n{:#}",
        r.report["replications"]
    );
    per[0]
}

async fn ingest_and_serve_incrementally(src: &Source) {
    src.seed_family().await;
    let s = &src.schema;
    let af = format!("{s}.est_act_af");
    let serving = Serving::start().await;
    let p = Project::with_connections(src, &serving_replications(s), &serving.connections_yaml());

    // 1. Daily load; each target is bootstrapped from the lake (a fresh target, so `reload`).
    p.run(&["est_1d"]).await.ok();
    for rep in SERVE {
        p.ok(&["cdc", "bootstrap", rep, "--mode", "reload"]).await;
    }
    assert_served(&p, &serving).await;
    for (t, _) in TARGETS {
        let (f, ts) = serving.detail(t, ("S0001-A", "2024-12-31", "SAL")).await;
        assert_eq!(f, 1e-300, "{t}: float8 exact end to end");
        assert!(
            ts.starts_with("2025-01-10") && ts.contains("08:00:00"),
            "{t}: {ts}"
        );
    }
    let r = p.run(&SERVE).await;
    r.ok();
    for rep in SERVE {
        assert_eq!(r.status(rep), "up_to_date", "{rep}");
    }

    // 2. Hourly delta: one republication and one new row reach every target, nothing else is
    //    rewritten.
    p.ok(&[
        "state",
        "init",
        "est_1h/act",
        "--from-target",
        "--member",
        &af,
    ])
    .await;
    src.sql(&format!(
        "UPDATE {af} SET val = 1.5, published_on = '2025-02-01 10:00:00' WHERE sec_id = 'S0001-A' AND metric = 'EPS';
         INSERT INTO {af} VALUES ('S0004-D', '2024-12-31', 'EPS', 4, 4.0, '2025-02-01 11:00:00', NULL),
                                 ('S0005-E', '2024-12-31', 'EPS', 5, 5.0, '2024-06-01 00:00:00', 'backfill')"
    ))
    .await;
    p.run(&["est_1h"]).await.ok();
    let r = p.run(&SERVE).await;
    r.ok();
    assert_eq!(served(&r, "est_act_af"), (1, 1, 0));
    assert_eq!(served(&r, "est_act_qf"), (0, 0, 0));
    assert_served(&p, &serving).await;
    for (t, _) in TARGETS {
        assert!(
            !serving
                .rows(t, "est_act_af")
                .await
                .iter()
                .any(|r| r.starts_with("S0005-E|")),
            "{t}"
        );
    }

    // 3. The daily merge catches the backfill below the cursor; only that row is served.
    p.run(&["est_1d"]).await.ok();
    let r = p.run(&SERVE).await;
    r.ok();
    assert_eq!(served(&r, "est_act_af"), (1, 0, 0));
    assert_served(&p, &serving).await;

    // 4. A row removed from the lake (as a mirror would) is deleted from every target.
    {
        let c = p.lake.open("lake").expect("lake");
        c.execute_batch("DELETE FROM lake.br.est_act_af WHERE sec_id = 'S0002-B'")
            .expect("delete");
    }
    let r = p.run(&SERVE).await;
    r.ok();
    assert_eq!(served(&r, "est_act_af"), (0, 0, 1));
    assert_served(&p, &serving).await;
    // 4 seeded + S0004-D + S0005-E − S0002-B.
    for (t, _) in TARGETS {
        let rows = serving.rows(t, "est_act_af").await;
        assert_eq!(rows.len(), 5, "{t}");
        assert!(!rows.iter().any(|r| r.starts_with("S0002-B|")), "{t}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_writes_reach_postgres_sqlite_and_turso() {
    let src = Source::pg16().await;
    ingest_and_serve_incrementally(&src).await;
}

// ------------------------------------------------------------------------- live Redshift

/// `REDSHIFT_*` from the environment, else from the workspace `.env`.
fn live_env() -> Option<[String; 5]> {
    let file = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env"))
        .unwrap_or_default();
    let get = |k: &str| {
        std::env::var(k).ok().or_else(|| {
            file.lines()
                .find_map(|l| l.strip_prefix(&format!("{k}=")).map(str::to_owned))
        })
    };
    Some([
        get("REDSHIFT_HOST")?,
        get("REDSHIFT_PORT").unwrap_or_else(|| "5439".into()),
        get("REDSHIFT_DATABASE")?,
        get("REDSHIFT_USER")?,
        get("REDSHIFT_PASSWORD")?,
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(all(feature = "network", feature = "redshift")),
    ignore = "needs live Redshift (--features network,redshift and REDSHIFT_* in .env)"
)]
async fn live_daily_and_hourly_families_with_an_adopted_cursor() {
    let [host, port, db, user, password] = live_env().expect("REDSHIFT_* not set");
    let schema = atlatl_testkit::unique("atlatl_test");
    let src = Source::connect(
        (&host, port.parse().expect("port"), &db, &user, &password),
        "require",
        schema.clone(),
    )
    .await;
    let r = tokio::spawn(async move {
        daily_hourly_and_cut_over(&src).await;
        src
    })
    .await;
    let cleanup = Source::connect(
        (&host, port.parse().expect("port"), &db, &user, &password),
        "require",
        schema.clone(),
    )
    .await;
    cleanup
        .sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    if let Err(e) = r {
        std::panic::resume_unwind(e.into_panic());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg_attr(
    not(all(feature = "network", feature = "redshift")),
    ignore = "needs live Redshift (--features network,redshift and REDSHIFT_* in .env)"
)]
async fn live_incremental_writes_reach_postgres_sqlite_and_turso() {
    let [host, port, db, user, password] = live_env().expect("REDSHIFT_* not set");
    let schema = atlatl_testkit::unique("atlatl_test");
    let src = Source::connect(
        (&host, port.parse().expect("port"), &db, &user, &password),
        "require",
        schema.clone(),
    )
    .await;
    let r = tokio::spawn(async move {
        ingest_and_serve_incrementally(&src).await;
        src
    })
    .await;
    let cleanup = Source::connect(
        (&host, port.parse().expect("port"), &db, &user, &password),
        "require",
        schema.clone(),
    )
    .await;
    cleanup
        .sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    if let Err(e) = r {
        std::panic::resume_unwind(e.into_panic());
    }
}
