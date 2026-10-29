//! Logical-replication CDC into a local lake, against PG 12 (protocol v1) and PG 14 and 16
//! (v2, streaming and binary values) with `wal_level=logical`.

mod apply;
mod lifecycle;
mod protocol;
mod resume;
mod tls;
mod types;

use std::sync::OnceLock;
use std::time::Duration;

use atlatl_conn_pgrepl::{
    Limits, PgChangeSource, ReplicationSpec, RunReport, SlotPolicy, StreamSpec, run_once,
};
use atlatl_conn_postgres::{ConnectOptions, Session, connect};
use atlatl_core::config::model::{ColumnsConfig, DeleteMode, PgChanges, PgProtocol, SlotsConfig};
use atlatl_testkit::{LocalLake, Postgres, unique};

static PG12: OnceLock<Result<Postgres, String>> = OnceLock::new();
static PG14: OnceLock<Result<Postgres, String>> = OnceLock::new();
static PG16: OnceLock<Result<Postgres, String>> = OnceLock::new();

pub fn server(v: u32) -> &'static Postgres {
    let cell = match v {
        12 => &PG12,
        14 => &PG14,
        16 => &PG16,
        _ => panic!("no PG {v} fixture"),
    };
    cell.get_or_init(|| {
        let b = Postgres::builder(v)
            .setting("wal_level", "logical")
            .setting("max_replication_slots", "64")
            .setting("max_wal_senders", "64");
        // The minimum, so v2 streams any transaction over about 64 kB of changes (PG 13+).
        let b = if v >= 13 {
            b.setting("logical_decoding_work_mem", "64kB")
        } else {
            b
        };
        b.start()
    })
    .as_ref()
    .unwrap_or_else(|e| panic!("postgres {v}: {e}"))
}

/// Runs `$f(version)` on PG 12, 14 and 16.
#[macro_export]
macro_rules! versions {
    ($f:ident) => {
        mod $f {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg12() {
                super::$f(12).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg14() {
                super::$f(14).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg16() {
                super::$f(16).await
            }
        }
    };
}

pub struct Env {
    pub version: u32,
    pub opts: ConnectOptions,
    pub sql: Session,
    pub lake: LocalLake,
    pub probe: duckdb::Connection,
    /// Replication name (slots are cluster-wide, so unique per test).
    pub name: String,
}

pub fn changes(allow_create: bool, temporary: bool) -> PgChanges {
    PgChanges {
        production: Some(false),
        slots: Some(SlotsConfig {
            allow_create: Some(allow_create),
            temporary: Some(temporary),
        }),
        protocol: None,
    }
}

pub fn policy(allow_create: bool, temporary: bool) -> SlotPolicy {
    SlotPolicy::resolve("pg", Some(&changes(allow_create, temporary)), None, None).expect("policy")
}

pub fn protocol(p: PgProtocol) -> SlotPolicy {
    let c = PgChanges {
        protocol: Some(p),
        ..changes(true, false)
    };
    SlotPolicy::resolve("pg", Some(&c), None, None).expect("policy")
}

pub fn stream(table: &str) -> StreamSpec {
    StreamSpec {
        id: format!("pg/{table}"),
        source_schema: "public".into(),
        source_table: table.into(),
        target_schema: "br".into(),
        target_table: table.into(),
        primary_key: None,
        deletes: DeleteMode::Hard,
        history: false,
        columns: None,
        config_hash: "h1".into(),
    }
}

/// `columns:` from its YAML shape as JSON.
pub fn columns(v: serde_json::Value) -> Option<ColumnsConfig> {
    Some(serde_json::from_value(v).expect("columns"))
}

impl Env {
    pub async fn new(version: u32) -> Self {
        let pg = server(version);
        let db = pg.fresh_db().expect("db");
        let opts = ConnectOptions::new(&pg.host, pg.port, &db, &pg.user, Some(pg.password.clone()));
        let sql = connect(&opts).await.expect("connect");
        let lake = LocalLake::file().expect("lake");
        let probe = lake.open("lake").expect("attach");
        Self {
            version,
            opts,
            sql,
            lake,
            probe,
            name: unique("r"),
        }
    }

    pub async fn exec(&self, sql: &str) {
        self.sql
            .client()
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Each entry in its own transaction.
    pub async fn each(&self, stmts: &[&str]) {
        for s in stmts {
            self.exec(s).await;
        }
    }

    pub async fn pg_rows(&self, sql: &str) -> Vec<String> {
        self.sql
            .client()
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect()
    }

    pub fn spec(&self, streams: Vec<StreamSpec>) -> ReplicationSpec {
        ReplicationSpec {
            name: self.name.clone(),
            streams,
            limits: Limits::default(),
        }
    }

    pub async fn source(&self, spec: ReplicationSpec, policy: SlotPolicy) -> PgChangeSource {
        PgChangeSource::open(self.opts.clone(), policy, spec)
            .await
            .expect("open")
    }

    pub fn duck(&self) -> duckdb::Connection {
        self.probe.try_clone().expect("clone")
    }

    pub async fn try_run(
        &self,
        spec: ReplicationSpec,
        p: SlotPolicy,
    ) -> atlatl_core::error::Result<RunReport> {
        let mut src = self.source(spec, p).await;
        let r = run_once(&mut src, self.duck(), "lake", &unique("run")).await;
        src.close().await.expect("close");
        r
    }

    pub async fn run(&self, spec: ReplicationSpec) -> RunReport {
        self.try_run(spec, policy(true, false)).await.expect("run")
    }

    /// Rows of a one-VARCHAR-column query on the lake.
    pub fn rows(&self, sql: &str) -> Vec<String> {
        let mut s = self
            .probe
            .prepare(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        s.query_map([], |r| r.get::<_, String>(0))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows")
    }

    pub fn cursor(&self, stream: &str) -> Option<String> {
        self.probe
            .query_row(
                "SELECT value->>'lsn' FROM lake._atlatl.stream_state WHERE stream_id = ? AND kind = 'lsn'",
                [stream],
                |r| r.get(0),
            )
            .ok()
    }

    pub async fn confirmed(&self) -> Option<String> {
        self.sql
            .client()
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&atlatl_conn_pgrepl::slot_name(&self.name)],
            )
            .await
            .expect("slot")
            .map(|r| r.get(0))
    }

    /// Waits until the slot is confirmed to `lsn` (standby status is asynchronous).
    pub async fn confirmed_reaches(&self, lsn: &str) -> bool {
        for _ in 0..100 {
            if self.confirmed().await.as_deref() == Some(lsn) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
}
