//! What a run needs once: the engine, attached lakes, resolved connections, state, locks, report
//! and events (01 §6 steps 4–5).

use std::sync::Arc;
use std::time::SystemTime;

use atlatl_conn_ducklake::Lake;
use atlatl_core::config::connection::Connection;
use atlatl_core::config::load::{Project, Stream};
use atlatl_core::config::model::ConnectionKind;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::lock::{LocalLocks, LockManager};
use atlatl_core::obs::events::EventSink;
use atlatl_core::obs::report::ReportHandle;
use atlatl_core::plan::Plan;
use atlatl_core::template::{EnvSource, Templates};
use atlatl_engine::Engine;
use atlatl_state::LakeState;
use indexmap::IndexMap;

pub struct Runtime {
    pub project: Arc<Project>,
    pub plan: Arc<Plan>,
    pub engine: Engine,
    /// Attached lakes by connection name.
    pub lakes: IndexMap<String, Lake>,
    /// Resolved (secrets included) connections by name.
    pub connections: IndexMap<String, Connection>,
    /// The state store and the alias of the lake it lives in.
    pub state: Option<LakeState>,
    pub locks: Arc<dyn LockManager>,
    pub report: ReportHandle,
    pub events: EventSink,
    pub run_id: String,
    pub external_run_id: Option<String>,
    pub started: SystemTime,
    pub templates: Templates,
    pub vars: Arc<dyn EnvSource>,
    /// REST clients by connection (one limiter and token cache per run), with the cassette each
    /// records to, if any.
    pub rest_clients:
        std::sync::Mutex<IndexMap<String, (atlatl_http::ApiClient, Option<atlatl_http::Cassette>)>>,
    /// Redshift wire pools by connection (`max_concurrency` sessions), shared by every unit in the
    /// run.
    pub redshift_wires: std::sync::Mutex<IndexMap<String, atlatl_conn_redshift::Wire>>,
    /// ClickHouse query slots by connection (`max_concurrency`), shared by every unit in the run.
    pub clickhouse_slots: std::sync::Mutex<IndexMap<String, Arc<tokio::sync::Semaphore>>>,
    /// `for_each` replications whose items couldn't be listed; they fail with this when run.
    pub for_each_errors: IndexMap<String, AtlatlError>,
    /// `atlatl run --dry-run`: stage, validate and diff, never commit.
    pub dry_run: Option<crate::dry::DryRun>,
    /// `atlatl rejects retry`: REST streams replay their open rejects only (03 §7).
    pub retry_rejects: bool,
    /// REST extracts in flight, for progress reporting.
    pub progress: Arc<crate::progress::Board>,
}

impl Runtime {
    pub fn stream(&self, id: &str) -> Result<&Stream> {
        self.project
            .stream(id)
            .map(|(_, s)| s)
            .ok_or_else(|| AtlatlError::internal(format!("planned stream `{id}` not in project")))
    }

    pub fn lake(&self, conn: &str) -> Result<&Lake> {
        self.lakes
            .get(conn)
            .ok_or_else(|| AtlatlError::internal(format!("lake `{conn}` not attached")))
    }

    pub fn connection(&self, name: &str) -> Result<&Connection> {
        self.connections
            .get(name)
            .ok_or_else(|| AtlatlError::internal(format!("connection `{name}` not resolved")))
    }

    pub fn state(&self) -> Result<&LakeState> {
        self.state.as_ref().ok_or_else(|| {
            AtlatlError::config("this replication keeps state (update_key or CDC bootstrap) but the project has no `state.lake`")
        })
    }

    /// Run-scoped scratch (downloads, extracted archives); `atlatl gc` removes leftovers.
    pub fn workdir(&self) -> std::path::PathBuf {
        workdir(&self.engine, &self.run_id)
    }

    pub fn started_epoch_s(&self) -> i64 {
        self.started
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

pub fn workdir(engine: &atlatl_engine::Engine, run_id: &str) -> std::path::PathBuf {
    engine
        .budget()
        .temp_directory
        .join(format!("atlatl-run-{run_id}"))
}

/// One connection, secrets included, for `env`.
pub fn resolve_connection(
    p: &Project,
    name: &str,
    env: Option<&str>,
    vars: Arc<dyn EnvSource>,
) -> Result<Connection> {
    let raw = p
        .connections
        .get(name)
        .ok_or_else(|| AtlatlError::config(format!("no connection `{name}`")))?;
    let mut d = atlatl_core::config::Diagnostics::default();
    let full = atlatl_core::config::env::Resolve::Full {
        vars,
        redactor: atlatl_core::redact::Redactor::global(),
    };
    atlatl_core::config::env::resolve_connection(p, raw, env, &full, &Templates::new(), &mut d)
        .ok_or_else(|| {
            let why = d
                .errors()
                .map(|e| e.error.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            AtlatlError::config(format!("connection `{name}` failed to resolve: {why}"))
        })
}

/// The lock backend: `state.locks` (a postgres connection or `<lake>.catalog`), else in-process.
pub fn lock_backend(
    p: &Project,
    conns: &IndexMap<String, Connection>,
) -> Result<Arc<dyn LockManager>> {
    let Some(spec) = p.state.as_ref().and_then(|s| s.locks.as_ref()) else {
        return Ok(Arc::new(LocalLocks::new()));
    };
    let name = spec.value.split('.').next().unwrap_or(&spec.value);
    let c = conns.get(name).ok_or_else(|| {
        AtlatlError::config(format!("lock backend `{}` not resolved", spec.value))
    })?;
    match &c.kind {
        ConnectionKind::Ducklake(d) => atlatl_conn_ducklake::lock_manager(d),
        ConnectionKind::Postgres(_) => {
            let o = atlatl_conn_postgres::ConnectOptions::from_connection(c)?;
            Ok(Arc::new(atlatl_conn_postgres::PgAdvisoryLocks::new(o)))
        }
        _ => Err(AtlatlError::config(format!(
            "`state.locks: {}` is not a postgres or ducklake connection",
            spec.value
        ))),
    }
}
