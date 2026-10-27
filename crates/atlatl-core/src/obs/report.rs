//! The run report (spec 10 §4): the source of truth for results and the contract with
//! `dagster-atlatl`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{AtlatlError, ErrorClass};
use crate::plan::{Plan, StreamRole};
use crate::retry::{AttemptRecord, RetryLevel};

pub const REPORT_VERSION: u32 = 1;

/// Exit codes (spec 03 §10).
pub mod exit {
    pub const OK: u8 = 0;
    pub const FAILED: u8 = 1;
    pub const CONFIG: u8 = 2;
    pub const INTERNAL: u8 = 3;
    pub const SIGINT: u8 = 130;
    pub const SIGTERM: u8 = 143;
}

/// `ReportError::code` for a panic caught at the replication boundary; forces exit 3.
pub const PANIC_CODE: &str = "panic";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Succeeded,
    PartialFailure,
    Failed,
    Killed,
    ConfigError,
    InternalError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationStatus {
    Succeeded,
    Failed,
    /// Already succeeded under the same external run id (`--skip-succeeded`), or an `after`
    /// dependency failed.
    Skipped,
    Killed,
    NotStarted,
    UpToDate,
}

impl ReplicationStatus {
    /// Committed (or legitimately had nothing to do) in this run.
    pub fn is_success(self) -> bool {
        matches!(self, Self::Succeeded | Self::UpToDate)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamStatus {
    Succeeded,
    /// This stream caused its replication to fail.
    Failed,
    /// A queue producer pulled in to feed a consumer; wrote nothing.
    FeedOnly,
    UpToDate,
    /// Nothing from this stream was committed (its replication failed, was killed or never ran).
    RolledBack,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HopStatus {
    Committed,
    Failed,
    NotStarted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KillReason {
    Sigint,
    Sigterm,
    /// `--timeout` expired.
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunReport {
    pub v: u32,
    pub run_id: String,
    pub external_run_id: Option<String>,
    pub atlatl_version: String,
    pub env: Option<String>,
    pub plan_hash: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: RunStatus,
    pub exit_code: u8,
    /// Set when the run was interrupted.
    pub killed_by: Option<KillReason>,
    /// Run-level failure (`config_error`, `internal_error`).
    pub error: Option<ReportError>,
    pub engine: EngineStats,
    pub replications: Vec<ReplicationReport>,
    pub warnings: Vec<ReportWarning>,
    /// `atlatl run --dry-run`: staged, validated and compared, nothing committed. Members carry a
    /// `diff`; `up_to_date` means the run would change nothing.
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EngineStats {
    pub duckdb_version: Option<String>,
    pub memory_limit: Option<String>,
    pub threads: Option<u32>,
    /// `engine.memory_mode` (01 §5).
    pub memory_mode: Option<crate::config::model::MemoryMode>,
    pub peak_rss_bytes: u64,
    pub peak_duckdb_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationReport {
    pub replication: String,
    pub op: String,
    pub status: ReplicationStatus,
    pub started_at: Option<String>,
    pub duration_ms: u64,
    /// Replication-level attempts (`replication_retries`); 0 if it never started.
    pub attempts: u32,
    pub hops: Vec<HopReport>,
    pub error: Option<ReportError>,
    pub streams: Vec<StreamReport>,
    /// CDC: bounds (`max_changes`, `max_snapshots`) stopped the window short of the lake head
    /// (`partial_caught_up`, 08 §8); the next run continues.
    #[serde(default)]
    pub partial: bool,
    /// A `for_each` instance: the replication it instantiates and its key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<InstanceRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct InstanceRef {
    pub of: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HopReport {
    pub target: String,
    pub status: HopStatus,
    /// Ingest hops: the snapshot created; serving hops: the lake snapshot served up to.
    pub lake_snapshot: Option<u64>,
    /// Why this hop failed (the replication's `error` is its first failed hop's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ReportError>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ReportError {
    pub class: ErrorClass,
    pub code: Option<String>,
    pub message: String,
    pub retryable: bool,
    pub samples: Vec<Value>,
    pub backtrace: Option<String>,
}

impl From<&AtlatlError> for ReportError {
    fn from(e: &AtlatlError) -> Self {
        Self {
            class: e.class,
            code: e.code.clone(),
            message: e.message.clone(),
            retryable: e.class.is_retryable(),
            samples: e.samples.clone(),
            backtrace: None,
        }
    }
}

impl From<AtlatlError> for ReportError {
    fn from(e: AtlatlError) -> Self {
        (&e).into()
    }
}

impl ReportError {
    pub fn panic(message: impl Into<String>, backtrace: Option<String>) -> Self {
        Self {
            class: ErrorClass::Internal,
            code: Some(PANIC_CODE.into()),
            message: message.into(),
            retryable: false,
            samples: Vec::new(),
            backtrace,
        }
    }

    pub fn is_panic(&self) -> bool {
        self.code.as_deref() == Some(PANIC_CODE)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StreamReport {
    pub stream_id: String,
    pub asset_key: String,
    pub status: StreamStatus,
    pub duration_ms: u64,
    pub members: Vec<MemberReport>,
}

impl StreamReport {
    pub fn new(
        stream_id: impl Into<String>,
        asset_key: impl Into<String>,
        feed_only: bool,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            asset_key: asset_key.into(),
            status: if feed_only {
                StreamStatus::FeedOnly
            } else {
                StreamStatus::RolledBack
            },
            duration_ms: 0,
            members: Vec::new(),
        }
    }

    /// Row counts summed over members.
    pub fn rows(&self) -> RowCounts {
        self.members.iter().fold(RowCounts::default(), |mut a, m| {
            a.add(&m.rows);
            a
        })
    }

    pub fn member_mut(&mut self, member: &str) -> &mut MemberReport {
        let i = match self.members.iter().position(|m| m.member == member) {
            Some(i) => i,
            None => {
                self.members.push(MemberReport::new(member));
                self.members.len() - 1
            }
        };
        &mut self.members[i]
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MemberReport {
    pub member: String,
    /// Resolved target table.
    pub object: Option<String>,
    pub duration_ms: u64,
    pub retries: Vec<RetryReport>,
    pub rows: RowCounts,
    pub bytes: ByteCounts,
    /// Cursor before and after: `update_key` watermark, sync values or CDC snapshot.
    pub state: StateChange,
    /// DuckLake snapshot created, or null if nothing changed.
    pub lake_snapshot: Option<u64>,
    pub chunks: u64,
    pub cdc: Option<CdcReport>,
    pub http: Option<HttpStats>,
    pub schema: Option<SchemaReport>,
    pub rejects: RejectReport,
    pub warnings: Vec<ReportWarning>,
    /// `--dry-run`: what the commit would change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<DiffReport>,
}

/// A dry run's comparison of the staged rows against the current target (spec 03 §11).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DiffReport {
    pub mode: String,
    /// Target rows now (0 if the table doesn't exist yet).
    pub target_rows: u64,
    pub inserts: u64,
    pub updates: u64,
    /// Only modes that delete: a full refresh.
    pub deletes: u64,
    pub unchanged: u64,
    /// Why only counts were compared (no primary key to match rows by); no samples then.
    pub counts_only: Option<String>,
    /// DDL the commit would run first (create the table, add or widen columns).
    pub ddl: Vec<String>,
    /// Up to the sample limit per op: `{op: insert|delete, key}`, and
    /// `{op: update, key, changed, staged, target}` with the changed columns' values.
    pub samples: Vec<Value>,
}

impl DiffReport {
    pub fn changes(&self) -> bool {
        self.inserts + self.updates + self.deletes > 0 || !self.ddl.is_empty()
    }
}

impl MemberReport {
    pub fn new(member: impl Into<String>) -> Self {
        Self {
            member: member.into(),
            ..Self::default()
        }
    }

    pub fn record_retry(&mut self, a: &AttemptRecord) {
        self.retries.push(a.into());
    }

    pub fn warn(&mut self, code: impl Into<String>, message: impl Into<String>) {
        self.warnings.push(ReportWarning {
            code: code.into(),
            message: message.into(),
        });
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RetryReport {
    pub level: RetryLevel,
    pub attempt: u32,
    pub class: ErrorClass,
    pub message: String,
    pub wait_ms: u64,
}

impl From<&AttemptRecord> for RetryReport {
    fn from(a: &AttemptRecord) -> Self {
        Self {
            level: a.level,
            attempt: a.attempt,
            class: a.class,
            message: a.message.clone(),
            wait_ms: a.wait_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RowCounts {
    pub extracted: u64,
    pub staged: u64,
    pub inserted: u64,
    pub updated: u64,
    pub deleted: u64,
    pub unchanged: u64,
    pub rejected: u64,
}

impl RowCounts {
    pub fn add(&mut self, o: &RowCounts) {
        self.extracted += o.extracted;
        self.staged += o.staged;
        self.inserted += o.inserted;
        self.updated += o.updated;
        self.deleted += o.deleted;
        self.unchanged += o.unchanged;
        self.rejected += o.rejected;
    }

    /// `dagster/row_count`: inserted + updated + deleted.
    pub fn written(&self) -> u64 {
        self.inserted + self.updated + self.deleted
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ByteCounts {
    pub read: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StateChange {
    pub before: Option<Value>,
    pub after: Option<Value>,
}

/// Spec 08 §11.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CdcReport {
    pub window: CdcWindow,
    pub touched_keys: u64,
    pub upserts_sent: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    /// Filtered by the `IS DISTINCT FROM` guard.
    pub rows_unchanged: u64,
    pub rows_deleted: u64,
    /// Lake deletes dropped under `deletes: ignore`.
    pub deletes_ignored: u64,
    pub snapshots_behind: u64,
    pub seconds_behind: f64,
    pub resynced: bool,
    /// Postgres logical-replication sources: the window and the slot after the run.
    #[serde(default)]
    pub slot: Option<SlotReport>,
}

/// A replication slot after a postgres `changes` run (08 §10.1, M6.3).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SlotReport {
    pub slot: String,
    /// The slot's confirmed position when the window started.
    pub from_lsn: Option<String>,
    /// The position recorded in the lake.
    pub to_lsn: Option<String>,
    pub confirmed_flush_lsn: Option<String>,
    /// WAL written since the confirmed flush.
    pub lag_bytes: Option<u64>,
    /// WAL the slot keeps on the server (from `restart_lsn`).
    pub retained_bytes: Option<u64>,
    /// `age(catalog_xmin)`.
    pub catalog_xmin_age: Option<i64>,
    pub transactions: u64,
    pub skipped_transactions: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CdcWindow {
    pub from_snapshot: Option<u64>,
    pub to_snapshot: Option<u64>,
    pub from_time: Option<String>,
    pub to_time: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HttpStats {
    pub requests: u64,
    pub retries: u64,
    pub throttle_wait_ms: u64,
    /// Response count per HTTP status code.
    pub status_counts: BTreeMap<String, u64>,
    /// `for_each` items left for later runs by `max_items` (07 §7).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub items_deferred: u64,
    /// `for_each` items left out by `except_skipped`: skipped within its TTL (07 §7).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub items_held: u64,
    pub skipped_items: Vec<SkippedItem>,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

impl HttpStats {
    pub fn record_status(&mut self, status: u16) {
        self.requests += 1;
        *self.status_counts.entry(status.to_string()).or_default() += 1;
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SkippedItem {
    pub key: String,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaStatus {
    Ok,
    Violated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMode {
    Enforce,
    Observe,
}

/// Asset-check severity: `warn` policies tripped (`warn`), or a `fail` policy did (`error`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SchemaReport {
    pub status: SchemaStatus,
    pub mode: SchemaMode,
    pub severity: Option<Severity>,
    /// Staged records evaluated.
    #[serde(default)]
    pub records: u64,
    pub new_fields: Vec<String>,
    pub missing_fields: Vec<String>,
    pub violations: Vec<SchemaViolation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ViolationKind {
    Type,
    Null,
    Domain,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SchemaViolation {
    pub field: String,
    pub kind: ViolationKind,
    pub count: u64,
    pub samples: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RejectReport {
    pub new: u64,
    pub resolved: u64,
    /// Deleted by `reject_ttl` (03 §7); in a dry run, would be.
    #[serde(default)]
    pub expired: u64,
    pub open: u64,
    pub samples: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ReportWarning {
    pub code: String,
    pub message: String,
}

/// Replication outcomes, as in `run.completed`. `up_to_date` counts as succeeded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RunCounts {
    pub succeeded: u32,
    pub failed: u32,
    pub skipped: u32,
    pub killed: u32,
    pub not_started: u32,
}

/// What a report starts from.
#[derive(Clone, Debug, Default)]
pub struct RunMeta {
    pub run_id: String,
    pub external_run_id: Option<String>,
    pub env: Option<String>,
    pub plan_hash: Option<String>,
}

impl RunReport {
    pub fn new(meta: RunMeta) -> Self {
        Self {
            v: REPORT_VERSION,
            run_id: meta.run_id,
            external_run_id: meta.external_run_id,
            atlatl_version: super::ATLATL_VERSION.into(),
            env: meta.env,
            plan_hash: meta.plan_hash,
            started_at: crate::ids::rfc3339(SystemTime::now()),
            finished_at: None,
            status: RunStatus::InternalError,
            exit_code: exit::INTERNAL,
            killed_by: None,
            error: None,
            engine: EngineStats::default(),
            replications: Vec::new(),
            warnings: Vec::new(),
            dry_run: false,
        }
    }

    /// Replications (not started), streams and concrete members from the plan.
    pub fn from_plan(plan: &Plan, external_run_id: Option<String>) -> Self {
        let mut r = Self::new(RunMeta {
            run_id: plan.run_id.clone().unwrap_or_else(crate::ids::ulid),
            external_run_id,
            env: plan.env.clone(),
            plan_hash: Some(plan.plan_hash.clone()),
        });
        r.warnings = plan
            .warnings
            .iter()
            .map(|w| ReportWarning {
                code: w.code.clone(),
                message: w.message.clone(),
            })
            .collect();
        r.replications = plan
            .replications
            .iter()
            .map(|rp| {
                let mut rep = ReplicationReport::new(&rp.name, &rp.op);
                rep.hops = rp
                    .hops
                    .iter()
                    .map(|h| HopReport {
                        target: h.target.name.clone(),
                        status: HopStatus::NotStarted,
                        lake_snapshot: None,
                        error: None,
                    })
                    .collect();
                rep.instance = rp.instance.as_ref().map(|i| InstanceRef {
                    of: i.of.clone(),
                    key: i.key.clone(),
                });
                rep.streams = rp
                    .streams
                    .iter()
                    .map(|s| {
                        let mut sr =
                            StreamReport::new(&s.id, &s.asset.key, s.role == StreamRole::FeedOnly);
                        sr.members = s
                            .members
                            .iter()
                            .filter(|m| !m.pattern)
                            .map(|m| MemberReport {
                                object: m.object.clone(),
                                ..MemberReport::new(&m.name)
                            })
                            .collect();
                        sr
                    })
                    .collect();
                rep
            })
            .collect();
        r
    }

    pub fn replication_mut(&mut self, name: &str) -> Option<&mut ReplicationReport> {
        self.replications.iter_mut().find(|r| r.replication == name)
    }

    pub fn counts(&self) -> RunCounts {
        let mut c = RunCounts::default();
        for r in &self.replications {
            match r.status {
                ReplicationStatus::Succeeded | ReplicationStatus::UpToDate => c.succeeded += 1,
                ReplicationStatus::Failed => c.failed += 1,
                ReplicationStatus::Skipped => c.skipped += 1,
                ReplicationStatus::Killed => c.killed += 1,
                ReplicationStatus::NotStarted => c.not_started += 1,
            }
        }
        c
    }

    pub fn derive_status(&self) -> RunStatus {
        if let Some(e) = &self.error {
            return if e.class == ErrorClass::Config {
                RunStatus::ConfigError
            } else {
                RunStatus::InternalError
            };
        }
        if self.killed_by.is_some() {
            return RunStatus::Killed;
        }
        let ok = |s: ReplicationStatus| s.is_success() || s == ReplicationStatus::Skipped;
        // A failed multi-hop replication whose lake hop committed still landed something.
        let landed = |r: &ReplicationReport| {
            r.status.is_success() || r.hops.iter().any(|h| h.status == HopStatus::Committed)
        };
        if self.replications.iter().all(|r| ok(r.status)) {
            RunStatus::Succeeded
        } else if self.replications.iter().any(landed) {
            RunStatus::PartialFailure
        } else {
            RunStatus::Failed
        }
    }

    /// Spec 03 §10, from `status` (see [`derive_status`](Self::derive_status)). A panic in any
    /// replication forces 3 unless the run was killed by a signal.
    pub fn derive_exit_code(&self) -> u8 {
        let panicked = self
            .replications
            .iter()
            .any(|r| r.error.as_ref().is_some_and(ReportError::is_panic));
        match (self.status, self.killed_by) {
            (RunStatus::Killed, Some(KillReason::Sigint)) => exit::SIGINT,
            (RunStatus::Killed, Some(KillReason::Sigterm)) => exit::SIGTERM,
            (RunStatus::ConfigError, _) => exit::CONFIG,
            (RunStatus::InternalError, _) => exit::INTERNAL,
            _ if panicked => exit::INTERNAL,
            (RunStatus::Succeeded, _) => exit::OK,
            _ => exit::FAILED,
        }
    }

    /// Settles every replication still open (in flight → `killed`, else `not_started`),
    /// normalises stream statuses, and sets `finished_at`, `status` and `exit_code`.
    pub fn finalize(&mut self) {
        for r in &mut self.replications {
            if r.status == ReplicationStatus::NotStarted && r.started_at.is_some() {
                r.status = ReplicationStatus::Killed;
            }
            r.settle_streams();
        }
        self.finished_at = Some(crate::ids::rfc3339(SystemTime::now()));
        self.status = self.derive_status();
        self.exit_code = self.derive_exit_code();
    }

    /// Pretty JSON with every string redacted.
    pub fn to_redacted_json(&self) -> serde_json::Result<Vec<u8>> {
        let mut b = serde_json::to_vec_pretty(&super::redacted_json(self)?)?;
        b.push(b'\n');
        Ok(b)
    }

    /// Redacts and writes atomically (tmp + fsync + rename).
    pub fn write(&self, path: &Path) -> io::Result<()> {
        write_atomic(path, &self.to_redacted_json().map_err(io::Error::other)?)
    }
}

impl ReplicationReport {
    pub fn new(name: impl Into<String>, op: impl Into<String>) -> Self {
        Self {
            replication: name.into(),
            op: op.into(),
            status: ReplicationStatus::NotStarted,
            started_at: None,
            duration_ms: 0,
            attempts: 0,
            hops: Vec::new(),
            error: None,
            streams: Vec::new(),
            partial: false,
            instance: None,
        }
    }

    pub fn stream_mut(&mut self, stream_id: &str) -> Option<&mut StreamReport> {
        self.streams.iter_mut().find(|s| s.stream_id == stream_id)
    }

    /// Placeholder (`rolled_back`) streams become `succeeded` / `up_to_date` when the
    /// replication committed. In any other outcome with no committed hop, streams marked
    /// `succeeded` / `up_to_date` become `rolled_back`.
    fn settle_streams(&mut self) {
        let to = match self.status {
            ReplicationStatus::Succeeded => Some(StreamStatus::Succeeded),
            ReplicationStatus::UpToDate => Some(StreamStatus::UpToDate),
            _ => None,
        };
        let committed = self.hops.iter().any(|h| h.status == HopStatus::Committed);
        for s in &mut self.streams {
            match (to, s.status) {
                (Some(t), StreamStatus::RolledBack) => s.status = t,
                (None, StreamStatus::Succeeded | StreamStatus::UpToDate) if !committed => {
                    s.status = StreamStatus::RolledBack
                }
                _ => {}
            }
        }
    }
}

/// The report shared by the executor's tasks. Clone freely; every method takes the lock briefly.
#[derive(Clone)]
pub struct ReportHandle(Arc<Mutex<State>>);

struct State {
    report: RunReport,
    started: HashMap<String, Instant>,
}

impl ReportHandle {
    pub fn new(report: RunReport) -> Self {
        Self(Arc::new(Mutex::new(State {
            report,
            started: HashMap::new(),
        })))
    }

    pub fn from_plan(plan: &Plan, external_run_id: Option<String>) -> Self {
        Self::new(RunReport::from_plan(plan, external_run_id))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn run_id(&self) -> String {
        self.lock().report.run_id.clone()
    }

    pub fn update<R>(&self, f: impl FnOnce(&mut RunReport) -> R) -> R {
        f(&mut self.lock().report)
    }

    /// `None` if the replication isn't in the report.
    pub fn replication<R>(
        &self,
        name: &str,
        f: impl FnOnce(&mut ReplicationReport) -> R,
    ) -> Option<R> {
        self.lock().report.replication_mut(name).map(f)
    }

    /// The member entry, created on first use (wildcard expansion). `None` if the replication or
    /// stream isn't in the report.
    pub fn member<R>(
        &self,
        replication: &str,
        stream_id: &str,
        member: &str,
        f: impl FnOnce(&mut MemberReport) -> R,
    ) -> Option<R> {
        self.replication(replication, |r| {
            r.stream_mut(stream_id).map(|s| f(s.member_mut(member)))
        })
        .flatten()
    }

    /// Marks an attempt of the replication as started (in flight until finished).
    pub fn start_replication(&self, name: &str) -> bool {
        let mut st = self.lock();
        let State { report, started } = &mut *st;
        let Some(r) = report.replication_mut(name) else {
            return false;
        };
        r.attempts += 1;
        if r.started_at.is_none() {
            r.started_at = Some(crate::ids::rfc3339(SystemTime::now()));
            started.insert(name.to_owned(), Instant::now());
        }
        true
    }

    /// Records the outcome and returns the settled entry, for `replication.completed`.
    pub fn finish_replication(
        &self,
        name: &str,
        status: ReplicationStatus,
        error: Option<ReportError>,
    ) -> Option<ReplicationReport> {
        let mut st = self.lock();
        let State { report, started } = &mut *st;
        let r = report.replication_mut(name)?;
        r.status = status;
        if error.is_some() {
            r.error = error;
        }
        if let Some(t) = started.get(name) {
            r.duration_ms = t.elapsed().as_millis() as u64;
        }
        r.settle_streams();
        Some(r.clone())
    }

    pub fn snapshot(&self) -> RunReport {
        self.lock().report.clone()
    }

    /// Finalizes and returns the report.
    pub fn finish(&self) -> RunReport {
        let mut st = self.lock();
        st.report.finalize();
        st.report.clone()
    }

    /// Finalizes and writes atomically. On error the caller should exit 3.
    pub fn write(&self, path: &Path) -> io::Result<RunReport> {
        let r = self.finish();
        r.write(path)?;
        Ok(r)
    }

    /// For the kill handler: marks the run killed, finalizes and writes within `budget`, without
    /// blocking past it (a stuck lock or disk just loses the report). Returns the exit code.
    pub fn write_on_kill(&self, path: Option<&Path>, reason: KillReason, budget: Duration) -> u8 {
        let deadline = Instant::now() + budget;
        let report = loop {
            match self.0.try_lock() {
                Ok(mut g) => break Some(kill_and_clone(&mut g, reason)),
                Err(TryLockError::Poisoned(p)) => {
                    break Some(kill_and_clone(&mut p.into_inner(), reason));
                }
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(TryLockError::WouldBlock) => break None,
            }
        };
        let fallback = match reason {
            KillReason::Sigint => exit::SIGINT,
            KillReason::Sigterm => exit::SIGTERM,
            KillReason::Timeout => exit::FAILED,
        };
        let (Some(report), Some(path)) = (report, path) else {
            return fallback;
        };
        let code = report.exit_code;
        let path = path.to_owned();
        let (tx, rx) = std::sync::mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("atlatl-report-kill".into())
            .spawn(move || {
                let _ = tx.send(report.write(&path));
            });
        if spawned.is_err() {
            return code;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left.max(Duration::from_millis(1))) {
            Ok(Ok(())) => code,
            _ if matches!(reason, KillReason::Timeout) => exit::INTERNAL,
            _ => code,
        }
    }
}

fn kill_and_clone(st: &mut State, reason: KillReason) -> RunReport {
    st.report.killed_by = Some(reason);
    st.report.finalize();
    st.report.clone()
}

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "report.json".into());
    path.with_file_name(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Writes `bytes` to a sibling temp file and fsyncs it. The caller renames it into place.
pub(crate) fn stage_tmp(path: &Path, bytes: &[u8]) -> io::Result<PathBuf> {
    let tmp = tmp_path(path);
    let res = (|| {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()
    })();
    match res {
        Ok(()) => Ok(tmp),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

pub(crate) fn commit_tmp(tmp: &Path, path: &Path) -> io::Result<()> {
    if let Err(e) = fs::rename(tmp, path) {
        let _ = fs::remove_file(tmp);
        return Err(e);
    }
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        let _ = File::open(dir).and_then(|d| d.sync_all());
    }
    Ok(())
}

/// Readers see either the old file or the complete new one, never a partial write.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = stage_tmp(path, bytes)?;
    commit_tmp(&tmp, path)
}

pub fn schema() -> schemars::Schema {
    schemars::schema_for!(RunReport)
}
