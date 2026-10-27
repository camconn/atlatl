//! The NDJSON event stream (spec 10 §3). One redacted JSON object per line, flushed per line.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::report::{
    ReplicationReport, RunCounts, RunStatus, SchemaMode, SchemaReport, SchemaViolation,
};
use crate::error::ErrorClass;
use crate::retry::{AttemptRecord, RetryLevel};

pub const EVENTS_VERSION: u32 = 1;
pub const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// One line of the event stream. Consumers must ignore unknown fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Event {
    pub v: u32,
    pub ts: String,
    pub run_id: String,
    #[serde(flatten)]
    pub body: EventBody,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum EventBody {
    #[serde(rename = "run.started")]
    RunStarted(RunStarted),
    #[serde(rename = "replication.started")]
    ReplicationStarted(ReplicationStarted),
    #[serde(rename = "stream.progress")]
    StreamProgress(StreamProgress),
    #[serde(rename = "retrying")]
    Retrying(Retrying),
    #[serde(rename = "hop.committed")]
    HopCommitted(HopCommitted),
    /// The replication's full report entry.
    #[serde(rename = "replication.completed")]
    ReplicationCompleted(ReplicationReport),
    #[serde(rename = "throttle")]
    Throttle(Throttle),
    #[serde(rename = "schema.drift")]
    SchemaDrift(SchemaDrift),
    #[serde(rename = "warning")]
    Warning(Warning),
    #[serde(rename = "run.completed")]
    RunCompleted(RunCompleted),
}

impl EventBody {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::RunStarted(_) => "run.started",
            Self::ReplicationStarted(_) => "replication.started",
            Self::StreamProgress(_) => "stream.progress",
            Self::Retrying(_) => "retrying",
            Self::HopCommitted(_) => "hop.committed",
            Self::ReplicationCompleted(_) => "replication.completed",
            Self::Throttle(_) => "throttle",
            Self::SchemaDrift(_) => "schema.drift",
            Self::Warning(_) => "warning",
            Self::RunCompleted(_) => "run.completed",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunSelection {
    pub replications: Vec<String>,
    pub streams: Vec<String>,
    pub tags: Vec<String>,
    pub with_deps: bool,
}

impl From<&crate::plan::Selection> for RunSelection {
    fn from(s: &crate::plan::Selection) -> Self {
        Self {
            replications: s.replications.clone(),
            streams: s.streams.clone(),
            tags: s.tags.clone(),
            with_deps: s.with_deps,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunStarted {
    pub env: Option<String>,
    pub selection: RunSelection,
    pub plan_hash: String,
    pub atlatl_version: String,
    pub duckdb_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ReplicationStarted {
    pub replication: String,
    pub op: String,
    pub streams: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Extract,
    Stage,
    Validate,
    Commit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StreamProgress {
    pub replication: String,
    pub stream_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    pub phase: Phase,
    pub rows_so_far: u64,
    pub bytes_so_far: u64,
    pub chunk: u64,
    /// A REST stream's requests while it extracts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpProgress>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HttpProgress {
    pub items_done: u64,
    /// Absent until known: a plain endpoint's pages, or a queue still filling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_total: Option<u64>,
    pub requests: u64,
    pub requests_per_min: f64,
    pub retries: u64,
    pub throttle_wait_ms: u64,
    pub status_counts: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_s: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Retrying {
    pub replication: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    pub level: RetryLevel,
    pub attempt: u32,
    pub error_class: ErrorClass,
    pub message: String,
    pub wait_ms: u64,
}

impl Retrying {
    pub fn new(
        replication: impl Into<String>,
        stream_id: Option<String>,
        member: Option<String>,
        a: &AttemptRecord,
    ) -> Self {
        Self {
            replication: replication.into(),
            stream_id,
            member,
            level: a.level,
            attempt: a.attempt,
            error_class: a.class,
            message: a.message.clone(),
            wait_ms: a.wait_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HopCommitted {
    pub replication: String,
    /// 0-based index into the replication's `hops`.
    pub hop: u32,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lake_snapshot: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Throttle {
    pub connection: String,
    pub wait_ms: u64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SchemaDrift {
    pub replication: String,
    pub stream_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    pub mode: SchemaMode,
    pub new_fields: Vec<String>,
    pub missing_fields: Vec<String>,
    pub violations: Vec<SchemaViolation>,
}

impl SchemaDrift {
    pub fn new(
        replication: impl Into<String>,
        stream_id: impl Into<String>,
        member: Option<String>,
        s: &SchemaReport,
    ) -> Self {
        Self {
            replication: replication.into(),
            stream_id: stream_id.into(),
            member,
            mode: s.mode,
            new_fields: s.new_fields.clone(),
            missing_fields: s.missing_fields.clone(),
            violations: s.violations.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Warning {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunCompleted {
    pub status: RunStatus,
    pub exit_code: u8,
    pub counts: RunCounts,
    pub report_path: Option<String>,
}

/// Where `--events` writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventTarget {
    /// An inherited file descriptor, e.g. `fd:3` from `dagster-atlatl`.
    Fd(i32),
    Path(PathBuf),
    Stdout,
}

impl FromStr for EventTarget {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "" => Err("empty events target".into()),
            "-" => Ok(Self::Stdout),
            _ => match s.strip_prefix("fd:") {
                Some(n) => n
                    .parse::<i32>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .map(Self::Fd)
                    .ok_or_else(|| format!("invalid file descriptor in `{s}`")),
                None => Ok(Self::Path(PathBuf::from(s))),
            },
        }
    }
}

impl fmt::Display for EventTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fd(n) => write!(f, "fd:{n}"),
            Self::Path(p) => write!(f, "{}", p.display()),
            Self::Stdout => f.write_str("-"),
        }
    }
}

impl EventTarget {
    /// Opens the target. For `fd:N`, call this before anything else opens files, so `N` is the
    /// inherited descriptor; the sink takes it over (close-on-exec) and closes the original.
    pub fn open(&self) -> io::Result<Box<dyn Write + Send>> {
        Ok(match self {
            Self::Stdout => Box::new(io::stdout()),
            Self::Path(p) => Box::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(p)?,
            ),
            Self::Fd(n) => Box::new(open_fd(*n)?),
        })
    }
}

#[cfg(unix)]
fn open_fd(n: i32) -> io::Result<File> {
    use std::os::fd::{BorrowedFd, FromRawFd, OwnedFd};
    // SAFETY: the borrow is only used to duplicate; a closed `n` fails with EBADF.
    let dup = unsafe { BorrowedFd::borrow_raw(n) }.try_clone_to_owned()?;
    // SAFETY: `n` is open (the dup succeeded) and was inherited; nothing else in-process owns it.
    drop(unsafe { OwnedFd::from_raw_fd(n) });
    Ok(File::from(dup))
}

#[cfg(not(unix))]
fn open_fd(_: i32) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "fd: events targets need a Unix platform",
    ))
}

/// A cheap, cloneable, thread-safe handle to the event stream. A disabled sink does nothing.
/// Write failures (e.g. the reader went away) are logged once and disable the sink.
#[derive(Clone, Default)]
pub struct EventSink(Option<Arc<Inner>>);

struct Inner {
    run_id: String,
    live: AtomicBool,
    out: Mutex<Box<dyn Write + Send>>,
    progress: Mutex<HashMap<(String, String), Instant>>,
}

impl fmt::Debug for EventSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSink")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl EventSink {
    pub fn disabled() -> Self {
        Self(None)
    }

    pub fn open(target: Option<&EventTarget>, run_id: impl Into<String>) -> io::Result<Self> {
        match target {
            None => Ok(Self::disabled()),
            Some(t) => Ok(Self::to_writer(t.open()?, run_id)),
        }
    }

    pub fn to_writer(w: impl Write + Send + 'static, run_id: impl Into<String>) -> Self {
        Self(Some(Arc::new(Inner {
            run_id: run_id.into(),
            live: AtomicBool::new(true),
            out: Mutex::new(Box::new(w)),
            progress: Mutex::new(HashMap::new()),
        })))
    }

    pub fn is_enabled(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|i| i.live.load(Ordering::Relaxed))
    }

    /// Writes one event. Returns whether it was written.
    pub fn emit(&self, body: EventBody) -> bool {
        match &self.0 {
            Some(i) if i.live.load(Ordering::Relaxed) => i.write(body),
            _ => false,
        }
    }

    /// Like [`emit`](Self::emit), but only builds the event when the sink is enabled.
    pub fn emit_with(&self, f: impl FnOnce() -> EventBody) -> bool {
        self.is_enabled() && self.emit(f())
    }

    /// Emits `stream.progress` unless one was emitted for this stream within the last second.
    pub fn progress(&self, p: StreamProgress) -> bool {
        let Some(i) = self.0.as_ref().filter(|i| i.live.load(Ordering::Relaxed)) else {
            return false;
        };
        {
            let now = Instant::now();
            let mut last = i.progress.lock().unwrap_or_else(|e| e.into_inner());
            let key = (p.replication.clone(), p.stream_id.clone());
            match last.get(&key) {
                Some(t) if now.duration_since(*t) < PROGRESS_INTERVAL => return false,
                _ => {
                    last.insert(key, now);
                }
            }
        }
        i.write(EventBody::StreamProgress(p))
    }
}

impl Inner {
    fn write(&self, body: EventBody) -> bool {
        let ev = Event {
            v: EVENTS_VERSION,
            ts: crate::ids::rfc3339(SystemTime::now()),
            run_id: self.run_id.clone(),
            body,
        };
        let mut line = match super::redacted_json(&ev) {
            Ok(v) => v.to_string(),
            Err(e) => {
                tracing::error!("event serialization failed: {e}");
                return false;
            }
        };
        line.push('\n');
        let mut out = self.out.lock().unwrap_or_else(|e| e.into_inner());
        if !self.live.load(Ordering::Relaxed) {
            return false;
        }
        match out.write_all(line.as_bytes()).and_then(|()| out.flush()) {
            Ok(()) => true,
            Err(e) => {
                self.live.store(false, Ordering::Relaxed);
                tracing::warn!("event stream disabled after write error: {e}");
                false
            }
        }
    }
}

pub fn schema() -> schemars::Schema {
    schemars::schema_for!(Event)
}
