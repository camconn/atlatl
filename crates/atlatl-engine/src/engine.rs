//! The process's one DuckDB instance (spec 01 §2): budget applied at open, a connection pool,
//! a bounded blocking pool, statement timeouts and kill through `duckdb_interrupt`.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arrow::array::RecordBatch;
use atlatl_core::config::model::MemoryMode;
use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use duckdb::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::alloc;
use crate::batch::{Batch, BatchStream, ByteBudget};
use crate::budget::Budget;
use crate::error::{DbResultExt, from_message};
use crate::ffi::{RawConn, RawDb, library_version};

/// The DuckDB release the `duckdb` crate pin (`=1.10506.1`) encodes.
pub const DUCKDB_VERSION: &str = "v1.5.6";
pub const EXTENSION_DIR_ENV: &str = "ATLATL_EXTENSION_DIR";
pub const STATEMENT_TIMEOUT: &str = "statement_timeout";
pub const KILLED: &str = "killed";

/// How often an interrupted call is interrupted again: an interrupt only stops the statement
/// running at that moment, and a closure may start another.
const REINTERRUPT: Duration = Duration::from_millis(25);
/// Rust-fed appends are flushed to DuckDB in chunks of about this size.
const APPEND_FLUSH_BYTES: usize = 16 << 20;
/// Calls at least this long return freed allocator memory when they finish ([`alloc::release`]).
pub const RELEASE_AFTER: Duration = Duration::from_secs(1);
/// `memory_mode: low` with 2+ threads: DuckDB purges its jemalloc arenas after freeing more than
/// this at once. Its 512 MiB default rarely fires during a chunked merge; on top of the periodic
/// purge it saves another 50–70 MiB for 15–20% more time (spike S3 §6.2).
pub const BULK_FLUSH_THRESHOLD: &str = "4MiB";

/// `explicit`, else `$ATLATL_EXTENSION_DIR`, else `<exe>/../share/atlatl/extensions`.
pub fn resolve_extension_dir(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        return Ok(p.to_path_buf());
    }
    if let Some(p) = std::env::var_os(EXTENSION_DIR_ENV).filter(|p| !p.is_empty()) {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe()
        .map_err(|e| AtlatlError::config(format!("cannot locate the atlatl binary: {e}")))?;
    let bin = exe
        .parent()
        .ok_or_else(|| AtlatlError::config("cannot locate the atlatl binary directory"))?;
    Ok(bin.join("../share/atlatl/extensions"))
}

pub fn check_version(actual: &str) -> Result<()> {
    if actual == DUCKDB_VERSION {
        return Ok(());
    }
    Err(AtlatlError::config(format!(
        "libduckdb is {actual}, but atlatl is built for {DUCKDB_VERSION}; a different libduckdb \
         was loaded (check the rpath and that lib/atlatl ships with the binary)"
    ))
    .code("duckdb_version_mismatch"))
}

#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub budget: Budget,
    /// See [`resolve_extension_dir`].
    pub extension_dir: Option<PathBuf>,
    /// Blocking pool size; default `max(threads, 2)`.
    pub pool_size: Option<usize>,
}

impl EngineOptions {
    pub fn new(budget: Budget) -> Self {
        Self {
            budget,
            extension_dir: None,
            pool_size: None,
        }
    }
}

/// Per-call controls. The engine's kill token is always observed as well.
#[derive(Clone, Debug, Default)]
pub struct StmtOptions {
    pub timeout: Option<Duration>,
    pub cancel: Option<CancellationToken>,
}

impl StmtOptions {
    pub fn timeout(d: Duration) -> Self {
        Self {
            timeout: Some(d),
            cancel: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamOptions {
    pub stmt: StmtOptions,
    /// Batches buffered between DuckDB and the consumer, on top of the byte budget.
    pub capacity: usize,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            stmt: StmtOptions::default(),
            capacity: 8,
        }
    }
}

/// Engine calls in flight, and the periodic releases done while any was.
#[derive(Default)]
struct Activity {
    active: AtomicUsize,
    purges: AtomicU64,
}

/// Counts a call as in flight while alive.
struct Busy(Arc<Activity>);

impl Busy {
    fn new(a: &Arc<Activity>) -> Self {
        a.active.fetch_add(1, Ordering::SeqCst);
        Self(a.clone())
    }
}

impl Drop for Busy {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Releases every `every` while a call is in flight; stops when the sender drops.
fn spawn_purger(every: Duration, act: Arc<Activity>) -> Result<std::sync::mpsc::Sender<()>> {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::Builder::new()
        .name("atlatl-purge".into())
        .spawn(move || {
            while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(every) {
                if act.active.load(Ordering::SeqCst) > 0 {
                    alloc::release();
                    act.purges.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
        .map_err(|e| AtlatlError::internal(format!("cannot start the purge thread: {e}")))?;
    Ok(tx)
}

struct Inner {
    budget: Budget,
    extension_dir: PathBuf,
    pool_size: usize,
    blocking: Arc<Semaphore>,
    idle: Mutex<Vec<Connection>>,
    bytes: ByteBudget,
    kill: CancellationToken,
    activity: Arc<Activity>,
    _purger: Option<std::sync::mpsc::Sender<()>>,
    db: RawDb,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.idle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// Cheap to clone; every clone shares the one database.
#[derive(Clone)]
pub struct Engine(Arc<Inner>);

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("budget", &self.0.budget)
            .field("extension_dir", &self.0.extension_dir)
            .field("pool_size", &self.0.pool_size)
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Opens the database with the budget in force from the start. Blocking; call before work.
    pub fn open(opts: EngineOptions) -> Result<Self> {
        check_version(&library_version())?;
        let b = &opts.budget;
        let extension_dir = resolve_extension_dir(opts.extension_dir.as_deref())?;
        if let Some(parent) = b.temp_directory.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                AtlatlError::config(format!(
                    "cannot create temp_directory {}: {e}",
                    parent.display()
                ))
            })?;
        }
        let path = |p: &Path| {
            p.to_str()
                .map(str::to_owned)
                .ok_or_else(|| AtlatlError::config(format!("path is not UTF-8: {}", p.display())))
        };
        let mut settings: Vec<(&str, String)> = vec![
            ("memory_limit", format!("{}KiB", b.memory_limit / 1024)),
            ("threads", b.threads.to_string()),
            ("temp_directory", path(&b.temp_directory)?),
            ("preserve_insertion_order", "false".into()),
            ("autoinstall_known_extensions", "false".into()),
            ("autoload_known_extensions", "false".into()),
            ("allow_unsigned_extensions", "false".into()),
            ("allow_community_extensions", "false".into()),
            ("extension_directory", path(&extension_dir)?),
        ];
        if b.memory_mode == MemoryMode::Low && b.threads > 1 {
            settings.push((
                "allocator_bulk_deallocation_flush_threshold",
                BULK_FLUSH_THRESHOLD.into(),
            ));
        }
        if let Some(m) = b.max_temp_directory_size {
            settings.push(("max_temp_directory_size", format!("{}KiB", m / 1024)));
        }
        let db = RawDb::open(&settings)
            .map_err(|m| AtlatlError::config(format!("cannot open DuckDB: {m}")))?;
        let setup = db.connection().db()?;
        setup.execute_batch("SET GLOBAL TimeZone = 'UTC'").db()?;
        setup
            .register_scalar_function::<crate::json_canon::JsonCanon>(crate::json_canon::FUNCTION)
            .db()?;
        setup
            .register_scalar_function::<crate::json_exact::JsonExact>(crate::json_exact::FUNCTION)
            .db()?;
        setup
            .register_scalar_function::<crate::decimal_text::DecimalText>(
                crate::decimal_text::FUNCTION,
            )
            .db()?;
        let pool_size = opts
            .pool_size
            .unwrap_or_else(|| (b.threads as usize).max(2))
            .max(1);
        let activity = Arc::<Activity>::default();
        let purger = b
            .purge_every_ms
            .map(|ms| spawn_purger(Duration::from_millis(ms.max(1)), activity.clone()))
            .transpose()?;
        Ok(Self(Arc::new(Inner {
            bytes: ByteBudget::new(b.channel_bytes),
            budget: opts.budget,
            extension_dir,
            pool_size,
            blocking: Arc::new(Semaphore::new(pool_size)),
            idle: Mutex::new(vec![setup]),
            kill: CancellationToken::new(),
            activity,
            _purger: purger,
            db,
        })))
    }

    pub fn budget(&self) -> &Budget {
        &self.0.budget
    }

    pub fn extension_dir(&self) -> &Path {
        &self.0.extension_dir
    }

    pub fn pool_size(&self) -> usize {
        self.0.pool_size
    }

    pub fn byte_budget(&self) -> &ByteBudget {
        &self.0.bytes
    }

    /// Periodic releases this engine has done during calls (`memory_mode: low`).
    pub fn purges(&self) -> u64 {
        self.0.activity.purges.load(Ordering::Relaxed)
    }

    /// Cancelling it interrupts every running call with `killed`, and fails new ones.
    pub fn kill_token(&self) -> &CancellationToken {
        &self.0.kill
    }

    pub fn kill(&self) {
        self.0.kill.cancel();
    }

    /// Runs `f` on a pooled connection on the blocking pool. The connection goes back to the pool
    /// only if `f` succeeded, so `f` must leave no open transaction or temp objects behind; use a
    /// [`Session`] for work that spans calls.
    pub async fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.with_conn_opts(&StmtOptions::default(), f).await
    }

    pub async fn with_conn_opts<T, F>(&self, opts: &StmtOptions, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.take_conn()?;
        let (conn, r) = run(&self.0, conn, opts, f).await;
        if let (Some(c), Ok(_)) = (conn, &r) {
            let mut idle = self.0.idle.lock().unwrap_or_else(PoisonError::into_inner);
            if idle.len() < self.0.pool_size {
                idle.push(c);
            }
        }
        r
    }

    /// A dedicated connection, for temp tables and transactions that span calls.
    pub fn session(&self) -> Result<Session> {
        Ok(Session {
            engine: self.clone(),
            conn: Some(self.0.db.connection().db()?),
            cancel: None,
        })
    }

    /// A session whose calls are interrupted when `cancel` fires, without touching other
    /// sessions (unlike [`Engine::kill`]).
    pub fn session_with_cancel(&self, cancel: CancellationToken) -> Result<Session> {
        let mut s = self.session()?;
        s.cancel = Some(cancel);
        Ok(s)
    }

    /// Streams `sql` as Arrow batches on its own connection (it sees attached catalogs and
    /// committed data, not another session's temp tables). Prepare and bind errors are returned
    /// here; later errors arrive as the stream's last item. The query holds a blocking-pool slot
    /// until it finishes or the stream is dropped.
    pub async fn stream(&self, sql: &str, opts: StreamOptions) -> Result<BatchStream> {
        let permit = acquire(&self.0, &opts.stmt).await?;
        let raw = Arc::new(self.0.db.raw_connection().map_err(AtlatlError::internal)?);
        let (tx, rx) = mpsc::channel(opts.capacity.max(1));
        let (schema_tx, schema_rx) = oneshot::channel();
        let cause = Arc::new(AtomicU8::new(0));
        let done = CancellationToken::new();
        let stop = CancellationToken::new();

        tokio::spawn(watch(
            raw.clone(),
            opts.stmt.clone(),
            self.0.kill.clone(),
            cause.clone(),
            done.clone(),
            stop.clone(),
            tx.clone(),
        ));

        let rt = tokio::runtime::Handle::current();
        let bytes = self.0.bytes.clone();
        let sql = sql.to_owned();
        let timeout = opts.stmt.timeout;
        let task_done = done.clone();
        let activity = self.0.activity.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _busy = Busy::new(&activity);
            let _done = task_done.drop_guard();
            let mut schema_tx = Some(schema_tx);
            let mut stopped = false;
            let started = std::time::Instant::now();
            let r = raw.stream(
                &sql,
                |s| {
                    if let Some(t) = schema_tx.take() {
                        let _ = t.send(Ok(s));
                    }
                },
                |b| {
                    let sent = rt.block_on(async {
                        tokio::select! {
                            biased;
                            _ = stop.cancelled() => None,
                            b = bytes.reserve(b) => tokio::select! {
                                biased;
                                _ = stop.cancelled() => None,
                                r = tx.send(Ok(b)) => Some(r.is_ok()),
                            },
                        }
                    });
                    match sent {
                        Some(true) => ControlFlow::Continue(()),
                        Some(false) => ControlFlow::Break(()),
                        None => {
                            stopped = true;
                            ControlFlow::Break(())
                        }
                    }
                },
            );
            if started.elapsed() >= RELEASE_AFTER {
                alloc::release();
            }
            let r = match r {
                Ok(_) if stopped => Err("INTERRUPT Error: Interrupted!".to_owned()),
                r => r,
            };
            if let Err(m) = r {
                let e = with_cause(from_message(&m), cause.load(Ordering::SeqCst), timeout);
                match schema_tx.take() {
                    Some(t) => {
                        let _ = t.send(Err(e));
                    }
                    None => {
                        let _ = tx.blocking_send(Err(e));
                    }
                }
            }
        });

        let schema = schema_rx
            .await
            .map_err(|_| AtlatlError::internal("engine stream task ended without a result"))??;
        Ok(BatchStream { schema, rx })
    }

    /// DuckDB's tracked memory (buffer manager), for the run report.
    pub async fn memory_usage(&self) -> Result<u64> {
        self.with_conn(|c| {
            c.query_row(
                "SELECT coalesce(sum(memory_usage_bytes), 0)::UBIGINT FROM duckdb_memory()",
                [],
                |r| r.get(0),
            )
            .db()
        })
        .await
    }

    fn take_conn(&self) -> Result<Connection> {
        let pooled = self
            .0
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        match pooled {
            Some(c) => Ok(c),
            None => self.0.db.connection().db(),
        }
    }
}

/// A connection leased for a sequence of calls. Temp tables and transactions persist across
/// [`Session::run`] calls; dropping the session closes the connection.
pub struct Session {
    engine: Engine,
    conn: Option<Connection>,
    /// Default cancellation for every call (a unit's token), unless the call's options set one.
    cancel: Option<CancellationToken>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("open", &self.conn.is_some())
            .finish()
    }
}

impl Session {
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The token that interrupts this session's calls, if it has one of its own.
    pub fn cancel_token(&self) -> Option<&CancellationToken> {
        self.cancel.as_ref()
    }

    pub async fn run<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.run_opts(&StmtOptions::default(), f).await
    }

    /// A timed-out or killed call leaves the session usable (DuckDB aborts the statement only).
    pub async fn run_opts<T, F>(&mut self, opts: &StmtOptions, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.conn.take().ok_or_else(|| {
            AtlatlError::internal("the session's connection was lost by an abandoned call")
        })?;
        let merged;
        let opts = match (&opts.cancel, &self.cancel) {
            (None, Some(c)) => {
                merged = StmtOptions {
                    cancel: Some(c.clone()),
                    ..opts.clone()
                };
                &merged
            }
            _ => opts,
        };
        let (conn, r) = run(&self.engine.0, conn, opts, f).await;
        self.conn = conn;
        r
    }

    /// Appends batches from `rx` into `temp.main.<table>` until the channel closes. Whatever has
    /// arrived (up to about 16 MiB) is appended per call, so the blocking slot is held only while
    /// appending, never while waiting on the producer. An `Err` item stops
    /// the load and is returned; rows already appended stay (the staging table is disposable).
    pub async fn append(
        &mut self,
        table: &str,
        mut rx: mpsc::Receiver<Result<Batch>>,
    ) -> Result<u64> {
        let mut rows = 0u64;
        while let Some(first) = rx.recv().await {
            let mut buf = vec![first?];
            let mut bytes = buf[0].get_array_memory_size();
            while bytes < APPEND_FLUSH_BYTES {
                match rx.try_recv() {
                    Ok(b) => {
                        let b = b?;
                        bytes += b.get_array_memory_size();
                        buf.push(b);
                    }
                    Err(_) => break,
                }
            }
            let t = table.to_owned();
            rows += self
                .run(move |c| append_temp(c, &t, buf.into_iter().map(Batch::into_inner)))
                .await?;
        }
        Ok(rows)
    }
}

/// Appends into `temp.main.<table>` through the Arrow appender.
pub fn append_temp(
    conn: &Connection,
    table: &str,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<u64> {
    let mut app = conn
        .appender_to_catalog_and_db(table, "temp", "main")
        .db()?;
    let mut rows = 0u64;
    for b in batches {
        rows += b.num_rows() as u64;
        app.append_record_batch(b).db()?;
    }
    app.flush().db()?;
    Ok(rows)
}

const CAUSE_TIMEOUT: u8 = 1;
const CAUSE_KILLED: u8 = 2;

async fn acquire(inner: &Inner, opts: &StmtOptions) -> Result<OwnedSemaphorePermit> {
    let sem = inner.blocking.clone();
    tokio::select! {
        biased;
        _ = cancelled(&inner.kill, opts.cancel.as_ref()) => Err(killed_error()),
        p = sem.acquire_owned() => Ok(p.expect("blocking semaphore is never closed")),
    }
}

async fn cancelled(kill: &CancellationToken, extra: Option<&CancellationToken>) {
    match extra {
        Some(t) => tokio::select! {
            _ = kill.cancelled() => {},
            _ = t.cancelled() => {},
        },
        None => kill.cancelled().await,
    }
}

async fn deadline(at: Option<Instant>) {
    match at {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

fn killed_error() -> AtlatlError {
    AtlatlError::new(ErrorClass::Killed, "cancelled").code(KILLED)
}

fn with_cause(e: AtlatlError, cause: u8, timeout: Option<Duration>) -> AtlatlError {
    match cause {
        CAUSE_TIMEOUT => AtlatlError::new(
            ErrorClass::Timeout,
            format!(
                "statement timed out after {:?}: {}",
                timeout.unwrap_or_default(),
                e.message
            ),
        )
        .code(STATEMENT_TIMEOUT),
        CAUSE_KILLED => {
            AtlatlError::new(ErrorClass::Killed, format!("killed: {}", e.message)).code(KILLED)
        }
        _ => e,
    }
}

/// Interrupts `raw` on deadline, kill, or the consumer going away, until `done`.
async fn watch(
    raw: Arc<RawConn>,
    opts: StmtOptions,
    kill: CancellationToken,
    cause: Arc<AtomicU8>,
    done: CancellationToken,
    stop: CancellationToken,
    tx: mpsc::Sender<Result<Batch>>,
) {
    let at = opts.timeout.map(|d| Instant::now() + d);
    let c = tokio::select! {
        _ = done.cancelled() => return,
        _ = deadline(at) => CAUSE_TIMEOUT,
        _ = cancelled(&kill, opts.cancel.as_ref()) => CAUSE_KILLED,
        _ = tx.closed() => 0,
    };
    drop(tx);
    cause.store(c, Ordering::SeqCst);
    stop.cancel();
    loop {
        raw.interrupt();
        tokio::select! {
            _ = done.cancelled() => return,
            _ = tokio::time::sleep(REINTERRUPT) => {}
        }
    }
}

/// Interrupts the connection if the caller abandons the call.
struct InterruptOnDrop(Option<Arc<duckdb::InterruptHandle>>);

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.interrupt();
        }
    }
}

async fn run<T, F>(
    inner: &Arc<Inner>,
    conn: Connection,
    opts: &StmtOptions,
    f: F,
) -> (Option<Connection>, Result<T>)
where
    F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let permit = match acquire(inner, opts).await {
        Ok(p) => p,
        Err(e) => return (Some(conn), Err(e)),
    };
    let handle = conn.interrupt_handle();
    let mut guard = InterruptOnDrop(Some(handle.clone()));
    let activity = inner.activity.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _busy = Busy::new(&activity);
        let mut conn = conn;
        let started = std::time::Instant::now();
        let r = f(&mut conn);
        if started.elapsed() >= RELEASE_AFTER {
            alloc::release();
        }
        (conn, r)
    });
    let at = opts.timeout.map(|d| Instant::now() + d);
    let mut cause = 0u8;
    let joined = loop {
        tokio::select! {
            biased;
            j = &mut task => break j,
            _ = deadline(at), if cause == 0 => {
                cause = CAUSE_TIMEOUT;
                handle.interrupt();
            }
            _ = cancelled(&inner.kill, opts.cancel.as_ref()), if cause == 0 => {
                cause = CAUSE_KILLED;
                handle.interrupt();
            }
            _ = tokio::time::sleep(REINTERRUPT), if cause != 0 => handle.interrupt(),
        }
    };
    guard.0 = None;
    match joined {
        Ok((conn, r)) => (
            Some(conn),
            r.map_err(|e| with_cause(e, cause, opts.timeout)),
        ),
        Err(e) => (None, Err(join_error(e))),
    }
}

/// A panic in `f` is reported like one at the replication boundary (03 §4, exit 3).
fn join_error(e: tokio::task::JoinError) -> AtlatlError {
    if !e.is_panic() {
        return AtlatlError::internal(format!("engine task failed: {e}"));
    }
    let p = e.into_panic();
    let msg = p
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".into());
    AtlatlError::internal(format!("panic: {msg}")).code(atlatl_core::obs::report::PANIC_CODE)
}
