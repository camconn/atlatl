//! `access: wire`: the same SQL over the Postgres protocol (`simple_query`), for environments
//! without Data API access (06 §4).

use std::time::Duration;

use atlatl_conn_postgres::{ConnectOptions, Keepalive, Pool, PoolOptions, connect_with};
use atlatl_core::config::model::{SslMode, WireConfig};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::retry::{AttemptRecord, Jitter, RandomJitter, RetryLevel, RetryPolicy};
use futures::StreamExt;
use tokio::time::Instant;
use tokio_postgres::SimpleQueryMessage;
use tokio_util::sync::CancellationToken;

use crate::error::classify_failed;
use crate::export::{
    Batcher, ExportOptions, ExportRequest, Exported, KeyValues, SESSION_SQL, Sink,
};
use crate::result::{Executed, Field, ResultSet};

/// How one export read ended short.
enum Short {
    /// The connection or statement failed; the read may resume.
    Read(AtlatlError),
    /// No data for `stall`; the backend may still be alive. Resumable.
    Stalled(AtlatlError),
    /// Killed, timed out, or the sink failed: not retried.
    Stop(AtlatlError),
}

pub const DEFAULT_PORT: u16 = 5439;

/// Cheap to clone: clones share the pool.
#[derive(Clone)]
pub struct Wire {
    pool: Pool,
    statement_timeout: Duration,
}

impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wire").field("pool", &self.pool).finish()
    }
}

pub fn connect_options(w: &WireConfig) -> Result<ConnectOptions> {
    let password = match &w.password {
        None => None,
        Some(SecretValue::Literal(s)) => Some(s.clone()),
        Some(SecretValue::Ref(_)) => {
            return Err(AtlatlError::internal(
                "wire.password was not resolved before connecting",
            ));
        }
    };
    let mut o = ConnectOptions::new(
        &w.host,
        w.port.unwrap_or(DEFAULT_PORT),
        &w.database,
        &w.user,
        password,
    );
    o.sslmode = w.sslmode.unwrap_or(SslMode::Require);
    o.keepalive = Some(Keepalive::from_config(w.keepalives.as_ref()));
    Ok(o)
}

impl Wire {
    pub fn new(
        opts: ConnectOptions,
        max_sessions: usize,
        statement_timeout: Duration,
    ) -> Result<Self> {
        let pool = Pool::new(
            opts,
            PoolOptions {
                max_size: max_sessions.max(1),
                // Liveness check on every checkout: members are minutes apart.
                ping_after_idle: Duration::ZERO,
                ..PoolOptions::default()
            },
        )?;
        Ok(Self {
            pool,
            statement_timeout,
        })
    }

    async fn run(&self, sql: &str, cancel: &CancellationToken) -> Result<Vec<SimpleQueryMessage>> {
        let s = self.pool.get().await?;
        let pid: i32 = s
            .client()
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(wrap)?
            .get(0);
        let q = s.client().simple_query(sql);
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ErrorClass::Killed),
            _ = tokio::time::sleep(self.statement_timeout) => Err(ErrorClass::Timeout),
            r = q => Ok(r),
        };
        match outcome {
            Ok(r) => r.map_err(wrap),
            Err(class) => {
                let how = self.cancel_backend(pid).await;
                s.discard();
                Err(AtlatlError::new(
                    class,
                    format!("Redshift statement cancelled on backend {pid} ({how})"),
                ))
            }
        }
    }

    /// `pg_cancel_backend` from a side connection.
    async fn cancel_backend(&self, pid: i32) -> String {
        let o = self.pool.options();
        let side = async {
            let tls = o.tls()?;
            let s = connect_with(o, tls.as_ref()).await?;
            // Postgres returns a boolean, Redshift an integer.
            let msgs = s
                .client()
                .simple_query(&format!("SELECT pg_cancel_backend({pid})"))
                .await
                .map_err(wrap)?;
            let ok = msgs.iter().any(|m| match m {
                SimpleQueryMessage::Row(r) => matches!(r.get(0), Some("t" | "true" | "1")),
                _ => false,
            });
            Ok::<_, AtlatlError>(ok)
        };
        match tokio::time::timeout(Duration::from_secs(10), side).await {
            Ok(Ok(ok)) => format!("pg_cancel_backend: {ok}"),
            Ok(Err(e)) => format!("pg_cancel_backend failed: {e}"),
            Err(_) => "pg_cancel_backend timed out".into(),
        }
    }

    /// Streams `req` into `sink`. A read that fails `retry`-ably resumes after the last key group
    /// handed over (or starts over without a key); `policy.max_attempts` counts consecutive
    /// failures without progress.
    pub async fn export<S: Sink>(
        &self,
        req: &ExportRequest,
        opts: &ExportOptions,
        policy: &RetryPolicy,
        sink: &mut S,
        cancel: &CancellationToken,
    ) -> Result<Exported> {
        let key = req.key_positions();
        let deadline = Instant::now() + self.statement_timeout;
        let mut out = Exported::default();
        let mut last: Option<KeyValues> = None;
        let mut jitter = RandomJitter::new();
        let mut attempt = 1u32;
        loop {
            let sql = req.sql(last.as_deref());
            let before = out.rows;
            let mut b = Batcher::new(&req.columns, key.clone());
            let e = match self
                .read(
                    &sql, &mut b, opts, sink, &mut last, &mut out, deadline, cancel,
                )
                .await
            {
                Ok(()) => return Ok(out),
                Err(Short::Stop(e)) => return Err(e),
                Err(Short::Read(e) | Short::Stalled(e)) => e,
            };
            if key.is_some() && out.rows > before {
                attempt = 1;
            }
            if !policy.retry_on.contains(&e.class) {
                return Err(e);
            }
            if attempt >= policy.max_attempts {
                return Err(AtlatlError {
                    message: format!("{} (after {attempt} attempts)", e.message),
                    ..e
                });
            }
            let wait = policy.wait_for(&e, attempt, jitter.factor());
            tracing::warn!(attempt, rows = out.rows, error = %e, "Redshift export dropped; {}", if key.is_some() { "resuming" } else { "starting over" });
            out.retries.push(AttemptRecord {
                level: RetryLevel::Query,
                attempt,
                class: e.class,
                message: e.redacted_message(),
                wait_ms: wait.as_millis() as u64,
            });
            if key.is_some() {
                out.resumes += 1;
            } else {
                sink.reset().await?;
                out.rows = 0;
                out.bytes = 0;
                out.restarts += 1;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(AtlatlError::new(ErrorClass::Killed, "Redshift export killed")),
                _ = tokio::time::sleep(wait) => {}
            }
            attempt += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read<S: Sink>(
        &self,
        sql: &str,
        b: &mut Batcher,
        opts: &ExportOptions,
        sink: &mut S,
        last: &mut Option<KeyValues>,
        out: &mut Exported,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), Short> {
        let s = self.pool.get().await.map_err(Short::Read)?;
        let setup = async {
            let pid: i32 = s
                .client()
                .query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get(0);
            s.client().batch_execute(SESSION_SQL).await?;
            Ok::<_, tokio_postgres::Error>(pid)
        };
        let pid = match setup.await {
            Ok(p) => p,
            Err(e) => {
                s.discard();
                return Err(Short::Read(wrap(e)));
            }
        };
        let mut hand_over = async |b: &mut Batcher, all: bool, out: &mut Exported| {
            if let Some((batch, k)) = b.take(all).map_err(Short::Stop)? {
                let n = batch.num_rows() as u64;
                sink.append(batch).await.map_err(Short::Stop)?;
                out.rows += n;
                *last = k;
            }
            Ok::<_, Short>(())
        };
        let stream = match s
            .client()
            .query_raw(
                sql,
                std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>(),
            )
            .await
        {
            Ok(st) => st,
            Err(e) => {
                s.discard();
                return Err(Short::Read(wrap(e)));
            }
        };
        let mut stream = std::pin::pin!(stream);
        let batch_rows = opts.batch_rows.max(1);
        let mut flowing = false;
        let ended = loop {
            let stall = async {
                if flowing {
                    tokio::time::sleep(opts.stall).await
                } else {
                    std::future::pending().await
                }
            };
            let m = tokio::select! {
                biased;
                _ = cancel.cancelled() => break Err(Short::Stop(AtlatlError::new(ErrorClass::Killed, "Redshift export killed"))),
                _ = tokio::time::sleep_until(deadline) => break Err(Short::Stop(AtlatlError::new(ErrorClass::Timeout, "Redshift export hit the statement timeout"))),
                _ = stall => break Err(Short::Stalled(AtlatlError::transient(format!(
                    "Redshift export received no data for {}s", opts.stall.as_secs_f64()
                )).code("export_stalled"))),
                m = stream.next() => m,
            };
            flowing = true;
            match m {
                None => break Ok(()),
                Some(Ok(r)) => {
                    if let Err(e) = b.push(r) {
                        break Err(Short::Stop(e));
                    }
                    if b.len() >= batch_rows
                        && let Err(e) = hand_over(b, false, out).await
                    {
                        break Err(e);
                    }
                }
                Some(Err(e)) => break Err(Short::Read(wrap(e))),
            }
        };
        out.bytes += b.bytes;
        match ended {
            Ok(()) => {
                hand_over(b, true, out).await?;
                Ok(())
            }
            Err(short @ (Short::Read(_) | Short::Stalled(_))) => {
                // Complete key groups received before the drop are kept; the trailing one may be cut.
                let kept = if b.has_key() {
                    hand_over(b, false, out).await
                } else {
                    Ok(())
                };
                b.clear();
                if matches!(short, Short::Stalled(_)) {
                    let how = self.cancel_backend(pid).await;
                    tracing::warn!(backend = pid, "stalled Redshift export: {how}");
                }
                s.discard();
                kept.and(Err(short))
            }
            Err(Short::Stop(e)) => {
                let how = self.cancel_backend(pid).await;
                s.discard();
                Err(Short::Stop(AtlatlError {
                    message: format!("{} (backend {pid}, {how})", e.message),
                    ..e
                }))
            }
        }
    }

    pub async fn execute(&self, sql: &str, cancel: &CancellationToken) -> Result<Executed> {
        let msgs = self.run(sql, cancel).await?;
        let rows = msgs.iter().rev().find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(*n as i64),
            _ => None,
        });
        Ok(Executed {
            rows,
            has_result_set: msgs.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))),
            ..Executed::default()
        })
    }

    pub async fn query(&self, sql: &str, cancel: &CancellationToken) -> Result<ResultSet> {
        let mut out = ResultSet::default();
        for m in self.run(sql, cancel).await? {
            match m {
                SimpleQueryMessage::RowDescription(cols) if out.columns.is_empty() => {
                    out.columns = cols.iter().map(|c| c.name().to_owned()).collect();
                }
                SimpleQueryMessage::Row(r) => {
                    if out.columns.is_empty() {
                        out.columns = r.columns().iter().map(|c| c.name().to_owned()).collect();
                    }
                    out.rows.push(
                        (0..r.len())
                            .map(|i| match r.get(i) {
                                Some(v) => Field::String(v.to_owned()),
                                None => Field::Null(true),
                            })
                            .collect(),
                    );
                }
                _ => {}
            }
        }
        Ok(out)
    }
}

/// Postgres classification, refined by the Redshift statement texts (a non-empty UNLOAD prefix).
fn wrap(e: tokio_postgres::Error) -> AtlatlError {
    let base = atlatl_conn_postgres::classify(&e);
    let (class, code) = classify_failed(&base.message);
    match code {
        Some(c) => AtlatlError { class, ..base }.code(c),
        None => base,
    }
}
