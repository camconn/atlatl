//! Session-level advisory locks (spec 03 §8).

use std::time::Duration;

use atlatl_core::error::Result;
use atlatl_core::lock::{
    BoxFuture, LockGuard, LockKey, LockManager, acquire_in_order, acquisition_order,
    lock_timeout_error,
};
use atlatl_core::redact::redact;
use tokio_postgres::Client;

use crate::connect::{ConnectOptions, Session, connect};
use crate::error::wrap;

const TRY_LOCK: &str = "SELECT pg_try_advisory_lock(hashtextextended($1, 0))";

const HOLDER: &str = "\
SELECT a.pid, coalesce(a.application_name, ''), host(a.client_addr)
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
CROSS JOIN (SELECT hashtextextended($1, 0) AS h) k
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
  AND l.pid <> pg_backend_pid()
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND l.classid::bigint = (k.h >> 32) & 4294967295
  AND l.objid::bigint = k.h & 4294967295
LIMIT 1";

/// Advisory locks on a dedicated session per acquisition. Cheap to clone.
#[derive(Clone, Debug)]
pub struct PgAdvisoryLocks {
    opts: ConnectOptions,
}

impl PgAdvisoryLocks {
    pub fn new(opts: ConnectOptions) -> Self {
        Self { opts }
    }

    pub async fn lock(&self, keys: &[LockKey], timeout: Duration) -> Result<PgLockGuard> {
        let order = acquisition_order(keys);
        let session = connect(&self.opts).await?;
        let c = session.client();
        let pid: i32 = c
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(wrap)?
            .get(0);
        let stmt = c.prepare(TRY_LOCK).await.map_err(wrap)?;
        let res = acquire_in_order(&order, timeout, |k| {
            let name = k.name();
            let stmt = &stmt;
            async move {
                c.query_one(stmt, &[&name])
                    .await
                    .map(|r| r.get::<_, bool>(0))
                    .map_err(wrap)
            }
        })
        .await;
        match res {
            Ok(()) => {
                tracing::debug!(pid, keys = ?names(&order), "advisory locks acquired");
                Ok(PgLockGuard {
                    session,
                    keys: order,
                    pid,
                })
            }
            Err(s) => {
                let key = &order[s.taken];
                let err = if s.timed_out {
                    let holder = holder(c, &key.name()).await;
                    lock_timeout_error(key, s.waited, holder.as_deref())
                } else {
                    s.error
                };
                if s.taken > 0 {
                    let _ = c.batch_execute("SELECT pg_advisory_unlock_all()").await;
                }
                Err(err)
            }
        }
    }

    pub async fn lock_group(&self, name: &str, timeout: Duration) -> Result<PgLockGuard> {
        self.lock(&[LockKey::group(name)], timeout).await
    }
}

impl LockManager for PgAdvisoryLocks {
    fn acquire<'a>(
        &'a self,
        keys: &'a [LockKey],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn LockGuard>>> {
        Box::pin(async move { Ok(Box::new(self.lock(keys, timeout).await?) as Box<dyn LockGuard>) })
    }
}

fn names(keys: &[LockKey]) -> Vec<String> {
    keys.iter().map(LockKey::name).collect()
}

/// Best effort: `pid N, application_name `x`, client 10.0.0.1`.
async fn holder(c: &Client, name: &str) -> Option<String> {
    let row = c.query_opt(HOLDER, &[&name]).await.ok()??;
    let pid: i32 = row.get(0);
    let app: String = row.get(1);
    let addr: Option<String> = row.get(2);
    let mut s = format!("pid {pid}");
    if !app.is_empty() {
        s.push_str(&format!(", application_name `{}`", redact(&app)));
    }
    if let Some(a) = addr {
        s.push_str(&format!(", client {a}"));
    }
    Some(s)
}

/// Holds the lock session. Dropping it ends the session, which releases every lock.
#[derive(Debug)]
pub struct PgLockGuard {
    session: Session,
    keys: Vec<LockKey>,
    pid: i32,
}

impl PgLockGuard {
    pub fn keys(&self) -> &[LockKey] {
        &self.keys
    }

    /// The lock session's backend pid.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Fails if the session is gone, in which case the locks are too.
    pub async fn check(&self) -> Result<()> {
        self.session
            .client()
            .simple_query("SELECT 1")
            .await
            .map(drop)
            .map_err(|e| {
                let mut err = wrap(e);
                err.message = format!("lock session lost: {}", err.message);
                err
            })
    }

    /// A session that already ended has released everything, so that's `Ok`.
    pub async fn release(self) -> Result<()> {
        if self.session.is_closed() {
            return Ok(());
        }
        self.session
            .client()
            .batch_execute("SELECT pg_advisory_unlock_all()")
            .await
            .map_err(wrap)
    }
}

impl LockGuard for PgLockGuard {
    fn keys(&self) -> &[LockKey] {
        &self.keys
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(PgLockGuard::check(self))
    }

    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(PgLockGuard::release(*self))
    }
}
