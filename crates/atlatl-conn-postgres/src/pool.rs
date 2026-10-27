//! A small health-checked session pool (spec 06 §3.1).

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use rustls::ClientConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::connect::{ConnectOptions, Session, connect_with};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolOptions {
    pub max_size: usize,
    /// How long [`Pool::get`] waits for a free slot. Connecting is bounded by `connect_timeout`.
    pub checkout_timeout: Duration,
    /// Idle sessions older than this are pinged on checkout; `ZERO` pings every time.
    pub ping_after_idle: Duration,
    pub ping_timeout: Duration,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            max_size: 4,
            checkout_timeout: Duration::from_secs(30),
            ping_after_idle: Duration::from_secs(10),
            ping_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStatus {
    pub idle: usize,
    pub in_use: usize,
    pub max_size: usize,
}

/// Cheap to clone. Sessions return to the pool on drop unless they are closed.
#[derive(Clone)]
pub struct Pool(Arc<Inner>);

struct Inner {
    opts: ConnectOptions,
    tls: Option<Arc<ClientConfig>>,
    cfg: PoolOptions,
    idle: Mutex<Vec<Idle>>,
    slots: Arc<Semaphore>,
}

struct Idle {
    session: Session,
    since: Instant,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("opts", &self.0.opts)
            .field("cfg", &self.0.cfg)
            .field("status", &self.status())
            .finish()
    }
}

impl Pool {
    pub fn new(opts: ConnectOptions, cfg: PoolOptions) -> Result<Self> {
        if cfg.max_size == 0 {
            return Err(AtlatlError::config("pool max_size must be at least 1"));
        }
        let tls = opts.tls()?;
        Ok(Self(Arc::new(Inner {
            opts,
            tls,
            cfg,
            idle: Mutex::new(Vec::new()),
            slots: Arc::new(Semaphore::new(cfg.max_size)),
        })))
    }

    pub fn options(&self) -> &ConnectOptions {
        &self.0.opts
    }

    pub fn status(&self) -> PoolStatus {
        PoolStatus {
            idle: self.idle().len(),
            in_use: self.0.cfg.max_size - self.0.slots.available_permits(),
            max_size: self.0.cfg.max_size,
        }
    }

    fn idle(&self) -> std::sync::MutexGuard<'_, Vec<Idle>> {
        self.0.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The most recently returned healthy session, or a new one. Dead idle sessions are dropped.
    pub async fn get(&self) -> Result<PooledSession> {
        let t = self.0.cfg.checkout_timeout;
        let permit = tokio::time::timeout(t, self.0.slots.clone().acquire_owned())
            .await
            .map_err(|_| {
                AtlatlError::new(
                    ErrorClass::Timeout,
                    format!(
                        "no postgres session free after {t:?} (pool of {} for {}:{})",
                        self.0.cfg.max_size, self.0.opts.host, self.0.opts.port
                    ),
                )
            })?
            .map_err(|_| AtlatlError::internal("pool semaphore closed"))?;
        loop {
            let Some(Idle { session, since }) = self.idle().pop() else {
                break;
            };
            if session.is_closed() {
                tracing::debug!("dropping closed pooled postgres session");
                continue;
            }
            if since.elapsed() >= self.0.cfg.ping_after_idle && !self.ping(&session).await {
                tracing::debug!("dropping pooled postgres session that failed its ping");
                session.abort();
                continue;
            }
            return Ok(self.wrap(session, permit));
        }
        let session = connect_with(&self.0.opts, self.0.tls.as_ref()).await?;
        Ok(self.wrap(session, permit))
    }

    async fn ping(&self, s: &Session) -> bool {
        matches!(
            tokio::time::timeout(self.0.cfg.ping_timeout, s.client().simple_query("")).await,
            Ok(Ok(_))
        )
    }

    fn wrap(&self, session: Session, permit: OwnedSemaphorePermit) -> PooledSession {
        PooledSession {
            session: Some(session),
            pool: self.clone(),
            _permit: permit,
        }
    }
}

/// Derefs to [`Session`].
pub struct PooledSession {
    session: Option<Session>,
    pool: Pool,
    _permit: OwnedSemaphorePermit,
}

impl PooledSession {
    /// Closes the session instead of returning it, e.g. after an error left it in doubt.
    pub fn discard(mut self) {
        if let Some(s) = self.session.take() {
            s.abort();
        }
    }
}

impl std::fmt::Debug for PooledSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledSession")
            .field("session", &self.session)
            .finish()
    }
}

impl Deref for PooledSession {
    type Target = Session;

    fn deref(&self) -> &Session {
        self.session.as_ref().expect("session present until drop")
    }
}

impl DerefMut for PooledSession {
    fn deref_mut(&mut self) -> &mut Session {
        self.session.as_mut().expect("session present until drop")
    }
}

impl Drop for PooledSession {
    fn drop(&mut self) {
        if let Some(session) = self.session.take()
            && !session.is_closed()
        {
            self.pool.idle().push(Idle {
                session,
                since: Instant::now(),
            });
        }
    }
}
