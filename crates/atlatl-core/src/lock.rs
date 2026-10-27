//! Target-table and group locks (spec 03 §8).
//!
//! Keys are always taken in [`acquisition_order`], so managers that share a backend can't
//! deadlock on overlapping sets.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

use crate::error::{AtlatlError, Result};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// `code` on the `conflict` error returned when `lock_timeout` elapses.
pub const LOCK_TIMEOUT_CODE: &str = "lock_timeout";

/// A lock's identity is its [`name`](LockKey::name): keys with equal names are the same lock.
#[derive(Clone, Debug)]
pub enum LockKey {
    Table {
        conn: String,
        schema: String,
        table: String,
    },
    Group(String),
}

impl LockKey {
    pub fn table(
        conn: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self::Table {
            conn: conn.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self::Group(name.into())
    }

    /// `atlatl:<conn>:<schema>.<table>` or `atlatl-group:<name>`.
    pub fn name(&self) -> String {
        match self {
            Self::Table {
                conn,
                schema,
                table,
            } => format!("atlatl:{conn}:{schema}.{table}"),
            Self::Group(n) => format!("atlatl-group:{n}"),
        }
    }
}

impl fmt::Display for LockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

impl PartialEq for LockKey {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for LockKey {}

impl Hash for LockKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl PartialOrd for LockKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LockKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name().cmp(&other.name())
    }
}

/// Sorted by name, deduplicated.
pub fn acquisition_order<'a>(keys: impl IntoIterator<Item = &'a LockKey>) -> Vec<LockKey> {
    keys.into_iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub const POLL_MIN: Duration = Duration::from_millis(10);
pub const POLL_MAX: Duration = Duration::from_secs(1);

/// Wait before retry `attempt` (0-based) of a try-lock: doubling from [`POLL_MIN`], capped at
/// [`POLL_MAX`].
pub fn poll_delay(attempt: u32) -> Duration {
    POLL_MIN
        .checked_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX))
        .map_or(POLL_MAX, |d| d.min(POLL_MAX))
}

pub fn lock_timeout_error(key: &LockKey, waited: Duration, holder: Option<&str>) -> AtlatlError {
    let mut m = format!(
        "timed out after {:.1}s waiting for lock `{key}`",
        waited.as_secs_f64()
    );
    if let Some(h) = holder {
        m.push_str(&format!(" (held by {h})"));
    }
    AtlatlError::conflict(m).code(LOCK_TIMEOUT_CODE)
}

/// Why [`acquire_in_order`] stopped: `order[taken]` is the key it was waiting for.
#[derive(Debug)]
pub struct Stalled {
    pub taken: usize,
    /// Set when `lock_timeout` elapsed rather than `try_lock` failing.
    pub timed_out: bool,
    pub waited: Duration,
    pub error: AtlatlError,
}

/// Polls `try_lock` for each key in turn until it returns `true`, with [`poll_delay`] between
/// attempts. `timeout` bounds the whole set; every key gets at least one attempt.
pub async fn acquire_in_order<F, Fut>(
    order: &[LockKey],
    timeout: Duration,
    mut try_lock: F,
) -> Result<(), Box<Stalled>>
where
    F: FnMut(&LockKey) -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let deadline = start.checked_add(timeout);
    for (i, key) in order.iter().enumerate() {
        let mut attempt = 0;
        loop {
            match try_lock(key).await {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    return Err(Box::new(Stalled {
                        taken: i,
                        timed_out: false,
                        waited: start.elapsed(),
                        error,
                    }));
                }
            }
            let now = Instant::now();
            let delay = poll_delay(attempt);
            let delay = match deadline {
                Some(d) if now >= d => {
                    let waited = now - start;
                    return Err(Box::new(Stalled {
                        taken: i,
                        timed_out: true,
                        waited,
                        error: lock_timeout_error(key, waited, None),
                    }));
                }
                Some(d) => delay.min(d - now),
                None => delay,
            };
            tokio::time::sleep(delay).await;
            attempt = attempt.saturating_add(1);
        }
    }
    Ok(())
}

/// A held set of locks. Dropping it releases them.
pub trait LockGuard: Send + Sync + fmt::Debug {
    /// In acquisition order.
    fn keys(&self) -> &[LockKey];
    /// Fails if the locks may have been lost, e.g. because the lock session died.
    fn check(&self) -> BoxFuture<'_, Result<()>>;
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

pub trait LockManager: Send + Sync {
    /// Takes every key (in [`acquisition_order`]) or none. Fails with `conflict`
    /// ([`LOCK_TIMEOUT_CODE`]) once `timeout` elapses.
    fn acquire<'a>(
        &'a self,
        keys: &'a [LockKey],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn LockGuard>>>;
}

/// In-process locks, for DuckDB-file catalogs and tests. Clones share state.
#[derive(Clone, Debug, Default)]
pub struct LocalLocks {
    held: Arc<Mutex<HashSet<String>>>,
}

impl LocalLocks {
    pub fn new() -> Self {
        Self::default()
    }

    fn held(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl LockManager for LocalLocks {
    fn acquire<'a>(
        &'a self,
        keys: &'a [LockKey],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn LockGuard>>> {
        Box::pin(async move {
            let order = acquisition_order(keys);
            let mut guard = LocalGuard {
                locks: self.clone(),
                keys: Vec::with_capacity(order.len()),
            };
            acquire_in_order(&order, timeout, |k| {
                let ok = self.held().insert(k.name());
                if ok {
                    guard.keys.push(k.clone());
                }
                std::future::ready(Ok(ok))
            })
            .await
            .map_err(|s| s.error)?;
            Ok(Box::new(guard) as Box<dyn LockGuard>)
        })
    }
}

#[derive(Debug)]
struct LocalGuard {
    locks: LocalLocks,
    keys: Vec<LockKey>,
}

impl Drop for LocalGuard {
    fn drop(&mut self) {
        let mut held = self.locks.held();
        for k in &self.keys {
            held.remove(&k.name());
        }
    }
}

impl LockGuard for LocalGuard {
    fn keys(&self) -> &[LockKey] {
        &self.keys
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        drop(self);
        Box::pin(std::future::ready(Ok(())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorClass;
    use std::cell::Cell;

    fn t(s: &str) -> LockKey {
        LockKey::table("lake", "br", s)
    }

    #[test]
    fn names() {
        assert_eq!(t("orders").name(), "atlatl:lake:br.orders");
        assert_eq!(LockKey::group("warehouse").name(), "atlatl-group:warehouse");
        assert_eq!(LockKey::group("g").to_string(), "atlatl-group:g");
    }

    #[test]
    fn identity_is_the_name() {
        assert_eq!(
            LockKey::table("a", "b.c", "d"),
            LockKey::table("a", "b", "c.d")
        );
        assert_ne!(t("x"), t("y"));
        assert_eq!(
            LockKey::table("a", "b", "c").cmp(&LockKey::table("a", "b", "d")),
            Ordering::Less
        );
    }

    #[test]
    fn order_is_sorted_and_deduplicated() {
        let keys = [
            t("zeta"),
            LockKey::group("nightly"),
            t("alpha"),
            t("zeta"),
            LockKey::table("app", "public", "users"),
            LockKey::group("nightly"),
        ];
        let names: Vec<_> = acquisition_order(&keys).iter().map(LockKey::name).collect();
        assert_eq!(
            names,
            [
                "atlatl-group:nightly",
                "atlatl:app:public.users",
                "atlatl:lake:br.alpha",
                "atlatl:lake:br.zeta",
            ]
        );
        let mut rev = keys.clone();
        rev.reverse();
        assert_eq!(acquisition_order(&rev), acquisition_order(&keys));
        assert!(acquisition_order(&[]).is_empty());
    }

    #[test]
    fn poll_delays_double_then_cap() {
        let ms: Vec<_> = (0..9).map(|a| poll_delay(a).as_millis()).collect();
        assert_eq!(ms, [10, 20, 40, 80, 160, 320, 640, 1000, 1000]);
        assert_eq!(poll_delay(40), POLL_MAX);
        assert_eq!(poll_delay(u32::MAX), POLL_MAX);
    }

    #[tokio::test(start_paused = true)]
    async fn polls_until_free() {
        let calls = Cell::new(0);
        let start = Instant::now();
        acquire_in_order(&[t("a")], Duration::from_secs(60), |_| {
            calls.set(calls.get() + 1);
            std::future::ready(Ok(calls.get() == 4))
        })
        .await
        .expect("acquired");
        assert_eq!(calls.get(), 4);
        assert_eq!(start.elapsed(), Duration::from_millis(10 + 20 + 40));
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_at_deadline() {
        let order = [t("a"), t("b"), t("c")];
        let start = Instant::now();
        let s = acquire_in_order(&order, Duration::from_millis(2500), |k| {
            std::future::ready(Ok(k.name() != t("b").name()))
        })
        .await
        .expect_err("times out");
        assert_eq!(start.elapsed(), Duration::from_millis(2500));
        assert_eq!(s.taken, 1);
        assert!(s.timed_out);
        assert_eq!(s.waited, Duration::from_millis(2500));
        assert_eq!(s.error.class, ErrorClass::Conflict);
        assert_eq!(s.error.code.as_deref(), Some(LOCK_TIMEOUT_CODE));
        assert!(
            s.error.message.contains("`atlatl:lake:br.b`"),
            "{}",
            s.error
        );
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeout_tries_each_key_once() {
        let calls = Cell::new(0);
        acquire_in_order(&[t("a"), t("b")], Duration::ZERO, |_| {
            calls.set(calls.get() + 1);
            std::future::ready(Ok(true))
        })
        .await
        .expect("acquired");
        assert_eq!(calls.get(), 2);
        let s = acquire_in_order(&[t("a")], Duration::ZERO, |_| std::future::ready(Ok(false)))
            .await
            .expect_err("busy");
        assert!(s.timed_out);
    }

    #[tokio::test(start_paused = true)]
    async fn unbounded_timeout_does_not_overflow() {
        let calls = Cell::new(0);
        acquire_in_order(&[t("a")], Duration::MAX, |_| {
            calls.set(calls.get() + 1);
            std::future::ready(Ok(calls.get() > 20))
        })
        .await
        .expect("acquired");
    }

    #[tokio::test(start_paused = true)]
    async fn try_lock_errors_propagate() {
        let s = acquire_in_order(&[t("a"), t("b")], Duration::from_secs(5), |k| {
            std::future::ready(if k.name().ends_with(".a") {
                Ok(true)
            } else {
                Err(AtlatlError::transient("connection reset"))
            })
        })
        .await
        .expect_err("fails");
        assert_eq!(s.taken, 1);
        assert!(!s.timed_out);
        assert_eq!(s.error.class, ErrorClass::Transient);
    }

    #[tokio::test(start_paused = true)]
    async fn local_locks_conflict_and_release() {
        let m = LocalLocks::new();
        let g = m
            .acquire(&[t("b"), t("a"), t("a")], Duration::ZERO)
            .await
            .expect("first");
        assert_eq!(g.keys(), &[t("a"), t("b")]);
        g.check().await.expect("held");

        let e = m
            .acquire(&[t("c"), t("b")], Duration::from_secs(3))
            .await
            .expect_err("contended");
        assert_eq!(e.class, ErrorClass::Conflict);
        assert!(e.message.contains("br.b"), "{e}");
        // The failed attempt gave `c` back.
        drop(m.acquire(&[t("c")], Duration::ZERO).await.expect("c free"));

        g.release().await.expect("release");
        drop(
            m.acquire(&[t("a"), t("b")], Duration::ZERO)
                .await
                .expect("free"),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn local_waiter_proceeds_after_drop() {
        let m = LocalLocks::new();
        let g = m.acquire(&[t("a")], Duration::ZERO).await.expect("first");
        let m2 = m.clone();
        let waiter = tokio::spawn(async move {
            let keys = [t("a")];
            m2.acquire(&keys, Duration::from_secs(60))
                .await
                .map(|g| g.keys().to_vec())
        });
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!waiter.is_finished());
        drop(g);
        assert_eq!(waiter.await.expect("join").expect("acquired"), [t("a")]);
    }

    #[tokio::test(start_paused = true)]
    async fn opposite_orders_do_not_deadlock() {
        let m = LocalLocks::new();
        let run = |keys: Vec<LockKey>| {
            let m = m.clone();
            tokio::spawn(async move {
                for _ in 0..20 {
                    let g = m.acquire(&keys, Duration::from_secs(60)).await?;
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    g.release().await?;
                }
                Ok::<_, AtlatlError>(())
            })
        };
        let a = run(vec![t("x"), t("y"), t("z")]);
        let b = run(vec![t("z"), t("y"), t("x")]);
        a.await.expect("join").expect("a");
        b.await.expect("join").expect("b");
    }
}
