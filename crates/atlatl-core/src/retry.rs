//! Retry and backoff (spec 03 §6).

use std::future::Future;
use std::time::Duration;

use rand::{RngExt, SeedableRng};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::model::{BackoffKind, RetryConfig};
use crate::error::{AtlatlError, ErrorClass};

#[derive(Clone, Debug, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffKind,
    pub base: Duration,
    pub max: Duration,
    pub retry_on: Vec<ErrorClass>,
    pub budget: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            backoff: BackoffKind::ExponentialJitter,
            base: Duration::from_secs(2),
            max: Duration::from_secs(300),
            retry_on: ErrorClass::RETRYABLE.to_vec(),
            budget: None,
        }
    }
}

impl RetryPolicy {
    /// Builds a policy from a merged config; unset fields take the defaults.
    pub fn from_config(c: &RetryConfig) -> Result<Self, String> {
        let d = Self::default();
        let p = Self {
            max_attempts: c.max_attempts.unwrap_or(d.max_attempts),
            backoff: c.backoff.unwrap_or(d.backoff),
            base: c.base.map(|x| x.0).unwrap_or(d.base),
            max: c.max.map(|x| x.0).unwrap_or(d.max),
            retry_on: c.retry_on.clone().unwrap_or(d.retry_on),
            budget: c.budget.map(|x| x.0),
        };
        if p.max_attempts == 0 {
            return Err(
                "retry.max_attempts must be at least 1 (it counts the first attempt)".into(),
            );
        }
        if p.base > p.max {
            return Err(format!(
                "retry.base ({:?}) exceeds retry.max ({:?})",
                p.base, p.max
            ));
        }
        if let Some(bad) = p.retry_on.iter().find(|c| !c.is_retryable()) {
            return Err(format!(
                "retry.retry_on: `{bad}` is never retried (allowed: transient, throttled, decode, conflict, timeout)"
            ));
        }
        Ok(p)
    }

    /// Wait after the `attempt`-th failure (1-based). `jitter` is a factor in [1, 1.5).
    pub fn computed_wait(&self, attempt: u32, jitter: f64) -> Duration {
        let n = attempt.max(1);
        let w = match self.backoff {
            BackoffKind::None => Duration::ZERO,
            BackoffKind::Constant => self.base,
            BackoffKind::Linear => self.base.saturating_mul(n),
            BackoffKind::Exponential | BackoffKind::ExponentialJitter => {
                self.base.saturating_mul(2u32.saturating_pow(n - 1))
            }
        };
        let w = if self.backoff == BackoffKind::ExponentialJitter {
            w.mul_f64(jitter)
        } else {
            w
        };
        w.min(self.max)
    }

    /// Server hints override the computed wait for `throttled`.
    pub fn wait_for(&self, e: &AtlatlError, attempt: u32, jitter: f64) -> Duration {
        match (e.class, e.retry_after) {
            (ErrorClass::Throttled, Some(hint)) => hint,
            _ => self.computed_wait(attempt, jitter),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetryLevel {
    Request,
    Query,
    Object,
    Commit,
    Replication,
}

/// One retried attempt, as reported in events and the run report.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AttemptRecord {
    pub level: RetryLevel,
    pub attempt: u32,
    pub class: ErrorClass,
    pub message: String,
    pub wait_ms: u64,
}

pub trait Jitter: Send {
    /// A factor in [1, 1.5).
    fn factor(&mut self) -> f64;
}

pub struct RandomJitter(rand::rngs::StdRng);

impl RandomJitter {
    pub fn new() -> Self {
        Self(rand::make_rng())
    }
    pub fn seeded(seed: u64) -> Self {
        Self(rand::rngs::StdRng::seed_from_u64(seed))
    }
}

impl Default for RandomJitter {
    fn default() -> Self {
        Self::new()
    }
}

impl Jitter for RandomJitter {
    fn factor(&mut self) -> f64 {
        self.0.random_range(1.0..1.5)
    }
}

pub struct FixedJitter(pub f64);

impl Jitter for FixedJitter {
    fn factor(&mut self) -> f64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GaveUp {
    /// The class isn't in `retry_on`.
    NotRetryable,
    Exhausted,
    Budget,
}

#[derive(Clone, Debug)]
pub struct RetryError {
    pub error: AtlatlError,
    pub attempts: u32,
    pub reason: GaveUp,
}

impl std::fmt::Display for RetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            GaveUp::NotRetryable => write!(f, "{}", self.error),
            GaveUp::Exhausted => write!(f, "{} (after {} attempts)", self.error, self.attempts),
            GaveUp::Budget => write!(
                f,
                "{} (retry budget exhausted after {} attempts)",
                self.error, self.attempts
            ),
        }
    }
}

impl std::error::Error for RetryError {}

/// Runs `op` until it succeeds or the policy gives up. `op` receives the 1-based attempt number;
/// `on_retry` sees each failure that will be retried, before the wait.
pub async fn retry<T, F, Fut>(
    policy: &RetryPolicy,
    level: RetryLevel,
    jitter: &mut dyn Jitter,
    mut on_retry: impl FnMut(&AttemptRecord),
    mut op: F,
) -> Result<T, RetryError>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, AtlatlError>>,
{
    let started = tokio::time::Instant::now();
    let mut attempt = 1u32;
    loop {
        let e = match op(attempt).await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        if !policy.retry_on.contains(&e.class) {
            return Err(RetryError {
                error: e,
                attempts: attempt,
                reason: GaveUp::NotRetryable,
            });
        }
        if attempt >= policy.max_attempts {
            return Err(RetryError {
                error: e,
                attempts: attempt,
                reason: GaveUp::Exhausted,
            });
        }
        let wait = policy.wait_for(&e, attempt, jitter.factor());
        if let Some(budget) = policy.budget
            && started.elapsed() + wait > budget
        {
            return Err(RetryError {
                error: e,
                attempts: attempt,
                reason: GaveUp::Budget,
            });
        }
        on_retry(&AttemptRecord {
            level,
            attempt,
            class: e.class,
            message: e.redacted_message(),
            wait_ms: wait.as_millis() as u64,
        });
        tokio::time::sleep(wait).await;
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::time::Instant;

    use super::*;
    use crate::config::types::Dur;

    fn policy(backoff: BackoffKind) -> RetryPolicy {
        RetryPolicy {
            backoff,
            base: Duration::from_secs(2),
            max: Duration::from_secs(20),
            ..Default::default()
        }
    }

    #[test]
    fn wait_sequences() {
        let s = |p: &RetryPolicy, j| {
            (1..=6)
                .map(|n| p.computed_wait(n, j).as_secs_f64())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            s(&policy(BackoffKind::Exponential), 1.0),
            [2.0, 4.0, 8.0, 16.0, 20.0, 20.0]
        );
        assert_eq!(
            s(&policy(BackoffKind::ExponentialJitter), 1.25),
            [2.5, 5.0, 10.0, 20.0, 20.0, 20.0]
        );
        assert_eq!(
            s(&policy(BackoffKind::Linear), 1.0),
            [2.0, 4.0, 6.0, 8.0, 10.0, 12.0]
        );
        assert_eq!(s(&policy(BackoffKind::Constant), 1.4), [2.0; 6]);
        assert_eq!(s(&policy(BackoffKind::None), 1.0), [0.0; 6]);
        let big = RetryPolicy {
            max: Duration::MAX,
            ..policy(BackoffKind::Exponential)
        };
        assert_eq!(
            big.computed_wait(200, 1.0),
            Duration::MAX.min(Duration::from_secs(2).saturating_mul(u32::MAX))
        );
    }

    #[test]
    fn random_jitter_in_range() {
        let mut j = RandomJitter::seeded(7);
        for _ in 0..1000 {
            let f = j.factor();
            assert!((1.0..1.5).contains(&f));
        }
    }

    #[test]
    fn from_config_validates() {
        let c = RetryConfig {
            max_attempts: Some(3),
            base: Some(Dur(Duration::from_secs(1))),
            ..Default::default()
        };
        let p = RetryPolicy::from_config(&c).expect("ok");
        assert_eq!(
            (p.max_attempts, p.base, p.max),
            (3, Duration::from_secs(1), Duration::from_secs(300))
        );
        let bad = RetryConfig {
            retry_on: Some(vec![ErrorClass::Data]),
            ..Default::default()
        };
        assert!(
            RetryPolicy::from_config(&bad)
                .expect_err("data")
                .contains("`data`")
        );
        assert!(
            RetryPolicy::from_config(&RetryConfig {
                max_attempts: Some(0),
                ..Default::default()
            })
            .is_err()
        );
        let inverted = RetryConfig {
            base: Some(Dur(Duration::from_secs(10))),
            max: Some(Dur(Duration::from_secs(1))),
            ..Default::default()
        };
        assert!(RetryPolicy::from_config(&inverted).is_err());
    }

    fn failing(
        n_failures: u32,
        e: AtlatlError,
    ) -> impl FnMut(u32) -> std::future::Ready<Result<u32, AtlatlError>> {
        move |attempt| {
            std::future::ready(if attempt <= n_failures {
                Err(e.clone())
            } else {
                Ok(attempt)
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_with_backoff_on_paused_clock() {
        let p = policy(BackoffKind::Exponential);
        let t0 = Instant::now();
        let mut recs = Vec::new();
        let r = retry(
            &p,
            RetryLevel::Request,
            &mut FixedJitter(1.0),
            |a| recs.push(a.clone()),
            failing(3, AtlatlError::transient("reset")),
        )
        .await
        .expect("ok");
        assert_eq!(r, 4);
        assert_eq!(t0.elapsed(), Duration::from_secs(2 + 4 + 8));
        assert_eq!(
            recs.iter()
                .map(|a| (a.attempt, a.wait_ms))
                .collect::<Vec<_>>(),
            [(1, 2000), (2, 4000), (3, 8000)]
        );
        assert!(
            recs.iter()
                .all(|a| a.class == ErrorClass::Transient && a.level == RetryLevel::Request)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_attempts() {
        let p = RetryPolicy {
            max_attempts: 3,
            ..policy(BackoffKind::Constant)
        };
        let t0 = Instant::now();
        let e = retry(
            &p,
            RetryLevel::Query,
            &mut FixedJitter(1.0),
            |_| {},
            failing(10, AtlatlError::transient("x")),
        )
        .await
        .expect_err("exhausted");
        assert_eq!((e.attempts, e.reason), (3, GaveUp::Exhausted));
        assert_eq!(t0.elapsed(), Duration::from_secs(4));
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_fails_immediately() {
        let p = policy(BackoffKind::Exponential);
        let t0 = Instant::now();
        let e = retry(
            &p,
            RetryLevel::Request,
            &mut FixedJitter(1.0),
            |_| panic!("no retry"),
            failing(1, AtlatlError::data("null pk")),
        )
        .await
        .expect_err("data");
        assert_eq!(
            (e.attempts, e.reason, e.error.class),
            (1, GaveUp::NotRetryable, ErrorClass::Data)
        );
        assert_eq!(t0.elapsed(), Duration::ZERO);
        let only_transient = RetryPolicy {
            retry_on: vec![ErrorClass::Transient],
            ..p
        };
        let e = retry(
            &only_transient,
            RetryLevel::Commit,
            &mut FixedJitter(1.0),
            |_| {},
            failing(1, AtlatlError::conflict("c")),
        )
        .await
        .expect_err("conflict");
        assert_eq!(e.reason, GaveUp::NotRetryable);
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_hint_overrides_backoff() {
        let p = policy(BackoffKind::Exponential);
        let t0 = Instant::now();
        let throttled = AtlatlError::new(ErrorClass::Throttled, "429")
            .retry_after(Some(Duration::from_secs(45)));
        let mut waits = Vec::new();
        retry(
            &p,
            RetryLevel::Request,
            &mut FixedJitter(1.0),
            |a| waits.push(a.wait_ms),
            failing(2, throttled),
        )
        .await
        .expect("ok");
        assert_eq!(waits, [45_000, 45_000]);
        assert_eq!(t0.elapsed(), Duration::from_secs(90));
        let transient_hint =
            AtlatlError::transient("503").retry_after(Some(Duration::from_secs(45)));
        let mut waits = Vec::new();
        retry(
            &p,
            RetryLevel::Request,
            &mut FixedJitter(1.0),
            |a| waits.push(a.wait_ms),
            failing(1, transient_hint),
        )
        .await
        .expect("ok");
        assert_eq!(waits, [2000], "hints only override for throttled");
    }

    #[tokio::test(start_paused = true)]
    async fn budget_stops_retrying() {
        let p = RetryPolicy {
            max_attempts: 100,
            budget: Some(Duration::from_secs(10)),
            ..policy(BackoffKind::Exponential)
        };
        let t0 = Instant::now();
        let e = retry(
            &p,
            RetryLevel::Request,
            &mut FixedJitter(1.0),
            |_| {},
            failing(100, AtlatlError::transient("x")),
        )
        .await
        .expect_err("budget");
        // 2 + 4 = 6s spent; the next wait (8s) would exceed the 10s budget.
        assert_eq!((e.reason, e.attempts), (GaveUp::Budget, 3));
        assert_eq!(t0.elapsed(), Duration::from_secs(6));
    }

    #[tokio::test(start_paused = true)]
    async fn budget_counts_time_inside_attempts() {
        let p = RetryPolicy {
            max_attempts: 100,
            budget: Some(Duration::from_secs(10)),
            ..policy(BackoffKind::Constant)
        };
        let calls = Arc::new(Mutex::new(0u32));
        let c = calls.clone();
        let e = retry(
            &p,
            RetryLevel::Object,
            &mut FixedJitter(1.0),
            |_| {},
            move |_| {
                let c = c.clone();
                async move {
                    *c.lock().expect("lock") += 1;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    Err::<(), _>(AtlatlError::transient("slow"))
                }
            },
        )
        .await
        .expect_err("budget");
        assert_eq!(e.reason, GaveUp::Budget);
        // Attempts end at 3s, 8s (8+2 = 10 is still within budget) and 13s (13+2 > 10).
        assert_eq!(*calls.lock().expect("lock"), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_messages_are_redacted() {
        crate::redact::register("pw-in-retry-test");
        let mut msgs = Vec::new();
        let _ = retry(
            &policy(BackoffKind::None),
            RetryLevel::Request,
            &mut FixedJitter(1.0),
            |a| msgs.push(a.message.clone()),
            failing(1, AtlatlError::transient("auth pw-in-retry-test")),
        )
        .await;
        assert_eq!(msgs, ["auth ****"]);
    }
}
