//! Per-connection request limiter (07 §8): GCRA for `rps`/`burst`, a hard `min_interval` between
//! request starts, `max_in_flight`, throttle pauses, adaptive slowdown and a request budget.
//! Built on `tokio::time` so tests run on a paused clock.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use atlatl_core::AtlatlError;
use atlatl_core::error::wait_hint;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::spec::RateLimitSpec;

const MIN_FACTOR: f64 = 1.0 / 64.0;

#[derive(Clone, Debug, PartialEq)]
pub struct LimiterConfig {
    pub rps: Option<f64>,
    pub burst: u32,
    pub min_interval: Duration,
    pub max_in_flight: u32,
    pub budget: Option<u64>,
    /// `Some(recover_after)` enables halving on throttle.
    pub adaptive: Option<Duration>,
    pub respect_headers: bool,
    /// Waits advance a virtual clock instead of sleeping (cassette replay): the pacing and its
    /// stats are computed as live, with no real delay.
    pub virtual_time: bool,
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            rps: None,
            burst: 1,
            min_interval: Duration::ZERO,
            max_in_flight: 1,
            budget: None,
            adaptive: None,
            respect_headers: true,
            virtual_time: false,
        }
    }
}

impl LimiterConfig {
    pub fn from_spec(s: &RateLimitSpec) -> Self {
        Self {
            rps: s.rps,
            burst: s.burst.unwrap_or(1),
            min_interval: s.min_interval.map(|d| d.0).unwrap_or_default(),
            max_in_flight: s.max_in_flight.unwrap_or(1),
            budget: s.budget.as_ref().map(|b| b.requests),
            adaptive: s
                .adaptive
                .as_ref()
                .map(|a| a.recover_after.map(|d| d.0).unwrap_or(Duration::MAX)),
            respect_headers: s.respect_headers,
            virtual_time: false,
        }
    }
}

#[derive(Debug)]
struct State {
    tat: Option<Instant>,
    last_start: Option<Instant>,
    paused_until: Option<Instant>,
    factor: f64,
    last_adjust: Option<Instant>,
    requests: u64,
    /// Virtual time elapsed (`virtual_time`).
    skew: Duration,
}

#[derive(Debug)]
pub struct Limiter {
    cfg: LimiterConfig,
    sem: Arc<Semaphore>,
    state: Mutex<State>,
}

/// Held for the duration of one request.
#[derive(Debug)]
pub struct Permit {
    _permit: OwnedSemaphorePermit,
    /// Total time spent waiting, including for an in-flight slot.
    pub waited: Duration,
    /// The part of `waited` spent in a throttle pause.
    pub throttle_waited: Duration,
}

impl Limiter {
    pub fn new(cfg: LimiterConfig) -> Arc<Self> {
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(cfg.max_in_flight.max(1) as usize)),
            cfg,
            state: Mutex::new(State {
                tat: None,
                last_start: None,
                paused_until: None,
                factor: 1.0,
                last_adjust: None,
                requests: 0,
                skew: Duration::ZERO,
            }),
        })
    }

    pub fn config(&self) -> &LimiterConfig {
        &self.cfg
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The limiter's clock: real time plus any virtual time elapsed.
    pub fn now(&self) -> Instant {
        Instant::now() + self.lock().skew
    }

    /// Sleeps for `d`, or advances the virtual clock under `virtual_time`.
    pub async fn sleep(&self, d: Duration) {
        if self.cfg.virtual_time {
            self.lock().skew += d;
        } else {
            tokio::time::sleep(d).await;
        }
    }

    fn interval(&self, factor: f64) -> Option<Duration> {
        self.cfg
            .rps
            .map(|r| Duration::from_secs_f64(1.0 / (r * factor)))
    }

    fn spacing(&self, factor: f64) -> Duration {
        if self.cfg.rps.is_none() && self.cfg.adaptive.is_some() {
            self.cfg.min_interval.div_f64(factor)
        } else {
            self.cfg.min_interval
        }
    }

    /// Waits for a slot. Fails with `config` once the request budget is spent.
    pub async fn acquire(&self) -> Result<Permit, AtlatlError> {
        let t0 = self.now();
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AtlatlError::internal("limiter closed"))?;
        let mut throttle_waited = Duration::ZERO;
        loop {
            let (ready, paused) = {
                let mut s = self.lock();
                let now = Instant::now() + s.skew;
                self.recover(&mut s, now);
                if let Some(b) = self.cfg.budget
                    && s.requests >= b
                {
                    return Err(AtlatlError::config(format!(
                        "request budget of {b} requests per run is exhausted"
                    ))
                    .code("request_budget"));
                }
                let mut ready = now;
                let mut paused = false;
                if let Some(p) = s.paused_until
                    && p > ready
                {
                    ready = p;
                    paused = true;
                }
                if let Some(last) = s.last_start {
                    let r = last + self.spacing(s.factor);
                    if r > ready {
                        ready = r;
                        paused = false;
                    }
                }
                if let (Some(t), Some(tat)) = (self.interval(s.factor), s.tat) {
                    let tau = t * (self.cfg.burst.max(1) - 1);
                    let r = tat.checked_sub(tau).unwrap_or(tat);
                    if r > ready {
                        ready = r;
                        paused = false;
                    }
                }
                if self.cfg.virtual_time && ready > now {
                    s.skew += ready - now;
                    if paused {
                        throttle_waited += ready - now;
                    }
                    continue;
                }
                if ready <= now {
                    if let Some(t) = self.interval(s.factor) {
                        s.tat = Some(s.tat.map_or(now, |x| x.max(now)) + t);
                    }
                    s.last_start = Some(now);
                    s.requests += 1;
                    return Ok(Permit {
                        _permit: permit,
                        waited: now - t0,
                        throttle_waited,
                    });
                }
                (ready, paused)
            };
            let before = Instant::now();
            tokio::time::sleep_until(ready).await;
            if paused {
                throttle_waited += Instant::now() - before;
            }
        }
    }

    fn recover(&self, s: &mut State, now: Instant) {
        let Some(after) = self.cfg.adaptive else {
            return;
        };
        if s.factor < 1.0
            && let Some(t) = s.last_adjust
            && now.saturating_duration_since(t) >= after
        {
            s.factor = (s.factor * 2.0).min(1.0);
            s.last_adjust = Some(now);
        }
    }

    /// Pauses every request start until `d` from now (or a later pause already set).
    pub fn pause(&self, d: Duration) {
        let mut s = self.lock();
        let until = Instant::now() + s.skew + d;
        s.paused_until = Some(s.paused_until.map_or(until, |p| p.max(until)));
    }

    /// A `throttled`-class response: pause for `wait` and, if adaptive, halve the rate.
    pub fn throttled(&self, wait: Duration) {
        self.pause(wait);
        if self.cfg.adaptive.is_some() {
            let mut s = self.lock();
            s.factor = (s.factor / 2.0).max(MIN_FACTOR);
            s.last_adjust = Some(Instant::now() + s.skew);
        }
    }

    /// Proactive wait on `X-RateLimit-Remaining: 0` / `RateLimit-Remaining: 0`. Returns the pause.
    pub fn observe<'a>(
        &self,
        headers: impl IntoIterator<Item = (&'a str, &'a str)> + Clone,
    ) -> Option<Duration> {
        if !self.cfg.respect_headers {
            return None;
        }
        let exhausted = headers.clone().into_iter().any(|(k, v)| {
            (k.eq_ignore_ascii_case("x-ratelimit-remaining")
                || k.eq_ignore_ascii_case("ratelimit-remaining"))
                && v.trim().parse::<f64>().is_ok_and(|n| n <= 0.0)
        });
        if !exhausted {
            return None;
        }
        let d = wait_hint(headers, SystemTime::now())?;
        self.pause(d);
        Some(d)
    }

    pub fn rate_factor(&self) -> f64 {
        self.lock().factor
    }

    pub fn requests(&self) -> u64 {
        self.lock().requests
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim(f: impl FnOnce(&mut LimiterConfig)) -> Arc<Limiter> {
        let mut c = LimiterConfig::default();
        f(&mut c);
        Limiter::new(c)
    }

    async fn starts(l: &Arc<Limiter>, n: usize) -> Vec<u128> {
        let t0 = Instant::now();
        let mut out = Vec::new();
        for _ in 0..n {
            let p = l.acquire().await.expect("acquire");
            out.push((Instant::now() - t0).as_millis());
            drop(p);
        }
        out
    }

    #[tokio::test(start_paused = true)]
    async fn min_interval_spaces_starts() {
        let l = lim(|c| c.min_interval = Duration::from_millis(250));
        assert_eq!(starts(&l, 4).await, [0, 250, 500, 750]);
    }

    #[tokio::test(start_paused = true)]
    async fn unlimited_by_default() {
        let l = lim(|_| {});
        assert_eq!(starts(&l, 3).await, [0, 0, 0]);
        assert_eq!(l.requests(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gcra_with_burst() {
        let l = lim(|c| {
            c.rps = Some(2.0);
            c.burst = 3;
        });
        assert_eq!(starts(&l, 6).await, [0, 0, 0, 500, 1000, 1500]);
        let l = lim(|c| c.rps = Some(0.2));
        assert_eq!(starts(&l, 3).await, [0, 5000, 10000]);
    }

    #[tokio::test(start_paused = true)]
    async fn max_in_flight_bounds_concurrency() {
        let l = lim(|c| c.max_in_flight = 2);
        let a = l.acquire().await.expect("a");
        let _b = l.acquire().await.expect("b");
        let l2 = l.clone();
        let t0 = Instant::now();
        let h = tokio::spawn(async move {
            let _c = l2.acquire().await.expect("c");
            Instant::now()
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        drop(a);
        let at = h.await.expect("join");
        assert_eq!((at - t0).as_secs(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_pauses_everyone() {
        let l = lim(|c| c.max_in_flight = 4);
        let _ = l.acquire().await.expect("first");
        l.throttled(Duration::from_secs(7));
        let t0 = Instant::now();
        let (a, b) = tokio::join!(l.acquire(), l.acquire());
        let (a, b) = (a.expect("a"), b.expect("b"));
        assert_eq!((Instant::now() - t0).as_secs(), 7);
        assert_eq!(a.throttle_waited, Duration::from_secs(7));
        assert_eq!(b.throttle_waited, Duration::from_secs(7));
        assert_eq!(l.rate_factor(), 1.0, "not adaptive");
    }

    #[tokio::test(start_paused = true)]
    async fn adaptive_halves_and_recovers() {
        let l = lim(|c| {
            c.rps = Some(10.0);
            c.adaptive = Some(Duration::from_secs(60));
        });
        let _ = l.acquire().await.expect("a");
        l.throttled(Duration::ZERO);
        l.throttled(Duration::ZERO);
        assert_eq!(l.rate_factor(), 0.25);
        let t0 = Instant::now();
        drop(l.acquire().await.expect("b"));
        drop(l.acquire().await.expect("c"));
        // b is due at the old TAT (100ms); c follows at 2.5 rps.
        assert_eq!((Instant::now() - t0).as_millis(), 500);
        tokio::time::sleep(Duration::from_secs(60)).await;
        drop(l.acquire().await.expect("d"));
        assert_eq!(l.rate_factor(), 0.5);
        for _ in 0..10 {
            l.throttled(Duration::ZERO);
        }
        assert_eq!(l.rate_factor(), MIN_FACTOR);
    }

    #[tokio::test(start_paused = true)]
    async fn adaptive_scales_min_interval_without_rps() {
        let l = lim(|c| {
            c.min_interval = Duration::from_millis(100);
            c.adaptive = Some(Duration::from_secs(60));
        });
        let _ = l.acquire().await.expect("a");
        l.throttled(Duration::ZERO);
        let t0 = Instant::now();
        drop(l.acquire().await.expect("b"));
        assert_eq!((Instant::now() - t0).as_millis(), 200);
    }

    #[tokio::test(start_paused = true)]
    async fn budget_is_a_config_error() {
        let l = lim(|c| c.budget = Some(2));
        starts(&l, 2).await;
        let e = l.acquire().await.expect_err("budget");
        assert_eq!(e.class, atlatl_core::ErrorClass::Config);
        assert_eq!(e.code.as_deref(), Some("request_budget"));
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_zero_waits_proactively() {
        let l = lim(|_| {});
        assert_eq!(
            l.observe([("x-ratelimit-remaining", "3"), ("x-ratelimit-reset", "9")]),
            None
        );
        assert_eq!(
            l.observe([("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "9")]),
            Some(Duration::from_secs(9))
        );
        let t0 = Instant::now();
        let p = l.acquire().await.expect("a");
        assert_eq!((Instant::now() - t0).as_secs(), 9);
        assert_eq!(p.throttle_waited, Duration::from_secs(9));
        let off = lim(|c| c.respect_headers = false);
        assert_eq!(
            off.observe([("RateLimit-Remaining", "0"), ("RateLimit-Reset", "9")]),
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn virtual_time_paces_without_sleeping() {
        let l = lim(|c| {
            c.rps = Some(0.2);
            c.min_interval = Duration::from_millis(250);
            c.virtual_time = true;
        });
        let t0 = Instant::now();
        let v0 = l.now();
        let mut waited = Vec::new();
        for _ in 0..3 {
            waited.push(l.acquire().await.expect("acquire").waited.as_millis());
        }
        assert_eq!(waited, [0, 5000, 5000]);
        l.throttled(Duration::from_secs(30));
        let p = l.acquire().await.expect("after throttle");
        assert_eq!(p.throttle_waited, Duration::from_secs(30));
        l.sleep(Duration::from_secs(60)).await;
        assert_eq!(Instant::now(), t0, "no real time passed");
        assert_eq!(l.now() - v0, Duration::from_secs(100));
    }
}
