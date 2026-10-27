//! Replication scheduling (spec 03 §2–§4): `after` ordering, bounded concurrency, independence,
//! `atomic_group` units, `lock_group` locks, `replication_retries`, timeouts, cancellation and
//! `--skip-succeeded`. What a replication *does* is the runner's job.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use tokio::time::Instant;

use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::error::{AtlatlError, ErrorClass};
use crate::lock::{BoxFuture, LockKey, LockManager};
use crate::plan::{Plan, ReplicationPlan};

pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(30 * 60);
pub const DEFAULT_CONCURRENCY: usize = 2;

pub use crate::obs::report::ReplicationStatus as RepStatus;

pub trait RunnerOutput: Send + 'static {
    /// `Succeeded` unless the runner found nothing to do (`UpToDate`).
    fn status(&self) -> RepStatus {
        RepStatus::Succeeded
    }
}

impl RunnerOutput for () {}

/// Runs one attempt of a unit: a single replication, or an `atomic_group` whose members commit in
/// one transaction. Must return promptly (with a `killed` error) once `cancel` fires.
pub trait ReplicationRunner: Send + Sync {
    type Output: RunnerOutput;

    fn run<'a>(
        &'a self,
        unit: &'a [&'a ReplicationPlan],
        attempt: u32,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<Self::Output, AtlatlError>>;
}

#[derive(Clone, Debug)]
pub struct ExecOptions {
    pub concurrency: usize,
    /// Replications that already succeeded under this external run id (`--skip-succeeded`).
    pub already_succeeded: HashSet<String>,
    pub default_lock_timeout: Duration,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            already_succeeded: HashSet::new(),
            default_lock_timeout: DEFAULT_LOCK_TIMEOUT,
        }
    }
}

#[derive(Debug)]
pub struct RepResult<O> {
    pub replication: String,
    pub status: RepStatus,
    /// Replication-level attempts (1 + `replication_retries` used).
    pub attempts: u32,
    pub error: Option<AtlatlError>,
    /// The runner's output, shared by every member of an atomic group (on the first member).
    pub output: Option<O>,
    pub duration: Duration,
    /// The attempt panicked; the process must exit non-zero (03 §10).
    pub panicked: bool,
}

struct Unit<'p> {
    reps: Vec<&'p ReplicationPlan>,
    deps: Vec<usize>,
    lock_groups: Vec<LockKey>,
    lock_timeout: Option<Duration>,
    retries: u32,
    timeout: Option<Duration>,
}

fn units(plan: &Plan) -> Vec<Unit<'_>> {
    let mut out: Vec<Unit<'_>> = Vec::new();
    let mut unit_of: HashMap<&str, usize> = HashMap::new();
    let mut group_unit: HashMap<&str, usize> = HashMap::new();
    // `after: [r]` on a `for_each` replication waits for every instance.
    let mut instances_of: HashMap<&str, Vec<usize>> = HashMap::new();
    for r in &plan.replications {
        let idx = match r
            .atomic_group
            .as_deref()
            .and_then(|g| group_unit.get(g).copied())
        {
            Some(i) => i,
            None => {
                out.push(Unit {
                    reps: Vec::new(),
                    deps: Vec::new(),
                    lock_groups: Vec::new(),
                    lock_timeout: None,
                    retries: 0,
                    timeout: None,
                });
                let i = out.len() - 1;
                if let Some(g) = r.atomic_group.as_deref() {
                    group_unit.insert(g, i);
                }
                i
            }
        };
        let u = &mut out[idx];
        u.reps.push(r);
        u.retries = u.retries.max(r.replication_retries);
        if let Some(g) = &r.lock_group {
            u.lock_groups.push(LockKey::group(g.clone()));
        }
        let ms = |v: Option<u64>| v.map(Duration::from_millis);
        u.lock_timeout = match (u.lock_timeout, ms(r.lock_timeout_ms)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        u.timeout = match (u.timeout, ms(r.timeout_ms)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        unit_of.insert(&r.name, idx);
        if let Some(inst) = &r.instance {
            instances_of.entry(inst.of.as_str()).or_default().push(idx);
        }
    }
    let deps: Vec<Vec<usize>> = out
        .iter()
        .enumerate()
        .map(|(i, u)| {
            let mut d: Vec<usize> = u
                .reps
                .iter()
                .flat_map(|r| r.after.iter())
                .flat_map(|a| {
                    let own = unit_of.get(a.as_str()).copied();
                    own.into_iter()
                        .chain(instances_of.get(a.as_str()).into_iter().flatten().copied())
                })
                .filter(|&j| j != i)
                .collect();
            d.sort_unstable();
            d.dedup();
            d
        })
        .collect();
    for (u, d) in out.iter_mut().zip(deps) {
        u.deps = d;
    }
    out
}

struct UnitDone<O> {
    idx: usize,
    status: RepStatus,
    attempts: u32,
    error: Option<AtlatlError>,
    output: Option<O>,
    duration: Duration,
    panicked: bool,
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".into())
}

async fn run_unit<'a, R: ReplicationRunner>(
    idx: usize,
    unit: &'a Unit<'a>,
    runner: &'a R,
    locks: &'a dyn LockManager,
    opts: &'a ExecOptions,
    cancel: CancellationToken,
) -> UnitDone<R::Output> {
    let started = Instant::now();
    let done = |status, attempts, error, output, panicked| UnitDone {
        idx,
        status,
        attempts,
        error,
        output,
        duration: started.elapsed(),
        panicked,
    };
    let guard = if unit.lock_groups.is_empty() {
        None
    } else {
        let timeout = unit.lock_timeout.unwrap_or(opts.default_lock_timeout);
        let acquired = tokio::select! {
            r = locks.acquire(&unit.lock_groups, timeout) => r,
            _ = cancel.cancelled() => Err(AtlatlError::new(ErrorClass::Killed, "killed while waiting for locks")),
        };
        match acquired {
            Ok(g) => Some(g),
            Err(e) => {
                let status = if e.class == ErrorClass::Killed {
                    RepStatus::Killed
                } else {
                    RepStatus::Failed
                };
                return done(status, 0, Some(e), None, false);
            }
        }
    };
    let mut attempt = 1;
    let result = loop {
        let child = cancel.child_token();
        let scope = unit
            .reps
            .first()
            .map(|r| r.name.as_str())
            .unwrap_or_default();
        let attempt_fut = async {
            crate::fault::hit_async("exec.replication", scope).await?;
            runner.run(&unit.reps, attempt, child.clone()).await
        };
        let fut = AssertUnwindSafe(attempt_fut).catch_unwind();
        let outcome = match unit.timeout {
            None => fut.await,
            Some(t) => {
                tokio::pin!(fut);
                tokio::select! {
                    r = &mut fut => r,
                    _ = tokio::time::sleep(t) => {
                        child.cancel();
                        let _ = fut.await;
                        Ok(Err(AtlatlError::new(ErrorClass::Killed, format!("replication timeout ({t:?}) expired")).code("timeout")))
                    }
                }
            }
        };
        match outcome {
            Err(p) => {
                let msg = panic_message(p.as_ref());
                break done(
                    RepStatus::Failed,
                    attempt,
                    Some(AtlatlError::internal(format!("panic: {msg}")).code("panic")),
                    None,
                    true,
                );
            }
            Ok(Ok(o)) => {
                let s = o.status();
                break done(s, attempt, None, Some(o), false);
            }
            Ok(Err(e)) if e.class == ErrorClass::Killed => {
                break done(RepStatus::Killed, attempt, Some(e), None, false);
            }
            Ok(Err(e)) => {
                if e.class.is_retryable() && attempt <= unit.retries && !cancel.is_cancelled() {
                    attempt += 1;
                    continue;
                }
                break done(RepStatus::Failed, attempt, Some(e), None, false);
            }
        }
    };
    if let Some(g) = guard {
        let _ = g.release().await;
    }
    result
}

/// Runs the plan's replications. `on_done` sees each replication as it finishes (for streaming
/// materializations). Results come back in plan order.
pub async fn execute<R: ReplicationRunner>(
    plan: &Plan,
    runner: &R,
    locks: &dyn LockManager,
    opts: &ExecOptions,
    cancel: CancellationToken,
    mut on_done: impl FnMut(&RepResult<R::Output>),
) -> Vec<RepResult<R::Output>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Slot {
        Pending,
        Running,
        /// For dependency purposes: `Succeeded` covers up-to-date and already-succeeded.
        Done(bool),
    }
    let units = units(plan);
    let mut slots = vec![Slot::Pending; units.len()];
    let mut results: HashMap<String, RepResult<R::Output>> = HashMap::new();
    let mut running = FuturesUnordered::new();
    let concurrency = opts.concurrency.max(1);

    let mut finish = |u: &Unit<'_>,
                      d: UnitDone<R::Output>,
                      results: &mut HashMap<String, RepResult<R::Output>>| {
        let mut output = d.output;
        for r in &u.reps {
            let res = RepResult {
                replication: r.name.clone(),
                status: d.status,
                attempts: d.attempts,
                error: d.error.clone(),
                output: output.take(),
                duration: d.duration,
                panicked: d.panicked,
            };
            on_done(&res);
            results.insert(r.name.clone(), res);
        }
    };
    let settled = |i: usize, status: RepStatus, error: AtlatlError| UnitDone {
        idx: i,
        status,
        attempts: 0,
        error: Some(error),
        output: None,
        duration: Duration::ZERO,
        panicked: false,
    };

    loop {
        let mut progressed = true;
        while progressed {
            progressed = false;
            for i in 0..units.len() {
                if slots[i] != Slot::Pending {
                    continue;
                }
                let u = &units[i];
                if cancel.is_cancelled() {
                    slots[i] = Slot::Done(false);
                    finish(
                        u,
                        settled(
                            i,
                            RepStatus::NotStarted,
                            AtlatlError::new(ErrorClass::Killed, "not started: run was killed"),
                        ),
                        &mut results,
                    );
                    progressed = true;
                    continue;
                }
                if u.reps
                    .iter()
                    .all(|r| opts.already_succeeded.contains(&r.name))
                {
                    slots[i] = Slot::Done(true);
                    let e = AtlatlError::new(
                        ErrorClass::Config,
                        "already succeeded under this external run id",
                    )
                    .code("already_succeeded");
                    finish(u, settled(i, RepStatus::Skipped, e), &mut results);
                    progressed = true;
                    continue;
                }
                if u.deps.iter().any(|&d| !matches!(slots[d], Slot::Done(_))) {
                    continue;
                }
                if let Some(&bad) = u.deps.iter().find(|&&d| slots[d] == Slot::Done(false)) {
                    let names: Vec<&str> =
                        units[bad].reps.iter().map(|r| r.name.as_str()).collect();
                    slots[i] = Slot::Done(false);
                    let e = AtlatlError::new(
                        ErrorClass::Config,
                        format!(
                            "skipped: `after` dependency {} did not succeed",
                            names.join(", ")
                        ),
                    )
                    .code("dependency_failed");
                    finish(u, settled(i, RepStatus::Skipped, e), &mut results);
                    progressed = true;
                    continue;
                }
                if running.len() >= concurrency {
                    continue;
                }
                slots[i] = Slot::Running;
                running.push(run_unit(i, u, runner, locks, opts, cancel.clone()));
                progressed = true;
            }
        }
        match running.next().await {
            Some(d) => {
                let i = d.idx;
                slots[i] = Slot::Done(matches!(
                    d.status,
                    RepStatus::Succeeded | RepStatus::UpToDate
                ));
                finish(&units[i], d, &mut results);
            }
            None => break,
        }
    }
    plan.replications
        .iter()
        .filter_map(|r| results.remove(&r.name))
        .collect()
}

#[cfg(test)]
mod tests;
