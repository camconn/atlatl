use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::time::Instant;

use super::*;
use crate::lock::LocalLocks;
use crate::plan::{ConnRef, Plan, RetryPlan, Selection};

fn rep(name: &str) -> ReplicationPlan {
    ReplicationPlan {
        name: name.into(),
        op: name.into(),
        file: format!("replications/{name}.yaml"),
        source: ConnRef {
            name: "lake".into(),
            kind: "ducklake".into(),
        },
        target: ConnRef {
            name: "lake".into(),
            kind: "ducklake".into(),
        },
        after: Vec::new(),
        atomic_group: None,
        lock_group: None,
        lock_timeout_ms: None,
        replication_retries: 0,
        timeout_ms: None,
        retry: RetryPlan {
            max_attempts: 5,
            backoff: "none".into(),
            base_ms: 0,
            max_ms: 0,
            retry_on: vec![],
            budget_ms: None,
        },
        streams: Vec::new(),
        steps: Vec::new(),
        hops: Vec::new(),
        for_each: None,
        instance: None,
        source_snapshot: None,
    }
}

fn plan(reps: Vec<ReplicationPlan>) -> Plan {
    Plan {
        v: 1,
        static_plan: false,
        env: None,
        env_source: None,
        run_id: None,
        project_root: "/p".into(),
        plan_hash: "h".into(),
        selection: Selection {
            replications: vec![],
            streams: vec![],
            tags: vec![],
            instances: vec![],
            with_deps: true,
            range: None,
            vars: Default::default(),
        },
        ops: vec![],
        replications: reps,
        connections: vec![],
        warnings: vec![],
    }
}

#[derive(Clone, Default)]
struct Behave {
    delay: Duration,
    fail: Vec<ErrorClass>,
    panic: bool,
    up_to_date: bool,
}

#[derive(Default)]
struct Fake {
    behave: HashMap<String, Behave>,
    log: Mutex<Vec<(String, &'static str, Instant)>>,
    in_flight: Mutex<(usize, usize)>,
    units: Mutex<Vec<Vec<String>>>,
}

#[derive(Debug)]
struct Out(bool);

impl RunnerOutput for Out {
    fn status(&self) -> RepStatus {
        if self.0 {
            RepStatus::UpToDate
        } else {
            RepStatus::Succeeded
        }
    }
}

impl ReplicationRunner for Fake {
    type Output = Out;

    fn run<'a>(
        &'a self,
        unit: &'a [&'a ReplicationPlan],
        attempt: u32,
        cancel: CancellationToken,
    ) -> BoxFuture<'a, Result<Out, AtlatlError>> {
        Box::pin(async move {
            let name = unit[0].name.clone();
            self.units
                .lock()
                .expect("lock")
                .push(unit.iter().map(|r| r.name.clone()).collect());
            let b = self.behave.get(&name).cloned().unwrap_or_default();
            {
                let mut f = self.in_flight.lock().expect("lock");
                f.0 += 1;
                f.1 = f.1.max(f.0);
            }
            self.log
                .lock()
                .expect("lock")
                .push((name.clone(), "start", Instant::now()));
            let r = tokio::select! {
                _ = tokio::time::sleep(b.delay) => Ok(()),
                _ = cancel.cancelled() => Err(AtlatlError::new(ErrorClass::Killed, "cancelled")),
            };
            self.in_flight.lock().expect("lock").0 -= 1;
            self.log
                .lock()
                .expect("lock")
                .push((name.clone(), "end", Instant::now()));
            r?;
            if b.panic {
                panic!("boom in {name}");
            }
            if let Some(c) = b.fail.get(attempt as usize - 1) {
                return Err(AtlatlError::new(*c, format!("attempt {attempt} failed")));
            }
            Ok(Out(b.up_to_date))
        })
    }
}

fn fake(b: &[(&str, Behave)]) -> Fake {
    Fake {
        behave: b
            .iter()
            .map(|(n, x)| ((*n).to_owned(), x.clone()))
            .collect(),
        ..Default::default()
    }
}

fn secs(n: u64) -> Behave {
    Behave {
        delay: Duration::from_secs(n),
        ..Default::default()
    }
}

async fn run(
    p: &Plan,
    f: &Fake,
    opts: ExecOptions,
    cancel: CancellationToken,
) -> Vec<RepResult<Out>> {
    execute(p, f, &LocalLocks::new(), &opts, cancel, |_| {}).await
}

fn statuses(r: &[RepResult<Out>]) -> Vec<(&str, RepStatus)> {
    r.iter()
        .map(|x| (x.replication.as_str(), x.status))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn concurrency_is_bounded() {
    let p = plan(vec![rep("a"), rep("b"), rep("c"), rep("d")]);
    let f = fake(&[
        ("a", secs(10)),
        ("b", secs(10)),
        ("c", secs(10)),
        ("d", secs(10)),
    ]);
    let t0 = Instant::now();
    let r = run(
        &p,
        &f,
        ExecOptions {
            concurrency: 2,
            ..Default::default()
        },
        CancellationToken::new(),
    )
    .await;
    assert!(r.iter().all(|x| x.status == RepStatus::Succeeded));
    assert_eq!(f.in_flight.lock().expect("lock").1, 2);
    assert_eq!(t0.elapsed(), Duration::from_secs(20));
}

#[tokio::test(start_paused = true)]
async fn after_orders_and_failures_skip_dependents_only() {
    let mut b = rep("b");
    b.after = vec!["a".into()];
    let mut c = rep("c");
    c.after = vec!["b".into()];
    let p = plan(vec![rep("a"), b, c, rep("x")]);
    let f = fake(&[
        (
            "a",
            Behave {
                fail: vec![ErrorClass::Data],
                ..secs(1)
            },
        ),
        ("x", secs(3)),
    ]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert_eq!(
        statuses(&r),
        [
            ("a", RepStatus::Failed),
            ("b", RepStatus::Skipped),
            ("c", RepStatus::Skipped),
            ("x", RepStatus::Succeeded)
        ]
    );
    assert_eq!(
        r[1].error.as_ref().and_then(|e| e.code.as_deref()),
        Some("dependency_failed")
    );
    assert!(
        r[1].error
            .as_ref()
            .is_some_and(|e| e.message.contains("`after` dependency a"))
    );

    let mut b = rep("b");
    b.after = vec!["a".into()];
    let p = plan(vec![b, rep("a")]);
    let f = fake(&[("a", secs(5)), ("b", secs(1))]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert!(r.iter().all(|x| x.status == RepStatus::Succeeded));
    let log = f.log.lock().expect("lock");
    let a_end = log
        .iter()
        .find(|(n, e, _)| n == "a" && *e == "end")
        .map(|x| x.2)
        .expect("a end");
    let b_start = log
        .iter()
        .find(|(n, e, _)| n == "b" && *e == "start")
        .map(|x| x.2)
        .expect("b start");
    assert!(
        b_start >= a_end,
        "b waits for a even though it is declared first"
    );
}

#[tokio::test(start_paused = true)]
async fn atomic_group_runs_as_one_unit() {
    let mut x = rep("x");
    x.atomic_group = Some("g".into());
    let mut y = rep("y");
    y.atomic_group = Some("g".into());
    let p = plan(vec![x, rep("solo"), y]);
    let f = fake(&[(
        "x",
        Behave {
            fail: vec![ErrorClass::Data],
            ..secs(1)
        },
    )]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert_eq!(
        statuses(&r),
        [
            ("x", RepStatus::Failed),
            ("solo", RepStatus::Succeeded),
            ("y", RepStatus::Failed)
        ]
    );
    let units = f.units.lock().expect("lock");
    assert!(
        units.contains(&vec!["x".to_owned(), "y".to_owned()]),
        "{units:?}"
    );
    assert_eq!(units.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn lock_groups_serialize() {
    let mut a = rep("a");
    a.lock_group = Some("warehouse".into());
    let mut b = rep("b");
    b.lock_group = Some("warehouse".into());
    let p = plan(vec![a, b, rep("c")]);
    let f = fake(&[("a", secs(10)), ("b", secs(10)), ("c", secs(10))]);
    let t0 = Instant::now();
    let r = run(
        &p,
        &f,
        ExecOptions {
            concurrency: 3,
            ..Default::default()
        },
        CancellationToken::new(),
    )
    .await;
    assert!(
        r.iter().all(|x| x.status == RepStatus::Succeeded),
        "{:?}",
        statuses(&r)
    );
    assert_eq!(
        t0.elapsed().as_secs(),
        20,
        "a and b share a lock_group; c runs alongside"
    );
}

#[tokio::test(start_paused = true)]
async fn lock_group_timeout_fails_the_replication() {
    let mut a = rep("a");
    a.lock_group = Some("g".into());
    let mut b = rep("b");
    b.lock_group = Some("g".into());
    b.lock_timeout_ms = Some(1_000);
    let p = plan(vec![a, b]);
    let f = fake(&[("a", secs(60))]);
    let r = run(
        &p,
        &f,
        ExecOptions {
            concurrency: 2,
            ..Default::default()
        },
        CancellationToken::new(),
    )
    .await;
    assert_eq!(
        statuses(&r),
        [("a", RepStatus::Succeeded), ("b", RepStatus::Failed)]
    );
    assert_eq!(
        r[1].error.as_ref().map(|e| e.class),
        Some(ErrorClass::Conflict)
    );
}

#[tokio::test(start_paused = true)]
async fn replication_retries_only_retryable() {
    let mut a = rep("a");
    a.replication_retries = 2;
    let mut b = rep("b");
    b.replication_retries = 2;
    let p = plan(vec![a, b]);
    let f = fake(&[
        (
            "a",
            Behave {
                fail: vec![ErrorClass::Transient, ErrorClass::Conflict],
                ..secs(1)
            },
        ),
        (
            "b",
            Behave {
                fail: vec![ErrorClass::Data],
                ..secs(1)
            },
        ),
    ]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert_eq!((r[0].status, r[0].attempts), (RepStatus::Succeeded, 3));
    assert_eq!((r[1].status, r[1].attempts), (RepStatus::Failed, 1));
}

#[tokio::test(start_paused = true)]
async fn timeout_kills_the_attempt() {
    let mut a = rep("a");
    a.timeout_ms = Some(5_000);
    let p = plan(vec![a, rep("b")]);
    let f = fake(&[("a", secs(3600)), ("b", secs(1))]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert_eq!(
        statuses(&r),
        [("a", RepStatus::Killed), ("b", RepStatus::Succeeded)]
    );
    assert_eq!(
        r[0].error.as_ref().and_then(|e| e.code.as_deref()),
        Some("timeout")
    );
    assert_eq!(r[0].duration, Duration::from_secs(5));
}

#[tokio::test(start_paused = true)]
async fn cancel_marks_killed_and_not_started() {
    let mut c = rep("c");
    c.after = vec!["b".into()];
    let p = plan(vec![rep("a"), rep("b"), c, rep("d")]);
    let f = fake(&[("a", secs(1)), ("b", secs(100)), ("d", secs(100))]);
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        c2.cancel();
    });
    let t0 = Instant::now();
    let r = run(
        &p,
        &f,
        ExecOptions {
            concurrency: 2,
            ..Default::default()
        },
        cancel,
    )
    .await;
    assert_eq!(
        statuses(&r),
        [
            ("a", RepStatus::Succeeded),
            ("b", RepStatus::Killed),
            ("c", RepStatus::NotStarted),
            ("d", RepStatus::Killed)
        ]
    );
    assert_eq!(
        t0.elapsed(),
        Duration::from_secs(10),
        "no drain after the kill"
    );
}

#[tokio::test(start_paused = true)]
async fn skip_succeeded_and_up_to_date_count_for_dependents() {
    let mut b = rep("b");
    b.after = vec!["a".into()];
    let mut d = rep("d");
    d.after = vec!["c".into()];
    let p = plan(vec![rep("a"), b, rep("c"), d]);
    let f = fake(&[(
        "c",
        Behave {
            up_to_date: true,
            ..secs(1)
        },
    )]);
    let opts = ExecOptions {
        already_succeeded: ["a".to_owned()].into(),
        ..Default::default()
    };
    let r = run(&p, &f, opts, CancellationToken::new()).await;
    assert_eq!(
        statuses(&r),
        [
            ("a", RepStatus::Skipped),
            ("b", RepStatus::Succeeded),
            ("c", RepStatus::UpToDate),
            ("d", RepStatus::Succeeded)
        ]
    );
    assert_eq!(
        r[0].error.as_ref().and_then(|e| e.code.as_deref()),
        Some("already_succeeded")
    );
    assert!(
        !f.units
            .lock()
            .expect("lock")
            .iter()
            .any(|u| u == &vec!["a".to_owned()])
    );
}

#[tokio::test(start_paused = true)]
async fn panics_are_contained() {
    let p = plan(vec![rep("a"), rep("b")]);
    let f = fake(&[
        (
            "a",
            Behave {
                panic: true,
                ..secs(1)
            },
        ),
        ("b", secs(2)),
    ]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert_eq!(
        statuses(&r),
        [("a", RepStatus::Failed), ("b", RepStatus::Succeeded)]
    );
    assert!(r[0].panicked);
    let e = r[0].error.as_ref().expect("error");
    assert_eq!(
        (e.class, e.message.as_str()),
        (ErrorClass::Internal, "panic: boom in a")
    );
}

#[tokio::test(start_paused = true)]
async fn results_stream_as_they_finish() {
    let p = plan(vec![rep("slow"), rep("fast")]);
    let f = fake(&[("slow", secs(10)), ("fast", secs(1))]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s2 = seen.clone();
    let r = execute(
        &p,
        &f,
        &LocalLocks::new(),
        &ExecOptions::default(),
        CancellationToken::new(),
        move |x| {
            s2.lock().expect("lock").push(x.replication.clone());
        },
    )
    .await;
    assert_eq!(*seen.lock().expect("lock"), ["fast", "slow"]);
    assert_eq!(
        r.iter().map(|x| x.replication.as_str()).collect::<Vec<_>>(),
        ["slow", "fast"],
        "results in plan order"
    );
}

fn instance(of: &str, key: &str) -> ReplicationPlan {
    let mut r = rep(&format!("{of}[{key}]"));
    r.instance = Some(crate::plan::InstancePlan {
        of: of.into(),
        key: key.into(),
        as_name: "tenant".into(),
        item: key.into(),
    });
    r
}

#[tokio::test(start_paused = true)]
async fn for_each_instances_are_independent_units_and_after_waits_for_all() {
    let mut after = rep("after");
    after.after = vec!["t".into()];
    let p = plan(vec![instance("t", "a"), instance("t", "b"), after]);
    let f = fake(&[
        ("t[a]", secs(1)),
        (
            "t[b]",
            Behave {
                fail: vec![ErrorClass::Data],
                ..secs(3)
            },
        ),
    ]);
    let r = run(&p, &f, ExecOptions::default(), CancellationToken::new()).await;
    assert_eq!(
        statuses(&r),
        [
            ("t[a]", RepStatus::Succeeded),
            ("t[b]", RepStatus::Failed),
            ("after", RepStatus::Skipped)
        ]
    );
    assert_eq!(
        f.units.lock().expect("lock").len(),
        2,
        "one unit per instance"
    );
    let r = run(
        &p,
        &fake(&[("t[a]", secs(1)), ("t[b]", secs(3))]),
        ExecOptions {
            already_succeeded: ["t[b]".to_owned()].into(),
            ..Default::default()
        },
        CancellationToken::new(),
    )
    .await;
    assert_eq!(
        statuses(&r),
        [
            ("t[a]", RepStatus::Succeeded),
            ("t[b]", RepStatus::Skipped),
            ("after", RepStatus::Succeeded)
        ],
        "retries redo only the instances that failed"
    );
}
