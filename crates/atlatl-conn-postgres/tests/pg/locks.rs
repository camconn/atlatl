use std::sync::Arc;
use std::time::{Duration, Instant};

use atlatl_conn_postgres::{ConnectOptions, PgAdvisoryLocks, connect};
use atlatl_core::lock::{LOCK_TIMEOUT_CODE, LockKey, LockManager};
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_testkit::Postgres;

use crate::fresh;

on_both!(
    acquire_and_release,
    contention_times_out_naming_holder,
    drop_releases_to_waiter,
    sorted_acquisition_avoids_deadlock,
    terminated_holder_frees_lock,
    group_locks,
    trait_object,
);

const ZERO: Duration = Duration::ZERO;

fn key(t: &str) -> LockKey {
    LockKey::table("app", "public", t)
}

async fn advisory_locks_of(o: &ConnectOptions, pid: i32) -> i64 {
    let s = connect(o).await.expect("connect");
    s.client()
        .query_one(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid = $1",
            &[&pid],
        )
        .await
        .expect("pg_locks")
        .get(0)
}

async fn acquire_and_release(pg: &'static Postgres) {
    let o = fresh(pg);
    let m = PgAdvisoryLocks::new(o.clone());
    let g = m
        .lock(&[key("b"), key("a"), key("b")], ZERO)
        .await
        .expect("lock");
    assert_eq!(g.keys(), &[key("a"), key("b")]);
    assert_eq!(advisory_locks_of(&o, g.pid()).await, 2);
    g.check().await.expect("held");
    let pid = g.pid();
    g.release().await.expect("release");
    assert_eq!(advisory_locks_of(&o, pid).await, 0);
    let g = m.lock(&[key("a"), key("b")], ZERO).await.expect("relock");
    assert_ne!(g.pid(), pid, "a session per acquisition");
}

async fn contention_times_out_naming_holder(pg: &'static Postgres) {
    let o = fresh(pg);
    let mut ho = o.clone();
    ho.application_name = "holder-app".into();
    let holder = PgAdvisoryLocks::new(ho)
        .lock(&[key("k")], ZERO)
        .await
        .expect("holder");

    let m = PgAdvisoryLocks::new(o.clone());
    let start = Instant::now();
    let e = m
        .lock(&[key("k"), key("a")], Duration::from_millis(400))
        .await
        .expect_err("contended");
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_millis(400) && waited < Duration::from_secs(5),
        "{waited:?}"
    );
    assert_eq!(e.class, ErrorClass::Conflict);
    assert_eq!(e.code.as_deref(), Some(LOCK_TIMEOUT_CODE));
    assert!(e.message.contains("`atlatl:app:public.k`"), "{e}");
    assert!(e.message.contains(&format!("pid {}", holder.pid())), "{e}");
    assert!(e.message.contains("holder-app"), "{e}");

    // `a` sorts first and was taken, then given back.
    drop(m.lock(&[key("a")], ZERO).await.expect("a is free"));
}

async fn drop_releases_to_waiter(pg: &'static Postgres) {
    let m = PgAdvisoryLocks::new(fresh(pg));
    let holder = m.lock(&[key("k")], ZERO).await.expect("holder");
    let m2 = m.clone();
    let waiter = tokio::spawn(async move {
        m2.lock(&[key("k")], Duration::from_secs(30))
            .await
            .map(|g| g.pid())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waiter.is_finished());
    let released = Instant::now();
    drop(holder);
    waiter.await.expect("join").expect("waiter acquires");
    assert!(released.elapsed() < Duration::from_secs(5));
}

async fn sorted_acquisition_avoids_deadlock(pg: &'static Postgres) {
    let m = PgAdvisoryLocks::new(fresh(pg));
    let run = |keys: Vec<LockKey>| {
        let m = m.clone();
        tokio::spawn(async move {
            for _ in 0..10 {
                let g = m.lock(&keys, Duration::from_secs(30)).await?;
                tokio::time::sleep(Duration::from_millis(20)).await;
                g.release().await?;
            }
            Ok::<_, AtlatlError>(())
        })
    };
    let a = run(vec![key("x"), key("y"), key("z")]);
    let b = run(vec![key("z"), key("y"), key("x")]);
    let (a, b) = tokio::time::timeout(Duration::from_secs(60), async { (a.await, b.await) })
        .await
        .expect("no deadlock");
    a.expect("join").expect("a");
    b.expect("join").expect("b");
}

async fn terminated_holder_frees_lock(pg: &'static Postgres) {
    let o = fresh(pg);
    let m = PgAdvisoryLocks::new(o.clone());
    let holder = m.lock(&[key("k")], ZERO).await.expect("holder");
    let admin = connect(&o).await.expect("admin");
    let killed: bool = admin
        .client()
        .query_one("SELECT pg_terminate_backend($1)", &[&holder.pid()])
        .await
        .expect("terminate")
        .get(0);
    assert!(killed);

    m.lock(&[key("k")], Duration::from_secs(10))
        .await
        .expect("freed by session end");
    let e = holder.check().await.expect_err("lost");
    assert_eq!(e.class, ErrorClass::Transient, "{e}");
    assert!(e.message.contains("lock session lost"), "{e}");
}

async fn group_locks(pg: &'static Postgres) {
    let m = PgAdvisoryLocks::new(fresh(pg));
    let g = m.lock_group("nightly", ZERO).await.expect("group");
    assert_eq!(g.keys(), &[LockKey::group("nightly")]);
    let e = m
        .lock_group("nightly", Duration::from_millis(200))
        .await
        .expect_err("held");
    assert_eq!(e.class, ErrorClass::Conflict);
    assert!(e.message.contains("`atlatl-group:nightly`"), "{e}");
    assert!(e.message.contains(&format!("pid {}", g.pid())), "{e}");
    drop(m.lock_group("other", ZERO).await.expect("other group"));
    drop(
        m.lock(&[key("nightly")], ZERO)
            .await
            .expect("table key is distinct"),
    );
    drop(g);
    drop(
        m.lock_group("nightly", Duration::from_secs(5))
            .await
            .expect("released"),
    );
}

async fn trait_object(pg: &'static Postgres) {
    let m: Arc<dyn LockManager> = Arc::new(PgAdvisoryLocks::new(fresh(pg)));
    let keys = [key("a"), LockKey::group("g")];
    let g = m.acquire(&keys, ZERO).await.expect("lock");
    assert_eq!(
        g.keys(),
        &[LockKey::group("g"), key("a")],
        "group locks sort before table locks"
    );
    g.check().await.expect("held");
    let e = m
        .acquire(&[LockKey::group("g")], Duration::from_millis(100))
        .await
        .expect_err("held");
    assert_eq!(e.class, ErrorClass::Conflict);
    g.release().await.expect("release");
    drop(m.acquire(&keys, ZERO).await.expect("free"));
}
