use std::time::Duration;

use atlatl_conn_postgres::{ConnectOptions, Pool, PoolOptions, PoolStatus, Session, connect};
use atlatl_core::ErrorClass;
use atlatl_testkit::Postgres;

use crate::fresh;

on_both!(
    reuses_sessions,
    ping_replaces_terminated_session,
    closed_session_is_replaced_without_ping,
    dead_session_is_not_returned,
    max_size_and_checkout_timeout,
);

async fn pid(s: &Session) -> i32 {
    s.client()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0)
}

async fn terminate(o: &ConnectOptions, pid: i32) {
    let admin = connect(o).await.expect("admin");
    let killed: bool = admin
        .client()
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .expect("terminate")
        .get(0);
    assert!(killed);
}

fn pool(o: &ConnectOptions, cfg: PoolOptions) -> Pool {
    Pool::new(o.clone(), cfg).expect("pool")
}

async fn reuses_sessions(pg: &'static Postgres) {
    let p = pool(&fresh(pg), PoolOptions::default());
    let a = p.get().await.expect("a");
    let b = p.get().await.expect("b");
    let (pa, pb) = (pid(&a).await, pid(&b).await);
    assert_ne!(pa, pb);
    assert_eq!(
        p.status(),
        PoolStatus {
            idle: 0,
            in_use: 2,
            max_size: 4
        }
    );
    drop(a);
    drop(b);
    assert_eq!(p.status().idle, 2);
    assert_eq!(p.status().in_use, 0);
    let c = p.get().await.expect("c");
    assert_eq!(pid(&c).await, pb, "LIFO reuse");
    c.discard();
    assert_eq!(p.status().idle, 1);
}

async fn ping_replaces_terminated_session(pg: &'static Postgres) {
    let o = fresh(pg);
    let p = pool(
        &o,
        PoolOptions {
            ping_after_idle: Duration::ZERO,
            ..PoolOptions::default()
        },
    );
    let s = p.get().await.expect("first");
    let old = pid(&s).await;
    drop(s);
    terminate(&o, old).await;
    let s = p.get().await.expect("replacement");
    let new = pid(&s).await;
    assert_ne!(new, old);
    s.client().batch_execute("SELECT 1").await.expect("works");
}

async fn closed_session_is_replaced_without_ping(pg: &'static Postgres) {
    let o = fresh(pg);
    let p = pool(
        &o,
        PoolOptions {
            ping_after_idle: Duration::from_secs(3600),
            ..PoolOptions::default()
        },
    );
    let s = p.get().await.expect("first");
    let old = pid(&s).await;
    drop(s);
    terminate(&o, old).await;
    // The connection task sees the FATAL and EOF, which `is_closed` reports.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let s = p.get().await.expect("replacement");
    assert_ne!(pid(&s).await, old);
}

async fn dead_session_is_not_returned(pg: &'static Postgres) {
    let o = fresh(pg);
    let p = pool(&o, PoolOptions::default());
    let s = p.get().await.expect("first");
    terminate(&o, pid(&s).await).await;
    assert!(s.client().batch_execute("SELECT 1").await.is_err());
    for _ in 0..20 {
        if s.is_closed() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(s);
    assert_eq!(p.status().idle, 0);
}

async fn max_size_and_checkout_timeout(pg: &'static Postgres) {
    let p = pool(
        &fresh(pg),
        PoolOptions {
            max_size: 1,
            checkout_timeout: Duration::from_millis(200),
            ..PoolOptions::default()
        },
    );
    let held = p.get().await.expect("held");
    let e = p.get().await.expect_err("full");
    assert_eq!(e.class, ErrorClass::Timeout, "{e}");
    assert!(e.message.contains("pool of 1"), "{e}");

    let p2 = p.clone();
    let waiter = tokio::spawn(async move {
        let s = p2.get().await?;
        Ok::<_, atlatl_core::AtlatlError>(pid(&s).await)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let held_pid = pid(&held).await;
    drop(held);
    assert_eq!(waiter.await.expect("join").expect("waiter"), held_pid);
    assert_eq!(
        Pool::new(
            p.options().clone(),
            PoolOptions {
                max_size: 0,
                ..PoolOptions::default()
            }
        )
        .expect_err("zero")
        .class,
        ErrorClass::Config
    );
}
