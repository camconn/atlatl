use std::time::{Duration, Instant};

use atlatl_conn_postgres::{Session, classify, connect};
use atlatl_core::ErrorClass;
use atlatl_testkit::Postgres;

use crate::fresh;

on_both!(
    syntax_error_is_config,
    unique_violation_is_data,
    lock_timeout_is_conflict,
    statement_timeout_is_timeout,
    closed_connection_is_transient,
    bad_password_is_auth,
    missing_database_is_config,
    refused_connection_is_transient,
    messages_are_redacted,
);

async fn session(pg: &Postgres) -> Session {
    connect(&fresh(pg)).await.expect("connect")
}

async fn syntax_error_is_config(pg: &'static Postgres) {
    let s = session(pg).await;
    let e = classify(
        &s.client()
            .batch_execute("SELEC 1")
            .await
            .expect_err("syntax"),
    );
    assert_eq!(e.class, ErrorClass::Config);
    assert_eq!(e.code.as_deref(), Some("42601"));
    assert!(e.message.contains("syntax error"), "{e}");
}

async fn unique_violation_is_data(pg: &'static Postgres) {
    let s = session(pg).await;
    let c = s.client();
    c.batch_execute("CREATE TABLE t (id int PRIMARY KEY); INSERT INTO t VALUES (1)")
        .await
        .expect("setup");
    let e = classify(
        &c.batch_execute("INSERT INTO t VALUES (1)")
            .await
            .expect_err("dup"),
    );
    assert_eq!(e.class, ErrorClass::Data);
    assert_eq!(e.code.as_deref(), Some("23505"));
    assert!(e.message.contains("Key (id)=(1) already exists"), "{e}");
    assert!(e.message.contains("constraint: t_pkey"), "{e}");
}

async fn lock_timeout_is_conflict(pg: &'static Postgres) {
    let o = fresh(pg);
    let a = connect(&o).await.expect("a");
    let b = connect(&o).await.expect("b");
    a.client()
        .batch_execute("CREATE TABLE t (id int)")
        .await
        .expect("create");
    a.client()
        .batch_execute("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE")
        .await
        .expect("lock");
    let e = classify(
        &b.client()
            .batch_execute("SET lock_timeout = '1ms'; SELECT * FROM t")
            .await
            .expect_err("lock timeout"),
    );
    assert_eq!(e.class, ErrorClass::Conflict);
    assert_eq!(e.code.as_deref(), Some("55P03"));
}

async fn statement_timeout_is_timeout(pg: &'static Postgres) {
    let mut o = fresh(pg);
    o.statement_timeout = Some(Duration::from_millis(50));
    let s = connect(&o).await.expect("connect");
    let start = Instant::now();
    let e = classify(
        &s.client()
            .batch_execute("SELECT pg_sleep(5)")
            .await
            .expect_err("timeout"),
    );
    assert!(start.elapsed() < Duration::from_secs(4));
    assert_eq!(e.class, ErrorClass::Timeout);
    assert_eq!(e.code.as_deref(), Some("57014"));
}

async fn closed_connection_is_transient(pg: &'static Postgres) {
    let o = fresh(pg);
    let victim = connect(&o).await.expect("victim");
    let admin = connect(&o).await.expect("admin");
    let pid: i32 = victim
        .client()
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    admin
        .client()
        .execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .expect("terminate");
    // The first error may be the FATAL 57P01 notice; after that the client is closed.
    let mut closed = false;
    for _ in 0..5 {
        let raw = victim
            .client()
            .batch_execute("SELECT 1")
            .await
            .expect_err("dead");
        let e = classify(&raw);
        assert_eq!(e.class, ErrorClass::Transient, "{e}");
        if raw.is_closed() {
            closed = true;
            break;
        }
        assert_eq!(e.code.as_deref(), Some("57P01"), "{e}");
    }
    assert!(closed);
    assert!(victim.is_closed());
}

async fn bad_password_is_auth(pg: &'static Postgres) {
    let mut o = fresh(pg);
    o.password = Some("definitely-wrong".into());
    let e = connect(&o).await.expect_err("auth");
    assert_eq!(e.class, ErrorClass::Auth, "{e}");
    assert_eq!(e.code.as_deref(), Some("28P01"));
}

async fn missing_database_is_config(pg: &'static Postgres) {
    let mut o = fresh(pg);
    o.database = "no_such_db".into();
    let e = connect(&o).await.expect_err("missing");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert_eq!(e.code.as_deref(), Some("3D000"));
}

async fn refused_connection_is_transient(pg: &'static Postgres) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port();
    let mut o = fresh(pg);
    o.host = "127.0.0.1".into();
    o.port = port;
    let e = connect(&o).await.expect_err("refused");
    assert_eq!(e.class, ErrorClass::Transient, "{e}");
    assert!(e.message.contains("error connecting to server"), "{e}");
}

async fn messages_are_redacted(pg: &'static Postgres) {
    atlatl_core::redact::register("sekrit-pg-err");
    let s = session(pg).await;
    let e = classify(
        &s.client()
            .batch_execute("SELECT 'sekrit-pg-err'::int")
            .await
            .expect_err("cast"),
    );
    assert_eq!(e.class, ErrorClass::Data);
    assert_eq!(e.code.as_deref(), Some("22P02"));
    assert!(!e.message.contains("sekrit"), "{}", e.message);
    assert!(e.message.contains("****"), "{}", e.message);
}
