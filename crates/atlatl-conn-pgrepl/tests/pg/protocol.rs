use atlatl_conn_postgres::connect;
use atlatl_core::ErrorClass;
use atlatl_core::config::model::PgProtocol;

use crate::apply::{T_INSERT, T_ROWS, T_WANT, TYPES_DDL};
use crate::{Env, protocol, stream, versions};

async fn negotiates_by_server_version(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE n (id int PRIMARY KEY, v text)")
        .await;
    let spec = env.spec(vec![stream("n")]);
    let r = env.run(spec).await;
    let w = &r.applied.window;
    assert_eq!(
        (w.protocol, w.binary),
        if v >= 14 { (2, true) } else { (1, false) }
    );
}
versions!(negotiates_by_server_version);

async fn typed_values(env: &Env, p: PgProtocol) -> (u32, bool) {
    env.exec(TYPES_DDL).await;
    let spec = env.spec(vec![stream("t")]);
    env.try_run(spec.clone(), protocol(p))
        .await
        .expect("backfill");
    env.exec(T_INSERT).await;
    let r = env.try_run(spec, protocol(p)).await.expect("stream");
    assert_eq!(env.rows(T_ROWS), T_WANT);
    (r.applied.window.protocol, r.applied.window.binary)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overrides_on_pg16() {
    assert_eq!(
        typed_values(&Env::new(16).await, PgProtocol::V1).await,
        (1, false)
    );
    assert_eq!(
        typed_values(&Env::new(16).await, PgProtocol::V2).await,
        (2, true)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_is_refused_before_pg14() {
    let env = Env::new(12).await;
    env.exec("CREATE TABLE n (id int PRIMARY KEY)").await;
    let e = env
        .try_run(env.spec(vec![stream("n")]), protocol(PgProtocol::V2))
        .await
        .expect_err("PG 12");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(env.confirmed().await.is_none(), "no slot is created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_falls_back_to_text_for_unknown_types() {
    let env = Env::new(16).await;
    env.exec(
        "CREATE TABLE f (id int PRIMARY KEY, addr inet); INSERT INTO f VALUES (1, '10.0.0.1');",
    )
    .await;
    let spec = env.spec(vec![stream("f")]);
    env.run(spec.clone()).await;
    env.exec("INSERT INTO f VALUES (2, '192.168.0.0/16')").await;
    let r = env.run(spec).await;
    assert_eq!(
        (r.applied.window.protocol, r.applied.window.binary),
        (2, false)
    );
    assert_eq!(
        env.rows("SELECT id::VARCHAR || ' ' || addr FROM lake.br.f ORDER BY id"),
        ["1 10.0.0.1", "2 192.168.0.0/16"]
    );
}

async fn same_rows(env: &Env) {
    let want = env
        .pg_rows("SELECT id || '|' || v FROM s ORDER BY id")
        .await;
    assert_eq!(
        env.rows("SELECT id::VARCHAR || '|' || v FROM lake.br.s ORDER BY id"),
        want
    );
}

/// With `logical_decoding_work_mem = 64kB`, these transactions arrive as stream blocks.
async fn streamed_transactions(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE s (id int PRIMARY KEY, v text); INSERT INTO s VALUES (0, 'orig');")
        .await;
    let spec = env.spec(vec![stream("s")]);
    env.run(spec.clone()).await;

    env.each(&[
        "BEGIN;
         INSERT INTO s SELECT i, repeat('x', 40) FROM generate_series(1, 3000) i;
         SAVEPOINT a;
         INSERT INTO s SELECT i, repeat('r', 40) FROM generate_series(5001, 8000) i;
         ROLLBACK TO a;
         SAVEPOINT b;
         UPDATE s SET v = 'kept' WHERE id = 1;
         RELEASE b;
         COMMIT",
        "UPDATE s SET v = 'regular' WHERE id = 0",
    ])
    .await;
    let r = env.run(spec.clone()).await;
    let w = &r.applied.window;
    assert!(w.streamed_transactions >= 1, "{w:?}");
    // The savepoint's rows were streamed before its Stream Abort, then dropped.
    assert!(w.changes > 4000, "{w:?}");
    assert_eq!(w.transactions, 2, "{w:?}");
    assert_eq!(r.applied.streams[0].counts.inserted, 3000);
    same_rows(&env).await;
    assert_eq!(
        env.rows("SELECT v FROM lake.br.s WHERE id IN (0, 1) ORDER BY id"),
        ["regular", "kept"]
    );

    env.exec(
        "BEGIN; INSERT INTO s SELECT i, repeat('y', 40) FROM generate_series(10001, 14000) i; ROLLBACK",
    )
    .await;
    // Already aborted when decoded, so the server drops it rather than streaming it (a
    // whole-transaction Stream Abort is covered by `source::tests`).
    let r = env.run(spec.clone()).await;
    assert_eq!(r.applied.streams[0].counts, Default::default());
    same_rows(&env).await;

    env.exec(
        "BEGIN; TRUNCATE s; INSERT INTO s SELECT i, repeat('z', 40) FROM generate_series(20001, 23000) i; COMMIT",
    )
    .await;
    let r = env.run(spec.clone()).await;
    assert!(r.applied.streams[0].truncated);
    assert!(r.applied.window.streamed_transactions >= 1);
    same_rows(&env).await;

    // Still open when the window ends: dropped, then redelivered in full after its commit.
    let other = connect(&env.opts).await.expect("connect");
    other
        .client()
        .batch_execute(
            "BEGIN; INSERT INTO s SELECT i, repeat('o', 40) FROM generate_series(30001, 33000) i",
        )
        .await
        .expect("open");
    let r = env.run(spec.clone()).await;
    assert!(
        r.applied.window.changes > 1000,
        "streamed, then dropped: {:?}",
        r.applied.window
    );
    assert_eq!(r.applied.streams[0].counts, Default::default());
    assert_eq!(
        env.rows("SELECT count(*)::VARCHAR FROM lake.br.s WHERE id > 30000"),
        ["0"]
    );
    other
        .client()
        .batch_execute("COMMIT")
        .await
        .expect("commit");
    let r = env.run(spec.clone()).await;
    assert_eq!(r.applied.streams[0].counts.inserted, 3000);
    same_rows(&env).await;

    // A regular transaction commits between the stream blocks of one that commits later.
    other
        .client()
        .batch_execute(
            "BEGIN; INSERT INTO s SELECT i, repeat('p', 40) FROM generate_series(40001, 43000) i",
        )
        .await
        .expect("open");
    env.each(&[
        "INSERT INTO s VALUES (50000, 'between')",
        "UPDATE s SET v = 'between' WHERE id = 20001",
    ])
    .await;
    other
        .client()
        .batch_execute("UPDATE s SET v = 'late' WHERE id = 40001; COMMIT")
        .await
        .expect("commit");
    let r = env.run(spec).await;
    let w = &r.applied.window;
    assert!(w.streamed_transactions >= 1 && w.transactions == 3, "{w:?}");
    assert_eq!(r.applied.streams[0].counts.inserted, 3001);
    same_rows(&env).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_transactions_pg14() {
    streamed_transactions(14).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_transactions_pg16() {
    streamed_transactions(16).await
}

/// A column type without a binary decoder that isn't in the catalog any more when the window
/// starts: the window restarts with text values instead of failing on every run.
async fn restarts_as_text_for_a_passing_type(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE x (id int PRIMARY KEY, v text)")
        .await;
    let spec = env.spec(vec![stream("x")]);
    env.run(spec.clone()).await;
    env.each(&[
        "ALTER TABLE x ADD COLUMN addr inet",
        "INSERT INTO x VALUES (1, 'a', '10.0.0.1')",
        "ALTER TABLE x DROP COLUMN addr",
        "INSERT INTO x VALUES (2, 'b')",
    ])
    .await;
    let r = env.run(spec.clone()).await;
    assert_eq!(
        (r.applied.window.protocol, r.applied.window.binary),
        (2, false)
    );
    assert_eq!(r.applied.streams[0].counts.inserted, 2);
    assert_eq!(
        env.rows("SELECT id::VARCHAR || v || coalesce(addr, '∅') FROM lake.br.x ORDER BY id"),
        ["1a10.0.0.1", "2b∅"]
    );
    env.exec("INSERT INTO x VALUES (3, 'c')").await;
    let r = env.run(spec).await;
    assert!(r.applied.window.binary);
    assert_eq!(r.applied.streams[0].counts.inserted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarts_as_text_pg14() {
    restarts_as_text_for_a_passing_type(14).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarts_as_text_pg16() {
    restarts_as_text_for_a_passing_type(16).await
}
