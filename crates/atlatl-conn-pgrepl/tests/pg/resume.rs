use std::time::Duration;

use atlatl_conn_pgrepl::Lsn;
use atlatl_core::AtlatlError;

use crate::{Env, policy, stream, versions};

const HISTORY: &str = "SELECT id::VARCHAR || ':' || coalesce(v, '∅') || ':' || _atlatl_op FROM lake.br.h ORDER BY _atlatl_lsn, id";

async fn kill_mid_window_resumes_exactly_once(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE h (id int PRIMARY KEY, v text)")
        .await;
    let mut s = stream("h");
    s.history = true;
    let spec = env.spec(vec![s]);
    env.run(spec.clone()).await;
    let c0 = env.confirmed().await.expect("slot");

    // Killed after staging, before the lake commit: nothing lands, nothing is confirmed.
    env.each(&[
        "INSERT INTO h VALUES (1, 'a')",
        "INSERT INTO h VALUES (2, 'b')",
    ])
    .await;
    let mut src = env.source(spec.clone(), policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-killed")
        .await
        .expect("window");
    assert_eq!(w.summary().changes, 2);
    drop(w);
    drop(src);
    assert_eq!(env.confirmed().await.as_deref(), Some(c0.as_str()));
    assert!(env.rows(HISTORY).is_empty());

    env.exec("UPDATE h SET v = 'b2' WHERE id = 2").await;
    let r = env.run(spec.clone()).await;
    assert_eq!(r.applied.window.transactions, 3);
    assert_eq!(env.rows(HISTORY), ["1:a:insert", "2:b2:update"]);

    // Killed after the lake commit, before the confirm: the redelivered transaction is skipped.
    env.exec("INSERT INTO h VALUES (3, 'c')").await;
    let mut src = env.source(spec.clone(), policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-unconfirmed")
        .await
        .expect("window");
    let applied = w.apply().expect("apply");
    assert!(applied.committed);
    drop(src);
    let cursor = env.cursor("pg/h").expect("cursor");
    assert_ne!(env.confirmed().await, Some(cursor.clone()));

    env.exec("INSERT INTO h VALUES (4, 'd')").await;
    let r = env.run(spec).await;
    assert_eq!(
        r.applied.window.skipped_transactions, 1,
        "{:?}",
        r.applied.window
    );
    assert_eq!(r.applied.window.transactions, 1);
    assert_eq!(
        env.rows(HISTORY),
        ["1:a:insert", "2:b2:update", "3:c:insert", "4:d:insert"]
    );
}
versions!(kill_mid_window_resumes_exactly_once);

async fn confirmed_only_after_commit(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE c (id int PRIMARY KEY, v text)")
        .await;
    let spec = env.spec(vec![stream("c")]);
    env.run(spec.clone()).await;
    let c1 = env.cursor("pg/c").expect("cursor");
    assert!(env.confirmed_reaches(&c1).await);

    env.exec("INSERT INTO c VALUES (1, 'a')").await;
    let mut src = env.source(spec.clone(), policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-fail")
        .await
        .expect("window");
    assert!(w.summary().upto > c1.parse::<Lsn>().expect("lsn"));
    let e = w
        .apply_with(|_| Err(AtlatlError::internal("injected before COMMIT")))
        .expect_err("injected");
    assert!(e.message.contains("injected"));
    src.close().await.expect("close");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(env.cursor("pg/c"), Some(c1.clone()));
    assert_eq!(env.confirmed().await, Some(c1.clone()));
    assert_eq!(env.rows("SELECT count(*)::VARCHAR FROM lake.br.c"), ["0"]);

    let mut src = env.source(spec.clone(), policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-ok")
        .await
        .expect("window");
    let applied = w.apply().expect("apply");
    let c2 = applied.confirm.expect("confirm");
    assert_eq!(env.cursor("pg/c"), Some(c2.to_string()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        env.confirmed().await,
        Some(c1.clone()),
        "confirmed before the call"
    );
    src.confirm(c2).await.expect("confirm");
    assert!(env.confirmed_reaches(&c2.to_string()).await);
    src.close().await.expect("close");
    assert_eq!(env.rows("SELECT id::VARCHAR || v FROM lake.br.c"), ["1a"]);
}
versions!(confirmed_only_after_commit);

async fn gap_is_a_data_error(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE gp (id int PRIMARY KEY)").await;
    let spec = env.spec(vec![stream("gp")]);
    env.run(spec.clone()).await;
    let cursor = env.cursor("pg/gp").expect("cursor");

    // Someone else consumes past the lake's cursor.
    env.exec("INSERT INTO gp VALUES (1)").await;
    let mut src = env.source(spec.clone(), policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-steal")
        .await
        .expect("window");
    let upto = w.summary().upto;
    drop(w);
    src.confirm(upto).await.expect("confirm");
    assert!(
        src.wait_confirmed(upto, Duration::from_secs(5))
            .await
            .expect("wait")
            >= upto
    );
    let e = src
        .check_retained(cursor.parse().expect("lsn"))
        .await
        .expect_err("gap");
    assert_eq!(e.code.as_deref(), Some("cursor_gap"));
    src.close().await.expect("close");

    let e = env
        .try_run(spec, policy(true, false))
        .await
        .expect_err("gap");
    assert_eq!(e.class, atlatl_core::ErrorClass::Data);
    assert_eq!(e.code.as_deref(), Some("cursor_gap"));
}
versions!(gap_is_a_data_error);

async fn confirm_reconnects_after_a_dropped_connection(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE rc (id int PRIMARY KEY)").await;
    let spec = env.spec(vec![stream("rc")]);
    env.run(spec.clone()).await;
    env.exec("INSERT INTO rc VALUES (1)").await;
    let mut src = env.source(spec, policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-drop")
        .await
        .expect("window");
    env.exec(&format!(
        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{}'",
        atlatl_conn_pgrepl::slot_name(&env.name)
    ))
    .await;
    let lsn = w.apply().expect("apply").confirm.expect("confirm");
    src.confirm(lsn).await.expect("confirm after reconnect");
    let ok = env.confirmed_reaches(&lsn.to_string()).await;
    assert!(ok, "confirmed {:?}, want {lsn}", env.confirmed().await);
    src.close().await.expect("close");
}
versions!(confirm_reconnects_after_a_dropped_connection);
