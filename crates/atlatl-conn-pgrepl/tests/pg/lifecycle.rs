use std::time::Duration;

use atlatl_conn_pgrepl::{decommission, slot_name, slot_stats};
use atlatl_core::ErrorClass;
use atlatl_core::config::diag::Diagnostics;
use atlatl_core::config::env::{Resolve, resolve_all};
use atlatl_core::config::load;
use atlatl_core::template::Templates;
use atlatl_testkit::TempProject;

use crate::{Env, policy, stream, versions};

async fn slot_count(env: &Env) -> usize {
    env.pg_rows(&format!(
        "SELECT slot_name::text FROM pg_replication_slots WHERE slot_name = '{}'",
        slot_name(&env.name)
    ))
    .await
    .len()
}

async fn decommission_drops_slot_and_publication(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE d (id int PRIMARY KEY); INSERT INTO d VALUES (1);")
        .await;
    let spec = env.spec(vec![stream("d")]);
    env.run(spec.clone()).await;
    assert_eq!(slot_count(&env).await, 1);

    // A running window holds the slot: refused without force, terminated with it.
    let mut src = env.source(spec.clone(), policy(true, false)).await;
    let w = src
        .changes(env.duck(), "lake", "run-busy")
        .await
        .expect("window");
    let e = decommission(&env.opts, &policy(true, false), &env.name, false)
        .await
        .expect_err("busy");
    assert_eq!(e.class, ErrorClass::Conflict, "{e}");
    let d = decommission(&env.opts, &policy(true, false), &env.name, true)
        .await
        .expect("forced");
    assert!(d.slot_dropped && d.publication_dropped, "{d:?}");
    drop(w);
    drop(src);
    assert_eq!(slot_count(&env).await, 0);
    let pubname = slot_name(&env.name);
    assert!(
        env.pg_rows(&format!(
            "SELECT pubname::text FROM pg_publication WHERE pubname = '{pubname}'"
        ))
        .await
        .is_empty()
    );
    let again = decommission(&env.opts, &policy(true, false), &env.name, false)
        .await
        .expect("idempotent");
    assert!(!again.slot_dropped && !again.publication_dropped);

    let e = env
        .try_run(spec, policy(true, false))
        .await
        .expect_err("cursor without slot");
    assert_eq!(e.class, ErrorClass::Data);
    assert_eq!(e.code.as_deref(), Some("slot_missing"));
}
versions!(decommission_drops_slot_and_publication);

async fn existing_slot_only_without_allow_create(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE a (id int PRIMARY KEY)").await;
    let spec = env.spec(vec![stream("a")]);
    let e = env
        .try_run(spec.clone(), policy(false, false))
        .await
        .expect_err("no slot");
    assert_eq!(e.code.as_deref(), Some("slot_absent"), "{e}");
    assert_eq!(slot_count(&env).await, 0);
    env.run(spec.clone()).await;
    env.exec("INSERT INTO a VALUES (7)").await;
    env.try_run(spec, policy(false, false))
        .await
        .expect("existing slot");
    assert_eq!(env.rows("SELECT id::VARCHAR FROM lake.br.a"), ["7"]);
}
versions!(existing_slot_only_without_allow_create);

async fn temporary_slots_live_for_one_run(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE tmp (id int PRIMARY KEY, v text); INSERT INTO tmp VALUES (1, 'a');")
        .await;
    let spec = env.spec(vec![stream("tmp")]);
    let mut src = env.source(spec.clone(), policy(true, true)).await;
    let r = atlatl_conn_pgrepl::run_once(&mut src, env.duck(), "lake", "run-tmp")
        .await
        .expect("run");
    assert!(r.slot.as_ref().is_some_and(|s| s.temporary), "{r:?}");
    src.close().await.expect("close");
    let mut gone = false;
    for _ in 0..100 {
        if slot_count(&env).await == 0 {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(gone, "temporary slot outlived its session");

    env.exec("UPDATE tmp SET v = 'b'").await;
    let r = env.try_run(spec, policy(true, true)).await.expect("resync");
    assert_eq!(r.applied.window.backfilled, vec!["pg/tmp"]);
    assert_eq!(env.rows("SELECT id::VARCHAR || v FROM lake.br.tmp"), ["1b"]);
}
versions!(temporary_slots_live_for_one_run);

async fn lag_is_reported(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE g (id int PRIMARY KEY, v text); CREATE TABLE quiet (x int);")
        .await;
    let spec = env.spec(vec![stream("g")]);
    let r = env.run(spec.clone()).await;
    let s = r.slot.expect("stats");
    assert_eq!(s.slot, slot_name(&env.name));
    assert!(!s.temporary);
    assert!(s.catalog_xmin_age.is_some_and(|a| a >= 0), "{s:?}");
    assert!(
        s.retained_bytes.is_some() && s.restart_lsn.is_some(),
        "{s:?}"
    );
    assert_eq!(s.confirmed_flush_lsn, r.confirmed);

    env.exec("INSERT INTO g SELECT i, repeat('x', 100) FROM generate_series(1, 2000) i")
        .await;
    let behind = slot_stats(&env.opts, &env.name)
        .await
        .expect("stats")
        .expect("slot");
    assert!(behind.lag_bytes.is_some_and(|b| b > 100_000), "{behind:?}");
    assert!(behind.current_wal_lsn > behind.confirmed_flush_lsn.expect("confirmed"));
    assert!(behind.retained_bytes >= behind.lag_bytes);

    env.run(spec.clone()).await;
    let caught_up = slot_stats(&env.opts, &env.name)
        .await
        .expect("stats")
        .expect("slot");
    // Lag is relative to a WAL other tests keep writing, so compare positions.
    assert!(
        caught_up.confirmed_flush_lsn >= Some(behind.current_wal_lsn),
        "{caught_up:?} vs {behind:?}"
    );

    // Writes the publication doesn't cover (PG 12 still sends their empty transactions) must
    // not leave the slot pinned behind them.
    env.exec("INSERT INTO quiet SELECT generate_series(1, 1000)")
        .await;
    let after: atlatl_conn_pgrepl::Lsn = env.pg_rows("SELECT pg_current_wal_lsn()::text").await[0]
        .parse()
        .expect("lsn");
    let r = env.run(spec).await;
    assert!(r.confirmed.is_some_and(|c| c >= after), "{r:?} vs {after}");
    assert_eq!(
        env.rows("SELECT count(*)::VARCHAR FROM lake.br.g"),
        ["2000"]
    );
}
versions!(lag_is_reported);

async fn production_slots_are_refused_from_dev(v: u32) {
    let env = Env::new(v).await;
    env.exec("CREATE TABLE p (id int PRIMARY KEY); INSERT INTO p VALUES (1);")
        .await;
    let lake_yaml = env.lake.connection_yaml("lake", "lake");
    let pg = format!(
        "  appdb_pg:\n    type: postgres\n    host: {h}\n    port: {p}\n    database: {d}\n    user: {u}\n    password: \"{pw}\"\n    sslmode: disable\n    read_only: true\n    changes: {{production: true, slots: {{allow_create: true}}}}\n",
        h = env.opts.host,
        p = env.opts.port,
        d = env.opts.database,
        u = env.opts.user,
        pw = env.opts.password.clone().unwrap_or_default(),
    );
    let project = format!(
        "version: 1\nenvironments:\n  names: [dev, prod]\n  base: prod\n  dev:\n    allow_base: [lake]\nconnections:\n{lake_yaml}{pg}"
    );
    let reps = format!(
        "replications:\n  {n}:\n    source: appdb_pg\n    target: lake\n    streams:\n      - {{id: appdb/p, mode: cdc, from: {{table: public.p}}, object: br.p}}\n",
        n = env.name
    );
    let dir = TempProject::new()
        .and_then(|t| t.file("atlatl.yaml", &project))
        .and_then(|t| t.file("replications/appdb.yaml", &reps))
        .expect("project");
    let (p, d) = load::load(dir.path());
    assert!(
        !d.has_errors(),
        "{:?}",
        d.errors().map(|e| e.error.to_string()).collect::<Vec<_>>()
    );
    let templates = Templates::new();
    let rep = &p.replications[0];
    let conn = |env_name: &str| {
        let mut diags = Diagnostics::default();
        let mut all = resolve_all(&p, Some(env_name), &Resolve::Static, &templates, &mut diags);
        all.shift_remove("appdb_pg").expect("appdb_pg")
    };

    let e = atlatl_conn_pgrepl::config::resolve(rep, &conn("dev"), Some("dev"), Some("prod"))
        .err()
        .expect("refused");
    assert_eq!(e.class, ErrorClass::Config);
    assert_eq!(e.code.as_deref(), Some("slot_refused"), "{e}");
    assert!(
        e.message.contains("appdb_pg") && e.message.contains("`dev`"),
        "{e}"
    );
    assert_eq!(slot_count(&env).await, 0);

    let ok = atlatl_conn_pgrepl::config::resolve(rep, &conn("prod"), Some("prod"), Some("prod"))
        .expect("prod");
    assert!(ok.policy.allow_create());
    assert_eq!(ok.spec.streams[0].target_schema, "br");
    let mut src = atlatl_conn_pgrepl::PgChangeSource::open(ok.opts, ok.policy, ok.spec)
        .await
        .expect("open");
    atlatl_conn_pgrepl::run_once(&mut src, env.duck(), "lake", "run-prod")
        .await
        .expect("run");
    src.close().await.expect("close");
    assert_eq!(env.rows("SELECT id::VARCHAR FROM lake.br.p"), ["1"]);
}
versions!(production_slots_are_refused_from_dev);
