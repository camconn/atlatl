//! Lake → SQLite / D1 / Turso CDC through the runner, and `cdc bootstrap` for them (M8.5).

use std::sync::Arc;

use atlatl_conn_sqlite::{Client, Value};
use atlatl_core::config::connection::Connection;
use atlatl_core::obs::report::{MemberReport, ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::serve::{BootstrapArgs, bootstrap_cmd};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::d1_fake::{self, Fake};
use atlatl_testkit::{Libsql, LocalLake, TempProject, libsql};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  serve:
    source: lake
    target: app
    defaults: {mode: cdc}
    streams:
      - id: s/entity
        from: {table: src.entity}
        object: entity
        primary_key: [object_id]
        columns:
          include: [object_id, name, kind, amount, seen_at]
          kind: {transform: "lower(kind)"}
          seen_at: {target_type: epoch_s}
      - id: s/event
        after: [s/entity]
        from: {table: src.event}
        object: main.event
        primary_key: [id]
        columns: {include: [id, entity_id, v]}
        deletes: {mode: hard}
"#;

enum Target {
    Sqlite(tempfile::TempDir),
    D1(Fake),
    Turso(Libsql),
}

impl Target {
    async fn all() -> Vec<Target> {
        vec![
            Target::Sqlite(tempfile::tempdir().expect("dir")),
            Target::D1(Fake::start().await),
            Target::Turso(tokio::task::spawn_blocking(libsql).await.expect("libsql")),
        ]
    }

    fn name(&self) -> &'static str {
        match self {
            Target::Sqlite(_) => "sqlite",
            Target::D1(_) => "d1",
            Target::Turso(_) => "turso",
        }
    }

    fn yaml(&self) -> String {
        let retry = "retry: {max_attempts: 2, base: 10ms, max: 10ms}";
        match self {
            Target::Sqlite(d) => format!(
                "{{type: sqlite, path: \"{}\"}}",
                d.path().join("app.db").display()
            ),
            Target::D1(f) => format!(
                "{{type: d1, account_id: {}, database: {}, api_token: {}, endpoint: \"{}\", {retry}}}",
                d1_fake::ACCOUNT,
                d1_fake::DATABASE_NAME,
                d1_fake::TOKEN,
                f.endpoint
            ),
            Target::Turso(l) => format!(
                "{{type: turso, url: \"{}\", transaction_rows: 10, {retry}}}",
                l.url
            ),
        }
    }
}

struct Env {
    lake: LocalLake,
    project: TempProject,
    _target: Target,
}

impl Env {
    fn lake(&self, sql: &str) {
        self.lake
            .open("lake")
            .expect("open")
            .execute_batch(sql)
            .expect(sql);
    }

    async fn client(&self) -> Client {
        let p = project(self);
        let conn: Connection =
            atlatl_run::runtime::resolve_connection(&p, "app", None, vars()).expect("resolve");
        Client::open(&conn).await.expect("client")
    }

    async fn rows(&self, sql: &str) -> Vec<String> {
        let mut c = self.client().await;
        c.query(sql, vec![])
            .await
            .expect(sql)
            .into_iter()
            .map(|r| {
                r.iter()
                    .map(|v| match v {
                        Value::Null => "NULL".to_owned(),
                        Value::Integer(i) => i.to_string(),
                        Value::Real(f) => f.to_string(),
                        Value::Text(s) => s.clone(),
                        Value::Blob(b) => format!("{b:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join(":")
            })
            .collect()
    }

    async fn exec(&self, sql: &str) {
        self.client().await.execute(sql).await.expect(sql);
    }
}

fn setup(target: Target) -> Env {
    let lake = LocalLake::file().expect("lake");
    lake.open("lake")
        .expect("open")
        .execute_batch(
            "CREATE SCHEMA lake.src;
             CREATE TABLE lake.src.entity AS SELECT * FROM (VALUES
               (1, 'one', 'A', 1.50::DECIMAL(10,2), TIMESTAMPTZ '2024-01-01 00:00:00+00'),
               (2, 'two', 'B', 2.00::DECIMAL(10,2), TIMESTAMPTZ '2024-01-02 00:00:00+00'),
               (3, 'three', 'A', NULL::DECIMAL(10,2), NULL::TIMESTAMPTZ)) t(object_id, name, kind, amount, seen_at);
             CREATE TABLE lake.src.event AS SELECT * FROM (VALUES (10, 1, 'x'), (11, 3, 'y')) t(id, entity_id, v);",
        )
        .expect("seed");
    let conns = format!(
        "{}  app: {}\n",
        lake.connection_yaml("lake", "lake"),
        target.yaml()
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, ""))
        .and_then(|p| p.replication("serve", REPS))
        .expect("project");
    Env {
        lake,
        project,
        _target: target,
    }
}

fn project(e: &Env) -> Arc<atlatl_core::config::load::Project> {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    Arc::new(p)
}

fn vars() -> Arc<dyn EnvSource> {
    Arc::new(MapEnv::default())
}

async fn go(e: &Env) -> RunReport {
    let args = RunArgs {
        plan: PlanRequest::all(),
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    run(
        project(e),
        args,
        vars(),
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run")
}

async fn boot(e: &Env, mode: atlatl_cdc::bootstrap::Mode, dry_run: bool) -> serde_json::Value {
    let a = BootstrapArgs {
        replication: "serve".into(),
        env: None,
        mode,
        at_snapshot: None,
        dry_run,
        advance_cursor_only: false,
        extension_dir: Some(atlatl_testkit::extension_dir()),
    };
    bootstrap_cmd(project(e), a, vars())
        .await
        .expect("bootstrap")
}

fn member<'a>(r: &'a RunReport, stream: &str) -> &'a MemberReport {
    r.replications[0]
        .streams
        .iter()
        .find(|s| s.stream_id == stream)
        .and_then(|s| s.members.first())
        .expect("member")
}

fn ok(r: &RunReport, name: &str) -> ReplicationStatus {
    let rep = &r.replications[0];
    assert!(rep.error.is_none(), "{name}: {:?}", rep.error);
    rep.status
}

use atlatl_cdc::bootstrap::Mode;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_then_serve_changes() {
    for t in Target::all().await {
        let name = t.name();
        let e = setup(t);
        let r = go(&e).await;
        let err = r.replications[0].error.clone().expect("needs bootstrap");
        assert_eq!(
            err.code.as_deref(),
            Some("bootstrap_required"),
            "{name}: {err:?}"
        );

        let d = boot(&e, Mode::Reload, false).await;
        assert_eq!(d["streams"][0]["rows_inserted"], 3, "{name}: {d}");
        assert_eq!(d["streams"][1]["rows_inserted"], 2, "{name}: {d}");
        assert_eq!(
            e.rows("SELECT object_id, name, kind, amount, seen_at, deleted_at FROM entity ORDER BY object_id").await,
            ["1:one:a:1.50:1704067200:NULL", "2:two:b:2.00:1704153600:NULL", "3:three:a:NULL:NULL:NULL"],
            "{name}: created, typed and loaded"
        );
        assert_eq!(
            e.rows("SELECT mode FROM _atlatl_cursors ORDER BY stream_id")
                .await,
            ["bootstrap", "bootstrap"],
            "{name}"
        );
        assert_eq!(
            ok(&go(&e).await, name),
            ReplicationStatus::UpToDate,
            "{name}"
        );

        e.lake(
            "UPDATE lake.src.entity SET name = 'TWO' WHERE object_id = 2;
             DELETE FROM lake.src.entity WHERE object_id = 1; INSERT INTO lake.src.entity VALUES (1, 'one', 'A', 1.50, TIMESTAMPTZ '2024-01-01 00:00:00+00');
             DELETE FROM lake.src.event WHERE entity_id = 3; DELETE FROM lake.src.entity WHERE object_id = 3;
             INSERT INTO lake.src.event VALUES (12, 2, 'z');",
        );
        let r = go(&e).await;
        assert_eq!(ok(&r, name), ReplicationStatus::Succeeded, "{name}");
        let cdc = member(&r, "s/entity").cdc.clone().expect("cdc");
        assert_eq!(
            (cdc.rows_inserted, cdc.rows_updated, cdc.rows_deleted),
            (0, 1, 1),
            "{name}: {cdc:?}"
        );
        assert_eq!(
            cdc.touched_keys, 3,
            "{name}: delete+insert of 1 is touched but unchanged"
        );
        let ev = member(&r, "s/event").cdc.clone().expect("cdc");
        assert_eq!((ev.rows_inserted, ev.rows_deleted), (1, 1), "{name}");
        assert_eq!(
            e.rows("SELECT object_id, name, deleted_at IS NOT NULL, deleted_at LIKE '20__-__-__T__:__:__.______Z' FROM entity ORDER BY object_id").await,
            ["1:one:0:NULL", "2:TWO:0:NULL", "3:three:1:1"],
            "{name}: soft delete stamped with ISO snapshot time"
        );
        assert_eq!(
            e.rows("SELECT id FROM event ORDER BY id").await,
            ["10", "12"],
            "{name}"
        );
        let to = cdc.window.to_snapshot.expect("to");
        assert_eq!(
            e.rows("SELECT mode || '@' || snapshot_id FROM _atlatl_cursors ORDER BY stream_id")
                .await,
            [format!("cdc@{to}"), format!("cdc@{to}")],
            "{name}"
        );

        e.lake("INSERT INTO lake.src.entity VALUES (3, 'back', 'B', NULL, NULL);");
        let r = go(&e).await;
        assert_eq!(
            member(&r, "s/entity").cdc.as_ref().map(|c| c.rows_updated),
            Some(1),
            "{name}"
        );
        assert_eq!(
            e.rows("SELECT name, deleted_at IS NULL FROM entity WHERE object_id = 3")
                .await,
            ["back:1"],
            "{name}: restore"
        );
        assert_eq!(
            ok(&go(&e).await, name),
            ReplicationStatus::UpToDate,
            "{name}"
        );

        e.exec("UPDATE entity SET name = 'drift' WHERE object_id = 2")
            .await;
        e.exec("INSERT INTO event (id, entity_id, v) VALUES (99, 1, 'stray')")
            .await;
        let d = boot(&e, Mode::Adopt, true).await;
        assert_eq!(
            (
                d["streams"][0]["upserts"].as_u64(),
                d["streams"][0]["deletes"].as_u64()
            ),
            (Some(1), Some(0)),
            "{name}: {d}"
        );
        assert_eq!(
            (
                d["streams"][1]["upserts"].as_u64(),
                d["streams"][1]["deletes"].as_u64()
            ),
            (Some(0), Some(1)),
            "{name}: {d}"
        );
        assert!(
            d["streams"][0]["samples"]
                .as_array()
                .is_some_and(|s| !s.is_empty())
        );
        assert_eq!(
            e.rows("SELECT name FROM entity WHERE object_id = 2").await,
            ["drift"],
            "{name}: dry run writes nothing"
        );
        let d = boot(&e, Mode::Adopt, false).await;
        assert_eq!(d["streams"][0]["rows_updated"], 1, "{name}: {d}");
        assert_eq!(
            e.rows("SELECT name FROM entity WHERE object_id = 2").await,
            ["TWO"],
            "{name}"
        );
        assert_eq!(
            e.rows("SELECT id FROM event ORDER BY id").await,
            ["10", "12"],
            "{name}"
        );
        assert_eq!(
            ok(&go(&e).await, name),
            ReplicationStatus::UpToDate,
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_window_is_reapplied_first() {
    for t in Target::all().await {
        let name = t.name();
        let e = setup(t);
        boot(&e, Mode::Reload, false).await;
        e.exec("CREATE TRIGGER boom BEFORE INSERT ON event WHEN NEW.v = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END").await;
        e.lake("INSERT INTO lake.src.event SELECT 100 + i, 2, CASE WHEN i = 60 THEN 'boom' ELSE 'e' || i END FROM range(80) r(i);");
        let r = go(&e).await;
        let err = r.replications[0].error.clone().expect("trigger");
        assert_eq!(
            err.class,
            atlatl_core::error::ErrorClass::Data,
            "{name}: {err:?}"
        );
        let window = e.rows("SELECT coalesce(pending_snapshot, 'none') FROM _atlatl_cursors WHERE stream_id = 's/event'").await;
        let landed: usize = e.rows("SELECT count(*) FROM event").await[0]
            .parse()
            .expect("n");
        if name == "sqlite" {
            assert_eq!(
                (window[0].as_str(), landed),
                ("none", 2),
                "one transaction: nothing landed"
            );
        } else {
            assert_ne!(window[0], "none", "{name}: the window is pending");
            assert!(landed < 82, "{name}: the apply stopped part-way ({landed})");
        }
        e.lake("INSERT INTO lake.src.event VALUES (500, 1, 'later');");
        e.exec("DROP TRIGGER boom").await;
        let r = go(&e).await;
        assert_eq!(ok(&r, name), ReplicationStatus::Succeeded, "{name}");
        let to = member(&r, "s/event")
            .cdc
            .as_ref()
            .and_then(|c| c.window.to_snapshot)
            .expect("to");
        let expect = if name == "sqlite" { "83" } else { "82" };
        assert_eq!(
            e.rows("SELECT count(*) FROM event").await,
            [expect],
            "{name}"
        );
        if name != "sqlite" {
            assert_eq!(
                window[0],
                to.to_string(),
                "{name}: the pending window's E, not the head"
            );
            assert!(
                r.replications[0].partial,
                "{name}: the head moved on meanwhile"
            );
            assert_eq!(
                ok(&go(&e).await, name),
                ReplicationStatus::Succeeded,
                "{name}"
            );
        }
        assert_eq!(e.rows("SELECT count(*) FROM event").await, ["83"], "{name}");
        assert_eq!(
            e.rows("SELECT count(*) FROM _atlatl_cursors WHERE pending_snapshot IS NOT NULL")
                .await,
            ["0"],
            "{name}"
        );
        assert_eq!(
            ok(&go(&e).await, name),
            ReplicationStatus::UpToDate,
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_target_schemas() {
    let e = setup(Target::Sqlite(tempfile::tempdir().expect("dir")));
    let reps = REPS.replace("object: main.event", "object: other.event");
    std::fs::write(e.project.path().join("replications/serve.yaml"), reps).expect("write");
    boot_err(&e, "no schemas").await;
}

async fn boot_err(e: &Env, needle: &str) {
    let a = BootstrapArgs {
        replication: "serve".into(),
        env: None,
        mode: Mode::Reload,
        at_snapshot: None,
        dry_run: false,
        advance_cursor_only: false,
        extension_dir: Some(atlatl_testkit::extension_dir()),
    };
    let err = bootstrap_cmd(project(e), a, vars())
        .await
        .expect_err("fails");
    assert!(err.message.contains(needle), "{err}");
}
