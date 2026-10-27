//! `atlatl run` of postgres → lake ingest (M8.2): engine-native reads, sorted wildcard members,
//! `full-refresh` by key with emptiness mirrored, one lake commit per replication. PG 12 and 16.

use std::sync::Arc;

use atlatl_conn_postgres::{ConnectOptions, Session, connect};
use atlatl_core::ErrorClass;
use atlatl_core::obs::report::{MemberReport, ReplicationReport, ReplicationStatus, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, Postgres, TempProject};
use tokio_util::sync::CancellationToken;

macro_rules! on_both {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg12() {
                super::$name(atlatl_testkit::pg12()).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg16() {
                super::$name(atlatl_testkit::pg16()).await
            }
        }
    )*};
}

on_both!(
    wildcard_full_refresh_is_one_commit,
    empty_source_policies,
    key_violations_commit_nothing,
    incremental_update_key,
    errors_are_classified,
    wildcard_members_resolve_their_own_keys,
    shared_snapshot_reads_members_consistently,
);

/// A server per version with `pg_stat_statements`, to see the statements the scanner sends.
fn stats_pg(v: u32) -> &'static Postgres {
    static PG12: std::sync::OnceLock<Postgres> = std::sync::OnceLock::new();
    static PG16: std::sync::OnceLock<Postgres> = std::sync::OnceLock::new();
    let cell = if v == 12 { &PG12 } else { &PG16 };
    cell.get_or_init(|| {
        Postgres::builder(v)
            .setting("shared_preload_libraries", "pg_stat_statements")
            .start()
            .expect("postgres with pg_stat_statements")
    })
}

mod cursor_pushdown {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pg12() {
        super::cursor_pushdown(super::stats_pg(12)).await
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pg16() {
        super::cursor_pushdown(super::stats_pg(16)).await
    }
}

struct Env {
    sql: Session,
    lake: LocalLake,
    project: TempProject,
}

fn pg_yaml(pg: &Postgres, db: &str, password: &str) -> String {
    format!(
        "  appdb_pg:\n    type: postgres\n    host: {}\n    port: {}\n    database: {db}\n    user: {}\n    password: \"{password}\"\n    sslmode: disable\n    read_only: true\n",
        pg.host, pg.port, pg.user
    )
}

async fn setup(pg: &'static Postgres, seed: &str, reps: &str) -> Env {
    setup_with(pg, seed, reps, None).await
}

async fn setup_with(pg: &'static Postgres, seed: &str, reps: &str, password: Option<&str>) -> Env {
    let db = pg.fresh_db().expect("db");
    let sql = connect(&ConnectOptions::new(
        &pg.host,
        pg.port,
        &db,
        &pg.user,
        Some(pg.password.clone()),
    ))
    .await
    .expect("connect");
    sql.client().batch_execute(seed).await.expect("seed");
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}{}",
        lake.connection_yaml("lake", "lake"),
        pg_yaml(pg, &db, password.unwrap_or(&pg.password))
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.replication("appdb", reps))
        .expect("project");
    Env { sql, lake, project }
}

async fn go(e: &Env, names: &[&str]) -> RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let args = RunArgs {
        plan: PlanRequest {
            replications: names.iter().map(|r| (*r).to_owned()).collect(),
            ..PlanRequest::all()
        },
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    run(
        Arc::new(p),
        args,
        vars,
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run")
}

fn rep<'a>(r: &'a RunReport, name: &str) -> &'a ReplicationReport {
    r.replications
        .iter()
        .find(|x| x.replication == name)
        .expect("replication")
}

fn ok<'a>(r: &'a RunReport, name: &str) -> Vec<&'a MemberReport> {
    let x = rep(r, name);
    assert!(x.status.is_success(), "{name}: {:?}", x.error);
    x.streams.iter().flat_map(|s| &s.members).collect()
}

fn failed(r: &RunReport, name: &str) -> (String, Option<String>, String) {
    let x = rep(r, name);
    assert_eq!(x.status, ReplicationStatus::Failed, "{name}");
    let e = x.error.as_ref().expect("error");
    (e.class.to_string(), e.code.clone(), e.message.clone())
}

fn one<T: duckdb::types::FromSql>(e: &Env, sql: &str) -> T {
    e.lake
        .open("lake")
        .expect("open")
        .query_row(sql, [], |r| r.get(0))
        .expect(sql)
}

/// Lake snapshots that changed anything outside `_atlatl` (run history and state live there).
fn snapshots(e: &Env) -> i64 {
    let c = e.lake.open("lake").expect("open");
    let ids: Vec<String> = c
        .prepare("SELECT t.table_id::VARCHAR FROM __ducklake_metadata_lake.ducklake_table t JOIN __ducklake_metadata_lake.ducklake_schema s USING (schema_id) WHERE s.schema_name = '_atlatl'")
        .expect("ids")
        .query_map([], |r| r.get(0))
        .expect("q")
        .collect::<Result<_, _>>()
        .expect("r");
    let changes: Vec<String> = c
        .prepare("SELECT changes::VARCHAR FROM ducklake_snapshots('lake')")
        .expect("snaps")
        .query_map([], |r| r.get(0))
        .expect("q")
        .collect::<Result<_, _>>()
        .expect("r");
    changes
        .iter()
        .filter(|ch| {
            ch.trim_matches(['{', '}']).split("], ").any(|entry| {
                let (_, vals) = entry.split_once("=[").unwrap_or(("", ""));
                vals.trim_end_matches(']').split(", ").any(|v| {
                    v == "br"
                        || v.starts_with("br.")
                        || (v.parse::<u64>().is_ok() && !ids.iter().any(|i| i == v))
                })
            })
        })
        .count() as i64
}

fn exists(e: &Env, table: &str) -> bool {
    one::<i64>(
        e,
        &format!(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'br' AND table_name = '{table}'"
        ),
    ) == 1
}

const APPDB: &str = r#"
CREATE SCHEMA appdb;
CREATE TYPE appdb.mood AS ENUM ('happy', 'sad');
CREATE TABLE appdb."Zeta" (id int PRIMARY KEY, name text);
INSERT INTO appdb."Zeta" VALUES (1, 'z1'), (2, 'z2');
CREATE TABLE appdb.alpha (id int PRIMARY KEY, m appdb.mood, tags text[], doc jsonb, amount numeric(12,4),
  big numeric, at timestamptz);
INSERT INTO appdb.alpha VALUES
  (1, 'happy', ARRAY['a', 'b c'], '{"k": [1, 2]}', 1.5, 12345678901234567890.000000000001, '2024-01-02 03:04:05.123456+05'),
  (2, 'sad', '{}', 'null', NULL, NULL, NULL);
CREATE TABLE appdb.empty_t (id int PRIMARY KEY, v text);
CREATE VIEW appdb.v AS SELECT 1 AS id;
"#;

const WILDCARD: &str = r#"replications:
  appdb:
    source: appdb_pg
    target: lake
    streams:
      - id: appdb/all
        from: {tables: "appdb.*"}
        object: "br.{{ member.table }}"
        mode: full-refresh
        primary_key: [id]
        on_empty: mirror
"#;

async fn wildcard_full_refresh_is_one_commit(pg: &'static Postgres) {
    let e = setup(pg, APPDB, WILDCARD).await;

    let s0 = snapshots(&e);
    let r = go(&e, &["appdb"]).await;
    let ms = ok(&r, "appdb");
    assert_eq!(
        ms.iter().map(|m| m.member.as_str()).collect::<Vec<_>>(),
        ["appdb.Zeta", "appdb.alpha", "appdb.empty_t"],
        "sorted byte-wise, views excluded"
    );
    assert_eq!(snapshots(&e), s0 + 1, "one commit for every member");
    let snap = ms[0].lake_snapshot;
    assert!(snap.is_some() && ms.iter().all(|m| m.lake_snapshot == snap));
    assert_eq!(
        ms.iter().map(|m| m.rows.inserted).collect::<Vec<_>>(),
        [2, 2, 0]
    );
    assert!(
        exists(&e, "empty_t"),
        "an empty source still creates its table"
    );

    let types: String = one(
        &e,
        "SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_catalog = 'lake' AND table_name = 'alpha'",
    );
    assert_eq!(
        types,
        "id INTEGER, m VARCHAR, tags VARCHAR, doc VARCHAR, amount DECIMAL(12,4), big VARCHAR, at TIMESTAMP WITH TIME ZONE"
    );
    let row: String = one(
        &e,
        "SELECT concat_ws('|', m, tags, doc, amount::VARCHAR, big, strftime(\"at\" AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f')) FROM lake.br.alpha WHERE id = 1",
    );
    assert_eq!(
        row,
        r#"happy|["a","b c"]|{"k": [1, 2]}|1.5000|12345678901234567890.000000000001|2024-01-01 22:04:05.123456"#
    );
    let row2: String = one(
        &e,
        "SELECT concat_ws('|', m, tags, doc) FROM lake.br.alpha WHERE id = 2",
    );
    assert_eq!(row2, "sad|[]|null");

    let r = go(&e, &["appdb"]).await;
    let ms = ok(&r, "appdb");
    assert_eq!(rep(&r, "appdb").status, ReplicationStatus::UpToDate);
    assert_eq!(
        snapshots(&e),
        s0 + 1,
        "an unchanged re-sync commits no snapshot"
    );
    assert!(
        ms.iter()
            .all(|m| m.lake_snapshot.is_none() && m.rows.written() == 0),
        "{ms:?}"
    );
    assert_eq!(ms.iter().map(|m| m.rows.unchanged).sum::<u64>(), 4);

    e.sql
        .client()
        .batch_execute(
            "UPDATE appdb.alpha SET m = 'happy', tags = ARRAY['x'] WHERE id = 2;
             INSERT INTO appdb.empty_t VALUES (7, 'v');
             TRUNCATE appdb.\"Zeta\";",
        )
        .await
        .expect("change");
    let r = go(&e, &["appdb"]).await;
    let ms = ok(&r, "appdb");
    assert_eq!(snapshots(&e), s0 + 2);
    let counts: Vec<(u64, u64, u64)> = ms
        .iter()
        .map(|m| (m.rows.inserted, m.rows.updated, m.rows.deleted))
        .collect();
    assert_eq!(counts, [(0, 0, 2), (0, 1, 0), (1, 0, 0)]);
    assert_eq!(
        one::<i64>(&e, "SELECT count(*) FROM lake.br.\"Zeta\""),
        0,
        "emptiness is mirrored"
    );
    assert_eq!(
        one::<String>(&e, "SELECT tags FROM lake.br.alpha WHERE id = 2"),
        r#"["x"]"#
    );
}

const EMPTY_REPS: &str = r#"replications:
  mirror:
    source: appdb_pg
    target: lake
    streams: [{id: e/mirror, from: {table: appdb.t}, object: br.t_mirror, mode: full-refresh, primary_key: [id]}]
  skip:
    source: appdb_pg
    target: lake
    streams: [{id: e/skip, from: {table: appdb.t}, object: br.t_skip, mode: full-refresh, primary_key: [id], on_empty: skip}]
  fail:
    source: appdb_pg
    target: lake
    streams: [{id: e/fail, from: {table: appdb.t}, object: br.t_fail, mode: full-refresh, primary_key: [id], on_empty: fail}]
  guarded:
    source: appdb_pg
    target: lake
    streams: [{id: e/guard, from: {table: appdb.t}, object: br.t_guard, mode: full-refresh, primary_key: [id], guard: {max_shrink: 0.5}}]
"#;

async fn empty_source_policies(pg: &'static Postgres) {
    let e = setup(pg, "CREATE SCHEMA appdb; CREATE TABLE appdb.t (id int PRIMARY KEY); INSERT INTO appdb.t SELECT generate_series(1, 4);", EMPTY_REPS).await;
    let all = ["mirror", "skip", "fail", "guarded"];
    let r = go(&e, &all).await;
    for n in all {
        ok(&r, n);
    }
    e.sql
        .client()
        .batch_execute("DELETE FROM appdb.t WHERE id > 1")
        .await
        .expect("shrink");
    let r = go(&e, &["guarded"]).await;
    let (class, code, _) = failed(&r, "guarded");
    assert_eq!(
        (class.as_str(), code.as_deref()),
        ("data", Some("guard_max_shrink"))
    );
    assert_eq!(one::<i64>(&e, "SELECT count(*) FROM lake.br.t_guard"), 4);

    e.sql
        .client()
        .batch_execute("TRUNCATE appdb.t")
        .await
        .expect("truncate");
    let r = go(&e, &["mirror", "skip", "fail"]).await;
    assert_eq!(ok(&r, "mirror")[0].rows.deleted, 4);
    assert_eq!(one::<i64>(&e, "SELECT count(*) FROM lake.br.t_mirror"), 0);
    ok(&r, "skip");
    assert_eq!(rep(&r, "skip").status, ReplicationStatus::UpToDate);
    assert_eq!(one::<i64>(&e, "SELECT count(*) FROM lake.br.t_skip"), 4);
    let (class, code, _) = failed(&r, "fail");
    assert_eq!(
        (class.as_str(), code.as_deref()),
        ("data", Some("empty_source"))
    );
    assert_eq!(one::<i64>(&e, "SELECT count(*) FROM lake.br.t_fail"), 4);
}

const KEY_REPS: &str = r#"replications:
  dupes:
    source: appdb_pg
    target: lake
    streams:
      - {id: k/dupes, from: {tables: "appdb.*"}, object: "br.{{ member.table }}", mode: full-refresh, primary_key: [id]}
  nulls:
    source: appdb_pg
    target: lake
    streams:
      - {id: k/nulls, from: {table: nk.t}, object: br.nulls, mode: full-refresh, primary_key: [id]}
  nulls_skipped:
    source: appdb_pg
    target: lake
    streams:
      - {id: k/nulls_skip, from: {table: nk.t}, object: br.nulls_skip, mode: full-refresh, primary_key: [id], null_keys: skip}
  no_column:
    source: appdb_pg
    target: lake
    streams:
      - {id: k/nocol, from: {table: nk.t}, object: br.nocol, mode: full-refresh, primary_key: [nope]}
"#;

async fn key_violations_commit_nothing(pg: &'static Postgres) {
    let seed = "CREATE SCHEMA appdb; CREATE TABLE appdb.a (id int PRIMARY KEY); INSERT INTO appdb.a VALUES (1);
                CREATE TABLE appdb.b (id int, v text); INSERT INTO appdb.b VALUES (1, 'x'), (1, 'y');
                CREATE SCHEMA nk; CREATE TABLE nk.t (id int, v text); INSERT INTO nk.t VALUES (1, 'a'), (NULL, 'b');";
    let e = setup(pg, seed, KEY_REPS).await;
    let s0 = snapshots(&e);
    let r = go(&e, &["dupes", "nulls", "nulls_skipped", "no_column"]).await;
    let (class, _, msg) = failed(&r, "dupes");
    assert_eq!(class, "data", "{msg}");
    assert!(
        !exists(&e, "a"),
        "appdb.a staged fine but the replication commits all or nothing"
    );
    let (class, _, msg) = failed(&r, "nulls");
    assert_eq!(class, "data", "{msg}");
    let (class, _, msg) = failed(&r, "no_column");
    assert_eq!(class, "config", "{msg}");
    assert_eq!(ok(&r, "nulls_skipped")[0].rows.inserted, 1);
    assert_eq!(snapshots(&e), s0 + 1);
}

const INC_REPS: &str = r#"replications:
  events:
    source: appdb_pg
    target: lake
    streams:
      - id: appdb/events
        from: {table: appdb.events}
        object: br.events
        mode: incremental
        primary_key: [id]
        update_key: at
        columns: {exclude: [junk]}
"#;

async fn incremental_update_key(pg: &'static Postgres) {
    let seed = "CREATE SCHEMA appdb; CREATE TABLE appdb.events (id int PRIMARY KEY, at timestamptz, tags text[], junk point);
                INSERT INTO appdb.events SELECT i, '2024-01-01'::timestamptz + i * interval '1 hour', ARRAY['t' || i], point(0, 0) FROM generate_series(1, 3) i;";
    let e = setup(pg, seed, INC_REPS).await;
    let r = go(&e, &["events"]).await;
    let m = ok(&r, "events")[0].clone();
    assert_eq!(m.rows.inserted, 3);
    assert_eq!(
        m.state.after.as_ref().and_then(|s| s["type"].as_str()),
        Some("TIMESTAMP WITH TIME ZONE")
    );
    e.sql
        .client()
        .batch_execute("INSERT INTO appdb.events VALUES (4, '2024-02-01T00:00:00Z', '{x}', NULL), (5, '2023-01-01T00:00:00Z', '{old}', NULL);")
        .await
        .expect("more");
    let r = go(&e, &["events"]).await;
    let m = ok(&r, "events")[0].clone();
    assert_eq!(
        (m.rows.staged, m.rows.inserted, m.rows.unchanged),
        (2, 1, 1),
        "inclusive watermark: the max row plus the newer one"
    );
    assert_eq!(one::<i64>(&e, "SELECT count(*) FROM lake.br.events"), 4);
    assert_eq!(
        one::<i64>(
            &e,
            "SELECT count(*) FROM information_schema.columns WHERE table_catalog = 'lake' AND table_name = 'events' AND column_name = 'junk'"
        ),
        0
    );
}

async fn errors_are_classified(pg: &'static Postgres) {
    let reps = r#"replications:
  appdb:
    source: appdb_pg
    target: lake
    streams: [{id: appdb/t, from: {table: appdb.t}, object: br.t, mode: full-refresh, primary_key: [id]}]
  missing:
    source: appdb_pg
    target: lake
    streams: [{id: appdb/missing, from: {table: appdb.nope}, object: br.nope, mode: full-refresh, primary_key: [id]}]
  no_match:
    source: appdb_pg
    target: lake
    streams: [{id: appdb/none, from: {tables: "appdb.zz_*"}, object: "br.{{ member.table }}", mode: full-refresh}]
"#;
    let seed = "CREATE SCHEMA appdb; CREATE TABLE appdb.t (id int PRIMARY KEY);";
    let e = setup(pg, seed, reps).await;
    let r = go(&e, &["missing", "no_match"]).await;
    let (class, code, msg) = failed(&r, "missing");
    assert_eq!(
        (class.as_str(), code.as_deref()),
        ("config", Some("42P01")),
        "{msg}"
    );
    let (class, code, _) = failed(&r, "no_match");
    assert_eq!(
        (class.as_str(), code.as_deref()),
        ("config", Some("no_match"))
    );

    let bad = setup_with(pg, seed, reps, Some("not-the-password")).await;
    let r = go(&bad, &["appdb"]).await;
    let (class, _, msg) = failed(&r, "appdb");
    assert_eq!(class, ErrorClass::Auth.to_string(), "{msg}");
    assert!(!msg.contains("not-the-password"), "redacted: {msg}");
}

#[test]
fn plans_statically_and_validates() {
    let lake = LocalLake::file().expect("lake");
    let pg = "  appdb_pg:\n    type: postgres\n    host: h\n    database: appdb\n    user: u\n    password: {secret: env:PG_PW}\n    read_only: true\n";
    let p = TempProject::new()
        .and_then(|p| {
            p.project(
                &format!("{}{pg}", lake.connection_yaml("lake", "lake")),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("appdb", WILDCARD))
        .expect("project");
    let (proj, d) = atlatl_core::config::load(p.path());
    assert!(!d.has_errors(), "{d:?}");
    let req = PlanRequest {
        static_plan: true,
        ..PlanRequest::all()
    };
    let (plan, _) = atlatl_core::plan::build(
        &proj,
        &req,
        Arc::new(MapEnv::default()),
        atlatl_core::redact::Redactor::global(),
    )
    .expect("static plan");
    let s = &plan.replications[0].streams[0];
    assert_eq!(
        (
            s.mode.as_str(),
            s.members[0].name.as_str(),
            s.members[0].pattern
        ),
        ("full-refresh", "appdb.*", true)
    );
    assert_eq!(s.asset.kinds, ["atlatl", "ducklake"]);

    let mixed = format!(
        "{WILDCARD}      - {{id: appdb/cdc, from: {{table: appdb.users}}, object: br.users_cdc, mode: cdc}}\n"
    );
    let p2 = TempProject::new()
        .and_then(|p| {
            p.project(
                &format!(
                    "{}{pg}    changes: {{production: false}}\n",
                    lake.connection_yaml("lake", "lake")
                ),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("appdb", &mixed))
        .expect("project");
    let (proj, d) = atlatl_core::config::load(p2.path());
    assert!(!d.has_errors(), "{d:?}");
    let d = atlatl_core::plan::build(
        &proj,
        &req,
        Arc::new(MapEnv::default()),
        atlatl_core::redact::Redactor::global(),
    )
    .expect_err("mixed modes");
    let msgs: Vec<String> = d.errors().map(|e| e.error.to_string()).collect();
    assert!(msgs.iter().any(|m| m.contains("can't mix")), "{msgs:?}");
}

const MEMBER_KEYS: &str = r#"replications:
  keys:
    source: appdb_pg
    target: lake
    streams:
      - id: appdb/keys
        from: {tables: "appdb.*"}
        object: "br.{{ member.table }}"
        mode: full-refresh
        members:
          appdb.d*: {primary_key: [code]}
  stream_key:
    source: appdb_pg
    target: lake
    streams:
      - {id: appdb/stream_key, from: {tables: "appdb.*"}, object: "br.s_{{ member.table }}", mode: full-refresh, primary_key: [id]}
  excluded:
    source: appdb_pg
    target: lake
    streams:
      - {id: appdb/excluded, from: {tables: "appdb.a"}, object: br.x_a, mode: full-refresh, columns: {exclude: [ID]}}
"#;

/// A discovered member: name, primary key, where the key came from, target.
type Resolved = (String, Option<Vec<String>>, Option<String>, Option<String>);

async fn wildcard_members_resolve_their_own_keys(pg: &'static Postgres) {
    let seed = "CREATE SCHEMA appdb;
        CREATE TABLE appdb.a (id int PRIMARY KEY, v text);
        CREATE TABLE appdb.b (v text, k2 int, k1 text, PRIMARY KEY (k1, k2));
        CREATE TABLE appdb.c (id int, v text);
        CREATE TABLE appdb.d (id int PRIMARY KEY, code text UNIQUE, v text);
        INSERT INTO appdb.a VALUES (1, 'x'), (2, 'y');
        INSERT INTO appdb.b VALUES ('x', 1, 'p'), ('y', 2, 'p'), ('z', 1, 'q');
        INSERT INTO appdb.c VALUES (1, 'x'), (1, 'y');
        INSERT INTO appdb.d VALUES (1, 'one', 'x'), (2, 'two', 'y');";
    let e = setup(pg, seed, MEMBER_KEYS).await;

    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let (mut plan, _) = atlatl_core::plan::build(
        &p,
        &PlanRequest {
            discover: true,
            replications: vec!["keys".into()],
            ..PlanRequest::all()
        },
        vars.clone(),
        atlatl_core::redact::Redactor::global(),
    )
    .expect("plan");
    atlatl_run::pgsrc::discover_plan(&p, &mut plan, vars)
        .await
        .expect("discover");
    let keys: Vec<Resolved> = plan.replications[0].streams[0]
        .members
        .iter()
        .map(|m| {
            (
                m.name.clone(),
                m.primary_key.clone(),
                m.primary_key_from.clone(),
                m.object.clone(),
            )
        })
        .collect();
    let k = |v: &[&str]| Some(v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>());
    let s = |x: &str| Some(x.to_owned());
    assert_eq!(
        keys,
        [
            ("appdb.a".into(), k(&["id"]), s("source"), s("br.a")),
            ("appdb.b".into(), k(&["k1", "k2"]), s("source"), s("br.b")),
            ("appdb.c".into(), None, None, s("br.c")),
            ("appdb.d".into(), k(&["code"]), s("members"), s("br.d")),
        ]
    );
    assert!(
        !plan.warnings.iter().any(|w| w.code == "unexpanded"),
        "{:?}",
        plan.warnings
    );

    let r = go(&e, &["keys"]).await;
    let ms = ok(&r, "keys");
    assert_eq!(
        ms.iter().map(|m| m.rows.inserted).collect::<Vec<_>>(),
        [2, 3, 2, 2],
        "a composite key allows k1 to repeat; a keyless member keeps its duplicates"
    );
    e.sql
        .client()
        .batch_execute(
            "UPDATE appdb.a SET v = 'x2' WHERE id = 1;
             UPDATE appdb.b SET v = 'x2' WHERE k1 = 'p' AND k2 = 1;
             UPDATE appdb.c SET v = 'x2' WHERE v = 'x';
             UPDATE appdb.d SET id = 3 WHERE code = 'one';",
        )
        .await
        .expect("change");
    let r = go(&e, &["keys"]).await;
    let counts: Vec<(u64, u64, u64)> = ok(&r, "keys")
        .iter()
        .map(|m| (m.rows.inserted, m.rows.updated, m.rows.deleted))
        .collect();
    assert_eq!(
        counts,
        [(0, 1, 0), (0, 1, 0), (2, 0, 2), (0, 1, 0)],
        "keyed members diff; the keyless one is replaced; `d` is keyed by its override"
    );

    // A stream key turns discovery off: `b`, keyed (k1, k2) in Postgres, has no `id`.
    let r = go(&e, &["stream_key"]).await;
    let (class, _, msg) = failed(&r, "stream_key");
    assert!(class == "config" && msg.contains("id"), "{class}: {msg}");

    let r = go(&e, &["excluded"]).await;
    let (class, _, msg) = failed(&r, "excluded");
    assert!(
        class == "config" && msg.contains("`appdb.a`'s primary key (id) needs column `id`"),
        "{class}: {msg}"
    );
}

async fn cursor_pushdown(pg: &'static Postgres) {
    let seed = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
        CREATE SCHEMA appdb;
        CREATE TABLE appdb.native_t (id int PRIMARY KEY, at timestamptz);
        CREATE TABLE appdb.cast_t (id int PRIMARY KEY, at timestamptz, tags text[]);
        CREATE TABLE appdb.cast_uk (id int PRIMARY KEY, n numeric, tags text[]);
        INSERT INTO appdb.native_t SELECT i, '2024-01-01'::timestamptz + i * interval '1 day' FROM generate_series(1, 4) i;
        INSERT INTO appdb.cast_t SELECT i, '2024-01-01'::timestamptz + i * interval '1 day', ARRAY['t' || i] FROM generate_series(1, 4) i;
        INSERT INTO appdb.cast_uk SELECT i, i * 1.5, ARRAY['t' || i] FROM generate_series(1, 4) i;";
    let reps = r#"replications:
  push:
    source: appdb_pg
    target: lake
    defaults: {mode: incremental, primary_key: [id]}
    streams:
      - {id: push/native, from: {table: appdb.native_t}, object: br.native_t, update_key: at}
      - {id: push/cast, from: {table: appdb.cast_t}, object: br.cast_t, update_key: at}
      - {id: push/cast_uk, from: {table: appdb.cast_uk}, object: br.cast_uk, update_key: n}
"#;
    let e = setup(pg, seed, reps).await;
    ok(&go(&e, &["push"]).await, "push");
    e.sql
        .client()
        .batch_execute(
            "SELECT pg_stat_statements_reset();
             INSERT INTO appdb.native_t VALUES (5, '2024-02-01'), (0, '2023-01-01');
             INSERT INTO appdb.cast_t VALUES (5, '2024-02-01', '{x}'), (0, '2023-01-01', '{old}');
             INSERT INTO appdb.cast_uk VALUES (5, 9.5, '{x}'), (0, 0.5, '{old}');",
        )
        .await
        .expect("more");
    let r = go(&e, &["push"]).await;
    let ms = ok(&r, "push");
    assert_eq!(
        ms.iter()
            .map(|m| (m.rows.staged, m.rows.inserted, m.rows.unchanged))
            .collect::<Vec<_>>(),
        [(2, 1, 1), (2, 1, 1), (2, 1, 1)],
        "the watermark row and the newer one, pushed down or not"
    );
    for t in ["native_t", "cast_t", "cast_uk"] {
        assert_eq!(
            one::<i64>(&e, &format!("SELECT count(*) FROM lake.br.{t}")),
            5,
            "{t}"
        );
    }
    let copies: Vec<String> = e
        .sql
        .client()
        .query(
            "SELECT query FROM pg_stat_statements WHERE query LIKE 'COPY%appdb%'",
            &[],
        )
        .await
        .expect("statements")
        .iter()
        .map(|r| r.get(0))
        .collect();
    let of = |t: &str| {
        copies
            .iter()
            .filter(|q| q.contains(&format!("\"appdb\".\"{t}\"")))
            .cloned()
            .collect::<Vec<_>>()
    };
    let has_cursor =
        |qs: &[String], col: &str| qs.iter().any(|q| q.contains(&format!("\"{col}\" >=")));
    assert!(
        has_cursor(&of("native_t"), "at"),
        "the scanner pushes its own: {copies:?}"
    );
    let cast = of("cast_t");
    assert!(
        cast.iter().any(|q| q.contains("to_json")) && has_cursor(&cast, "at"),
        "a member with converted columns reads through postgres_query with the cursor inside: {copies:?}"
    );
    let uk = of("cast_uk");
    assert!(
        !uk.is_empty() && !has_cursor(&uk, "n"),
        "a converted cursor column stays engine-side: {copies:?}"
    );
}

async fn shared_snapshot_reads_members_consistently(pg: &'static Postgres) {
    let seed = "CREATE EXTENSION IF NOT EXISTS dblink;
        CREATE SCHEMA appdb;
        CREATE TABLE appdb.b (id int PRIMARY KEY, v int);
        CREATE TABLE appdb.c (id int PRIMARY KEY, v int);
        INSERT INTO appdb.b VALUES (1, 0); INSERT INTO appdb.c VALUES (1, 0);";
    let reps = r#"replications:
  per_member:
    source: appdb_pg
    target: lake
    streams:
      - {id: snap/per_member, from: {tables: "appdb.*", include_views: true}, object: "br.pm_{{ member.table }}", mode: full-refresh}
  shared:
    source: appdb_pg
    target: lake
    source_snapshot: shared
    streams:
      - {id: snap/shared, from: {tables: "appdb.*", include_views: true}, object: "br.sh_{{ member.table }}", mode: full-refresh}
"#;
    let e = setup(pg, seed, reps).await;
    // Read between `b` and `c` (members are sorted), this view commits a writer's change to both.
    let db: String = e
        .sql
        .client()
        .query_one("SELECT current_database()", &[])
        .await
        .expect("db")
        .get(0);
    e.sql
        .client()
        .batch_execute(&format!(
            "CREATE VIEW appdb.b_writer AS SELECT 1 AS id, dblink_exec('host=localhost port=5432 dbname={db} user={} password={}', \
             'UPDATE appdb.b SET v = v + 1; UPDATE appdb.c SET v = v + 1') AS r",
            pg.user, pg.password
        ))
        .await
        .expect("view");

    let r = go(&e, &["per_member"]).await;
    let ms = ok(&r, "per_member");
    assert_eq!(
        ms.iter().map(|m| m.member.as_str()).collect::<Vec<_>>(),
        ["appdb.b", "appdb.b_writer", "appdb.c"]
    );
    let seen = |p: &str| -> (i32, i32) {
        (
            one(&e, &format!("SELECT v FROM lake.br.{p}_b")),
            one(&e, &format!("SELECT v FROM lake.br.{p}_c")),
        )
    };
    assert_eq!(
        seen("pm"),
        (0, 1),
        "one snapshot per member: `c` sees the write `b` didn't"
    );

    let r = go(&e, &["shared"]).await;
    ok(&r, "shared");
    assert_eq!(seen("sh"), (1, 1), "one snapshot for every member");
    let (b, c): (i32, i32) = {
        let row = e
            .sql
            .client()
            .query_one("SELECT b.v, c.v FROM appdb.b, appdb.c", &[])
            .await
            .expect("pg");
        (row.get(0), row.get(1))
    };
    assert_eq!((b, c), (2, 2), "the writer did commit mid-read");
    let holders: i64 = e
        .sql
        .client()
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'",
            &[],
        )
        .await
        .expect("activity")
        .get(0);
    assert_eq!(holders, 0, "the exported snapshot is released");
}
