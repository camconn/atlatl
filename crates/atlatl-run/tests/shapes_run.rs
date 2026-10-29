//! Pipeline shapes through the runner (M8.6, 11 §3): `then:` multi-hop and fan-out into Postgres
//! (PG 16 and PG 12), and per-tenant `for_each` driven by lake and Postgres queries.

use std::io::Write;
use std::sync::{Arc, Mutex};

use atlatl_core::obs::events::EventSink;
use atlatl_core::obs::report::{
    HopStatus, ReplicationReport, ReplicationStatus, RunReport, RunStatus, StreamStatus,
};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::serve::{BootstrapArgs, bootstrap_cmd};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, Postgres, TempProject};
use tokio_util::sync::CancellationToken;

struct Env {
    lake: LocalLake,
    project: TempProject,
    pg: atlatl_conn_postgres::ConnectOptions,
}

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buf").extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Buf {
    fn events(&self) -> Vec<serde_json::Value> {
        let b = self.0.lock().expect("buf");
        String::from_utf8_lossy(&b)
            .lines()
            .map(|l| serde_json::from_str(l).expect("event"))
            .collect()
    }
}

async fn setup(
    pg: &'static Postgres,
    reps: &str,
    seed: &str,
    pg_seed: &str,
    files: &[(&str, &str)],
) -> Env {
    let lake = LocalLake::file().expect("lake");
    lake.open("lake")
        .expect("open")
        .execute_batch(seed)
        .expect("seed");
    let db = pg.fresh_db().expect("db");
    let mut opts = atlatl_conn_postgres::ConnectOptions::new(
        &pg.host,
        pg.port,
        &db,
        &pg.user,
        Some(pg.password.clone()),
    );
    opts.sslmode = atlatl_core::config::model::SslMode::Disable;
    let conns = format!(
        "{}  app: {{type: postgres, host: {}, port: {}, database: {db}, user: {}, password: \"{}\", sslmode: disable}}\n  files: {{type: files}}\n",
        lake.connection_yaml("lake", "lake"),
        pg.host,
        pg.port,
        pg.user,
        pg.password
    );
    let mut project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.replication("shapes", reps))
        .expect("project");
    for (path, text) in files {
        project = project.file(path, text).expect("file");
    }
    let e = Env {
        lake,
        project,
        pg: opts,
    };
    if !pg_seed.is_empty() {
        pg_exec(&e, pg_seed).await;
    }
    e
}

async fn pg_exec(e: &Env, sql: &str) {
    let s = atlatl_conn_postgres::connect(&e.pg).await.expect("pg");
    s.client().batch_execute(sql).await.expect(sql);
}

async fn pg_rows(e: &Env, sql: &str) -> Vec<String> {
    let s = atlatl_conn_postgres::connect(&e.pg).await.expect("pg");
    s.client()
        .query(sql, &[])
        .await
        .expect(sql)
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

fn lake_exec(e: &Env, sql: &str) {
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch(sql)
        .expect(sql);
}

fn lake_rows(e: &Env, sql: &str) -> Vec<String> {
    let c = e.lake.open("lake").expect("open");
    let mut s = c.prepare(sql).expect(sql);
    s.query_map([], |r| r.get::<_, String>(0))
        .expect(sql)
        .collect::<Result<_, _>>()
        .expect(sql)
}

fn project(e: &Env) -> Arc<atlatl_core::config::load::Project> {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    Arc::new(p)
}

fn vars() -> Arc<dyn EnvSource> {
    Arc::new(MapEnv::default())
}

async fn go(e: &Env, events: &Buf) -> RunReport {
    let args = RunArgs {
        plan: PlanRequest::all(),
        extension_dir: Some(atlatl_testkit::extension_dir()),
        events: EventSink::to_writer(events.clone(), "test"),
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

async fn boot(e: &Env, rep: &str) -> atlatl_core::error::Result<serde_json::Value> {
    let a = BootstrapArgs {
        replication: rep.into(),
        env: None,
        mode: atlatl_cdc::bootstrap::Mode::Adopt,
        at_snapshot: None,
        dry_run: false,
        advance_cursor_only: false,
        extension_dir: Some(atlatl_testkit::extension_dir()),
    };
    bootstrap_cmd(project(e), a, vars()).await
}

fn rep<'a>(r: &'a RunReport, name: &str) -> &'a ReplicationReport {
    r.replications
        .iter()
        .find(|x| x.replication == name)
        .unwrap_or_else(|| {
            panic!(
                "no `{name}` in {:?}",
                r.replications
                    .iter()
                    .map(|x| &x.replication)
                    .collect::<Vec<_>>()
            )
        })
}

fn hops(r: &ReplicationReport) -> Vec<(&str, HopStatus, bool)> {
    r.hops
        .iter()
        .map(|h| (h.target.as_str(), h.status, h.lake_snapshot.is_some()))
        .collect()
}

fn streams(r: &ReplicationReport) -> Vec<(&str, StreamStatus)> {
    r.streams
        .iter()
        .map(|s| (s.stream_id.as_str(), s.status))
        .collect()
}

const MULTI_HOP: &str = r#"replications:
  orders:
    source: lake
    target: lake
    streams:
      - id: shape/orders
        mode: incremental
        from: {table: src.orders}
        object: br.orders
        primary_key: [id]
        update_key: updated_at
    then:
      - target: app
        streams:
          - id: shape/serve_orders
            mode: cdc
            from: {table: br.orders}
            object: dagster.orders
            primary_key: [id]
            columns: {include: [id, tenant, amount]}
            deletes: {mode: hard}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_hop_commits_upstream_first_and_keeps_the_lake_hop_on_failure() {
    let e = setup(
        atlatl_testkit::pg16(),
        MULTI_HOP,
        "CREATE SCHEMA lake.src;
         CREATE TABLE lake.src.orders AS SELECT * FROM (VALUES (1, 'acme', 10, TIMESTAMP '2026-01-01'), (2, 'zed', 20, TIMESTAMP '2026-01-02'), (3, 'acme', 30, TIMESTAMP '2026-01-03')) t(id, tenant, amount, updated_at);",
        "CREATE SCHEMA dagster; CREATE TABLE dagster.orders (id bigint PRIMARY KEY, tenant text, amount bigint);",
        &[],
    )
    .await;

    // No serving cursor yet: the lake hop commits, the serve hop fails, the run is a partial failure.
    let ev = Buf::default();
    let r = go(&e, &ev).await;
    let o = rep(&r, "orders");
    assert_eq!(o.status, ReplicationStatus::Failed);
    assert_eq!(
        o.error.as_ref().and_then(|x| x.code.as_deref()),
        Some("bootstrap_required"),
        "{:?}",
        o.error
    );
    assert_eq!(
        hops(o),
        [
            ("lake", HopStatus::Committed, true),
            ("app", HopStatus::Failed, false)
        ]
    );
    assert_eq!(
        streams(o),
        [
            ("shape/orders", StreamStatus::Succeeded),
            ("shape/serve_orders", StreamStatus::Failed)
        ]
    );
    assert_eq!((r.status, r.exit_code), (RunStatus::PartialFailure, 1));
    assert_eq!(
        lake_rows(&e, "SELECT count(*)::VARCHAR FROM lake.br.orders"),
        ["3"],
        "the lake hop stays committed"
    );
    let evs = ev.events();
    let hop: Vec<&serde_json::Value> = evs
        .iter()
        .filter(|v| v["type"] == "hop.committed")
        .collect();
    assert_eq!(hop.len(), 1, "{evs:?}");
    assert_eq!(
        (
            hop[0]["replication"].as_str(),
            hop[0]["hop"].as_u64(),
            hop[0]["target"].as_str()
        ),
        (Some("orders"), Some(0), Some("lake"))
    );
    assert!(hop[0]["lake_snapshot"].as_u64().is_some());
    let done = evs
        .iter()
        .find(|v| v["type"] == "replication.completed")
        .expect("completed");
    assert_eq!(done["hops"][1]["status"], "failed");

    let b = boot(&e, "orders").await.expect("bootstrap");
    assert_eq!(
        (
            b["streams"][0]["stream"].as_str(),
            b["streams"][0]["hop"].as_u64(),
            b["streams"][0]["upserts"].as_u64()
        ),
        (Some("shape/serve_orders"), Some(1), Some(3)),
        "{b}"
    );
    assert_eq!(
        pg_rows(&e, "SELECT count(*)::text FROM dagster.orders").await,
        ["3"]
    );

    lake_exec(
        &e,
        "INSERT INTO lake.src.orders VALUES (4, 'zed', 40, TIMESTAMP '2026-01-04'); UPDATE lake.src.orders SET amount = 11, updated_at = TIMESTAMP '2026-01-05' WHERE id = 1;",
    );
    let ev = Buf::default();
    let r = go(&e, &ev).await;
    let o = rep(&r, "orders");
    assert_eq!(o.status, ReplicationStatus::Succeeded, "{:?}", o.error);
    assert_eq!(
        hops(o),
        [
            ("lake", HopStatus::Committed, true),
            ("app", HopStatus::Committed, true)
        ],
        "a serving hop reports the snapshot it served up to"
    );
    assert_eq!(
        streams(o),
        [
            ("shape/orders", StreamStatus::Succeeded),
            ("shape/serve_orders", StreamStatus::Succeeded)
        ]
    );
    assert_eq!(r.status, RunStatus::Succeeded);
    let cdc = o.streams[1].members[0].cdc.clone().expect("cdc");
    assert_eq!(
        (cdc.rows_inserted, cdc.rows_updated),
        (1, 1),
        "the serve hop catches up from its cursor: {cdc:?}"
    );
    assert_eq!(
        pg_rows(
            &e,
            "SELECT id || ':' || amount FROM dagster.orders ORDER BY id"
        )
        .await,
        ["1:11", "2:20", "3:30", "4:40"]
    );
    let order: Vec<(String, u64)> = ev
        .events()
        .iter()
        .filter(|v| v["type"] == "hop.committed")
        .map(|v| {
            (
                v["target"].as_str().unwrap_or_default().to_owned(),
                v["hop"].as_u64().unwrap_or(9),
            )
        })
        .collect();
    assert_eq!(
        order,
        [("lake".to_owned(), 0), ("app".to_owned(), 1)],
        "upstream first"
    );
}

const FAN_OUT: &str = r#"replications:
  prices:
    source: files
    target: lake
    streams:
      - id: fan/prices
        mode: full-refresh
        from: {files: {url: in, glob: "prices.csv"}}
        object: br.prices
        primary_key: [ticker]
        columns: {px: {type: BIGINT}}
    then:
      - target: app
        defaults: {deletes: {mode: hard}}
        streams:
          - id: fan/serve_prices
            mode: cdc
            from: {table: br.prices}
            object: dagster.prices
            primary_key: [ticker]
      - target: app
        defaults: {deletes: {mode: hard}}
        streams:
          - id: fan/serve_missing
            mode: cdc
            from: {table: br.prices}
            object: dagster.missing
            primary_key: [ticker]
      - target: lake
        streams:
          - id: fan/ag_prices
            mode: full-refresh
            from: {query: "SELECT ticker, px * 2 AS px2 FROM lake.br.prices"}
            object: ag.prices
            primary_key: [ticker]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_out_feeds_the_lake_and_serving_tables_from_one_extract() {
    let e = setup(
        atlatl_testkit::pg12(),
        FAN_OUT,
        "SELECT 1",
        "CREATE SCHEMA dagster; CREATE TABLE dagster.prices (ticker text PRIMARY KEY, px bigint);",
        &[("in/prices.csv", "ticker,px\nAAPL,1\nMSFT,2\n")],
    )
    .await;
    let r = go(&e, &Buf::default()).await;
    let p = rep(&r, "prices");
    assert_eq!(
        hops(p),
        [
            ("lake", HopStatus::Committed, true),
            ("app", HopStatus::Failed, false),
            ("app", HopStatus::Failed, false),
            ("lake", HopStatus::Committed, true)
        ],
        "a failed hop doesn't stop its siblings"
    );
    assert_eq!(
        lake_rows(
            &e,
            "SELECT ticker || ':' || px2 FROM lake.ag.prices ORDER BY 1"
        ),
        ["AAPL:2", "MSFT:4"]
    );
    let rows = |id: &str| {
        p.streams
            .iter()
            .find(|s| s.stream_id == id)
            .map(|s| s.rows().extracted)
    };
    assert_eq!(rows("fan/prices"), Some(2), "one extract");

    assert_eq!(
        p.hops[2].error.as_ref().and_then(|x| x.code.as_deref()),
        Some("bootstrap_required")
    );
    // The missing table can't be bootstrapped; its sibling's cursor is set first.
    let err = boot(&e, "prices").await.expect_err("missing table");
    assert!(err.message.contains("missing"), "{err:?}");
    std::fs::write(
        e.project.path().join("in/prices.csv"),
        "ticker,px\nAAPL,5\nMSFT,2\nNVDA,3\n",
    )
    .expect("csv");
    let r = go(&e, &Buf::default()).await;
    let p = rep(&r, "prices");
    assert_eq!(p.status, ReplicationStatus::Failed);
    assert_eq!(
        p.error.as_ref().and_then(|x| x.code.as_deref()),
        Some("bootstrap_required")
    );
    assert_eq!(
        streams(p),
        [
            ("fan/prices", StreamStatus::Succeeded),
            ("fan/serve_prices", StreamStatus::Succeeded),
            ("fan/serve_missing", StreamStatus::Failed),
            ("fan/ag_prices", StreamStatus::Succeeded)
        ]
    );
    assert_eq!(
        pg_rows(
            &e,
            "SELECT ticker || ':' || px FROM dagster.prices ORDER BY 1"
        )
        .await,
        ["AAPL:5", "MSFT:2", "NVDA:3"]
    );
    assert_eq!(
        lake_rows(
            &e,
            "SELECT ticker || ':' || px2 FROM lake.ag.prices ORDER BY 1"
        ),
        ["AAPL:10", "MSFT:4", "NVDA:6"]
    );
}

type Entry<'a> = (&'a str, ReplicationStatus, Option<(&'a str, &'a str)>);

const TENANTS: &str = r#"replications:
  tenants:
    source: lake
    target: lake
    for_each: {over: {query: "SELECT name FROM lake.src.tenants ORDER BY name", connection: lake}, as: tenant}
    streams:
      - id: t/orders
        mode: incremental
        from: {query: "SELECT * FROM lake.src.orders WHERE tenant = '{{ tenant }}'"}
        object: br.tenant_orders
        primary_key: [tenant, id]
        update_key: updated_at
  pg_tenants:
    source: lake
    target: lake
    after: [tenants]
    for_each: {over: {query: "SELECT id, slug FROM tenants ORDER BY id", connection: app}, as: t, key: slug}
    streams:
      - id: t/counts
        mode: full-refresh
        from: {query: "SELECT {{ t.id }} AS tenant_id, count(*)::BIGINT AS n FROM lake.br.tenant_orders WHERE tenant = '{{ t.slug }}'"}
        object: "br.counts_{{ t.slug }}"
  listed:
    source: lake
    target: lake
    for_each: {over: [acme], as: tenant}
    streams:
      - id: t/listed
        mode: full-refresh
        from: {query: "SELECT '{{ tenant }}' AS tenant"}
        object: "br.listed_{{ tenant }}"
  none:
    source: lake
    target: lake
    for_each: {over: {query: "SELECT name FROM lake.src.tenants WHERE false", connection: lake}, as: tenant}
    streams:
      - {id: t/none, mode: incremental, from: {table: src.orders}, object: br.none, primary_key: [id]}
  broken:
    source: lake
    target: lake
    for_each: {over: {query: "SELECT nope FROM lake.src.missing", connection: lake}, as: tenant}
    streams:
      - {id: t/broken, mode: incremental, from: {table: src.orders}, object: br.broken, primary_key: [id]}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_each_instances_are_independent_units() {
    let e = setup(
        atlatl_testkit::pg16(),
        TENANTS,
        "CREATE SCHEMA lake.src;
         CREATE TABLE lake.src.tenants AS SELECT * FROM (VALUES ('acme'), ('bad'), ('zed')) t(name);
         CREATE TABLE lake.src.orders AS SELECT * FROM (VALUES
           (1, 'acme', TIMESTAMP '2026-01-01'), (2, 'acme', TIMESTAMP '2026-01-02'),
           (1, 'bad', TIMESTAMP '2026-01-01'), (1, 'bad', TIMESTAMP '2026-01-03'),
           (1, 'zed', TIMESTAMP '2026-02-01')) t(id, tenant, updated_at);",
        "CREATE TABLE tenants (id int PRIMARY KEY, slug text); INSERT INTO tenants VALUES (1, 'acme'), (2, 'zed');",
        &[],
    )
    .await;
    let r = go(&e, &Buf::default()).await;
    let st: Vec<Entry<'_>> = r
        .replications
        .iter()
        .map(|x| {
            (
                x.replication.as_str(),
                x.status,
                x.instance.as_ref().map(|i| (i.of.as_str(), i.key.as_str())),
            )
        })
        .collect();
    assert_eq!(
        st,
        [
            (
                "tenants[acme]",
                ReplicationStatus::Succeeded,
                Some(("tenants", "acme"))
            ),
            (
                "tenants[bad]",
                ReplicationStatus::Failed,
                Some(("tenants", "bad"))
            ),
            (
                "tenants[zed]",
                ReplicationStatus::Succeeded,
                Some(("tenants", "zed"))
            ),
            (
                "pg_tenants[acme]",
                ReplicationStatus::Skipped,
                Some(("pg_tenants", "acme"))
            ),
            (
                "pg_tenants[zed]",
                ReplicationStatus::Skipped,
                Some(("pg_tenants", "zed"))
            ),
            (
                "listed[acme]",
                ReplicationStatus::Succeeded,
                Some(("listed", "acme"))
            ),
            ("none", ReplicationStatus::UpToDate, None),
            ("broken", ReplicationStatus::Failed, None),
        ],
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    assert_eq!(
        rep(&r, "tenants[bad]")
            .error
            .as_ref()
            .map(|x| x.class.as_str()),
        Some("data"),
        "duplicate keys fail that tenant only"
    );
    assert!(
        rep(&r, "broken")
            .error
            .as_ref()
            .is_some_and(|x| x.message.contains("missing")),
        "{:?}",
        rep(&r, "broken").error
    );
    assert_eq!(
        rep(&r, "pg_tenants[zed]")
            .error
            .as_ref()
            .and_then(|x| x.code.as_deref()),
        Some("dependency_failed"),
        "`after` waits for every instance"
    );
    assert_eq!(
        lake_rows(
            &e,
            "SELECT tenant || ':' || count(*) FROM lake.br.tenant_orders GROUP BY tenant ORDER BY 1"
        ),
        ["acme:2", "zed:1"]
    );
    assert_eq!(
        lake_rows(
            &e,
            "SELECT stream_id || '=' || (value->>'value') FROM lake._atlatl.stream_state WHERE kind = 'update_key' ORDER BY 1"
        ),
        [
            "t/orders@acme=2026-01-02 00:00:00",
            "t/orders@zed=2026-02-01 00:00:00"
        ],
        "one watermark per instance"
    );

    lake_exec(
        &e,
        "DELETE FROM lake.src.orders WHERE tenant = 'bad'; INSERT INTO lake.src.orders VALUES (3, 'acme', TIMESTAMP '2026-03-01'), (0, 'zed', TIMESTAMP '2026-01-15');",
    );
    let r = go(&e, &Buf::default()).await;
    assert!(
        r.replications
            .iter()
            .filter(|x| x.replication != "broken")
            .all(|x| x.status.is_success()),
        "{:?}",
        r.replications
            .iter()
            .map(|x| (&x.replication, x.status, &x.error))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        lake_rows(
            &e,
            "SELECT tenant || ':' || count(*) FROM lake.br.tenant_orders GROUP BY tenant ORDER BY 1"
        ),
        ["acme:3", "zed:1"],
        "zed's watermark is its own: its older row stays behind it"
    );
    assert_eq!(
        (
            lake_rows(&e, "SELECT tenant_id || ':' || n FROM lake.br.counts_acme"),
            lake_rows(&e, "SELECT tenant_id || ':' || n FROM lake.br.counts_zed")
        ),
        (vec!["1:3".to_owned()], vec!["2:1".to_owned()]),
        "postgres-driven instances bind map items"
    );
    let pg = rep(&r, "pg_tenants[zed]");
    assert_eq!(
        pg.streams[0].members[0].object.as_deref(),
        Some(r#""lake"."br"."counts_zed""#)
    );
}

const RETRY_HOP: &str = r#"replications:
  retry_hop:
    source: lake
    target: lake
    replication_retries: 1
    streams:
      - id: rh/snap
        mode: snapshot
        from: {table: src.people}
        object: br.people_snap
        metadata: {loaded_at: {name: _loaded_at, type: epoch_s_int}}
    then:
      - target: lake
        streams:
          - {id: rh/copy, mode: full-refresh, from: {table: br.people_snap}, object: ag.people, primary_key: [id, _loaded_at]}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_multi_hop_replication_keeps_its_committed_hops() {
    let e = setup(
        atlatl_testkit::pg16(),
        RETRY_HOP,
        "CREATE SCHEMA lake.src; CREATE TABLE lake.src.people AS SELECT range AS id FROM range(3);",
        "",
        &[],
    )
    .await;
    atlatl_core::fault::set("hop.start@retry_hop#1:error:transient:once").expect("fault");
    let r = go(&e, &Buf::default()).await;
    atlatl_core::fault::clear();
    let x = rep(&r, "retry_hop");
    assert_eq!(
        (x.status, x.attempts),
        (ReplicationStatus::Succeeded, 2),
        "{:?}",
        x.error
    );
    assert_eq!(
        hops(x),
        [
            ("lake", HopStatus::Committed, true),
            ("lake", HopStatus::Committed, true)
        ]
    );
    assert!(x.hops[1].error.is_none());
    assert_eq!(
        lake_rows(&e, "SELECT count(*)::VARCHAR FROM lake.br.people_snap"),
        ["3"],
        "the lake hop ran once"
    );
    assert_eq!(
        lake_rows(&e, "SELECT count(*)::VARCHAR FROM lake.ag.people"),
        ["3"]
    );
}

/// A one-response-per-connection HTTP server answering `f(target)`; returns its URL and log.
async fn mock(
    f: impl Fn(&str) -> String + Send + Sync + 'static,
) -> (String, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", l.local_addr().expect("addr"));
    let log: Arc<Mutex<Vec<String>>> = Arc::default();
    let (lg, f) = (log.clone(), Arc::new(f));
    tokio::spawn(async move {
        while let Ok((sock, _)) = l.accept().await {
            let (lg, f) = (lg.clone(), f.clone());
            tokio::spawn(async move {
                let mut r = BufReader::new(sock);
                let mut line = String::new();
                if r.read_line(&mut line).await.is_err() {
                    return;
                }
                let target = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).await.is_err() || h.trim().is_empty() {
                        break;
                    }
                }
                lg.lock().expect("log").push(target.clone());
                let body = f(&target);
                let out = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let mut sock = r.into_inner();
                let _ = sock.write_all(out.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (url, log)
}

const USERS_SPEC: &str = r#"
base_url: "{{ env.T_BASE }}"
endpoints:
  accounts:
    request: {path: /accounts, params: {user: "{{ user.id }}", since: "{{ coalesce(sync.max_id, 0) }}"}}
    records: {path: "rows", primary_key: [id]}
    sync:
      max_id: {value: record.id, aggregate: max}
"#;

const USERS: &str = r#"replications:
  users:
    source: api
    target: lake
    for_each: {over: [{id: 1, name: a}, {id: 2, name: b}], as: user, key: id}
    streams:
      - id: api/accounts
        from: {endpoint: accounts}
        object: "br.accounts_{{ user.name }}"
        mode: incremental
        primary_key: [id]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_each_binds_the_item_in_rest_requests_with_sync_state_per_instance() {
    let (url, log) = mock(|t| {
        let rows = match (t.contains("user=1"), t.contains("since=0")) {
            (true, true) => r#"[{"id": 1}, {"id": 2}]"#,
            (false, true) => r#"[{"id": 5}]"#,
            _ => "[]",
        };
        format!(r#"{{"rows": {rows}}}"#)
    })
    .await;
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  api: {{type: rest, spec: apis/users.yaml}}\n",
        lake.connection_yaml("lake", "lake")
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.file("apis/users.yaml", USERS_SPEC))
        .and_then(|p| p.replication("users", USERS))
        .expect("project");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_BASE", url.as_str())]));
    let go = || async {
        let (p, d) = atlatl_core::config::load(project.path());
        assert!(!d.has_errors(), "{d:?}");
        let args = RunArgs {
            plan: PlanRequest::all(),
            extension_dir: Some(atlatl_testkit::extension_dir()),
            ..Default::default()
        };
        run(
            Arc::new(p),
            args,
            vars.clone(),
            CancellationToken::new(),
            || None,
            |_, _| {},
        )
        .await
        .expect("run")
    };
    let r = go().await;
    let st: Vec<(&str, ReplicationStatus)> = r
        .replications
        .iter()
        .map(|x| (x.replication.as_str(), x.status))
        .collect();
    assert_eq!(
        st,
        [
            ("users[1]", ReplicationStatus::Succeeded),
            ("users[2]", ReplicationStatus::Succeeded)
        ],
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    let c = lake.open("lake").expect("open");
    let q = |sql: &str| c.query_row(sql, [], |r| r.get::<_, String>(0)).expect(sql);
    assert_eq!(
        (
            q("SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM lake.br.accounts_a"),
            q("SELECT string_agg(id::VARCHAR, ',') FROM lake.br.accounts_b")
        ),
        ("1,2".into(), "5".into())
    );
    drop(c);

    log.lock().expect("log").clear();
    let r = go().await;
    assert!(
        r.replications.iter().all(|x| x.status.is_success()),
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    let mut targets = log.lock().expect("log").clone();
    targets.sort();
    assert_eq!(
        targets,
        ["/accounts?user=1&since=2", "/accounts?user=2&since=5"],
        "each instance resumes from its own sync values"
    );
}

const PICKED: &str = r#"replications:
  q:
    source: lake
    target: lake
    for_each: {over: {query: "SELECT name FROM lake.src.tenants ORDER BY name", connection: lake}, as: tenant}
    streams:
      - {id: p/q, mode: full-refresh, from: {query: "SELECT '{{ tenant }}' AS tenant"}, object: "br.q_{{ tenant }}"}
  l:
    source: lake
    target: lake
    for_each: {over: [a, b], as: tenant}
    streams:
      - {id: p/l, mode: full-refresh, from: {query: "SELECT '{{ tenant }}' AS tenant"}, object: "br.l_{{ tenant }}"}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instance_selection_runs_only_the_named_instances() {
    let e = setup(
        atlatl_testkit::pg16(),
        PICKED,
        "CREATE SCHEMA lake.src; CREATE TABLE lake.src.tenants AS SELECT * FROM (VALUES ('acme'), ('zed')) t(name);",
        "",
        &[],
    )
    .await;
    let picked = |instances: &[&str]| {
        let args = RunArgs {
            plan: PlanRequest {
                instances: instances.iter().map(|i| (*i).to_owned()).collect(),
                ..PlanRequest::all()
            },
            extension_dir: Some(atlatl_testkit::extension_dir()),
            ..Default::default()
        };
        run(
            project(&e),
            args,
            vars(),
            CancellationToken::new(),
            || None,
            |_, _| {},
        )
    };
    let r = picked(&["q[zed]", "l[b]"]).await.expect("run");
    let st: Vec<(&str, ReplicationStatus)> = r
        .replications
        .iter()
        .map(|x| (x.replication.as_str(), x.status))
        .collect();
    assert_eq!(
        st,
        [
            ("q[zed]", ReplicationStatus::Succeeded),
            ("l[b]", ReplicationStatus::Succeeded)
        ]
    );
    assert_eq!(
        lake_rows(
            &e,
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'br' ORDER BY 1"
        ),
        ["l_b", "q_zed"]
    );

    let r = picked(&["q[nope]", "q[acme]"]).await.expect("run");
    assert_eq!(r.replications.len(), 1, "{:?}", r.replications);
    let q = rep(&r, "q");
    let err = q.error.as_ref().expect("error");
    assert_eq!(
        (q.status, err.class.as_str(), err.code.as_deref()),
        (
            ReplicationStatus::Failed,
            "config",
            Some("unknown_instance")
        )
    );
    assert!(
        err.message.contains("`nope`") && err.message.contains("acme, zed"),
        "{}",
        err.message
    );

    let e2 = picked(&["l[c]"]).await.expect_err("plan-time");
    assert!(e2.message.contains("has no instance `c`"), "{e2}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_each_items_keep_their_exact_values() {
    let e = setup(
        atlatl_testkit::pg16(),
        r#"replications:
  wide:
    source: lake
    target: lake
    for_each: {over: {query: "SELECT 170141183460469231731687303715884105727::HUGEINT AS id", connection: lake}, as: t}
    streams:
      - {id: w/wide, mode: incremental, primary_key: [id], from: {query: "SELECT '{{ t }}' AS id"}, object: br.wide}
  scaled:
    source: lake
    target: lake
    for_each: {over: {query: "SELECT 1.10::numeric(4,2) AS v", connection: app}, as: t}
    streams:
      - {id: w/scaled, mode: incremental, primary_key: [v], from: {query: "SELECT '{{ t }}' AS v"}, object: br.scaled}
"#,
        "",
        "",
        &[],
    )
    .await;
    let r = go(&e, &Buf::default()).await;
    let names: Vec<&str> = r
        .replications
        .iter()
        .map(|x| x.replication.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "wide[170141183460469231731687303715884105727]",
            "scaled[1.1]"
        ],
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    assert!(r.replications.iter().all(|x| x.status.is_success()));
    assert_eq!(
        lake_rows(&e, "SELECT id FROM lake.br.wide"),
        ["170141183460469231731687303715884105727"]
    );
    assert_eq!(lake_rows(&e, "SELECT v FROM lake.br.scaled"), ["1.1"]);
}
