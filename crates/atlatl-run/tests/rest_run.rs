//! REST → lake extract (`rest::stage`) against an in-process HTTP server and a local lake: queues and
//! feed-only producers, JSON staging, sync state, `--range`, row-mode rejects, and query iteration
//! over a lake and Postgres (M7.1). The commit is simulated with the documented contract.

use std::sync::{Arc, Mutex};

use atlatl_core::ErrorClass;
use atlatl_core::plan::{PlanRequest, StreamRole};
use atlatl_core::redact::Redactor;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::rest::{self, RestStaged};
use atlatl_run::runtime::Runtime;
use atlatl_testkit::{LocalLake, TempProject};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

// ----------------------------------------------------------------------------- mock server

type Handler = dyn Fn(&str, usize) -> (u16, &'static str, String) + Send + Sync;

struct Mock {
    url: String,
    log: Arc<Mutex<Vec<String>>>,
}

impl Mock {
    /// `f(target, n)` → `(status, content type, body)`.
    async fn start(
        f: impl Fn(&str, usize) -> (u16, &'static str, String) + Send + Sync + 'static,
    ) -> Self {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", l.local_addr().expect("addr"));
        let log: Arc<Mutex<Vec<String>>> = Arc::default();
        let h: Arc<Handler> = Arc::new(f);
        let lg = log.clone();
        tokio::spawn(async move {
            while let Ok((sock, _)) = l.accept().await {
                let (lg, h) = (lg.clone(), h.clone());
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
                    let mut len = 0usize;
                    loop {
                        let mut hdr = String::new();
                        if r.read_line(&mut hdr).await.is_err() || hdr.trim().is_empty() {
                            break;
                        }
                        if let Some((k, v)) = hdr.split_once(':')
                            && k.eq_ignore_ascii_case("content-length")
                        {
                            len = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0; len];
                    let _ = r.read_exact(&mut body).await;
                    let n = {
                        let mut l = lg.lock().expect("log");
                        l.push(target.clone());
                        l.len() - 1
                    };
                    let (status, ct, body) = h(&target, n);
                    let out = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: {ct}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let mut sock = r.into_inner();
                    let _ = sock.write_all(out.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        Self { url, log }
    }

    fn targets(&self) -> Vec<String> {
        self.log.lock().expect("log").clone()
    }
}

fn param(target: &str, k: &str) -> Option<String> {
    target.split_once('?')?.1.split('&').find_map(|p| {
        p.split_once('=')
            .filter(|(n, _)| *n == k)
            .map(|(_, v)| v.replace("%2C", ",").replace("%3A", ":"))
    })
}

fn json(body: String) -> (u16, &'static str, String) {
    (200, "application/json", body)
}

// ----------------------------------------------------------------------------- harness

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn env(spec: &str, reps: &str, extra_conns: &str) -> Env {
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  api:\n    type: rest\n    spec: apis/t.yaml\n    secrets: {{api_key: {{secret: env:T_API_KEY}}}}\n    retry: {{max_attempts: 2, backoff: constant, base: 5ms}}\n{extra_conns}",
        lake.connection_yaml("lake", "lake")
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.file("apis/t.yaml", spec))
        .and_then(|p| p.replication("r", reps))
        .expect("project");
    Env { lake, project }
}

async fn runtime(e: &Env, req: PlanRequest) -> Runtime {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k-rest-run-secret")]));
    let (plan, _) = atlatl_core::plan::build(&p, &req, vars.clone(), Redactor::global())
        .unwrap_or_else(|d| panic!("{d:?}"));
    atlatl_run::run::runtime(
        Arc::new(p),
        Arc::new(plan),
        vars,
        None,
        Some(atlatl_testkit::extension_dir()),
        atlatl_core::obs::events::EventSink::disabled(),
    )
    .await
    .expect("runtime")
}

fn all() -> PlanRequest {
    PlanRequest::all()
}

/// Stages every stream of the first replication in extract order; `Err` from the first failure.
async fn stage_all(
    rt: &Runtime,
    session: &mut atlatl_engine::Session,
) -> Result<Vec<(String, RestStaged)>, atlatl_core::AtlatlError> {
    let rep = &rt.plan.replications[0];
    let mut out = Vec::new();
    for i in rest::extract_order(rt, rep)? {
        let sp = &rep.streams[i];
        let s = rt.stream(&sp.id)?;
        let name = atlatl_ingest::staging::staging_name(&rt.run_id, i, 0);
        out.push((
            sp.id.clone(),
            rest::stage(rt, session, rep, sp, s, &sp.members[0], &name).await?,
        ));
    }
    Ok(out)
}

/// What the caller does in its commit: write the target (here: append by name), state and rejects.
async fn commit(
    rt: &Runtime,
    session: &mut atlatl_engine::Session,
    staged: &[(String, RestStaged)],
) {
    let st = rt.state.clone().expect("state");
    let rep = rt.plan.replications[0].clone();
    let staged: Vec<(String, RestStaged)> = staged.to_vec();
    session
        .run(move |c| {
            c.execute_batch("BEGIN").map_err(atlatl_ingest::sql::db)?;
            for (id, s) in &staged {
                let sp = rep.streams.iter().find(|x| &x.id == id).expect("sp");
                if let (Some(stg), false) = (&s.staging, s.empty) {
                    let obj = sp.members[0].object.clone().expect("object");
                    let (schema, _) = atlatl_run::lake::split_object(&obj);
                    let (sc, t) = atlatl_run::lake::split_object(&obj);
                    let sql = match atlatl_ingest::staging::table_columns(c, "lake", &sc, &t)? {
                        None => format!("CREATE SCHEMA IF NOT EXISTS lake.{schema}; CREATE TABLE lake.{obj} AS SELECT * FROM temp.main.{stg}"),
                        Some(_) => format!("CREATE OR REPLACE TEMP TABLE merged AS SELECT * FROM lake.{obj} UNION ALL BY NAME SELECT * FROM temp.main.{stg}; CREATE OR REPLACE TABLE lake.{obj} AS SELECT * FROM temp.main.merged"),
                    };
                    c.execute_batch(&sql).map_err(atlatl_ingest::sql::db)?;
                }
                if let Some(row) = &s.state {
                    st.put(c, row)?;
                }
                for r in s.rejects.iter().chain(&s.refreshed) {
                    st.upsert_reject(c, r)?;
                }
                st.resolve_rejects(c, id, &s.member, &s.resolved)?;
            }
            c.execute_batch("COMMIT").map_err(atlatl_ingest::sql::db)?;
            for (_, s) in &staged {
                for t in s.staging.iter().chain(&s.temp_tables) {
                    atlatl_ingest::staging::drop(c, t)?;
                }
            }
            Ok(())
        })
        .await
        .expect("commit");
}

async fn rows(session: &mut atlatl_engine::Session, sql: &str) -> Vec<String> {
    let (sql, what) = (sql.to_owned(), sql.to_owned());
    session
        .run(move |c| {
            let mut s = c
                .prepare(&format!("SELECT to_json(t)::VARCHAR FROM ({sql}) t"))
                .map_err(atlatl_ingest::sql::db)?;
            let r = s
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(atlatl_ingest::sql::db)?;
            r.collect::<Result<Vec<_>, _>>()
                .map_err(atlatl_ingest::sql::db)
        })
        .await
        .expect(&what)
}

// ----------------------------------------------------------------------------- tests

const QUEUE_SPEC: &str = r#"
base_url: "{{ env.T_BASE }}"
defaults:
  params: {key: "{{ secrets.api_key }}"}
endpoints:
  companies:
    request: {path: /companies}
    paginate: {type: page, param: page, start: 1}
    records: {path: "data", primary_key: [id]}
    emit: {ids: record.id}
  fundamentals:
    for_each: {over: {queue: ids, unique: true}, as: company_id, concurrency: 2}
    request: {path: "/companies/{{ company_id }}/fundamentals"}
    records: {path: "items", primary_key: [company_id, period]}
    fields: {company_id: item}
"#;

const QUEUE_REPS: &str = r#"replications:
  r:
    source: api
    target: lake
    streams:
      - {id: api/companies, from: {endpoint: companies}, object: br.companies, mode: full-refresh, primary_key: [id]}
      - {id: api/fundamentals, from: {endpoint: fundamentals}, object: br.fundamentals, mode: full-refresh, primary_key: [company_id, period]}
"#;

fn with_base(spec: &str, url: &str) -> String {
    spec.replace("\"{{ env.T_BASE }}\"", url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_only_producer_fills_a_queue_and_records_stage_exactly() {
    let m = Mock::start(|t, _| {
        let path = t.split('?').next().unwrap_or_default();
        match (path, param(t, "page").as_deref()) {
            ("/companies", Some("1")) => json(r#"{"data": [{"id": 123456789012345678901234567890}, {"id": 2}, {"id": 2}]}"#.into()),
            ("/companies", _) => json(r#"{"data": []}"#.into()),
            (p, _) => {
                let id = p.split('/').nth(2).unwrap_or_default();
                json(format!(r#"{{"items": [{{"period": "2025Q1", "eps": 1.10, "meta": {{"src": [1, 2]}}, "flag": true}}, {{"period": "2025Q2", "eps": null, "extra": "x{id}"}}]}}"#))
            }
        }
    })
    .await;
    let e = env(&with_base(QUEUE_SPEC, &m.url), QUEUE_REPS, "");
    let rt = runtime(
        &e,
        PlanRequest {
            streams: vec!["api/fundamentals".into()],
            ..all()
        },
    )
    .await;
    let rep = &rt.plan.replications[0];
    assert_eq!(
        rep.streams
            .iter()
            .map(|s| (s.id.as_str(), s.role))
            .collect::<Vec<_>>(),
        vec![
            ("api/companies", StreamRole::FeedOnly),
            ("api/fundamentals", StreamRole::Selected)
        ]
    );
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");

    let (_, producer) = &staged[0];
    assert!(producer.staging.is_none(), "feed-only writes nothing");
    assert_eq!(producer.rows, 2, "run-wide dedup by PK");
    assert_eq!(producer.temp_tables.len(), 1);
    let q = rows(
        &mut session,
        &format!(
            "SELECT value FROM temp.main.{} ORDER BY seq",
            producer.temp_tables[0]
        ),
    )
    .await;
    assert_eq!(
        q,
        vec![
            r#"{"value":"123456789012345678901234567890"}"#,
            r#"{"value":"2"}"#
        ],
        "exact JSON values"
    );

    let (_, consumer) = &staged[1];
    let stg = consumer.staging.clone().expect("staging");
    assert_eq!(consumer.rows, 4);
    let got = rows(&mut session, &format!("SELECT * FROM temp.main.{stg}")).await;
    assert_eq!(
        got[0],
        r#"{"period":"2025Q1","eps":"1.10","meta":"{\"src\":[1,2]}","flag":"true","company_id":"123456789012345678901234567890","extra":null}"#
    );
    assert_eq!(
        got[3],
        r#"{"period":"2025Q2","eps":null,"meta":null,"flag":null,"company_id":"2","extra":"x2"}"#
    );
    let types = rows(
        &mut session,
        &format!("SELECT DISTINCT column_type FROM (DESCRIBE temp.main.{stg}) "),
    )
    .await;
    assert!(
        types.iter().all(|t| t.contains("VARCHAR")),
        "infer: text: {types:?}"
    );
    let targets = m.targets();
    assert!(
        targets.iter().all(|t| t.contains("key=k-rest-run-secret")),
        "{targets:?}"
    );
    assert!(
        targets
            .iter()
            .any(|t| t.starts_with("/companies/123456789012345678901234567890/")),
        "{targets:?}"
    );

    let report = rt.report.finish();
    let member = &report.replications[0]
        .streams
        .iter()
        .find(|s| s.stream_id == "api/fundamentals")
        .expect("s")
        .members[0];
    let http = member.http.as_ref().expect("http stats");
    assert_eq!(http.requests, 2);
    assert_eq!(http.status_counts.get("200"), Some(&2));

    // A consumer staged before its producer fails clearly instead of iterating nothing.
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let rep = &rt.plan.replications[0];
    let sp = &rep.streams[1];
    let err = rest::stage(
        &rt,
        &mut session,
        rep,
        sp,
        rt.stream(&sp.id).expect("s"),
        &sp.members[0],
        "stg_x",
    )
    .await
    .expect_err("no queue");
    assert!(
        err.message.contains("producer must be extracted first"),
        "{}",
        err.message
    );
}

const SYNC_SPEC: &str = r#"
base_url: "{{ env.T_BASE }}"
endpoints:
  cal:
    for_each:
      over: 'range(coalesce(sync.floor, date("2025-01-01")), date("2025-12-31"), "3mo")'
      as: window_start
    request: {path: /cal, params: {d1: '{{ window_start | date_format("%Y-%m-%d") }}'}}
    records: {path: "rows", primary_key: [id]}
    sync:
      floor: {value: 'coalesce(sync.next, date("2025-10-01"))', aggregate: set}
      next: {value: 'date("2025-07-01")', aggregate: set}
      max_id: {value: record.id, aggregate: max}
  live:
    history: snapshot_only
    request: {path: /live}
    records: {path: "rows"}
"#;

const SYNC_REPS: &str = r#"replications:
  r:
    source: api
    target: lake
    streams:
      - {id: api/cal, from: {endpoint: cal}, object: br.cal, mode: incremental, primary_key: [id]}
      - {id: api/live, from: {endpoint: live}, object: br.live, mode: full-refresh}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_state_persists_and_range_overrides_windows() {
    let m = Mock::start(|t, n| {
        json(format!(
            r#"{{"rows": [{{"id": {}, "d1": "{}"}}]}}"#,
            100 + n,
            param(t, "d1").unwrap_or_default()
        ))
    })
    .await;
    let e = env(&with_base(SYNC_SPEC, &m.url), SYNC_REPS, "");
    let rt = runtime(
        &e,
        PlanRequest {
            streams: vec!["api/cal".into()],
            ..all()
        },
    )
    .await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert_eq!(m.targets().len(), 4, "first run backfills from the floor");
    let st = staged[0].1.state.clone().expect("sync state");
    assert_eq!(
        st.value,
        serde_json::json!({"floor": "2025-10-01T00:00:00Z", "next": "2025-07-01T00:00:00Z", "max_id": 103})
    );
    assert!(staged[0].1.state_before.is_none());
    commit(&rt, &mut session, &staged).await;

    // Second run: walks from the stored floor; `sync.next` from the stored state feeds `floor`.
    let rt = runtime(
        &e,
        PlanRequest {
            streams: vec!["api/cal".into()],
            ..all()
        },
    )
    .await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert_eq!(m.targets().len(), 5, "one window: 2025-10-01");
    assert_eq!(param(&m.targets()[4], "d1").as_deref(), Some("2025-10-01"));
    let s = &staged[0].1;
    assert_eq!(
        s.state_before
            .as_ref()
            .and_then(|v| v.get("floor"))
            .and_then(|v| v.as_str()),
        Some("2025-10-01T00:00:00Z")
    );
    assert_eq!(
        s.state.as_ref().map(|r| r.value["floor"].clone()),
        Some(serde_json::json!("2025-07-01T00:00:00Z"))
    );
    commit(&rt, &mut session, &staged).await;
    let c = e.lake.open("lake").expect("open");
    let stored: String = c.query_row("SELECT value::VARCHAR FROM lake._atlatl.stream_state WHERE stream_id = 'api/cal' AND kind = 'sync'", [], |r| r.get(0)).expect("state");
    assert!(stored.contains("\"max_id\":104"), "{stored}");

    // --range: windows from the override; max doesn't regress below the stored value.
    let rt = runtime(
        &e,
        PlanRequest {
            streams: vec!["api/cal".into()],
            range: Some("2019-01-01..2019-06-30".into()),
            ..all()
        },
    )
    .await;
    assert_eq!(
        rt.plan.selection.range.as_deref(),
        Some("2019-01-01..2019-06-30")
    );
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    let d1s: Vec<String> = m.targets()[5..]
        .iter()
        .filter_map(|t| param(t, "d1"))
        .collect();
    assert_eq!(d1s, vec!["2019-01-01", "2019-04-01"]);
    assert_eq!(
        staged[0]
            .1
            .state
            .as_ref()
            .map(|r| r.value["max_id"].clone()),
        Some(serde_json::json!(106))
    );

    // history: snapshot_only rejects --range.
    let rt = runtime(
        &e,
        PlanRequest {
            streams: vec!["api/live".into()],
            range: Some("2019-01-01..".into()),
            ..all()
        },
    )
    .await;
    let mut session = rt.engine.session().expect("session");
    let err = stage_all(&rt, &mut session)
        .await
        .expect_err("snapshot_only");
    assert_eq!(err.class, ErrorClass::Config);

    // A malformed --range is a plan error.
    let (p, _) = atlatl_core::config::load(e.project.path());
    // Secrets are redacted process-wide: a one-letter one would mask other tests' errors.
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k-range-secret")]));
    assert!(
        atlatl_core::plan::build(
            &p,
            &PlanRequest {
                range: Some("2019".into()),
                ..all()
            },
            vars,
            Redactor::global()
        )
        .is_err()
    );
}

const ROW_SPEC: &str = r#"
base_url: "{{ env.T_BASE }}"
endpoints:
  feed:
    request: {path: /feed, params: {key: "{{ secrets.api_key }}"}}
    decode: {format: jsonl}
    records: {primary_key: [id]}
"#;

fn row_reps(threshold: &str) -> String {
    format!(
        "replications:\n  r:\n    source: api\n    target: lake\n    streams:\n      - {{id: api/feed, from: {{endpoint: feed}}, object: br.feed, mode: incremental, primary_key: [id], on_decode_error: {{retry: row{threshold}}}}}\n"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_mode_rejects_are_recorded_retried_and_resolved() {
    let fixed = Arc::new(Mutex::new(false));
    let f = fixed.clone();
    let m = Mock::start(move |_, _| {
        let body = if *f.lock().expect("f") {
            "{\"id\": 1}\n{\"id\": 2, \"v\": \"ok\"}\n{\"id\": 3}\n"
        } else {
            "{\"id\": 1}\n{\"id\": 2, \"v\": \n{\"id\": 3}\n"
        };
        (200, "application/x-ndjson", body.to_owned())
    })
    .await;

    // Default threshold: any reject fails the replication with `data`, and nothing is persisted.
    let e = env(&with_base(ROW_SPEC, &m.url), &row_reps(""), "");
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let err = stage_all(&rt, &mut session).await.expect_err("threshold");
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Data, Some("reject_threshold"))
    );
    let report = rt.report.finish();
    let mr = &report.replications[0].streams[0].members[0];
    assert_eq!(mr.rejects.new, 1);
    assert_eq!(mr.rejects.samples.len(), 1, "the report carries samples");
    assert!(
        !serde_json::to_string(&mr.rejects.samples)
            .expect("json")
            .contains("k-rest-run-secret")
    );

    // Under a threshold the good rows stage and the reject is written in the commit.
    let e = env(
        &with_base(ROW_SPEC, &m.url),
        &row_reps(", reject_threshold: {ratio: 0.5, min_count: 1}"),
        "",
    );
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    let s = &staged[0].1;
    assert_eq!(s.rows, 2);
    assert_eq!(s.rejects.len(), 1);
    assert_eq!(s.rejects[0].error_class, "decode");
    assert!(
        !s.rejects[0]
            .request
            .to_string()
            .contains("k-rest-run-secret"),
        "{}",
        s.rejects[0].request
    );
    commit(&rt, &mut session, &staged).await;
    let c = e.lake.open("lake").expect("open");
    let n: i64 = c
        .query_row(
            "SELECT count(*) FROM lake._atlatl.rejects WHERE stream_id = 'api/feed'",
            [],
            |r| r.get(0),
        )
        .expect("q");
    assert_eq!(n, 1);
    drop(c);

    // Still broken, same error: retried, and not a write by itself; a commit bumps its attempts.
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert!(staged[0].1.resolved.is_empty());
    assert!(staged[0].1.rejects.is_empty());
    assert_eq!(staged[0].1.refreshed.len(), 1);
    commit(&rt, &mut session, &staged).await;
    let c = e.lake.open("lake").expect("open");
    let attempts: i32 = c
        .query_row(
            "SELECT attempts FROM lake._atlatl.rejects WHERE stream_id = 'api/feed'",
            [],
            |r| r.get(0),
        )
        .expect("q");
    assert_eq!(attempts, 2);
    drop(c);

    // Fixed upstream: the replayed row lands and the reject is deleted in the same commit.
    *fixed.lock().expect("f") = true;
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    let s = &staged[0].1;
    assert_eq!(s.resolved.len(), 1);
    assert!(s.rejects.is_empty());
    assert_eq!(
        s.rows, 3,
        "the replay's copy of row 2 dedups against the fresh one"
    );
    commit(&rt, &mut session, &staged).await;
    let report = rt.report.finish();
    assert_eq!(
        report.replications[0].streams[0].members[0]
            .rejects
            .resolved,
        1
    );
    let c = e.lake.open("lake").expect("open");
    let n: i64 = c
        .query_row("SELECT count(*) FROM lake._atlatl.rejects", [], |r| {
            r.get(0)
        })
        .expect("q");
    assert_eq!(n, 0);
    let v: String = c
        .query_row("SELECT v FROM lake.br.feed WHERE id = '2'", [], |r| {
            r.get(0)
        })
        .expect("q");
    assert_eq!(v, "ok");
}

const SAFE_REPS: &str = r#"replications:
  r:
    source: api
    target: lake
    streams:
      - id: api/typed
        from: {endpoint: typed}
        object: br.typed
        mode: full-refresh
        infer: safe
        columns: {exclude: [skipme], later: {type: DATE}}
        metadata: {run_id: {}}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn infer_safe_types_from_the_full_staged_set() {
    let m = Mock::start(|_, _| json(r#"[{"i": 1, "d": 1.10, "b": true, "day": "2025-01-01", "zip": "007", "skipme": 1}, {"i": 9007199254740993, "d": 2, "b": false, "day": "2025-01-02", "zip": "010"}]"#.into())).await;
    let spec = format!(
        "base_url: {}\nendpoints:\n  typed:\n    request: {{path: /t}}\n    records: {{path: \"[]\"}}\n",
        m.url
    );
    let e = env(&spec, SAFE_REPS, "");
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    let stg = staged[0].1.staging.clone().expect("stg");
    let cols = rows(
        &mut session,
        &format!("SELECT column_name, column_type FROM (DESCRIBE temp.main.{stg})"),
    )
    .await;
    assert_eq!(
        cols,
        vec![
            r#"{"column_name":"i","column_type":"BIGINT"}"#,
            r#"{"column_name":"d","column_type":"DECIMAL(38,2)"}"#,
            r#"{"column_name":"b","column_type":"BOOLEAN"}"#,
            r#"{"column_name":"day","column_type":"DATE"}"#,
            r#"{"column_name":"zip","column_type":"VARCHAR"}"#,
            r#"{"column_name":"later","column_type":"VARCHAR"}"#,
            r#"{"column_name":"_atlatl_run_id","column_type":"VARCHAR"}"#,
        ]
    );
    let v = rows(
        &mut session,
        &format!("SELECT i, d::VARCHAR AS d FROM temp.main.{stg}"),
    )
    .await;
    assert_eq!(
        v,
        vec![
            r#"{"i":1,"d":"1.10"}"#,
            r#"{"i":9007199254740993,"d":"2.00"}"#
        ]
    );
}

const QUERY_REPS: &str = r#"replications:
  r:
    source: api
    target: lake
    streams:
      - {id: api/profile, from: {endpoint: profile}, object: br.profile, mode: incremental, primary_key: [isin]}
"#;

fn query_spec(url: &str, conn: &str, query: &str) -> String {
    format!(
        "base_url: {url}\nendpoints:\n  profile:\n    for_each:\n      over: {{query: \"{query}\", connection: {conn}, except_landed: [isin]}}\n      as: isin\n      batch: {{size: 2, as: isins}}\n    request: {{path: /p, params: {{ids: \"{{{{ isins | join(',') }}}}\"}}}}\n    records: {{path: \"[]\"}}\n"
    )
}

fn profile_mock() -> impl Fn(&str, usize) -> (u16, &'static str, String) + Send + Sync + 'static {
    |t, _| {
        let ids = param(t, "ids").unwrap_or_default();
        json(format!(
            "[{}]",
            ids.split(',')
                .map(|i| format!(r#"{{"isin": "{i}", "name": "n-{i}"}}"#))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_iteration_over_the_lake_with_an_anti_join_worklist() {
    let m = Mock::start(profile_mock()).await;
    let e = env(
        &query_spec(
            &m.url,
            "lake",
            "SELECT isin FROM lake.au.instrument ORDER BY isin",
        ),
        QUERY_REPS,
        "",
    );
    let c = e.lake.open("lake").expect("open");
    c.execute_batch("CREATE SCHEMA lake.au; CREATE TABLE lake.au.instrument AS SELECT 'US' || lpad(range::VARCHAR, 3, '0') AS isin FROM range(5);").expect("seed");
    drop(c);

    // First run: no target yet, so every item is pending; batches of two.
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert_eq!(staged[0].1.rows, 5);
    let ids: Vec<String> = m.targets().iter().filter_map(|t| param(t, "ids")).collect();
    assert_eq!(ids, vec!["US000,US001", "US002,US003", "US004"]);
    commit(&rt, &mut session, &staged).await;

    // New instruments: only keys not yet landed are requested.
    let c = e.lake.open("lake").expect("open");
    c.execute_batch("INSERT INTO lake.au.instrument VALUES ('US900'), ('US901');")
        .expect("more");
    drop(c);
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert_eq!(staged[0].1.rows, 2);
    let ids: Vec<String> = m.targets()[3..]
        .iter()
        .filter_map(|t| param(t, "ids"))
        .collect();
    assert_eq!(ids, vec!["US900,US901"]);
    commit(&rt, &mut session, &staged).await;

    // Nothing pending: no requests, and an empty staging shaped like the target.
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    let s = &staged[0].1;
    assert_eq!((s.rows, s.empty), (0, false));
    assert_eq!(m.targets().len(), 4);
    let cols = rows(
        &mut session,
        &format!(
            "SELECT column_name FROM (DESCRIBE temp.main.{})",
            s.staging.clone().expect("stg")
        ),
    )
    .await;
    assert_eq!(
        cols,
        vec![r#"{"column_name":"isin"}"#, r#"{"column_name":"name"}"#]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_iteration_over_postgres() {
    let pg = atlatl_testkit::pg16();
    let db = pg.fresh_db().expect("db");
    let dsn = pg.dsn(&db);
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt").block_on(async {
            let (c, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await.expect("connect");
            tokio::spawn(conn);
            c.batch_execute("CREATE TABLE instrument (isin text, listed boolean); INSERT INTO instrument VALUES ('DE001', true), ('DE002', true), ('DE003', false);").await.expect("seed");
        })
    })
    .join()
    .expect("seed thread");
    let m = Mock::start(profile_mock()).await;
    let pg_conn = format!(
        "  appdb:\n    type: postgres\n    host: {}\n    port: {}\n    database: {db}\n    user: {}\n    password: \"{}\"\n    sslmode: disable\n    read_only: true\n",
        pg.host, pg.port, pg.user, pg.password
    );
    let e = env(
        &query_spec(
            &m.url,
            "appdb",
            "SELECT isin FROM instrument WHERE listed ORDER BY isin",
        ),
        QUERY_REPS,
        &pg_conn,
    );
    let c = e.lake.open("lake").expect("open");
    c.execute_batch("CREATE SCHEMA lake.br; CREATE TABLE lake.br.profile AS SELECT 'DE002' AS isin, 'old' AS name;").expect("seed");
    drop(c);
    let rt = runtime(&e, all()).await;
    assert!(
        rt.connections.contains_key("appdb"),
        "query connections are resolved by the plan"
    );
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert_eq!(staged[0].1.rows, 1);
    let ids: Vec<String> = m.targets().iter().filter_map(|t| param(t, "ids")).collect();
    assert_eq!(ids, vec!["DE001"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_iteration_over_a_duckdb_file() {
    let dir = tempfile::tempdir().expect("dir");
    let seed = dir.path().join("seed.duckdb");
    duckdb::Connection::open(&seed)
        .and_then(|c| c.execute_batch("CREATE TABLE instrument (isin VARCHAR, listed BOOLEAN); INSERT INTO instrument VALUES ('FR001', true), ('FR002', false), ('FR003', true);"))
        .expect("seed");
    let m = Mock::start(profile_mock()).await;
    let seed_conn = format!("  seed:\n    type: duckdb\n    path: {}\n", seed.display());
    let e = env(
        &query_spec(
            &m.url,
            "seed",
            "SELECT isin FROM instrument WHERE listed ORDER BY isin",
        ),
        QUERY_REPS,
        &seed_conn,
    );
    let before = std::fs::metadata(&seed)
        .expect("meta")
        .modified()
        .expect("mtime");
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let staged = stage_all(&rt, &mut session).await.expect("stage");
    assert_eq!(staged[0].1.rows, 2);
    let ids: Vec<String> = m.targets().iter().filter_map(|t| param(t, "ids")).collect();
    assert_eq!(ids, vec!["FR001,FR003"]);
    assert_eq!(
        std::fs::metadata(&seed)
            .expect("meta")
            .modified()
            .expect("mtime"),
        before,
        "opened read-only"
    );

    std::fs::remove_file(&seed).expect("rm");
    let rt = runtime(&e, all()).await;
    let mut session = rt.engine.session().expect("session");
    let err = stage_all(&rt, &mut session).await.expect_err("missing");
    assert!(err.to_string().contains("does not exist"), "{err}");
}

// ----------------------------------------------------------------------------- through `atlatl run`

async fn run_all(e: &Env, req: PlanRequest) -> atlatl_core::obs::report::RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k-rest-run-secret")]));
    let args = atlatl_run::RunArgs {
        plan: req,
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    atlatl_run::run(
        Arc::new(p),
        args,
        vars,
        tokio_util::sync::CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run")
}

fn lake_i64(e: &Env, sql: &str) -> i64 {
    e.lake
        .open("lake")
        .expect("open")
        .query_row(sql, [], |r| r.get(0))
        .expect(sql)
}

fn ok(r: &atlatl_core::obs::report::RunReport) {
    for x in &r.replications {
        assert_eq!(
            x.status,
            atlatl_core::obs::report::ReplicationStatus::Succeeded,
            "{}: {:?}",
            x.replication,
            x.error
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_feeds_queues_from_feed_only_producers() {
    let m = Mock::start(|t, _| {
        let path = t.split('?').next().unwrap_or_default();
        match (path, param(t, "page").as_deref()) {
            ("/companies", Some("1")) => json(r#"{"data": [{"id": 1}, {"id": 2}]}"#.into()),
            ("/companies", _) => json(r#"{"data": []}"#.into()),
            _ => json(
                r#"{"items": [{"period": "2025Q1", "eps": 1.10}, {"period": "2025Q2", "eps": 2}]}"#
                    .into(),
            ),
        }
    })
    .await;
    let e = env(&with_base(QUEUE_SPEC, &m.url), QUEUE_REPS, "");
    let r = run_all(
        &e,
        PlanRequest {
            streams: vec!["api/fundamentals".into()],
            ..all()
        },
    )
    .await;
    ok(&r);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.fundamentals"), 4);
    assert_eq!(
        lake_i64(
            &e,
            "SELECT count(DISTINCT company_id) FROM lake.br.fundamentals"
        ),
        2
    );
    assert_eq!(
        lake_i64(
            &e,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'companies'"
        ),
        0,
        "feed-only writes nothing"
    );
    let mr = &r.replications[0]
        .streams
        .iter()
        .find(|s| s.stream_id == "api/fundamentals")
        .expect("s")
        .members[0];
    assert_eq!(mr.rows.inserted, 4);
    assert_eq!(mr.http.as_ref().map(|h| h.requests), Some(2));

    // Both streams selected: the producer lands too, and a rerun of unchanged data changes nothing.
    ok(&run_all(&e, all()).await);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.companies"), 2);
    let r = run_all(&e, all()).await;
    let c = &r.replications[0]
        .streams
        .iter()
        .find(|s| s.stream_id == "api/companies")
        .expect("s")
        .members[0];
    assert_eq!(
        (c.rows.inserted, c.rows.updated, c.rows.unchanged),
        (0, 0, 2)
    );
}

const LANDED_QUEUE_SPEC: &str = r#"
base_url: "{{ env.T_BASE }}"
endpoints:
  instruments:
    request: {path: /instruments}
    records: {path: "@", primary_key: [symbol]}
    emit: {usids: '{"usid": record.usid} if record.usid is not none else none'}
  symbols:
    for_each: {over: {queue: usids, unique: true, except_landed: [usid]}, as: s}
    request: {path: "/symbols/{{ s.usid }}"}
    records: {path: "@", primary_key: [usid]}
"#;

const LANDED_QUEUE_REPS: &str = r#"replications:
  r:
    source: api
    target: lake
    streams:
      - {id: api/instruments, from: {endpoint: instruments}, object: br.instruments, mode: full-refresh, primary_key: [symbol]}
      - {id: api/symbols, from: {endpoint: symbols}, object: br.symbols, mode: incremental, primary_key: [usid]}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_iteration_skips_keys_already_landed() {
    let listed = Arc::new(Mutex::new(r#"[{"symbol": "A", "usid": "u1"}, {"symbol": "B", "usid": "u2"}, {"symbol": "B2", "usid": "u2"}, {"symbol": "C", "usid": null}]"#.to_owned()));
    let body = listed.clone();
    let m = Mock::start(move |t, _| {
        let path = t.split('?').next().unwrap_or_default();
        match path.strip_prefix("/symbols/") {
            Some(id) => json(format!(r#"{{"usid": "{id}", "name": "n-{id}"}}"#)),
            None => json(body.lock().expect("lock").clone()),
        }
    })
    .await;
    let e = env(&with_base(LANDED_QUEUE_SPEC, &m.url), LANDED_QUEUE_REPS, "");
    let symbol_paths = |m: &Mock| -> Vec<String> {
        m.targets()
            .into_iter()
            .filter(|t| t.starts_with("/symbols/"))
            .collect()
    };

    // No target yet: every distinct id is looked up once, and a null id emits nothing.
    ok(&run_all(&e, all()).await);
    assert_eq!(symbol_paths(&m), ["/symbols/u1", "/symbols/u2"]);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.symbols"), 2);

    // Unchanged: nothing pending. A new id: only it is looked up, in the same run that lists it.
    let r = run_all(&e, all()).await;
    assert_eq!(
        r.replications[0].status,
        atlatl_core::obs::report::ReplicationStatus::UpToDate,
        "{:?}",
        r.replications[0].error
    );
    assert_eq!(symbol_paths(&m).len(), 2);
    *listed.lock().expect("lock") =
        r#"[{"symbol": "A", "usid": "u1"}, {"symbol": "D", "usid": "u3"}]"#.into();
    ok(&run_all(&e, all()).await);
    assert_eq!(symbol_paths(&m)[2..], ["/symbols/u3"]);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.symbols"), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_items_commits_in_batches_and_reports_the_rest() {
    let m = Mock::start(|t, _| {
        let path = t.split('?').next().unwrap_or_default();
        match path.strip_prefix("/symbols/") {
            Some(id) => json(format!(r#"{{"usid": "{id}", "name": "n-{id}"}}"#)),
            None => json(
                r#"[{"symbol": "A", "usid": "u1"}, {"symbol": "B", "usid": "u2"}, {"symbol": "C", "usid": "u3"}, {"symbol": "D", "usid": "u4"}]"#
                    .into(),
            ),
        }
    })
    .await;
    let spec = LANDED_QUEUE_SPEC.replace(
        "except_landed: [usid]}, as: s}",
        "except_landed: [usid]}, as: s, max_items: 'vars.batch'}",
    );
    let reps = LANDED_QUEUE_REPS.replace(
        "mode: incremental, primary_key: [usid]}",
        "mode: incremental, primary_key: [usid], vars: {batch: 1}}",
    );
    let e = env(&with_base(&spec, &m.url), &reps, "");
    let looked_up = |m: &Mock| -> Vec<String> {
        m.targets()
            .into_iter()
            .filter(|t| t.starts_with("/symbols/"))
            .collect()
    };
    let member = |r: &atlatl_core::obs::report::RunReport| {
        r.replications[0]
            .streams
            .iter()
            .find(|s| s.stream_id == "api/symbols")
            .expect("symbols")
            .members[0]
            .clone()
    };

    let r = run_all(&e, all()).await;
    ok(&r);
    assert_eq!(looked_up(&m), ["/symbols/u1"]);
    let s = member(&r);
    assert_eq!(s.http.as_ref().map(|h| h.items_deferred), Some(3));
    assert!(
        s.warnings.iter().any(|w| w.code == "items_deferred"),
        "{:?}",
        s.warnings
    );
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.symbols"), 1);

    // `--var` raises the cap for one run; the landed key is skipped, and nothing is deferred.
    let r = run_all(
        &e,
        PlanRequest {
            vars: [("batch".to_owned(), "5".to_owned())].into(),
            ..all()
        },
    )
    .await;
    ok(&r);
    assert_eq!(
        looked_up(&m)[1..],
        ["/symbols/u2", "/symbols/u3", "/symbols/u4"]
    );
    let s = member(&r);
    assert_eq!(s.http.as_ref().map(|h| h.items_deferred), Some(0));
    assert!(s.warnings.iter().all(|w| w.code != "items_deferred"));
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.symbols"), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_skipped_key_sits_out_its_ttl_instead_of_starving_capped_batches() {
    let m = Mock::start(|t, _| {
        let path = t.split('?').next().unwrap_or_default();
        match path.strip_prefix("/symbols/") {
            Some("gone") => (404, "application/json", r#"{"error": "not found"}"#.into()),
            Some(id) => json(format!(r#"{{"usid": "{id}", "name": "n-{id}"}}"#)),
            None => json(
                r#"[{"symbol": "A", "usid": "u1"}, {"symbol": "G", "usid": "gone"}, {"symbol": "B", "usid": "u2"}]"#
                    .into(),
            ),
        }
    })
    .await;
    let spec = LANDED_QUEUE_SPEC
        .replace(
            "except_landed: [usid]}, as: s}",
            "except_landed: [usid]}, as: s, max_items: 1, except_skipped: {ttl: 7d}}",
        )
        .replace(
            "    records: {path: \"@\", primary_key: [usid]}\n",
            "    records: {path: \"@\", primary_key: [usid]}\n    record_skips: true\n    rules:\n      - {when: \"response.status == 404\", action: skip}\n",
        );
    let e = env(&with_base(&spec, &m.url), LANDED_QUEUE_REPS, "");
    let looked_up = |m: &Mock| -> Vec<String> {
        m.targets()
            .into_iter()
            .filter_map(|t| t.strip_prefix("/symbols/").map(str::to_owned))
            .collect()
    };
    let http = |r: &atlatl_core::obs::report::RunReport| {
        r.replications[0]
            .streams
            .iter()
            .find(|s| s.stream_id == "api/symbols")
            .and_then(|s| s.members[0].http.clone())
            .expect("http")
    };

    // One item a run: u1, then the dead key (skipped and recorded), then u2 although the dead
    // key is still pending; then nothing, while the skip sits out its TTL.
    for _ in 0..4 {
        let _ = run_all(&e, all()).await;
    }
    assert_eq!(looked_up(&m), ["u1", "gone", "u2"]);
    let r = run_all(&e, all()).await;
    assert_eq!((http(&r).items_held, http(&r).items_deferred), (1, 0));
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.symbols"), 2);
    assert_eq!(
        lake_i64(
            &e,
            "SELECT count(*) FROM lake._atlatl.rejects WHERE error_class = 'skipped'"
        ),
        1
    );

    // Past the TTL it's retried once, and the retry restarts the TTL.
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch("UPDATE lake._atlatl.rejects SET last_seen_at = now() - INTERVAL 8 DAY")
        .expect("age");
    let _ = run_all(&e, all()).await;
    assert_eq!(looked_up(&m)[3..], ["gone"]);
    let _ = run_all(&e, all()).await;
    assert_eq!(looked_up(&m).len(), 4, "held again after the retry");
}

#[test]
fn full_refresh_over_except_landed_is_refused_before_the_run() {
    let reps = LANDED_QUEUE_REPS.replace(
        "object: br.symbols, mode: incremental",
        "object: br.symbols, mode: full-refresh",
    );
    let e = env(
        &with_base(LANDED_QUEUE_SPEC, "http://127.0.0.1:9"),
        &reps,
        "",
    );
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k")]));
    let (plan, _) = atlatl_core::plan::build(&p, &all(), vars, Redactor::global())
        .unwrap_or_else(|d| panic!("{d:?}"));
    let err = atlatl_run::run::check_rest_specs(&p, &plan).expect_err("refused");
    assert_eq!(err.class, ErrorClass::Config);
    assert!(
        err.message.contains("api/symbols") && err.message.contains("`incremental`"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_records_and_resolves_rejects_in_the_commit() {
    let fixed = Arc::new(Mutex::new(false));
    let f = fixed.clone();
    let m = Mock::start(move |_, _| {
        let body = if *f.lock().expect("f") {
            "{\"id\": 1}\n{\"id\": 2, \"v\": \"ok\"}\n{\"id\": 3}\n"
        } else {
            "{\"id\": 1}\n{\"id\": 2, \"v\": \n{\"id\": 3}\n"
        };
        (200, "application/x-ndjson", body.to_owned())
    })
    .await;
    let e = env(
        &with_base(ROW_SPEC, &m.url),
        &row_reps(", reject_threshold: {ratio: 0.5, min_count: 1}"),
        "",
    );
    let r = run_all(&e, all()).await;
    ok(&r);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.feed"), 2);
    assert_eq!(
        lake_i64(
            &e,
            "SELECT count(*) FROM lake._atlatl.rejects WHERE stream_id = 'api/feed'"
        ),
        1
    );
    assert_eq!(r.replications[0].streams[0].members[0].rejects.new, 1);

    *fixed.lock().expect("f") = true;
    let r = run_all(&e, all()).await;
    ok(&r);
    assert_eq!(r.replications[0].streams[0].members[0].rejects.resolved, 1);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"), 0);
    assert_eq!(
        lake_i64(&e, "SELECT count(*) FROM lake.br.feed WHERE v = 'ok'"),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_persists_sync_state() {
    let m = Mock::start(|t, _| {
        let path = t.split('?').next().unwrap_or_default();
        let d = param(t, "d1").unwrap_or_default();
        match path {
            "/cal" => json(format!(r#"{{"rows": [{{"id": "{d}"}}]}}"#)),
            _ => json(r#"{"rows": [{"x": 1}]}"#.into()),
        }
    })
    .await;
    let e = env(&with_base(SYNC_SPEC, &m.url), SYNC_REPS, "");
    ok(&run_all(
        &e,
        PlanRequest {
            streams: vec!["api/cal".into()],
            ..all()
        },
    )
    .await);
    let n1 = lake_i64(&e, "SELECT count(*) FROM lake.br.cal");
    assert_eq!(n1, 4, "quarterly windows over 2025");
    let floor: String = e.lake.open("lake").expect("o").query_row("SELECT value->>'floor' FROM lake._atlatl.stream_state WHERE stream_id = 'api/cal' AND kind = 'sync'", [], |r| r.get(0)).expect("state");
    assert!(floor.starts_with("2025-10-01"), "{floor}");
    ok(&run_all(
        &e,
        PlanRequest {
            streams: vec!["api/cal".into()],
            ..all()
        },
    )
    .await);
    let second: Vec<String> = m
        .targets()
        .into_iter()
        .filter(|t| t.starts_with("/cal"))
        .skip(4)
        .collect();
    assert!(
        !second.is_empty() && second.iter().all(|t| t.contains("d1=2025-10-01")),
        "second run walks from the stored floor: {second:?}"
    );
}

const SCHEMA_REPS: &str = r#"replications:
  r:
    source: api
    target: lake
    streams:
      - id: api/feed
        from: {endpoint: feed}
        object: br.feed
        mode: full-refresh
        primary_key: [id]
        schema:
          mode: __MODE__
          fields:
            id: {type: integer}
            v: {type: integer}
          on_type_violation: fail
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_enforces_and_observes_source_schemas() {
    let m = Mock::start(|_, _| {
        (
            200,
            "application/x-ndjson",
            "{\"id\": 1, \"v\": 5}\n{\"id\": 2, \"v\": \"five\"}\n".to_owned(),
        )
    })
    .await;
    let e = env(
        &with_base(ROW_SPEC, &m.url),
        &SCHEMA_REPS.replace("__MODE__", "enforce"),
        "",
    );
    let r = run_all(&e, all()).await;
    let rep = &r.replications[0];
    assert_eq!(
        rep.status,
        atlatl_core::obs::report::ReplicationStatus::Failed
    );
    assert_eq!(
        rep.error.as_ref().and_then(|x| x.code.as_deref()),
        Some("schema_violation"),
        "{:?}",
        rep.error
    );
    let sch = rep.streams[0].members[0]
        .schema
        .as_ref()
        .expect("schema section even on failure");
    assert!(sch.violations.iter().any(|v| v.field == "v"), "{sch:?}");
    assert_eq!(
        lake_i64(
            &e,
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'feed'"
        ),
        0
    );

    let e = env(
        &with_base(ROW_SPEC, &m.url),
        &SCHEMA_REPS.replace("__MODE__", "observe"),
        "",
    );
    let r = run_all(&e, all()).await;
    ok(&r);
    assert!(
        r.replications[0].streams[0].members[0]
            .schema
            .as_ref()
            .is_some_and(|s| s.severity.is_some())
    );
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.feed"), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renamed_key_column_is_reported_as_schema_drift() {
    let m = Mock::start(|_, _| {
        (
            200,
            "application/x-ndjson",
            "{\"key\": 1, \"v\": 5}\n{\"key\": 2, \"v\": 6}\n".to_owned(),
        )
    })
    .await;
    let spec = with_base(ROW_SPEC, &m.url);
    let e = env(&spec, &SCHEMA_REPS.replace("__MODE__", "enforce"), "");
    let r = run_all(&e, all()).await;
    let rep = &r.replications[0];
    let err = rep.error.as_ref().expect("error");
    assert_eq!(err.code.as_deref(), Some("schema_violation"), "{err:?}");
    let sch = rep.streams[0].members[0].schema.as_ref().expect("schema");
    assert_eq!(
        (sch.missing_fields.as_slice(), sch.new_fields.as_slice()),
        (&["id".to_owned()][..], &["key".to_owned()][..]),
        "{sch:?}"
    );

    let unchecked = SCHEMA_REPS.split("        schema:").next().expect("reps");
    let e = env(&spec, unchecked, "");
    let r = run_all(&e, all()).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert_eq!(err.code.as_deref(), Some("missing_column"), "{err:?}");
    assert!(r.replications[0].streams[0].members[0].schema.is_none());
}

const LIMITED_SPEC: &str = r#"
base_url: "{{ env.T_BASE }}"
rate_limit: {min_interval: 200ms}
endpoints:
  a:
    request: {path: /a}
    paginate: {type: page, param: page, start: 1}
    records: {path: "data", primary_key: [id]}
  b:
    request: {path: /b}
    paginate: {type: page, param: page, start: 1}
    records: {path: "data", primary_key: [id]}
"#;

const LIMITED_REPS: &str = r#"replications:
  ra:
    source: api
    target: lake
    streams:
      - {id: api/a, from: {endpoint: a}, object: br.a, mode: full-refresh, primary_key: [id]}
  rb:
    source: api
    target: lake
    streams:
      - {id: api/b, from: {endpoint: b}, object: br.b, mode: full-refresh, primary_key: [id]}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_replications_share_one_limiter_per_connection() {
    let m = Mock::start(
        |t, _| match param(t, "page").and_then(|p| p.parse::<u32>().ok()) {
            Some(p) if p <= 4 => json(format!(r#"{{"data": [{{"id": {p}}}]}}"#)),
            _ => json(r#"{"data": []}"#.into()),
        },
    )
    .await;
    let e = env(&with_base(LIMITED_SPEC, &m.url), LIMITED_REPS, "");
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k")]));
    let args = atlatl_run::RunArgs {
        plan: all(),
        concurrency: Some(2),
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    let t = std::time::Instant::now();
    let r = atlatl_run::run(
        Arc::new(p),
        args,
        vars,
        tokio_util::sync::CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run");
    ok(&r);
    assert_eq!(m.targets().len(), 10);
    // 10 request starts at ≥ 200 ms spacing on one limiter; a limiter per unit would take ~0.8 s.
    assert!(
        t.elapsed() >= std::time::Duration::from_millis(1800),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(
        lake_i64(&e, "SELECT count(*) FROM lake.br.a")
            + lake_i64(&e, "SELECT count(*) FROM lake.br.b"),
        8
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_replication_keeps_its_retry_on_a_shared_connection() {
    let hits = Arc::new(Mutex::new(std::collections::HashMap::<String, u32>::new()));
    let h = hits.clone();
    let m = Mock::start(move |t, _| {
        let path = t.split('?').next().unwrap_or_default().to_owned();
        let n = {
            let mut g = h.lock().expect("h");
            let e = g.entry(path).or_default();
            *e += 1;
            *e
        };
        match (n, param(t, "page").as_deref()) {
            (1, _) => (503, "text/plain", "busy".into()),
            (_, Some("1")) => json(r#"{"data": [{"id": 1}]}"#.into()),
            _ => json(r#"{"data": []}"#.into()),
        }
    })
    .await;
    let lake = LocalLake::file().expect("lake");
    // No `retry` on the connection: each replication's own policy applies.
    let conns = format!(
        "{}  api: {{type: rest, spec: apis/t.yaml, secrets: {{api_key: {{secret: env:T_API_KEY}}}}}}\n",
        lake.connection_yaml("lake", "lake")
    );
    let reps = r#"replications:
  ra:
    source: api
    target: lake
    retry: {max_attempts: 1}
    streams:
      - {id: api/a, from: {endpoint: a}, object: br.a, mode: full-refresh, primary_key: [id]}
  rb:
    source: api
    target: lake
    retry: {max_attempts: 3, backoff: constant, base: 5ms}
    streams:
      - {id: api/b, from: {endpoint: b}, object: br.b, mode: full-refresh, primary_key: [id]}
"#;
    // A rule without `max_attempts` defers to the run's retry policy (built-in 5xx rules carry their own).
    let spec = with_base(LIMITED_SPEC, &m.url).replace(
        "rate_limit: {min_interval: 200ms}\n",
        "rules:\n  - {when: 'response.status == 503', action: retry, class: transient}\n",
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.file("apis/t.yaml", &spec))
        .and_then(|p| p.replication("r", reps))
        .expect("project");
    let e = Env { lake, project };
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k")]));
    let args = atlatl_run::RunArgs {
        plan: all(),
        concurrency: Some(1),
        extension_dir: Some(atlatl_testkit::extension_dir()),
        ..Default::default()
    };
    let r = atlatl_run::run(
        Arc::new(p),
        args,
        vars,
        tokio_util::sync::CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run");
    let status = |n: &str| {
        r.replications
            .iter()
            .find(|x| x.replication == n)
            .map(|x| x.status)
            .expect("rep")
    };
    use atlatl_core::obs::report::ReplicationStatus::{Failed, Succeeded};
    assert_eq!(
        (status("ra"), status("rb")),
        (Failed, Succeeded),
        "{:?}",
        r.replications.iter().map(|x| &x.error).collect::<Vec<_>>()
    );
    assert_eq!(
        hits.lock().expect("h").get("/a").copied(),
        Some(1),
        "ra: one attempt"
    );
}

// ----------------------------------------------------------------------------- still-failing rejects

/// A feed whose body the test swaps between runs, under a threshold that lets one reject in three
/// through.
async fn reject_feed(body: &str) -> (Env, Arc<Mutex<String>>, Mock) {
    reject_feed_with(body, "incremental", "", "").await
}

/// [`reject_feed`] with the stream's mode, extra `on_decode_error` keys and extra replications.
async fn reject_feed_with(
    body: &str,
    mode: &str,
    decode: &str,
    more: &str,
) -> (Env, Arc<Mutex<String>>, Mock) {
    let b = Arc::new(Mutex::new(body.to_owned()));
    let b2 = b.clone();
    let m = Mock::start(move |_, _| {
        (
            200,
            "application/x-ndjson",
            b2.lock().expect("body").clone(),
        )
    })
    .await;
    let reps = format!(
        "replications:\n  r:\n    source: api\n    target: lake\n    streams:\n      - {{id: api/feed, from: {{endpoint: feed}}, object: br.feed, mode: {mode}, primary_key: [id], on_decode_error: {{retry: row, reject_threshold: {{ratio: 0.5, min_count: 1}}{decode}}}}}\n{more}"
    );
    let e = env(&with_base(ROW_SPEC, &m.url), &reps, "");
    (e, b, m)
}

const BROKEN: &str = "{\"id\": 1}\n{\"id\": 2, \"v\": \n{\"id\": 3}\n";

/// `(attempts, error, last_seen_run)` of the one open reject.
fn reject_row(e: &Env) -> (i32, String, String) {
    e.lake
        .open("lake")
        .expect("open")
        .query_row(
            "SELECT attempts, error, last_seen_run FROM lake._atlatl.rejects WHERE stream_id = 'api/feed'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .expect("reject")
}

/// Snapshots whose changes touched `schema.table` (by name or table id).
fn touched(e: &Env, schema: &str, table: &str) -> usize {
    let c = e.lake.open("lake").expect("open");
    let id: i64 = c
        .query_row(
            "SELECT i.table_id FROM ducklake_table_info('lake') i JOIN __ducklake_metadata_lake.ducklake_schema s USING (schema_id) WHERE s.end_snapshot IS NULL AND s.schema_name = ? AND i.table_name = ?",
            [schema, table],
            |r| r.get(0),
        )
        .expect("table id");
    let mut s = c
        .prepare("SELECT changes::VARCHAR FROM ducklake_snapshots('lake')")
        .expect("snapshots");
    s.query_map([], |r| r.get::<_, String>(0))
        .expect("snapshots")
        .map(|r| r.expect("row"))
        .filter(|ch| {
            ch.contains(&format!("{schema}.{table}"))
                || ch
                    .split(['[', ']', ',', ' ', '{', '}', '='])
                    .any(|t| t.trim() == id.to_string())
        })
        .count()
}

fn feed_member(r: &atlatl_core::obs::report::RunReport) -> &atlatl_core::obs::report::MemberReport {
    &r.replications[0].streams[0].members[0]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_rejects_alone_commit_nothing() {
    use atlatl_core::obs::report::{ReplicationStatus, StreamStatus};
    let (e, body, _m) = reject_feed(BROKEN).await;
    let first = run_all(&e, all()).await;
    ok(&first);
    let (attempts, error, seen) = reject_row(&e);
    assert_eq!((attempts, seen.as_str()), (1, first.run_id.as_str()));
    let (data, rejects) = (touched(&e, "br", "feed"), touched(&e, "_atlatl", "rejects"));

    // Same rows, same failure: nothing to commit, and the reject's counters stay put.
    let again = run_all(&e, all()).await;
    let rep = &again.replications[0];
    assert!(rep.error.is_none(), "{:?}", rep.error);
    assert_eq!(rep.status, ReplicationStatus::UpToDate);
    assert_eq!(rep.streams[0].status, StreamStatus::UpToDate);
    let m = feed_member(&again);
    assert_eq!(m.lake_snapshot, None);
    assert_eq!(
        (
            m.rows.rejected,
            m.rejects.new,
            m.rejects.resolved,
            m.rejects.open
        ),
        (1, 0, 0, 1),
        "the open count is still reported"
    );
    assert_eq!(reject_row(&e), (1, error.clone(), first.run_id.clone()));
    assert_eq!(
        (touched(&e, "br", "feed"), touched(&e, "_atlatl", "rejects")),
        (data, rejects),
        "no snapshot touched the target or the rejects"
    );

    // A real change commits, and the still-failing reject is refreshed in that commit.
    *body.lock().expect("body") = BROKEN.replace("{\"id\": 1}", "{\"id\": 1, \"v\": \"new\"}");
    let changed = run_all(&e, all()).await;
    ok(&changed);
    let m = feed_member(&changed);
    assert!(m.lake_snapshot.is_some());
    assert_eq!((m.rows.updated, m.rejects.open), (1, 1));
    assert_eq!(reject_row(&e), (2, error, changed.run_id.clone()));
    assert_eq!(touched(&e, "_atlatl", "rejects"), rejects + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_and_resolved_rejects_commit() {
    let (e, body, _m) = reject_feed(BROKEN).await;
    ok(&run_all(&e, all()).await);
    let (_, error, _) = reject_row(&e);

    // Same rows, a different failure on the same record: the new error is a write.
    *body.lock().expect("body") = BROKEN.replace("\"v\": \n", "\"v\": tru}\n");
    let r = run_all(&e, all()).await;
    ok(&r);
    let m = feed_member(&r);
    assert!(m.lake_snapshot.is_some(), "{m:?}");
    assert_eq!((m.rows.written(), m.rejects.new, m.rejects.open), (0, 0, 1));
    let (attempts, changed, seen) = reject_row(&e);
    assert_ne!(changed, error);
    assert_eq!((attempts, seen), (2, r.run_id.clone()));

    // Fixed upstream: the row lands and the reject is deleted in the same commit.
    *body.lock().expect("body") = BROKEN.replace("\"v\": \n", "\"v\": \"ok\"}\n");
    let rejects = touched(&e, "_atlatl", "rejects");
    let r = run_all(&e, all()).await;
    ok(&r);
    let m = feed_member(&r);
    assert!(m.lake_snapshot.is_some());
    assert_eq!((m.rejects.resolved, m.rejects.open), (1, 0));
    assert_eq!(touched(&e, "_atlatl", "rejects"), rejects + 1);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"), 0);
    assert_eq!(
        lake_i64(&e, "SELECT count(*) FROM lake.br.feed WHERE v = 'ok'"),
        1
    );
}

// ----------------------------------------------------------------------------- reject_ttl

async fn run_args(e: &Env, args: atlatl_run::RunArgs) -> atlatl_core::obs::report::RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k-rest-run-secret")]));
    atlatl_run::run(
        Arc::new(p),
        atlatl_run::RunArgs {
            extension_dir: Some(atlatl_testkit::extension_dir()),
            ..args
        },
        vars,
        tokio_util::sync::CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run")
}

fn age_rejects(e: &Env, days: u32) {
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch(&format!(
            "UPDATE lake._atlatl.rejects SET first_seen_at = now() - INTERVAL {days} DAY"
        ))
        .expect("age");
}

const WITHOUT_2: &str = "{\"id\": 1}\n{\"id\": 3}\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_ttl_expires_old_rejects_in_the_commit() {
    use atlatl_core::obs::report::ReplicationStatus;
    let (e, body, _m) = reject_feed_with(BROKEN, "incremental", ", reject_ttl: 1d", "").await;
    ok(&run_all(&e, all()).await);

    // Younger than the ttl: kept, retried, and (failing the same way) nothing to commit.
    let r = run_all(&e, all()).await;
    assert_eq!(r.replications[0].status, ReplicationStatus::UpToDate);
    assert_eq!(
        (
            feed_member(&r).rejects.expired,
            feed_member(&r).rejects.open
        ),
        (0, 1)
    );
    assert_eq!(reject_row(&e).0, 1);

    // Past it, and the row is gone upstream: the expiry alone commits.
    age_rejects(&e, 2);
    *body.lock().expect("body") = WITHOUT_2.into();
    let rejects = touched(&e, "_atlatl", "rejects");
    let dry = run_args(
        &e,
        atlatl_run::RunArgs {
            plan: all(),
            dry_run: Some(atlatl_run::dry::DryRun::default()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(dry.replications[0].status, ReplicationStatus::Succeeded);
    let m = feed_member(&dry);
    assert_eq!((m.rejects.expired, m.rejects.open), (1, 0));
    assert_eq!(
        lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"),
        1,
        "a dry run deletes nothing"
    );
    assert_eq!(touched(&e, "_atlatl", "rejects"), rejects);

    let r = run_all(&e, all()).await;
    ok(&r);
    let m = feed_member(&r);
    assert!(m.lake_snapshot.is_some());
    assert_eq!(m.rows.written(), 0, "{:?}", m.rows);
    assert_eq!((m.rejects.expired, m.rejects.open), (1, 0));
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"), 0);
    assert_eq!(touched(&e, "_atlatl", "rejects"), rejects + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_ttl_restarts_a_reject_the_extract_hits_again() {
    // The ttl on the endpoint this time.
    let m = Mock::start(|_, _| (200, "application/x-ndjson", BROKEN.to_owned())).await;
    let spec = with_base(ROW_SPEC, &m.url).replace(
        "records: {primary_key: [id]}",
        "records: {primary_key: [id]}\n    on_decode_error: {retry: row, reject_ttl: 1d}",
    );
    let e = env(
        &spec,
        &row_reps(", reject_threshold: {ratio: 0.5, min_count: 1}"),
        "",
    );
    ok(&run_all(&e, all()).await);
    age_rejects(&e, 2);
    let r = run_all(&e, all()).await;
    ok(&r);
    let m = feed_member(&r);
    assert!(m.lake_snapshot.is_some());
    assert_eq!(
        (m.rejects.expired, m.rejects.new, m.rejects.open),
        (1, 1, 1)
    );
    let (attempts, _, seen) = reject_row(&e);
    assert_eq!((attempts, seen), (1, r.run_id.clone()), "recorded afresh");
    assert_eq!(
        lake_i64(
            &e,
            "SELECT count(*) FROM lake._atlatl.rejects WHERE first_seen_at > now() - INTERVAL 1 HOUR"
        ),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_reject_ttl_nothing_expires() {
    use atlatl_core::obs::report::ReplicationStatus;
    let (e, _body, _m) = reject_feed(BROKEN).await;
    ok(&run_all(&e, all()).await);
    age_rejects(&e, 3650);
    let r = run_all(&e, all()).await;
    assert_eq!(r.replications[0].status, ReplicationStatus::UpToDate);
    assert_eq!(
        (
            feed_member(&r).rejects.expired,
            feed_member(&r).rejects.open
        ),
        (0, 1)
    );
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"), 1);
}

// ----------------------------------------------------------------------------- rejects retry

async fn retry(
    e: &Env,
    streams: &[&str],
) -> atlatl_core::error::Result<atlatl_core::obs::report::RunReport> {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let streams: Vec<String> = streams.iter().map(|s| (*s).to_owned()).collect();
    let plan = atlatl_run::rejects_cmd::retry_request(&p, None, &streams, None)?;
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([("T_API_KEY", "k-rest-run-secret")]));
    atlatl_run::run(
        Arc::new(p),
        atlatl_run::RunArgs {
            plan,
            retry_rejects: true,
            extension_dir: Some(atlatl_testkit::extension_dir()),
            ..Default::default()
        },
        vars,
        tokio_util::sync::CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
}

const OTHER_REP: &str = "  lakeside:\n    source: lake\n    target: lake\n    streams:\n      - {id: l/x, mode: full-refresh, from: {query: \"SELECT 1 AS a\"}, object: br.x}\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_lands_only_the_replayed_rows() {
    use atlatl_core::obs::report::ReplicationStatus;
    let (e, body, m) = reject_feed_with(BROKEN, "full-refresh", "", OTHER_REP).await;
    ok(&run_all(
        &e,
        PlanRequest {
            streams: vec!["api/feed".into()],
            ..all()
        },
    )
    .await);
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.feed"), 2);
    let runs_before = lake_i64(&e, "SELECT count(*) FROM lake._atlatl.runs");

    // Fixed upstream (and row 1 changed): the retry lands row 2 only, merged by key; the
    // full-refresh target keeps its other rows.
    *body.lock().expect("body") =
        "{\"id\": 1, \"v\": \"changed\"}\n{\"id\": 2, \"v\": \"ok\"}\n{\"id\": 3}\n".into();
    let before = m.targets().len();
    let r = retry(&e, &["api/feed"]).await.expect("retry");
    ok(&r);
    assert_eq!(
        m.targets().len(),
        before + 1,
        "one replayed request, no extract"
    );
    let fm = feed_member(&r);
    assert_eq!(
        (fm.rejects.resolved, fm.rejects.open, fm.rows.inserted),
        (1, 0, 1)
    );
    assert!(fm.lake_snapshot.is_some());
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake.br.feed"), 3);
    assert_eq!(
        lake_i64(&e, "SELECT count(*) FROM lake.br.feed WHERE v = 'changed'"),
        0,
        "rows outside the rejects aren't re-read"
    );
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"), 0);
    assert_eq!(
        lake_i64(
            &e,
            &format!(
                "SELECT count(*) FROM lake._atlatl.runs WHERE kind = 'rejects retry' AND run_id = '{}'",
                r.run_id
            )
        ),
        1
    );
    assert_eq!(
        lake_i64(&e, "SELECT count(*) FROM lake._atlatl.runs"),
        runs_before + 1
    );
    let v = atlatl_run::rejects_cmd::render_retry(&r);
    assert_eq!(v["replications"][0]["streams"][0]["resolved"], 1);

    // Nothing open: no request, nothing committed.
    let before = m.targets().len();
    let r = retry(&e, &["api/feed"]).await.expect("retry");
    assert_eq!(r.replications[0].status, ReplicationStatus::UpToDate);
    assert_eq!(m.targets().len(), before);

    let err = retry(&e, &["l/x"]).await.expect_err("not rest");
    assert_eq!(
        err.code.as_deref(),
        Some("rejects_retry_unsupported"),
        "{err}"
    );
    let err = retry(&e, &["nope/x"]).await.expect_err("unknown");
    assert_eq!(err.class, ErrorClass::Config);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_keeps_still_failing_rejects_open() {
    let (e, _body, _m) = reject_feed(BROKEN).await;
    ok(&run_all(&e, all()).await);
    // `reject_threshold` ratio 0.5: one failing replay of one would trip it in a run.
    let r = retry(&e, &["api/feed"]).await.expect("retry");
    let rep = &r.replications[0];
    assert!(rep.error.is_none(), "{:?}", rep.error);
    let fm = feed_member(&r);
    assert_eq!(
        (fm.rejects.resolved, fm.rows.rejected, fm.rejects.open),
        (0, 1, 1)
    );
    assert_eq!(lake_i64(&e, "SELECT count(*) FROM lake._atlatl.rejects"), 1);
}
