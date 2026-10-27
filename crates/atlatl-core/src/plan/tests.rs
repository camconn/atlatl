use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::config::load;
use crate::template::MapEnv;

const PROJECT: &str = r#"version: 1
environments:
  names: [dev, prod]
  default: dev
  base: prod
  dev:
    allow_base: [record_queue]
state: {lake: lake, locks: lake.catalog}
defaults:
  retry: {max_attempts: 5, base: 2s}
connections:
  lake:
    type: ducklake
    alias: catalog
    catalog: {type: postgres, host: prod-meta, database: ducklake_meta, user: dagster, password: {secret: env:PG_PW}, sslmode: require}
    data_path: s3://prod-bucket/lake/
    storage: {type: s3, provider: credential_chain, region: us-east-1}
    environments:
      dev:
        catalog: {host: localhost, port: 20947, password: {secret: env:DEV_PG_PW}, sslmode: disable}
        data_path: "{{ env.DUCKLAKE_DATA_PATH }}"
        storage: {type: local}
  app_pg:
    type: postgres
    host: prod-app
    database: postgres
    user: dagster
    password: {secret: env:PG_PW}
    environments:
      dev: {host: localhost, password: {secret: env:DEV_PG_PW}}
  appdb_pg:
    type: postgres
    host: prod-appdb
    database: appdb
    user: dagster
    read_only: true
  record_queue:
    type: rest
    spec: apis/record_queue.yaml
    secrets: {api_key: {secret: env:RECORD_QUEUE_API_KEY}}
  seed:
    type: duckdb
    path: seed.duckdb
  ch:
    type: clickhouse
    url: https://ch.example:8443
    user: reader
    password: {secret: env:CH_PW}
    read_only: true
"#;

const API: &str = r#"name: record_queue
endpoints:
  instruments:
    emit: {symbols: record.symbol}
  ratings:
    for_each: {over: {queue: symbols}, as: symbol}
"#;

const WAREHOUSE: &str = r#"replications:
  warehouse_hourly_est_consensus:
    source: lake
    target: lake
    asset: {group: warehouse_hourly}
    lock_group: warehouse
    defaults:
      mode: incremental
      object: "br.{{ member.table }}"
      window: "cons_start >= current_date - 45"
    streams:
      - id: warehouse_hourly/est_v2/est_core_consf
        asset: {key: br/warehouse_hourly/est_v2/est_core_consf}
        from: {tables: "est_v2.est_core_cons_*f"}
        primary_key: [sym_id, item_code, period_end, cons_start]
      - id: warehouse_hourly/est_v2/est_core_cons_rec
        asset: {key: br/warehouse_hourly/est_v2/est_core_cons_rec}
        from: {table: est_v2.est_core_cons_rec}
        primary_key: [sym_id, item_code, cons_start]
"#;

const RECORD_QUEUE: &str = r#"replications:
  record_queue:
    source: record_queue
    target: lake
    asset: {group: record_queue}
    after: [warehouse_hourly_est_consensus]
    defaults:
      mode: full-refresh
      object: "br.{{ stream.vars.table }}"
    streams:
      - id: record_queue/instruments
        from: {endpoint: instruments}
        asset: {key: br/record_queue/instruments}
        vars: {table: instruments}
        tags: [reference]
      - id: record_queue/ratings
        from: {endpoint: ratings}
        asset: {key: br/record_queue/ratings}
        vars: {table: ratings}
"#;

const LOADS: &str = r#"replications:
  org_event:
    source: lake
    target: app_pg
    asset: {group: org_event}
    defaults:
      mode: cdc
      deletes: {mode: soft, column: deleted_at}
    streams:
      - id: org_event/rating
        after: [org_event/hub]
        from: {table: au.org_event_rating}
        object: app.org_event_rating
        primary_key: [event_id]
        asset: {key: org_event/load/app/org_event_rating, deps: [org_event_rating]}
      - id: org_event/hub
        from: {table: au.org_event}
        object: app.org_event
        primary_key: [object_id]
        columns:
          include: [object_id, type]
          type: {target_type: app.event_type}
        asset: {key: org_event/load/app/org_event, deps: [org_event]}
"#;

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new(extra: &[(&str, &str)]) -> Self {
        let dir = tempfile::tempdir().expect("tmp");
        let mut files = vec![
            ("atlatl.yaml", PROJECT),
            ("apis/record_queue.yaml", API),
            ("replications/est_hourly.yaml", WAREHOUSE),
            ("replications/record_queue.yaml", RECORD_QUEUE),
            ("replications/loads.yaml", LOADS),
        ];
        for (p, t) in extra {
            if let Some(slot) = files.iter_mut().find(|(fp, _)| fp == p) {
                slot.1 = t;
            } else {
                files.push((p, t));
            }
        }
        for (path, text) in files {
            let p = dir.path().join(path);
            std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
            std::fs::write(p, text).expect("write");
        }
        Self { dir }
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }

    fn plan(&self, req: &PlanRequest) -> Result<(Plan, Diagnostics), Diagnostics> {
        let (p, d) = load::load(self.root());
        if d.has_errors() {
            return Err(d);
        }
        let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([
            ("PG_PW", "prod-pw"),
            ("DEV_PG_PW", "dev-pw-secret"),
            ("DUCKLAKE_DATA_PATH", "/tmp/lake/"),
            ("RECORD_QUEUE_API_KEY", "rq-key-secret"),
            ("CH_PW", "ch-pw-secret"),
        ]));
        build(&p, req, vars, &Redactor::new())
    }

    fn errors(&self, req: &PlanRequest) -> Vec<String> {
        match self.plan(req) {
            Ok((p, _)) => panic!(
                "expected errors, got plan with {} replications",
                p.replications.len()
            ),
            Err(d) => d
                .errors()
                .map(|e| format!("[{}] {}", e.code, e.error))
                .collect(),
        }
    }
}

fn static_all() -> PlanRequest {
    PlanRequest {
        static_plan: true,
        ..PlanRequest::all()
    }
}

#[test]
fn static_plan_lists_assets_ops_and_groups() {
    let f = Fixture::new(&[]);
    let (plan, _) = f.plan(&static_all()).expect("plan");
    assert!(
        plan.static_plan
            && plan.env.is_none()
            && plan.run_id.is_none()
            && plan.connections.is_empty()
    );
    let assets: Vec<(&str, Option<&str>, &str)> = plan
        .replications
        .iter()
        .flat_map(|r| {
            r.streams.iter().map(move |s| {
                (
                    s.asset.key.as_str(),
                    s.asset.group.as_deref(),
                    r.op.as_str(),
                )
            })
        })
        .collect();
    assert_eq!(
        assets,
        [
            (
                "br/warehouse_hourly/est_v2/est_core_consf",
                Some("warehouse_hourly"),
                "est_hourly"
            ),
            (
                "br/warehouse_hourly/est_v2/est_core_cons_rec",
                Some("warehouse_hourly"),
                "est_hourly"
            ),
            (
                "org_event/load/app/org_event_rating",
                Some("org_event"),
                "loads"
            ),
            ("org_event/load/app/org_event", Some("org_event"), "loads"),
            (
                "br/record_queue/instruments",
                Some("record_queue"),
                "record_queue"
            ),
            (
                "br/record_queue/ratings",
                Some("record_queue"),
                "record_queue"
            ),
        ]
    );
    let hub = &plan.replications[1].streams[1];
    assert_eq!(
        hub.asset.key_path,
        ["org_event", "load", "app", "org_event"]
    );
    assert_eq!(hub.asset.kinds, ["atlatl", "postgres"]);
    assert_eq!(hub.asset.deps, ["org_event"]);
    let wild = &plan.replications[0].streams[0];
    assert!(wild.members[0].pattern && wild.members[0].object.is_none());
    assert!(plan.warnings.iter().any(|w| w.code == "unexpanded"));
    assert_eq!(
        plan.replications[0].lock_group.as_deref(),
        Some("warehouse")
    );
    assert_eq!(
        plan.replications[2].after,
        ["warehouse_hourly_est_consensus"]
    );
}

#[test]
fn static_plan_is_deterministic() {
    let f = Fixture::new(&[]);
    let a = f.plan(&static_all()).expect("a").0.plan_hash;
    let b = f.plan(&static_all()).expect("b").0.plan_hash;
    assert_eq!(a, b);
}

#[test]
fn feed_only_producers_are_added() {
    let f = Fixture::new(&[]);
    let req = PlanRequest {
        streams: vec!["record_queue/ratings".into()],
        ..static_all()
    };
    let (plan, _) = f.plan(&req).expect("plan");
    assert_eq!(plan.replications.len(), 1);
    let r = &plan.replications[0];
    let roles: Vec<_> = r.streams.iter().map(|s| (s.id.as_str(), s.role)).collect();
    assert_eq!(
        roles,
        [
            ("record_queue/instruments", StreamRole::FeedOnly),
            ("record_queue/ratings", StreamRole::Selected)
        ]
    );
    let ids: Vec<&str> = r.steps.iter().map(|s| s.id.as_str()).collect();
    assert!(
        !ids.contains(&"apply:record_queue/instruments"),
        "feed-only writes nothing: {ids:?}"
    );
    assert!(!ids.contains(&"stage:record_queue/instruments:instruments"));
    let consumer = r
        .steps
        .iter()
        .find(|s| s.id == "extract:record_queue/ratings:ratings")
        .expect("extract");
    assert_eq!(
        consumer.depends_on,
        ["extract:record_queue/instruments:instruments"]
    );
    assert_eq!(
        r.steps.last().map(|s| s.depends_on.clone()),
        Some(vec!["apply:record_queue/ratings".to_owned()])
    );
    assert!(r.after.is_empty(), "unselected `after` targets are ignored");
}

#[test]
fn no_deps_fails_on_missing_producer() {
    let f = Fixture::new(&[]);
    let req = PlanRequest {
        streams: vec!["record_queue/ratings".into()],
        with_deps: false,
        ..static_all()
    };
    let errs = f.errors(&req);
    assert!(
        errs.iter()
            .any(|e| e.contains("[queue-producer]") && e.contains("record_queue/instruments")),
        "{errs:?}"
    );
}

#[test]
fn selection_by_replication_and_tag() {
    let f = Fixture::new(&[]);
    let (plan, _) = f
        .plan(&PlanRequest {
            tags: vec!["reference".into()],
            ..static_all()
        })
        .expect("tag");
    let ids: Vec<&str> = plan
        .replications
        .iter()
        .flat_map(|r| r.streams.iter().map(|s| s.id.as_str()))
        .collect();
    assert_eq!(ids, ["record_queue/instruments"]);
    let (plan, _) = f
        .plan(&PlanRequest {
            replications: vec!["org_event".into()],
            ..static_all()
        })
        .expect("rep");
    assert_eq!(plan.replications.len(), 1);
    assert_eq!(
        plan.ops.iter().map(|o| o.name.as_str()).collect::<Vec<_>>(),
        ["loads"]
    );
    let errs = f.errors(&PlanRequest {
        replications: vec!["nope".into()],
        ..static_all()
    });
    assert!(
        errs.iter().any(|e| e.contains("no such replication")),
        "{errs:?}"
    );
    let errs = f.errors(&PlanRequest {
        streams: vec!["a/b".into()],
        ..static_all()
    });
    assert!(
        errs.iter().any(|e| e.contains("no such stream")),
        "{errs:?}"
    );
    let errs = f.errors(&PlanRequest {
        tags: vec!["nothing".into()],
        ..static_all()
    });
    assert!(
        errs.iter()
            .any(|e| e.contains("matched no enabled streams")),
        "{errs:?}"
    );
    let errs = f.errors(&PlanRequest {
        replications: vec!["org_event".into()],
        streams: vec!["record_queue/instruments".into()],
        ..static_all()
    });
    assert!(errs.iter().any(|e| e.contains("not selected")), "{errs:?}");
}

#[test]
fn cdc_applies_parent_first_in_one_commit() {
    let f = Fixture::new(&[]);
    let (plan, _) = f
        .plan(&PlanRequest {
            replications: vec!["org_event".into()],
            ..static_all()
        })
        .expect("plan");
    let steps = &plan.replications[0].steps;
    let applies: Vec<&str> = steps
        .iter()
        .filter(|s| s.kind == StepKind::Apply)
        .map(|s| s.id.as_str())
        .collect();
    assert_eq!(applies, ["apply:org_event/hub", "apply:org_event/rating"]);
    let shard = steps
        .iter()
        .find(|s| s.id == "apply:org_event/rating")
        .expect("shard");
    assert!(shard.depends_on.contains(&"apply:org_event/hub".to_owned()));
    assert!(
        shard
            .depends_on
            .iter()
            .any(|d| d.starts_with("validate:org_event/hub")),
        "applies wait for every validate"
    );
    assert_eq!(
        steps.iter().filter(|s| s.kind == StepKind::Commit).count(),
        1
    );
}

#[test]
fn live_plan_renders_objects_and_redacts() {
    let f = Fixture::new(&[]);
    let (plan, _) = f
        .plan(&PlanRequest {
            env: Some("dev".into()),
            ..PlanRequest::all()
        })
        .expect("plan");
    assert_eq!(
        (plan.env.as_deref(), plan.env_source.as_deref()),
        (Some("dev"), Some("flag"))
    );
    assert!(plan.run_id.as_ref().is_some_and(|r| r.len() == 26));
    let find = |id: &str| {
        plan.replications
            .iter()
            .flat_map(|r| &r.streams)
            .find(|s| s.id == id)
            .expect("stream")
    };
    assert_eq!(
        find("warehouse_hourly/est_v2/est_core_cons_rec").members[0]
            .object
            .as_deref(),
        Some("br.est_core_cons_rec")
    );
    assert_eq!(
        find("record_queue/instruments").members[0]
            .object
            .as_deref(),
        Some("br.instruments")
    );
    let json = serde_json::to_string(&plan).expect("json");
    for secret in ["dev-pw-secret", "rq-key-secret"] {
        assert!(!json.contains(secret), "{secret} leaked");
    }
    let lake = plan
        .connections
        .iter()
        .find(|c| c["name"] == "lake")
        .expect("lake");
    assert_eq!(lake["data_path"], "/tmp/lake/");
    assert_eq!(lake["resolved_from"], "environments.dev");
}

#[test]
fn live_plan_enforces_environment_base_rule() {
    let f = Fixture::new(&[(
        "atlatl.yaml",
        &PROJECT.replace("  appdb_pg:\n", "  scratch_pg:\n    type: postgres\n    host: prod-scratch\n    database: x\n    user: u\n  appdb_pg:\n"),
    )]);
    let errs = f.errors(&PlanRequest {
        env: Some("dev".into()),
        ..PlanRequest::all()
    });
    assert!(
        errs.iter()
            .any(|e| e.contains("[environment-base]") && e.contains("scratch_pg")),
        "{errs:?}"
    );
}

fn with_rep(yaml: &str) -> Fixture {
    Fixture::new(&[("replications/extra.yaml", yaml)])
}

#[test]
fn static_checks() {
    let cases: &[(&str, &str)] = &[
        (
            "replications:\n  x: {source: lake, target: appdb_pg, streams: [{id: x/a, mode: cdc, from: {table: g.t}, object: o, primary_key: [id]}]}\n",
            "read_only",
        ),
        (
            "replications:\n  x: {source: lake, target: app_pg, streams: [{id: x/a, mode: cdc, from: {table: g.t}, object: o}]}\n",
            "needs `primary_key`",
        ),
        (
            "replications:\n  x: {source: lake, target: app_pg, streams: [{id: x/a, mode: full-refresh, from: {table: g.t}, object: o}]}\n",
            "writes into the lake",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: full-refresh, from: {table: g.t}, object: o, update_key: u}]}\n",
            "`update_key` only applies",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {endpoint: e}, object: o}]}\n",
            "does not fit a ducklake source",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a, query: b}, object: o}]}\n",
            "exactly one of",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}}]}\n",
            "has no `object`",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, from: {table: a}, object: o}]}\n",
            "has no `mode`",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: snapshot, from: {table: a}, object: o}]}\n",
            "metadata.loaded_at",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}, object: \"{{ member.tabel }}\"}]}\n",
            "`member.tabel` is not defined",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}, object: \"{{ vars.nope }}\"}]}\n",
            "`vars.nope` is not defined",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}, object: o, asset: {key: br/record_queue/instruments}}]}\n",
            "used by more than one stream",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}, object: o, after: [x/zz]}]}\n",
            "not a stream in replication",
        ),
        (
            "replications:\n  x: {source: lake, target: nope, streams: [{id: x/a, mode: incremental, from: {table: a}, object: o}]}\n",
            "not a defined connection",
        ),
        (
            "replications:\n  x: {source: record_queue, target: app_pg, streams: [{id: x/a, mode: cdc, from: {endpoint: instruments}, object: o, primary_key: [k]}]}\n",
            "DuckLake must be the source, or DuckLake or DuckDB the target",
        ),
        (
            "replications:\n  x: {source: record_queue, target: lake, streams: [{id: x/a, mode: full-refresh, from: {endpoint: nope}, object: o}]}\n",
            "is not defined in API spec",
        ),
        (
            "replications:\n  x: {source: record_queue, target: lake, streams: [{id: x/a, mode: full-refresh, from: {endpoint: ratings}, object: o}]}\n",
            "which no stream in replication",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}, object: o, chunk: {by: hash, columns: [a]}}]}\n",
            "buckets",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, retry: {retry_on: [data]}, streams: [{id: x/a, mode: incremental, from: {table: a}, object: o}]}\n",
            "never retried",
        ),
    ];
    for (yaml, expect) in cases {
        let f = with_rep(yaml);
        let errs = f.errors(&static_all());
        assert!(
            errs.iter().any(|e| e.contains(expect)),
            "expected `{expect}` for\n{yaml}\ngot {errs:?}"
        );
    }
}

#[test]
fn replication_graph_checks() {
    let f = with_rep(
        "replications:\n  a: {source: lake, target: lake, after: [b], streams: [{id: g/a, mode: incremental, from: {table: a}, object: o}]}\n  b: {source: lake, target: lake, after: [a], streams: [{id: g/b, mode: incremental, from: {table: b}, object: o}]}\n",
    );
    assert!(f.errors(&static_all()).iter().any(|e| e.contains("cycle")));
    let f = with_rep(
        "replications:\n  a: {source: lake, target: lake, atomic_group: g, streams: [{id: g/a, mode: incremental, from: {table: a}, object: o}]}\n  b: {source: lake, target: app_pg, atomic_group: g, streams: [{id: g/b, mode: cdc, from: {table: b}, object: o, primary_key: [k]}]}\n",
    );
    assert!(
        f.errors(&static_all())
            .iter()
            .any(|e| e.contains("must share one target"))
    );
    let f = with_rep(
        "replications:\n  a: {source: lake, target: lake, op: shared, engine: {threads: 2}, streams: [{id: g/a, mode: incremental, from: {table: a}, object: o}]}\n  b: {source: lake, target: lake, op: shared, engine: {threads: 4}, streams: [{id: g/b, mode: incremental, from: {table: b}, object: o}]}\n",
    );
    assert!(
        f.errors(&static_all())
            .iter()
            .any(|e| e.contains("different `engine` overrides"))
    );
}

#[test]
fn hashes_track_the_right_fields() {
    let base = Fixture::new(&[]);
    let changed_window = Fixture::new(&[(
        "replications/est_hourly.yaml",
        &WAREHOUSE.replace("- 45", "- 60"),
    )]);
    let changed_desc = Fixture::new(&[(
        "replications/est_hourly.yaml",
        &WAREHOUSE.replace(
            "asset: {key: br/warehouse_hourly/est_v2/est_core_cons_rec}",
            "asset: {key: br/warehouse_hourly/est_v2/est_core_cons_rec, description: d}",
        ),
    )]);
    let get = |f: &Fixture| {
        let (p, _) = f.plan(&static_all()).expect("plan");
        let s = p
            .replications
            .iter()
            .flat_map(|r| r.streams.clone())
            .find(|s| s.id == "warehouse_hourly/est_v2/est_core_cons_rec")
            .expect("s");
        (s.config_hash, s.asset.code_version)
    };
    let (h0, c0) = get(&base);
    let (h1, c1) = get(&changed_window);
    let (h2, c2) = get(&changed_desc);
    assert_ne!(h0, h1, "window changes state meaning");
    assert_ne!(c0, c1);
    assert_eq!(h0, h2, "a description doesn't");
    assert_ne!(c0, c2, "but it is a code change");
}

#[test]
fn discover_is_not_available_yet() {
    let f = Fixture::new(&[]);
    let errs = f.errors(&PlanRequest {
        discover: true,
        env: Some("dev".into()),
        ..PlanRequest::all()
    });
    assert!(errs.iter().any(|e| e.contains("[discover]")), "{errs:?}");
}

#[test]
fn delta_source_checks() {
    let project = format!(
        "{PROJECT}  br:\n    type: delta\n    root: s3://prod-br/delta/\n    storage: {{type: s3, region: us-east-1}}\n    environments:\n      dev: {{root: /tmp/delta, storage: {{type: local}}}}\n"
    );
    let ok = "replications:\n  d:\n    source: br\n    target: lake\n    streams:\n      - {id: d/a, mode: full-refresh, from: {table: monthly/calendar}, object: br.a}\n      - {id: d/b, mode: incremental, from: {table: monthly/b, version: 3}, object: br.b, primary_key: [id], update_key: ts, window: \"dt >= '2024-01-01'\"}\n";
    let f = Fixture::new(&[("atlatl.yaml", &project), ("replications/extra.yaml", ok)]);
    let (plan, _) = f.plan(&static_all()).expect("delta plan");
    assert!(plan.replications.iter().any(|r| r.name == "d"));
    let cases: &[(&str, &str)] = &[
        (
            "replications:\n  x: {source: br, target: lake, streams: [{id: x/a, mode: snapshot, from: {table: t}, object: o, metadata: {loaded_at: {name: l}}}]}\n",
            "delta source supports full-refresh | incremental",
        ),
        (
            "replications:\n  x: {source: br, target: lake, streams: [{id: x/a, mode: full-refresh, from: {query: select 1}, object: o}]}\n",
            "does not fit a delta source",
        ),
        (
            "replications:\n  x: {source: br, target: lake, streams: [{id: x/a, mode: full-refresh, from: {tables: \"a/*\"}, object: o}]}\n",
            "does not fit a delta source",
        ),
        (
            "replications:\n  x: {source: br, target: app_pg, streams: [{id: x/a, mode: cdc, from: {table: t}, object: o, primary_key: [k]}]}\n",
            "DuckLake must be the source, or DuckLake or DuckDB the target",
        ),
        (
            "replications:\n  x: {source: lake, target: br, streams: [{id: x/a, mode: full-refresh, from: {table: s.t}, object: o}]}\n",
            "read_only",
        ),
        (
            "replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: full-refresh, from: {table: s.t, version: 1}, object: o}]}\n",
            "`from.version` only applies to delta sources",
        ),
    ];
    for (yaml, expect) in cases {
        let f = Fixture::new(&[("atlatl.yaml", &project), ("replications/extra.yaml", yaml)]);
        let errs = f.errors(&static_all());
        assert!(
            errs.iter().any(|e| e.contains(expect)),
            "expected `{expect}` for\n{yaml}\ngot {errs:?}"
        );
    }
}

#[test]
fn pg_changes_checks() {
    let rep = |streams: &str| {
        format!(
            "replications:\n  appdb_cdc: {{source: appdb_pg, target: lake, streams: [{streams}]}}\n"
        )
    };
    let ok = rep("{id: appdb/users, mode: cdc, from: {table: public.users}, object: br.users}");
    let f = with_rep(&ok);
    let errs = f.errors(&static_all());
    assert!(
        errs.iter().any(|e| e.contains("has no `changes:` block")),
        "{errs:?}"
    );

    let project = PROJECT.replace(
        "    read_only: true\n  record_queue:",
        "    read_only: true\n    changes: {production: true, slots: {allow_create: true}}\n  record_queue:",
    );
    let with = |yaml: &str| {
        Fixture::new(&[
            ("atlatl.yaml", project.as_str()),
            ("replications/extra.yaml", yaml),
        ])
    };
    let hist = rep(
        "{id: appdb/users, mode: cdc, from: {table: public.users}, object: o, history: true, deletes: {mode: soft}}",
    );
    for y in [&ok, &hist] {
        if let Err(d) = with(y).plan(&static_all()) {
            let errs: Vec<String> = d.errors().map(|e| e.error.to_string()).collect();
            panic!("{y}: {errs:?}");
        }
    }

    for (streams, expect) in [
        (
            "{id: appdb/u, mode: cdc, from: {table: public.users}, object: o, window: \"x > 1\"}",
            "`window` does not apply",
        ),
        (
            "{id: appdb/u, mode: cdc, from: {tables: \"public.*\"}, object: o}",
            "cdc streams read one table",
        ),
        (
            "{id: appdb/u, mode: cdc, from: {table: public.users}, object: o, deletes: {mode: soft, column: gone}}",
            "`deletes.column`/`value` are not supported",
        ),
    ] {
        let errs = with(&rep(streams)).errors(&static_all());
        assert!(
            errs.iter().any(|e| e.contains(expect)),
            "{expect}: {errs:?}"
        );
    }

    let errs = with("replications:\n  x: {source: lake, target: lake, streams: [{id: x/a, mode: incremental, from: {table: a}, object: o, history: true}]}\n")
        .errors(&static_all());
    assert!(
        errs.iter().any(|e| e.contains("`history` only applies")),
        "{errs:?}"
    );

    let clash = "replications:\n  a-b: {source: appdb_pg, target: lake, streams: [{id: x/a, mode: cdc, from: {table: public.a}, object: a}]}\n  a_b: {source: appdb_pg, target: lake, streams: [{id: x/b, mode: cdc, from: {table: public.b}, object: b}]}\n";
    let errs = with(clash).errors(&static_all());
    assert!(
        errs.iter()
            .any(|e| e.contains("both map to slot `atlatl_a_b`")),
        "{errs:?}"
    );
}

#[test]
fn slot_names() {
    use crate::config::model::PgChanges;
    assert_eq!(PgChanges::slot_name("appdb_cdc"), "atlatl_appdb_cdc");
    assert_eq!(PgChanges::slot_name("Appdb-CDC.v2"), "atlatl_appdb_cdc_v2");
    let long = "x".repeat(80);
    let n = PgChanges::slot_name(&long);
    assert_eq!(n.len(), 63);
    assert!(n.starts_with("atlatl_xxx"));
    assert_ne!(n, PgChanges::slot_name(&"x".repeat(81)));
}

const SHAPES: &str = r#"replications:
  earnings:
    source: record_queue
    target: lake
    asset: {group: shapes}
    streams:
      - id: shapes/stocks
        from: {endpoint: instruments}
        mode: full-refresh
        object: br.stocks
        primary_key: [symbol]
    then:
      - target: app_pg
        streams:
          - id: shapes/serve_stocks
            from: {table: br.stocks}
            mode: cdc
            object: app.stocks
            primary_key: [symbol]
      - target: lake
        streams:
          - id: shapes/ag_stocks
            from: {query: "SELECT * FROM br.stocks"}
            mode: full-refresh
            object: ag.stocks
        then:
          - target: app_pg
            asset: {group: served}
            streams:
              - id: shapes/serve_ag
                from: {table: ag.stocks}
                mode: cdc
                object: app.ag_stocks
                primary_key: [symbol]
"#;

#[test]
fn multi_hop_plans_hops_steps_and_kinds() {
    let f = Fixture::new(&[("replications/shapes.yaml", SHAPES)]);
    for req in [static_all(), PlanRequest::all()] {
        let (plan, _) = f.plan(&req).expect("plan");
        let r = plan
            .replications
            .iter()
            .find(|r| r.name == "earnings")
            .expect("earnings");
        let hops: Vec<_> = r
            .hops
            .iter()
            .map(|h| {
                (
                    h.hop,
                    h.parent,
                    h.source.name.as_str(),
                    h.target.name.as_str(),
                    h.streams.clone(),
                )
            })
            .collect();
        assert_eq!(
            hops,
            [
                (
                    0,
                    None,
                    "record_queue",
                    "lake",
                    vec!["shapes/stocks".to_owned()]
                ),
                (
                    1,
                    Some(0),
                    "lake",
                    "app_pg",
                    vec!["shapes/serve_stocks".to_owned()]
                ),
                (
                    2,
                    Some(0),
                    "lake",
                    "lake",
                    vec!["shapes/ag_stocks".to_owned()]
                ),
                (
                    3,
                    Some(2),
                    "lake",
                    "app_pg",
                    vec!["shapes/serve_ag".to_owned()]
                ),
            ]
        );
        let kinds: Vec<(&str, usize, Vec<String>, Option<&str>)> = r
            .streams
            .iter()
            .map(|s| {
                (
                    s.id.as_str(),
                    s.hop,
                    s.asset.kinds.clone(),
                    s.asset.group.as_deref(),
                )
            })
            .collect();
        let k = |t: &str| vec!["atlatl".to_owned(), t.to_owned()];
        assert_eq!(
            kinds,
            [
                ("shapes/stocks", 0, k("ducklake"), Some("shapes")),
                ("shapes/serve_stocks", 1, k("postgres"), Some("shapes")),
                ("shapes/ag_stocks", 2, k("ducklake"), Some("shapes")),
                ("shapes/serve_ag", 3, k("postgres"), Some("served")),
            ]
        );
        let step = |id: &str| {
            r.steps
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("{id}"))
        };
        assert_eq!(
            step("extract:shapes/serve_stocks:br.stocks").depends_on,
            ["commit"]
        );
        assert_eq!(
            step("extract:shapes/serve_ag:ag.stocks").depends_on,
            ["commit:2"]
        );
        assert_eq!(step("commit:3").depends_on, ["apply:shapes/serve_ag"]);
        assert_eq!((step("commit:1").hop, step("commit").hop), (1, 0));

        let one = r.hop_plan(3);
        assert_eq!(
            (
                one.name.as_str(),
                one.source.name.as_str(),
                one.target.name.as_str()
            ),
            ("earnings", "lake", "app_pg")
        );
        assert_eq!(one.streams.len(), 1);
        assert!(one.hops.is_empty() && one.steps.iter().all(|s| s.hop == 3));
    }
    let json = serde_json::to_value(f.plan(&static_all()).expect("plan").0).expect("json");
    let single = json["replications"]
        .as_array()
        .expect("reps")
        .iter()
        .find(|r| r["name"] == "record_queue")
        .expect("rq");
    assert!(
        single.get("hops").is_none(),
        "single-hop plans are unchanged"
    );
    assert!(single["streams"][0].get("hop").is_none());
}

#[test]
fn stream_selection_narrows_hops() {
    let f = Fixture::new(&[("replications/shapes.yaml", SHAPES)]);
    let req = PlanRequest {
        streams: vec!["shapes/serve_ag".into()],
        ..static_all()
    };
    let (plan, _) = f.plan(&req).expect("plan");
    let r = &plan.replications[0];
    assert_eq!(r.streams.len(), 1);
    assert_eq!(r.hops[3].streams, ["shapes/serve_ag"]);
    assert!(r.hops[0].streams.is_empty());
    let x = r
        .steps
        .iter()
        .find(|s| s.kind == StepKind::Extract)
        .expect("extract");
    assert!(
        x.depends_on.is_empty(),
        "no planned upstream hop to wait for"
    );
}

#[test]
fn duckdb_connections_are_targets_or_list_sources() {
    let ok = Fixture::new(&[(
        "replications/seeded.yaml",
        "replications:\n  seeded:\n    source: record_queue\n    target: lake\n    for_each: {over: {query: 'SELECT id FROM members', connection: seed}, as: m, key: id}\n    streams: [{id: seeded/a, from: {endpoint: instruments}, mode: incremental, object: br.seeded, primary_key: [id]}]\n  into_file:\n    source: record_queue\n    target: seed\n    streams: [{id: into_file/a, from: {endpoint: instruments}, mode: incremental, object: br.instruments, primary_key: [id]}]\n",
    )]);
    let (plan, _) = ok.plan(&static_all()).unwrap_or_else(|d| panic!("{d:?}"));
    assert!(plan.replications.iter().any(|r| r.name == "seeded"));
    assert!(plan.replications.iter().any(|r| r.name == "into_file"));
    let errs = shape_errors(
        "replications:\n  s:\n    source: seed\n    target: lake\n    streams: [{id: s/a, from: {table: x.a}, mode: full-refresh, object: br.a}]\n",
    );
    assert_eq!(
        errs,
        [
            "[duckdb-role] source `seed` is a duckdb connection, which is an ingest target or feeds `for_each.over.query` and `bind`"
        ]
    );
    let errs = shape_errors(
        "replications:\n  s:\n    source: lake\n    target: seed\n    streams: [{id: s/a, from: {table: x.a}, mode: cdc, object: br.a, primary_key: [id]}]\n",
    );
    assert!(
        errs.iter().any(|e| e.starts_with("[mode]")),
        "cdc doesn't serve into a duckdb file: {errs:?}"
    );
}

#[test]
fn clickhouse_and_bind_checks() {
    let stream = |extra: &str| {
        format!(
            "replications:\n  c:\n    source: ch\n    target: seed\n    streams:\n      - id: c/a\n        mode: full-refresh\n        object: br.a\n{extra}"
        )
    };
    let ok = Fixture::new(&[(
        "replications/c.yaml",
        &stream(
            "        bind: {ids: {query: 'SELECT id FROM ag.scope', connection: seed, batch: 500}, cur: {query: 'SELECT 1', connection: lake}}\n        from: {query: 'SELECT * FROM t WHERE id IN ({{ bind.ids }}) AND c IN ({{ bind.cur }})'}\n",
        ),
    )]);
    let (plan, _) = ok
        .plan(&PlanRequest {
            env: Some("dev".into()),
            ..PlanRequest::all()
        })
        .unwrap_or_else(|d| panic!("{d:?}"));
    let m = &plan.replications[0].streams[0].members[0];
    assert_eq!(
        m.query.as_deref(),
        Some("SELECT * FROM t WHERE id IN (/*atlatl:bind:ids*/) AND c IN (/*atlatl:bind:cur*/)")
    );

    let cases = [
        (
            "        from: {table: t}\n        update_key: {column: ts}\n",
            "[update-key] stream `c/a`: clickhouse sources don't take `update_key` (filter in `from.query`)",
        ),
        (
            "        from: {tables: 't*'}\n",
            "[from] `from.tables` does not fit a clickhouse source (use: table, query)",
        ),
        (
            "        from: {table: t}\n        bind: {ids: {query: 'SELECT 1', connection: seed}}\n",
            "[bind] stream `c/a`: `bind` lists are spliced into `from.query`, which it lacks",
        ),
        (
            "        from: {query: 'SELECT {{ bind.ids }}'}\n        bind: {ids: {query: 'SELECT 1', connection: seed}, extra: {query: 'SELECT 1', connection: seed}}\n",
            "[bind] stream `c/a`: `bind.extra` is not used in `from.query` (splice it in as {{ bind.extra }})",
        ),
        (
            "        from: {query: 'SELECT {{ bind.ids }}, {{ bind.nope }}'}\n        bind: {ids: {query: 'SELECT 1', connection: seed}}\n",
            "[bind] stream `c/a`: `from.query` uses `bind.nope`, which `bind` doesn't define",
        ),
        (
            "        from: {query: 'SELECT 1'}\n        bind: {ids: {query: 'SELECT 1', connection: record_queue}}\n",
            "[bind] stream `c/a`: `bind.ids.connection: record_queue` must be a lake, postgres or duckdb connection, not rest",
        ),
        (
            "        from: {query: 'SELECT 1'}\n        bind: {ids: {query: 'SELECT 1', connection: nope}}\n",
            "[reference] stream `c/a`: `bind.ids.connection: nope` is not a defined connection",
        ),
        (
            "        from: {query: 'SELECT 1'}\n        bind: {ids: {query: ' ', connection: seed, batch: 0}}\n",
            "[bind] stream `c/a`: `bind.ids.batch` must be at least 1",
        ),
        (
            "        from: {query: 'SELECT 1'}\n        bind: {a: {query: 'SELECT 1', connection: seed, batch: 1}, b: {query: 'SELECT 1', connection: seed, batch: 1}}\n",
            "[bind] stream `c/a`: only one bind list can be batched (found a, b)",
        ),
        (
            "        from: {query: 'SELECT 1'}\n        bind: {a-b: {query: 'SELECT 1', connection: seed}}\n",
            "[bind] stream `c/a`: bind name `a-b` must be an identifier",
        ),
    ];
    for (extra, want) in cases {
        let errs = shape_errors(&stream(extra));
        assert!(errs.iter().any(|e| e == want), "{extra}: {errs:?}");
    }
    let errs = shape_errors(
        &stream("        from: {query: 'SELECT 1'}\n")
            .replace("mode: full-refresh", "mode: snapshot"),
    );
    assert!(
        errs.iter().any(
            |e| e.starts_with("[mode] a clickhouse source supports full-refresh | incremental")
        ),
        "{errs:?}"
    );
    let errs = shape_errors(
        "replications:\n  c:\n    source: lake\n    target: lake\n    streams: [{id: c/a, mode: full-refresh, object: br.a, from: {query: 'SELECT 1'}, bind: {ids: {query: 'SELECT 1', connection: seed}}}]\n",
    );
    assert!(
        errs.iter()
            .any(|e| e == "[bind] stream `c/a`: `bind` applies to clickhouse sources, not ducklake"),
        "{errs:?}"
    );
}

fn shape_errors(yaml: &str) -> Vec<String> {
    let f = Fixture::new(&[("replications/shapes.yaml", yaml)]);
    match f.plan(&static_all()) {
        Ok(_) => panic!("expected errors for\n{yaml}"),
        Err(d) => d
            .errors()
            .map(|e| format!("[{}] {}", e.code, e.error.message))
            .collect(),
    }
}

#[test]
fn multi_hop_static_errors() {
    let base = |then: &str, extra: &str| {
        format!(
            "replications:\n  x:\n    source: lake\n    target: lake\n{extra}    streams: [{{id: x/a, from: {{table: src.a}}, mode: full-refresh, object: br.a}}]\n    then:\n{then}"
        )
    };
    let cases = [
        (
            base(
                "      - {target: nope, streams: [{id: x/b, from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]}\n",
                "",
            ),
            "[reference] `then` target `nope` is not a defined connection",
        ),
        (
            base(
                "      - {target: appdb_pg, streams: [{id: x/b, from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]}\n",
                "",
            ),
            "[read-only-target] `then` target `appdb_pg` is read_only",
        ),
        (
            base(
                "      - target: app_pg\n        streams: [{id: x/b, from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]\n        then: [{target: lake, streams: [{id: x/c, from: {table: d.a}, mode: full-refresh, object: br.c}]}]\n",
                "",
            ),
            "[then] `then` hop to `lake` would read `app_pg`, which is postgres",
        ),
        (
            base(
                "      - {target: app_pg, streams: [{id: x/b, from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]}\n",
                "    atomic_group: g\n",
            ),
            "[then] replication `x` has `then` hops and `atomic_group: g`",
        ),
        (
            base(
                "      - {target: lake, streams: [{id: x/b, from: {table: br.a}, mode: full-refresh, object: src.a}]}\n",
                "",
            ),
            "[then-cycle] replication `x`: hops read what they (or a downstream hop) write: lake:br.a <- lake:src.a <- lake:br.a",
        ),
        (
            base(
                "      - {target: lake, streams: [{id: x/b, from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]}\n",
                "",
            ),
            "[mode] mode `cdc` needs a serving target that can apply changes; `lake` is ducklake",
        ),
        (
            base(
                "      - {target: app_pg, streams: [{id: x/b, after: [x/a], from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]}\n",
                "",
            ),
            "[after] stream `x/b` lists `x/a` in `after`, which is in a different hop",
        ),
    ];
    for (yaml, want) in cases {
        let errs = shape_errors(&yaml);
        assert!(
            errs.iter().any(|e| e.starts_with(want)),
            "want {want}\ngot {errs:?}\n{yaml}"
        );
    }
}

#[test]
fn hop_reading_an_unwritten_table_warns() {
    let yaml = "replications:\n  x:\n    source: lake\n    target: lake\n    streams: [{id: x/a, from: {table: src.a}, mode: full-refresh, object: br.a}]\n    then: [{target: app_pg, streams: [{id: x/b, from: {table: br.other}, mode: cdc, object: d.a, primary_key: [id]}]}]\n";
    let f = Fixture::new(&[("replications/shapes.yaml", yaml)]);
    let (_, d) = f.plan(&static_all()).expect("plan");
    assert!(d.warnings().any(|w| w.code == "then-source"), "{d:?}");
}

const TENANTS: &str = r#"replications:
  tenants:
    source: lake
    target: lake
    for_each: {over: [acme, 7], as: tenant}
    lock_group: tenants
    streams:
      - id: tenants/orders
        from: {query: "SELECT * FROM src.orders"}
        mode: full-refresh
        object: "br.orders_{{ tenant }}"
      - id: tenants/events
        from: {table: src.events}
        mode: incremental
        object: br.events
        primary_key: [id]
        window: "tenant = '{{ tenant }}'"
        update_key: updated_at
  per_user:
    source: lake
    target: lake
    after: [tenants]
    for_each: {over: {query: "SELECT id, name FROM src.users", connection: app_pg}, as: user, key: id}
    streams:
      - id: users/accounts
        from: {table: src.accounts}
        mode: incremental
        object: br.accounts
        primary_key: [user_id, id]
        window: "user_id = {{ user.id }}"
"#;

#[test]
fn for_each_lists_expand_into_instances() {
    let f = Fixture::new(&[("replications/tenants.yaml", TENANTS)]);
    let req = PlanRequest {
        replications: vec!["tenants".into(), "per_user".into()],
        ..PlanRequest::all()
    };
    let (plan, _) = f.plan(&req).expect("plan");
    let names: Vec<&str> = plan.replications.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["tenants[acme]", "tenants[7]", "per_user"]);
    let acme = &plan.replications[0];
    let i = acme.instance.as_ref().expect("instance");
    assert_eq!(
        (i.of.as_str(), i.key.as_str(), i.item.clone()),
        ("tenants", "acme", serde_json::json!("acme"))
    );
    assert_eq!(acme.config_name(), "tenants");
    let m = &acme.streams[0].members[0];
    assert_eq!(m.object.as_deref(), Some("br.orders_acme"));
    assert_eq!(
        acme.streams[1].members[0].window.as_deref(),
        Some("tenant = 'acme'")
    );
    assert_eq!(acme.streams[1].state_id(), "tenants/events@acme");
    assert_eq!(
        plan.replications[1].streams[0].members[0].object.as_deref(),
        Some("br.orders_7")
    );
    assert_eq!(plan.replications[1].lock_group.as_deref(), Some("tenants"));
    assert!(acme.for_each.is_none());

    let q = &plan.replications[2];
    assert!(
        q.instance.is_none() && q.for_each.is_some(),
        "a query iteration expands at run time"
    );
    assert_eq!(q.after, ["tenants"]);
    assert!(
        plan.warnings
            .iter()
            .any(|w| w.code == "unexpanded" && w.message.contains("per_user"))
    );

    let items: IndexMap<String, Vec<serde_json::Value>> = [(
        "per_user".to_owned(),
        vec![
            serde_json::json!({"id": 3, "name": "a"}),
            serde_json::json!({"id": 4, "name": "b"}),
        ],
    )]
    .into();
    let (p, _) = load::load(f.root());
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([
        ("PG_PW", "x"),
        ("DEV_PG_PW", "y"),
        ("DUCKLAKE_DATA_PATH", "/tmp/l/"),
        ("RECORD_QUEUE_API_KEY", "k"),
    ]));
    let req = PlanRequest {
        replications: vec!["per_user".into()],
        ..PlanRequest::all()
    };
    let (plan, _) = build_with(&p, &req, vars.clone(), &Redactor::new(), &items).expect("plan");
    let got: Vec<(&str, Option<&str>, &str)> = plan
        .replications
        .iter()
        .map(|r| {
            (
                r.name.as_str(),
                r.streams[0].members[0].window.as_deref(),
                r.streams[0].state_id(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            ("per_user[3]", Some("user_id = 3"), "users/accounts@3"),
            ("per_user[4]", Some("user_id = 4"), "users/accounts@4")
        ]
    );

    let empty: IndexMap<String, Vec<serde_json::Value>> = [("per_user".to_owned(), vec![])].into();
    let (plan, _) = build_with(&p, &req, vars, &Redactor::new(), &empty).expect("plan");
    assert_eq!(plan.replications.len(), 1);
    assert!(
        plan.replications[0].for_each.is_some(),
        "no items: the replication stays, with nothing to do"
    );

    let (plan, _) = f.plan(&static_all()).expect("static");
    let t = plan
        .replications
        .iter()
        .find(|r| r.name == "tenants")
        .expect("tenants");
    assert!(
        t.instance.is_none() && t.for_each.is_some(),
        "a static plan lists the replication once"
    );
    assert_eq!(
        serde_json::to_value(t).expect("json")["for_each"],
        serde_json::json!({"over": ["acme", 7], "as": "tenant", "key": null})
    );
}

#[test]
fn for_each_static_errors() {
    let one = |fe: &str, extra: &str, stream: &str| {
        format!(
            "replications:\n  t:\n    source: lake\n    target: lake\n    for_each: {fe}\n{extra}    streams:\n      - {stream}\n"
        )
    };
    let inc = "{id: t/a, from: {table: src.a}, mode: incremental, object: br.a, primary_key: [id]}";
    let cases = [
        (
            one("{over: [a], as: u}", "    atomic_group: g\n", inc),
            "[for-each] replication `t` has `for_each`, whose instances are independent units",
        ),
        (
            one(
                "{over: [a], as: u}",
                "    then: [{target: app_pg, streams: [{id: t/b, from: {table: br.a}, mode: cdc, object: d.a, primary_key: [id]}]}]\n",
                inc,
            ),
            "[for-each] replication `t`: `for_each` with `then` hops isn't supported",
        ),
        (
            one(
                "{over: [a], as: u}",
                "",
                "{id: t/a, from: {table: src.a}, mode: full-refresh, object: br.a}",
            ),
            "[for-each-object] full-refresh stream `t/a` writes `br.a` for every `for_each` instance",
        ),
        (
            one("{over: [a], as: env}", "", inc),
            "[for-each] `for_each.as: env` must be an identifier",
        ),
        (
            one("{over: [], as: u}", "", inc),
            "[for-each] replication `t`: `for_each.over` is an empty list",
        ),
        (
            one("{over: [a, a], as: u}", "", inc),
            "[for-each] replication `t`: `for_each` key `a` repeats",
        ),
        (
            one("{over: [{id: 1}], as: u}", "", inc),
            "[for-each] replication `t`: `for_each` item {\"id\":1} is a map",
        ),
        (
            one("{over: [{id: 1}, {x: 2}], as: u, key: id}", "", inc),
            "[for-each] replication `t`: `for_each` item {\"x\":2} has no scalar",
        ),
        (
            one(
                "{over: {query: 'SELECT 1', connection: record_queue}, as: u}",
                "",
                inc,
            ),
            "[for-each] `for_each.over.connection: record_queue` must be a lake, postgres or duckdb connection",
        ),
        (
            one(
                "{over: [a], as: u}",
                "",
                "{id: t/a, from: {table: src.a}, mode: incremental, object: br.a, primary_key: [id], window: '{{ nope }} = 1'}",
            ),
            "[template] `nope` is not available",
        ),
    ];
    for (yaml, want) in cases {
        let errs = shape_errors(&yaml);
        assert!(
            errs.iter().any(|e| e.starts_with(want)),
            "want {want}\ngot {errs:?}\n{yaml}"
        );
    }
    let ok = one(
        "{over: [a], as: u}",
        "",
        "{id: t/a, from: {table: src.a}, mode: incremental, object: br.a, primary_key: [id], window: \"k = '{{ u }}'\"}",
    );
    Fixture::new(&[("replications/shapes.yaml", &ok)])
        .plan(&static_all())
        .expect("the item is a plan-phase root");
}

#[test]
fn instance_selection() {
    let f = Fixture::new(&[("replications/tenants.yaml", TENANTS)]);
    let sel = |instances: &[&str], reps: &[&str]| PlanRequest {
        instances: instances.iter().map(|i| (*i).to_owned()).collect(),
        replications: reps.iter().map(|r| (*r).to_owned()).collect(),
        ..PlanRequest::all()
    };
    let names = |req: &PlanRequest| -> Vec<String> {
        f.plan(req)
            .expect("plan")
            .0
            .replications
            .iter()
            .map(|r| r.name.clone())
            .collect()
    };
    assert_eq!(names(&sel(&["tenants[7]"], &[])), ["tenants[7]"]);
    assert_eq!(
        names(&sel(&["tenants[7]", "tenants[acme]"], &["tenants"])),
        ["tenants[acme]", "tenants[7]"],
        "plan order, not flag order"
    );
    assert_eq!(
        names(&sel(&["per_user[3]"], &[])),
        ["per_user"],
        "a query iteration is selected and narrowed when the run starts"
    );
    let (plan, _) = f.plan(&sel(&["tenants[7]"], &[])).expect("plan");
    assert_eq!(plan.selection.instances, ["tenants[7]"]);
    assert!(
        !plan.connections.iter().any(|c| c["name"] == "app_pg"),
        "per_user's query connection isn't resolved"
    );

    let errs = |instances: &[&str], reps: &[&str]| f.errors(&sel(instances, reps));
    for (inst, reps, want) in [
        (
            "tenants[nope]",
            &[][..],
            "[selection] --instance `tenants[nope]`: replication `tenants` has no instance `nope` (its instances: acme, 7)",
        ),
        (
            "tenants",
            &[],
            "[selection] --instance `tenants`: expected `<replication>[<key>]`",
        ),
        (
            "record_queue[x]",
            &[],
            "[selection] --instance `record_queue[x]`: replication `record_queue` has no `for_each`",
        ),
        (
            "gone[x]",
            &[],
            "[selection] --instance `gone[x]`: no such replication `gone`",
        ),
        (
            "tenants[acme]",
            &["per_user"],
            "[selection] --instance `tenants[acme]`: replication `tenants` is not selected",
        ),
    ] {
        let e = errs(&[inst], reps);
        assert!(
            e.iter().any(|x| x.starts_with(want)),
            "want {want}\ngot {e:?}"
        );
    }

    let items: IndexMap<String, Vec<serde_json::Value>> = [(
        "per_user".to_owned(),
        vec![
            serde_json::json!({"id": 3, "name": "a"}),
            serde_json::json!({"id": 4, "name": "b"}),
        ],
    )]
    .into();
    let (p, _) = load::load(f.root());
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::new([
        ("PG_PW", "x"),
        ("DEV_PG_PW", "y"),
        ("DUCKLAKE_DATA_PATH", "/tmp/l/"),
        ("RECORD_QUEUE_API_KEY", "k"),
    ]));
    let (plan, _) = build_with(
        &p,
        &sel(&["per_user[4]"], &[]),
        vars,
        &Redactor::new(),
        &items,
    )
    .expect("plan");
    let got: Vec<&str> = plan.replications.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(got, ["per_user[4]"]);
}

const PG_MEMBERS: &str = r#"replications:
  appdb:
    source: appdb_pg
    target: lake
    source_snapshot: shared
    streams:
      - id: appdb/all
        from: {tables: "appdb.*"}
        object: "br.{{ member.table }}"
        mode: full-refresh
        members:
          appdb.audit_*: {primary_key: [ts, seq]}
          appdb.users: {primary_key: [id]}
"#;

#[test]
fn member_keys_and_shared_snapshots_plan() {
    let f = Fixture::new(&[("replications/appdb.yaml", PG_MEMBERS)]);
    let (plan, _) = f.plan(&static_all()).expect("plan");
    let r = plan
        .replications
        .iter()
        .find(|r| r.name == "appdb")
        .expect("appdb");
    assert_eq!(r.source_snapshot, Some(SourceSnapshot::Shared));
    let s = &r.streams[0];
    let keys: Vec<(&str, Vec<&str>)> = s
        .member_primary_keys
        .iter()
        .map(|(m, k)| (m.as_str(), k.iter().map(String::as_str).collect()))
        .collect();
    assert_eq!(
        keys,
        [
            ("appdb.audit_*", vec!["ts", "seq"]),
            ("appdb.users", vec!["id"])
        ]
    );
    let j = serde_json::to_value(r).expect("json");
    assert_eq!(j["source_snapshot"], "shared");
    assert_eq!(
        j["streams"][0]["member_primary_keys"]["appdb.users"][0],
        "id"
    );

    let (p, _) = load::load(f.root());
    let (_, st) = p.stream("appdb/all").expect("stream");
    let c = &st.config;
    assert_eq!(
        c.member_config("appdb.users")
            .and_then(|m| m.primary_key.clone()),
        Some(vec!["id".to_owned()])
    );
    assert_eq!(
        c.member_config("appdb.audit_2024")
            .and_then(|m| m.primary_key.clone()),
        Some(vec!["ts".to_owned(), "seq".to_owned()])
    );
    assert!(c.member_config("appdb.orders").is_none());

    // The state hash only moves when `members` keys are set.
    let bare = PG_MEMBERS
        .replace("        members:\n", "")
        .replace("          appdb.audit_*: {primary_key: [ts, seq]}\n", "")
        .replace("          appdb.users: {primary_key: [id]}\n", "");
    let f2 = Fixture::new(&[("replications/appdb.yaml", &bare)]);
    let (p2, _) = load::load(f2.root());
    let (_, st2) = p2.stream("appdb/all").expect("stream");
    let before = sha(&serde_json::json!({
        "mode": "full-refresh",
        "from": st2.config.from.as_ref().map(|f| &f.value),
        "window": null,
        "update_key": null,
        "primary_key": null,
    }));
    assert_eq!(
        state_config_hash(st2),
        before,
        "unchanged without `members`"
    );
    assert_ne!(state_config_hash(st), before);
}

#[test]
fn member_and_snapshot_static_errors() {
    let cases = [
        (
            "replications:\n  x:\n    source: lake\n    target: lake\n    source_snapshot: shared\n    streams: [{id: x/a, from: {table: src.a}, mode: full-refresh, object: br.a}]\n",
            "[source-snapshot] replication `x`: `source_snapshot` applies to postgres sources",
        ),
        (
            "replications:\n  x:\n    source: appdb_pg\n    target: lake\n    source_snapshot: shared\n    streams: [{id: x/a, from: {table: src.a}, mode: full-refresh, object: br.a, chunk: {by: hash, columns: [id], buckets: 4}}]\n",
            "[source-snapshot] replication `x`: `source_snapshot: shared` can't be combined with `chunk`",
        ),
        (
            "replications:\n  x:\n    source: appdb_pg\n    target: lake\n    streams: [{id: x/a, from: {table: src.a}, mode: full-refresh, object: br.a, members: {src.a: {primary_key: [id]}}}]\n",
            "[members] `members` applies to `from.tables` streams",
        ),
        (
            "replications:\n  x:\n    source: appdb_pg\n    target: lake\n    streams: [{id: x/a, from: {tables: src.*}, mode: full-refresh, object: br.a, members: {src.a: {primary_key: []}}}]\n",
            "[primary-key] stream `x/a`: `members.\"src.a\".primary_key` is empty",
        ),
    ];
    for (yaml, want) in cases {
        let errs = shape_errors(yaml);
        assert!(
            errs.iter().any(|e| e.starts_with(want)),
            "want {want}\ngot {errs:?}"
        );
    }
    let f = Fixture::new(&[(
        "replications/shapes.yaml",
        "replications:\n  x:\n    source: appdb_pg\n    target: lake\n    streams: [{id: x/a, from: {tables: src.*}, mode: full-refresh, object: br.a, members: {src.a: {primary_key: [id], nope: 1}}}]\n",
    )]);
    let (_, d) = load::load(f.root());
    assert!(
        d.errors().any(|e| e.error.message.contains("nope")),
        "members deny unknown fields: {d:?}"
    );
}

#[test]
fn var_overrides_keep_the_type_they_replace() {
    let base = serde_json::json!({"limit": 100, "on": true, "ids": [1], "name": "a", "none": null});
    let o = |pairs: &[(&str, &str)]| -> IndexMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    };
    let v = super::stream_vars(
        base.clone(),
        &o(&[
            ("limit", "200"),
            ("on", "false"),
            ("ids", "[2, 3]"),
            ("name", "7"),
            ("none", "x"),
            ("new", "5"),
        ]),
    );
    assert_eq!(
        v,
        serde_json::json!({"limit": 200, "on": false, "ids": [2, 3], "name": "7", "none": "x", "new": "5"})
    );
    let v = super::stream_vars(base, &o(&[("limit", "lots")]));
    assert_eq!(v["limit"], "lots", "not a number: kept as given");
    assert_eq!(
        super::stream_vars(serde_json::Value::Null, &o(&[("a", "1")])),
        serde_json::json!({"a": "1"})
    );
}
