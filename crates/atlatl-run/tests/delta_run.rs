//! Delta → lake through the runner (M2.1), on the S7 fixtures.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use atlatl_core::obs::report::{ReplicationStatus, RowCounts, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, TempProject};
use tokio_util::sync::CancellationToken;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../atlatl-conn-delta/tests/fixtures")
}

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  dl: {{type: delta, root: \"{}\"}}\n",
        lake.connection_yaml("lake", "lake"),
        fixtures().display()
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .expect("project");
    Env { lake, project }
}

fn reps(e: &Env, version: u64) {
    let y = format!(
        r#"replications:
  delta_full:
    source: dl
    target: lake
    streams:
      - {{id: d/full, mode: full-refresh, from: {{table: partitioned, version: {version}}}, object: br.part, primary_key: [id]}}
  delta_inc:
    source: dl
    target: lake
    streams:
      - {{id: d/inc, mode: incremental, from: {{table: partitioned, version: {version}}}, object: br.part_inc, primary_key: [id], update_key: {{column: id}}}}
  delta_lookback:
    source: dl
    target: lake
    streams:
      - {{id: d/lb, mode: incremental, from: {{table: types}}, object: br.types, primary_key: [id], update_key: {{column: ts_tz, lookback: 2h}}}}
"#
    );
    std::fs::create_dir_all(e.project.path().join("replications")).expect("mkdir");
    std::fs::write(e.project.path().join("replications/delta.yaml"), y).expect("write");
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
    let r = run(
        Arc::new(p),
        args,
        vars,
        CancellationToken::new(),
        || None,
        |_, _| {},
    )
    .await
    .expect("run");
    for x in &r.replications {
        assert!(x.status.is_success(), "{}: {:?}", x.replication, x.error);
    }
    r
}

fn rows(r: &RunReport, rep: &str) -> RowCounts {
    let x = r
        .replications
        .iter()
        .find(|x| x.replication == rep)
        .expect("rep");
    x.streams
        .iter()
        .flat_map(|s| &s.members)
        .fold(RowCounts::default(), |mut a, m| {
            a.add(&m.rows);
            a
        })
}

fn q(e: &Env, sql: &str) -> i64 {
    e.lake
        .open("lake")
        .expect("open")
        .query_row(sql, [], |r| r.get(0))
        .expect(sql)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_refresh_follows_pinned_versions() {
    let e = setup();
    reps(&e, 2);
    let r = go(&e, &["delta_full"]).await;
    assert_eq!(rows(&r, "delta_full").inserted, 3000);
    reps(&e, 3);
    let r = go(&e, &["delta_full"]).await;
    let n = rows(&r, "delta_full");
    assert_eq!((n.inserted, n.deleted), (0, 100));
    assert_eq!(q(&e, "SELECT count(*) FROM lake.br.part"), 2900);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_keeps_watermark_across_versions() {
    let e = setup();
    reps(&e, 0);
    let r = go(&e, &["delta_inc"]).await;
    assert_eq!(rows(&r, "delta_inc").inserted, 1000);
    reps(&e, 2);
    let r = go(&e, &["delta_inc"]).await;
    let n = rows(&r, "delta_inc");
    assert_eq!(
        (n.staged, n.inserted, n.unchanged),
        (2001, 2000, 1),
        "inclusive watermark"
    );
    assert_eq!(q(&e, "SELECT count(*) FROM lake.br.part_inc"), 3000);
    let st: String = e
        .lake
        .open("lake")
        .expect("o")
        .query_row(
            "SELECT value->>'value' FROM lake._atlatl.stream_state WHERE stream_id = 'd/inc'",
            [],
            |r| r.get(0),
        )
        .expect("state");
    assert_eq!(st, "2999");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookback_rereads_with_a_typed_bound() {
    let e = setup();
    reps(&e, 0);
    let first = go(&e, &["delta_lookback"]).await;
    let total = q(&e, "SELECT count(*) FROM lake.br.types");
    assert_eq!(rows(&first, "delta_lookback").inserted as i64, total);
    let r = go(&e, &["delta_lookback"]).await;
    let n = rows(&r, "delta_lookback");
    assert!(
        n.staged >= 1 && (n.staged as i64) < total,
        "lookback re-reads only the tail: {n:?}"
    );
    assert_eq!((n.inserted, n.updated), (0, 0));
    assert_eq!(r.replications[0].status, ReplicationStatus::UpToDate);
}
