//! Files → lake through the runner (M4.1).

use std::sync::Arc;

use atlatl_core::obs::report::{ReplicationStatus, RowCounts, RunReport};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, TempProject};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  prices:
    source: files
    target: lake
    streams:
      - id: f/prices
        mode: full-refresh
        from: {files: {url: "in/{{ vars.day }}", glob: "prices_*.csv"}}
        vars: {day: d1}
        object: br.prices
        primary_key: [ticker]
        columns: {px: {type: DOUBLE}}
  chunked:
    source: files
    target: lake
    streams:
      - id: f/chunked
        mode: full-refresh
        from: {files: {url: "in/d1", glob: "prices_*.csv"}}
        object: br.prices_chunked
        primary_key: [ticker]
        chunk: {by: files, size: 1B}
  missing:
    source: files
    target: lake
    streams:
      - id: f/missing
        mode: full-refresh
        from: {files: {url: "in", glob: "nope_*.csv"}, on_no_match: warn}
        object: br.missing
  nulled:
    source: files
    target: lake
    streams:
      - id: f/nulled
        mode: full-refresh
        from: {files: {url: "in/amt", glob: "*.csv"}}
        object: br.nulled
        columns: {include: [id, amt], amt: {type: "DECIMAL(10,2)"}}
        on_cast_error: {set_null: true}
  strict:
    source: files
    target: lake
    streams:
      - id: f/strict
        mode: full-refresh
        from: {files: {url: "in/amt", glob: "*.csv"}}
        object: br.strict
        columns: {amt: {type: "DECIMAL(10,2)"}}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  files: {{type: files}}\n",
        lake.connection_yaml("lake", "lake")
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.replication("files", REPS))
        .and_then(|p| p.file("in/d1/prices_a.csv", "ticker,px\nAAPL,1.5\nMSFT,2\n"))
        .and_then(|p| p.file("in/d1/prices_b.csv", "ticker,px\nNVDA,3.25\n"))
        .and_then(|p| p.file("in/d1/other.csv", "ticker,px\nIGNORED,0\n"))
        .and_then(|p| {
            p.file(
                "in/amt/a.csv",
                "id,amt,note\n1,12.50,a\n2,12345.6789,b\n3,x,c\n",
            )
        })
        .expect("project");
    Env { lake, project }
}

async fn go(e: &Env, names: &[&str]) -> RunReport {
    let r = attempt(e, names).await;
    for x in &r.replications {
        assert!(x.status.is_success(), "{}: {:?}", x.replication, x.error);
    }
    r
}

async fn attempt(e: &Env, names: &[&str]) -> RunReport {
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

fn q<T: duckdb::types::FromSql>(e: &Env, sql: &str) -> T {
    e.lake
        .open("lake")
        .expect("open")
        .query_row(sql, [], |r| r.get(0))
        .expect(sql)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn glob_with_templated_url_and_declared_types() {
    let e = setup();
    let r = go(&e, &["prices"]).await;
    assert_eq!(rows(&r, "prices").inserted, 3);
    assert_eq!(q::<f64>(&e, "SELECT sum(px) FROM lake.br.prices"), 6.75);
    assert_eq!(
        q::<String>(&e, "SELECT typeof(px) FROM lake.br.prices LIMIT 1"),
        "DOUBLE"
    );
    let m = &r.replications[0].streams[0].members[0];
    assert!(m.bytes.read > 0);
    std::fs::write(
        e.project.path().join("in/d1/prices_b.csv"),
        "ticker,px\nNVDA,4\n",
    )
    .expect("rewrite");
    let r = go(&e, &["prices"]).await;
    let n = rows(&r, "prices");
    assert_eq!((n.inserted, n.updated, n.unchanged), (0, 1, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunks_by_file_in_one_commit() {
    let e = setup();
    let r = go(&e, &["chunked"]).await;
    let m = &r.replications[0].streams[0].members[0];
    assert_eq!(m.chunks, 2);
    assert_eq!(rows(&r, "chunked").inserted, 3);
    std::fs::remove_file(e.project.path().join("in/d1/prices_b.csv")).expect("rm");
    let r = go(&e, &["chunked"]).await;
    assert_eq!(
        rows(&r, "chunked").deleted,
        1,
        "keyed full refresh across chunks deletes missing keys"
    );
    assert_eq!(
        q::<i64>(&e, "SELECT count(*) FROM lake.br.prices_chunked"),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_match_warns_and_skips() {
    let e = setup();
    let r = go(&e, &["missing"]).await;
    let m = &r.replications[0].streams[0].members[0];
    assert!(!m.warnings.is_empty() || !r.warnings.is_empty(), "{m:?}");
    let exists: i64 = q(
        &e,
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'missing'",
    );
    assert_eq!(exists, 0);
    assert_eq!(r.replications[0].status, ReplicationStatus::UpToDate);
}

fn tar_with(path: &std::path::Path, member: &str, body: &str) {
    let mut b = tar::Builder::new(std::fs::File::create(path).expect("create"));
    let mut h = tar::Header::new_gnu();
    h.set_size(body.len() as u64);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, member, body.as_bytes())
        .expect("append");
    b.finish().expect("finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_units_get_their_own_workdir() {
    const TARS: &str = r#"replications:
  tar_a:
    source: files
    target: lake
    streams:
      - id: t/a
        mode: full-refresh
        from: {files: {url: "in/a.tar", archive: {member: data.csv}}}
        object: br.tar_a
  tar_b:
    source: files
    target: lake
    streams:
      - id: t/b
        mode: full-refresh
        from: {files: {url: "in/b.tar", archive: {member: data.csv}}}
        object: br.tar_b
"#;
    let lake = LocalLake::file().expect("lake");
    let conns = format!(
        "{}  files: {{type: files}}\n",
        lake.connection_yaml("lake", "lake")
    );
    let project = TempProject::new()
        .and_then(|p| p.project(&conns, "state: {lake: lake}\n"))
        .and_then(|p| p.replication("tars", TARS))
        .expect("project");
    std::fs::create_dir_all(project.path().join("in")).expect("mkdir");
    tar_with(&project.path().join("in/a.tar"), "data.csv", "v\na\n");
    tar_with(&project.path().join("in/b.tar"), "data.csv", "v\nb\n");
    let e = Env { lake, project };
    // Both units extract before either commits, so a shared workdir would hand one the other's member.
    atlatl_core::fault::set("lake.after_stage@tar_a:sleep:1000,lake.after_stage@tar_b:sleep:1000")
        .expect("fault");
    let r = go(&e, &["tar_a", "tar_b"]).await;
    atlatl_core::fault::clear();
    assert_eq!(r.replications.len(), 2);
    assert_eq!(
        q::<String>(&e, "SELECT string_agg(v) FROM lake.br.tar_a"),
        "a"
    );
    assert_eq!(
        q::<String>(&e, "SELECT string_agg(v) FROM lake.br.tar_b"),
        "b"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_types_follow_on_cast_error() {
    let e = setup();
    let r = attempt(&e, &["strict"]).await;
    let err = r.replications[0].error.as_ref().expect("error");
    assert_eq!(err.code.as_deref(), Some("cast_failed"), "{err:?}");
    assert!(
        err.message
            .contains("`amt` \"12345.6789\" as DECIMAL(10,2)"),
        "{}",
        err.message
    );

    let r = go(&e, &["nulled"]).await;
    let m = &r.replications[0].streams[0].members[0];
    let w = m
        .warnings
        .iter()
        .find(|w| w.code == "cast_set_null")
        .expect("warning");
    assert!(w.message.starts_with("2 values "), "{}", w.message);
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(coalesce(amt::VARCHAR, '-'), ',' ORDER BY id) FROM lake.br.nulled"
        ),
        "12.50,-,-"
    );
    assert_eq!(
        q::<String>(
            &e,
            "SELECT string_agg(column_name, ',' ORDER BY column_index) FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'nulled'"
        ),
        "id,amt"
    );
}
