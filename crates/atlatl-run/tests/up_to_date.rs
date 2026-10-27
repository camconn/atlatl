//! M8.8: an ingest rerun with nothing to change commits nothing and reports `up_to_date`
//! (05 §4, 10 §4): no data rows move, the state value is unchanged, and run-varying metadata
//! (`loaded_at`, `run_id`) isn't a change by itself.

use std::sync::Arc;

use atlatl_core::obs::report::{ReplicationStatus, RunReport, StreamStatus};
use atlatl_core::plan::PlanRequest;
use atlatl_core::template::{EnvSource, MapEnv};
use atlatl_run::{RunArgs, run};
use atlatl_testkit::{LocalLake, TempProject};
use tokio_util::sync::CancellationToken;

const REPS: &str = r#"replications:
  inc_pk:
    source: lake
    target: lake
    streams:
      - {id: u/inc, mode: incremental, from: {table: src.people}, object: br.inc, primary_key: [id]}
  inc_uk:
    source: lake
    target: lake
    streams:
      - {id: u/uk, mode: incremental, from: {table: src.events}, object: br.uk, primary_key: [id], update_key: updated_at}
  full_pk:
    source: lake
    target: lake
    streams:
      - {id: u/full, mode: full-refresh, from: {table: src.people}, object: br.full, primary_key: [id]}
  stamped:
    source: lake
    target: lake
    streams:
      - id: u/stamped
        mode: incremental
        from: {table: src.people}
        object: br.stamped
        primary_key: [id]
        metadata: {loaded_at: {name: _legacy_loaded_at, type: epoch_s_int}, run_id: {}}
  two:
    source: lake
    target: lake
    streams:
      - {id: u/a, mode: full-refresh, from: {table: src.people}, object: br.two_a, primary_key: [id]}
      - {id: u/b, mode: full-refresh, from: {table: src.events}, object: br.two_b, primary_key: [id]}
  snap:
    source: lake
    target: lake
    streams:
      - {id: u/snap, mode: snapshot, from: {table: src.people}, object: br.snap, metadata: {loaded_at: {}}}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    lake.open("lake")
        .expect("open")
        .execute_batch(
            "CREATE SCHEMA lake.src;
             CREATE TABLE lake.src.people AS SELECT range AS id, 'p' || range AS name FROM range(1, 11);
             CREATE TABLE lake.src.events AS SELECT range AS id, TIMESTAMP '2025-01-01' + INTERVAL (range) HOUR AS updated_at, 'v1' AS v FROM range(1, 6);",
        )
        .expect("seed");
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml("lake", "lake"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", REPS))
        .expect("project");
    Env { lake, project }
}

async fn go(e: &Env, reps: &[&str]) -> RunReport {
    let (p, d) = atlatl_core::config::load(e.project.path());
    assert!(!d.has_errors(), "{d:?}");
    let vars: Arc<dyn EnvSource> = Arc::new(MapEnv::default());
    let args = RunArgs {
        plan: PlanRequest {
            replications: reps.iter().map(|r| (*r).to_owned()).collect(),
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

fn rep<'a>(r: &'a RunReport, name: &str) -> &'a atlatl_core::obs::report::ReplicationReport {
    r.replications
        .iter()
        .find(|x| x.replication == name)
        .unwrap_or_else(|| panic!("no {name}"))
}

fn status(r: &RunReport, name: &str) -> ReplicationStatus {
    let x = rep(r, name);
    assert!(x.error.is_none(), "{name}: {:?}", x.error);
    x.status
}

fn stream_status(r: &RunReport, name: &str, stream: &str) -> StreamStatus {
    rep(r, name)
        .streams
        .iter()
        .find(|s| s.stream_id == stream)
        .map(|s| s.status)
        .expect("stream")
}

fn member<'a>(
    r: &'a RunReport,
    name: &str,
    stream: &str,
) -> &'a atlatl_core::obs::report::MemberReport {
    rep(r, name)
        .streams
        .iter()
        .find(|s| s.stream_id == stream)
        .and_then(|s| s.members.first())
        .expect("member")
}

fn strings(e: &Env, sql: &str) -> Vec<String> {
    let c = e.lake.open("lake").expect("open");
    let mut s = c.prepare(sql).expect(sql);
    s.query_map([], |r| r.get::<_, Option<String>>(0))
        .expect(sql)
        .map(|r| r.expect("row").unwrap_or_default())
        .collect()
}

fn one(e: &Env, sql: &str) -> String {
    strings(e, sql).into_iter().next().expect(sql)
}

/// Snapshots that changed the table (by id or, when it was created, by name).
fn touched(e: &Env, schema: &str, table: &str) -> usize {
    let id = one(
        e,
        &format!(
            "SELECT i.table_id::VARCHAR FROM ducklake_table_info('lake') i JOIN __ducklake_metadata_lake.ducklake_schema s USING (schema_id) WHERE s.end_snapshot IS NULL AND s.schema_name = '{schema}' AND i.table_name = '{table}'"
        ),
    );
    strings(e, "SELECT changes::VARCHAR FROM ducklake_snapshots('lake')")
        .iter()
        .filter(|ch| {
            ch.contains(&format!("{schema}.{table}"))
                || ch
                    .split(['[', ']', ',', ' ', '{', '}', '='])
                    .any(|t| t.trim() == id)
        })
        .count()
}

fn state_of(e: &Env, stream: &str) -> (String, String) {
    let v = strings(
        e,
        &format!(
            "SELECT value::VARCHAR || chr(31) || run_id FROM lake._atlatl.stream_state WHERE stream_id = '{stream}'"
        ),
    );
    let (a, b) = v[0].split_once('\u{1f}').expect("state row");
    (a.to_owned(), b.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_reruns_commit_nothing() {
    let e = setup();
    let reps = ["inc_pk", "inc_uk", "full_pk", "stamped"];
    let first = go(&e, &reps).await;
    for r in reps {
        assert_eq!(status(&first, r), ReplicationStatus::Succeeded, "{r}");
    }
    let tables = [
        ("br", "inc"),
        ("br", "uk"),
        ("br", "full"),
        ("br", "stamped"),
        ("_atlatl", "stream_state"),
    ];
    let before: Vec<usize> = tables.iter().map(|(s, t)| touched(&e, s, t)).collect();
    let stamps = || {
        strings(
            &e,
            "SELECT DISTINCT _legacy_loaded_at::VARCHAR || ' ' || _atlatl_run_id FROM lake.br.stamped",
        )
    };
    let stamped = stamps();
    assert_eq!(stamped.len(), 1);
    let (uk_value, uk_run) = state_of(&e, "u/uk");
    assert_eq!(uk_run, first.run_id);

    // `loaded_at` is epoch seconds: make sure the second run's stamp differs.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let again = go(&e, &reps).await;
    for (r, s) in [
        ("inc_pk", "u/inc"),
        ("inc_uk", "u/uk"),
        ("full_pk", "u/full"),
        ("stamped", "u/stamped"),
    ] {
        assert_eq!(status(&again, r), ReplicationStatus::UpToDate, "{r}");
        assert_eq!(stream_status(&again, r, s), StreamStatus::UpToDate, "{s}");
        let m = member(&again, r, s);
        assert_eq!(m.lake_snapshot, None, "{s}");
        assert_eq!(m.rows.written(), 0, "{s}: {:?}", m.rows);
        assert!(m.rows.unchanged > 0, "{s}: {:?}", m.rows);
    }
    assert_eq!(again.exit_code, 0);
    let after: Vec<usize> = tables.iter().map(|(s, t)| touched(&e, s, t)).collect();
    assert_eq!(
        after, before,
        "no snapshot touched a target or the state table"
    );
    assert_eq!(stamps(), stamped, "unchanged rows keep their load stamp");
    assert_eq!(
        state_of(&e, "u/uk"),
        (uk_value.clone(), first.run_id.clone()),
        "an unchanged watermark isn't rewritten: its run_id is the run that set it"
    );
    let m = member(&again, "inc_uk", "u/uk");
    assert_eq!(m.state.before, m.state.after);
    let recorded = strings(
        &e,
        &format!(
            "SELECT status FROM lake._atlatl.run_replications WHERE run_id = '{}' ORDER BY replication",
            again.run_id
        ),
    );
    assert_eq!(
        recorded, ["up_to_date"; 4],
        "run history records the outcome"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_at_moves_only_with_the_row() {
    let e = setup();
    let first = go(&e, &["stamped"]).await;
    assert_eq!(status(&first, "stamped"), ReplicationStatus::Succeeded);
    let old = one(
        &e,
        "SELECT DISTINCT _legacy_loaded_at::VARCHAR FROM lake.br.stamped",
    );
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch(
            "UPDATE lake.src.people SET name = 'renamed' WHERE id = 1;
             INSERT INTO lake.src.people VALUES (11, 'p11');",
        )
        .expect("mutate");
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let r = go(&e, &["stamped"]).await;
    assert_eq!(status(&r, "stamped"), ReplicationStatus::Succeeded);
    let n = member(&r, "stamped", "u/stamped").rows;
    assert_eq!((n.inserted, n.updated, n.unchanged), (1, 1, 9));
    let fresh = strings(
        &e,
        &format!(
            "SELECT id::VARCHAR FROM lake.br.stamped WHERE _legacy_loaded_at::VARCHAR <> '{old}' AND _atlatl_run_id = '{}' ORDER BY id",
            r.run_id
        ),
    );
    assert_eq!(
        fresh,
        ["1", "11"],
        "new and changed rows get the run's stamp"
    );
    assert_eq!(
        one(
            &e,
            &format!(
                "SELECT count(*)::VARCHAR FROM lake.br.stamped WHERE _legacy_loaded_at::VARCHAR = '{old}' AND _atlatl_run_id = '{}'",
                first.run_id
            )
        ),
        "9",
        "the rest keep the first run's"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_moved_state_value_still_commits() {
    let e = setup();
    go(&e, &["inc_uk"]).await;
    let (value, _) = state_of(&e, "u/uk");
    assert!(value.contains("2025-01-01 05:00:00"), "{value}");
    let rewound = value.replace("05:00:00", "03:00:00");
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch(&format!(
            "UPDATE lake._atlatl.stream_state SET value = '{rewound}' WHERE stream_id = 'u/uk'"
        ))
        .expect("rewind");
    let data = touched(&e, "br", "uk");
    let state = touched(&e, "_atlatl", "stream_state");

    let r = go(&e, &["inc_uk"]).await;
    assert_eq!(
        status(&r, "inc_uk"),
        ReplicationStatus::Succeeded,
        "the floor moved"
    );
    let m = member(&r, "inc_uk", "u/uk");
    assert_eq!(
        (m.rows.staged, m.rows.written(), m.rows.unchanged),
        (3, 0, 3),
        "re-read from the rewound floor, all unchanged"
    );
    assert!(m.lake_snapshot.is_some());
    assert_eq!(state_of(&e, "u/uk"), (value, r.run_id.clone()));
    assert_eq!(touched(&e, "br", "uk"), data, "no data rows moved");
    assert_eq!(touched(&e, "_atlatl", "stream_state"), state + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_streams_of_a_committed_replication_are_up_to_date() {
    let e = setup();
    go(&e, &["two", "snap"]).await;
    e.lake
        .open("lake")
        .expect("open")
        .execute_batch("UPDATE lake.src.people SET name = 'renamed' WHERE id = 2")
        .expect("mutate");
    let r = go(&e, &["two", "snap"]).await;
    assert_eq!(status(&r, "two"), ReplicationStatus::Succeeded);
    assert_eq!(stream_status(&r, "two", "u/a"), StreamStatus::Succeeded);
    assert_eq!(stream_status(&r, "two", "u/b"), StreamStatus::UpToDate);
    assert_eq!(member(&r, "two", "u/a").rows.updated, 1);

    // A snapshot appends every run: never a no-op.
    assert_eq!(status(&r, "snap"), ReplicationStatus::Succeeded);
    assert_eq!(member(&r, "snap", "u/snap").rows.inserted, 10);
    assert_eq!(one(&e, "SELECT count(*)::VARCHAR FROM lake.br.snap"), "20");
}
