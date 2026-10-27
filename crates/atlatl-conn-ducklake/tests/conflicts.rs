//! Commit conflicts between two engines (two processes) on one Postgres catalog (spike S3 §3).
//! DuckLake's own retry stays on: it resolves the race for the next snapshot id, and what's left
//! is a per-table logical conflict, classed `conflict` / `commit_conflict`.

use atlatl_conn_ducklake::{Lake, attach};
use atlatl_core::ErrorClass;
use atlatl_core::config::model::{DuckLakeConn, EngineConfig};
use atlatl_core::config::types::{Auto, ByteSize};
use atlatl_engine::error::COMMIT_CONFLICT;
use atlatl_engine::{Budget, BudgetParams, DbResultExt, Detected, Engine, EngineOptions, Session};
use atlatl_testkit::{Postgres, pg16};
use serde_json::json;

struct Side {
    s: Session,
    lake: Lake,
    _engine: Engine,
    _spill: tempfile::TempDir,
}

impl Side {
    async fn open(c: &DuckLakeConn) -> Self {
        let spill = tempfile::tempdir().expect("tempdir");
        let cfg = EngineConfig {
            memory: Some(Auto::Value(ByteSize(1 << 30))),
            threads: Some(Auto::Value(2)),
            temp_directory: Some(spill.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        let b = Budget::compute(&cfg, None, &Detected::detect(), &BudgetParams::default())
            .expect("budget");
        let mut opts = EngineOptions::new(b);
        opts.extension_dir = Some(atlatl_testkit::extension_dir());
        let engine = Engine::open(opts).expect("engine");
        let lake = attach(&engine, "lake", c).await.expect("attach");
        Self {
            s: engine.session().expect("session"),
            lake,
            _engine: engine,
            _spill: spill,
        }
    }

    async fn exec(&mut self, sql: &str) -> atlatl_core::error::Result<()> {
        let sql = sql.to_owned();
        self.s.run(move |c| c.execute_batch(&sql).db()).await
    }

    async fn ok(&mut self, sql: &str) {
        self.exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn catalog(pg: &Postgres, db: &str, data: &str, inlining: Option<u64>) -> DuckLakeConn {
    let mut v = json!({
        "catalog": {"type": "postgres", "host": pg.host, "port": pg.port, "database": db,
                    "user": pg.user, "password": pg.password, "sslmode": "disable"},
        "data_path": data,
        "storage": {"type": "local"},
    });
    if let Some(n) = inlining {
        v["data_inlining_row_limit"] = json!(n);
    }
    serde_json::from_value(v).expect("ducklake connection")
}

/// (case, A's statements, B's statements, B's commit conflicts)
const MATRIX: [(&str, &str, &str, bool); 7] = [
    (
        "insert x / insert x",
        "INSERT INTO lake.x VALUES (1000, 1)",
        "INSERT INTO lake.x VALUES (1001, 1)",
        false,
    ),
    (
        "update x / update y",
        "UPDATE lake.x SET v = 1 WHERE id = 1",
        "UPDATE lake.y SET v = 1 WHERE id = 1",
        false,
    ),
    (
        "update x / update other rows of x",
        "UPDATE lake.x SET v = 2 WHERE id = 2",
        "UPDATE lake.x SET v = 2 WHERE id = 3",
        true,
    ),
    (
        "merge x / merge other rows of x",
        "MERGE INTO lake.x t USING (SELECT 5 id, 9 v) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
        "MERGE INTO lake.x t USING (SELECT 6 id, 9 v) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
        true,
    ),
    (
        "delete x / insert x",
        "DELETE FROM lake.x WHERE id = 7",
        "INSERT INTO lake.x VALUES (2001, 1)",
        true,
    ),
    (
        "alter x / insert x",
        "ALTER TABLE lake.x ADD COLUMN w INT",
        "INSERT INTO lake.x (id, v) VALUES (2000, 1)",
        true,
    ),
    (
        "insert x / delete-all + insert x",
        "INSERT INTO lake.x VALUES (3000, 1)",
        "DELETE FROM lake.x; INSERT INTO lake.x VALUES (1, 1)",
        true,
    ),
];

async fn matrix(inlining: Option<u64>) {
    let pg = pg16();
    let db = pg.fresh_db().expect("db");
    let data = tempfile::tempdir().expect("data");
    let c = catalog(pg, &db, &format!("{}/", data.path().display()), inlining);
    let (mut a, mut b) = (Side::open(&c).await, Side::open(&c).await);
    for (case, sa, sb, conflicts) in MATRIX {
        a.ok("DROP TABLE IF EXISTS lake.x; DROP TABLE IF EXISTS lake.y;
              CREATE TABLE lake.x AS SELECT i AS id, 0 AS v FROM range(100) t(i);
              CREATE TABLE lake.y AS SELECT i AS id, 0 AS v FROM range(100) t(i);")
            .await;
        a.ok("BEGIN").await;
        b.ok("BEGIN").await;
        a.ok(sa).await;
        b.ok(sb).await;
        a.ok("COMMIT").await;
        match (b.exec("COMMIT").await, conflicts) {
            (Ok(()), false) => {}
            (Err(e), true) => {
                assert_eq!(
                    (e.class, e.code.as_deref()),
                    (ErrorClass::Conflict, Some(COMMIT_CONFLICT)),
                    "{case}: {e}"
                );
                assert!(e.message.contains("Transaction conflict"), "{case}: {e}");
                let _ = b.exec("ROLLBACK").await;
                let lake = b.lake.clone();
                let named =
                    b.s.run(move |c| Ok(lake.name_conflict(c, e)))
                        .await
                        .expect("name");
                assert!(
                    named.message.contains("table `main.x` (index \""),
                    "{case}: {named}"
                );
                assert_eq!(named.code.as_deref(), Some(COMMIT_CONFLICT));
                // 03 §6: the retry re-runs the whole transaction; the failed one is gone.
                b.ok(&format!("BEGIN; {sb}; COMMIT")).await;
            }
            (r, _) => panic!("{case} (inlining {inlining:?}): B's commit gave {r:?}"),
        }
    }
    let msg = "Transaction conflict - attempting to insert into table with index \"999999\" - x";
    let unknown = Lake {
        alias: "nope".into(),
        ..b.lake.clone()
    };
    for lake in [b.lake.clone(), unknown] {
        let e = atlatl_core::AtlatlError::new(ErrorClass::Conflict, msg).code(COMMIT_CONFLICT);
        let got =
            b.s.run(move |c| Ok(lake.name_conflict(c, e)))
                .await
                .expect("name");
        assert_eq!(got.message, msg, "falls back to the id");
    }
}

#[tokio::test]
async fn conflict_matrix_parquet() {
    matrix(Some(0)).await;
}

#[tokio::test]
async fn conflict_matrix_inlined() {
    matrix(None).await;
}

#[tokio::test]
async fn with_ducklake_retries_off_disjoint_commits_race_for_the_snapshot_id() {
    let pg = pg16();
    let db = pg.fresh_db().expect("db");
    let data = tempfile::tempdir().expect("data");
    let c = catalog(pg, &db, &format!("{}/", data.path().display()), Some(0));
    let (mut a, mut b) = (Side::open(&c).await, Side::open(&c).await);
    a.ok("CREATE TABLE lake.x AS SELECT 1 AS id; CREATE TABLE lake.y AS SELECT 1 AS id")
        .await;
    b.ok("SET ducklake_max_retry_count = 0").await;
    a.ok("BEGIN; INSERT INTO lake.x VALUES (2)").await;
    b.ok("BEGIN; INSERT INTO lake.y VALUES (2)").await;
    a.ok("COMMIT").await;
    let e = b.exec("COMMIT").await.expect_err("no DuckLake retry");
    assert_eq!(
        (e.class, e.code.as_deref()),
        (ErrorClass::Conflict, Some(COMMIT_CONFLICT)),
        "{e}"
    );
    assert!(e.message.contains("ducklake_snapshot_pkey"), "{e}");
}
