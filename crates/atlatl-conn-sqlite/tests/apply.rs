//! The SQLite-family applier against a SQLite file, the D1 fake and a libSQL server.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, ListArray, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use atlatl_conn_sqlite::{
    ApplyCounts, Client, CursorMode, CursorPlan, CursorWrite, DELETE_TS, Deletes, Target, Value,
};
use atlatl_core::config::connection::Connection;
use atlatl_core::config::model::{
    ConnectionCommon, ConnectionKind, D1Conn, DeleteValue, RetryConfig, SqliteConn, TursoConn,
};
use atlatl_core::config::node::Span;
use atlatl_core::config::types::{Dur, SecretValue};
use atlatl_core::error::ErrorClass;
use atlatl_testkit::d1_fake::{self, Fake};
use atlatl_testkit::{Libsql, libsql};
use futures::stream;

enum Backend {
    Sqlite(tempfile::TempDir),
    D1(Fake),
    Turso(Libsql),
}

impl Backend {
    async fn all() -> Vec<Backend> {
        vec![
            Backend::Sqlite(tempfile::tempdir().expect("dir")),
            Backend::D1(Fake::start().await),
            Backend::Turso(tokio::task::spawn_blocking(libsql).await.expect("libsql")),
        ]
    }

    fn name(&self) -> &'static str {
        match self {
            Backend::Sqlite(_) => "sqlite",
            Backend::D1(_) => "d1",
            Backend::Turso(_) => "turso",
        }
    }

    fn connection(&self, transaction_rows: u64) -> Connection {
        let kind = match self {
            Backend::Sqlite(d) => ConnectionKind::Sqlite(SqliteConn {
                path: d.path().join("app.db").display().to_string(),
                busy_timeout: None,
                create_tables: None,
            }),
            Backend::D1(f) => ConnectionKind::D1(D1Conn {
                account_id: d1_fake::ACCOUNT.into(),
                database_id: Some(d1_fake::DATABASE_ID.into()),
                database: None,
                api_token: SecretValue::Literal(d1_fake::TOKEN.into()),
                endpoint: Some(f.endpoint.clone()),
                statements_per_request: None,
                create_tables: None,
            }),
            Backend::Turso(l) => ConnectionKind::Turso(TursoConn {
                url: l.url.clone(),
                auth_token: None,
                transaction_rows: Some(transaction_rows),
                create_tables: None,
            }),
        };
        conn(kind)
    }

    async fn client(&self) -> Client {
        self.client_with(10_000).await
    }

    async fn client_with(&self, transaction_rows: u64) -> Client {
        let mut c = Client::open(&self.connection(transaction_rows))
            .await
            .expect("open");
        c.ensure_state().await.expect("state");
        c
    }
}

fn conn(kind: ConnectionKind) -> Connection {
    let fast = RetryConfig {
        max_attempts: Some(4),
        base: Some(Dur(Duration::from_millis(5))),
        max: Some(Dur(Duration::from_millis(20))),
        ..Default::default()
    };
    Connection {
        name: "app".into(),
        span: Span::new(Arc::from(Path::new("c.yaml")), 1, 1),
        common: ConnectionCommon {
            retry: Some(fast),
            ..Default::default()
        },
        kind,
        resolved_from: "base".into(),
    }
}

async fn text(c: &mut Client, sql: &str) -> Vec<String> {
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
                .join("|")
        })
        .collect()
}

fn target(table: &str, deletes: Deletes) -> Target {
    Target {
        table: table.into(),
        primary_key: vec!["id".into()],
        columns: vec!["id".into(), "v".into()],
        target_types: BTreeMap::new(),
        deletes,
    }
}

fn soft() -> Deletes {
    Deletes::Soft {
        column: "deleted_at".into(),
        value: DeleteValue::SnapshotTime,
        restore: true,
    }
}

fn rows(ids: &[i64], vs: &[&str]) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        ("v", Arc::new(StringArray::from(vs.to_vec())) as ArrayRef),
    ])
    .expect("batch")
}

fn keys(ids: &[i64], ts_us: i64) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        (
            DELETE_TS,
            Arc::new(TimestampMicrosecondArray::from(vec![ts_us; ids.len()]).with_timezone("UTC"))
                as ArrayRef,
        ),
    ])
    .expect("batch")
}

fn plan(stream: &str, expected: Option<i64>, to: i64, mode: CursorMode) -> CursorPlan {
    CursorPlan {
        expected,
        write: CursorWrite {
            stream_id: stream.into(),
            source_table: "src.t".into(),
            source_table_id: 7,
            snapshot_id: to,
            snapshot_time: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            mode,
            config_hash: "h".into(),
        },
    }
}

fn one<T>(b: T) -> impl futures::Stream<Item = atlatl_core::error::Result<T>> {
    stream::iter([Ok(b)])
}

async fn apply(
    c: &mut Client,
    t: &Target,
    plans: Vec<CursorPlan>,
    up: Option<RecordBatch>,
    del: Option<RecordBatch>,
) -> atlatl_core::error::Result<(ApplyCounts, ApplyCounts)> {
    let n = up.as_ref().map_or(0, |b| b.num_rows()) + del.as_ref().map_or(0, |b| b.num_rows());
    let mut tx = c.begin(plans, n as u64).await?;
    let u = match up {
        Some(b) => tx.upsert(t, one(b)).await?,
        None => ApplyCounts::default(),
    };
    let d = match del {
        Some(b) => tx.delete(t, one(b)).await?,
        None => ApplyCounts::default(),
    };
    tx.commit().await?;
    Ok((u, d))
}

fn counts(c: ApplyCounts) -> (u64, u64, u64, u64) {
    (
        c.rows_inserted,
        c.rows_updated,
        c.rows_unchanged,
        c.rows_deleted,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_guard_and_cursor_cas() {
    for b in Backend::all().await {
        let name = b.name();
        let mut c = b.client().await;
        let t = target("item", Deletes::Hard);
        c.prepare(&t, &rows(&[], &[]).schema())
            .await
            .expect("create");
        let (u, _) = apply(
            &mut c,
            &t,
            vec![plan("s", None, 5, CursorMode::Bootstrap)],
            Some(rows(&[1, 2, 3], &["a", "b", "c"])),
            None,
        )
        .await
        .expect(name);
        assert_eq!(counts(u), (3, 0, 0, 0), "{name}");
        assert_eq!(u.upserts_sent, 3);

        let (u, _) = apply(
            &mut c,
            &t,
            vec![plan("s", Some(5), 6, CursorMode::Cdc)],
            Some(rows(&[1, 2, 3, 4], &["a", "B", "c", "d"])),
            None,
        )
        .await
        .expect(name);
        assert_eq!(
            counts(u),
            (1, 1, 2, 0),
            "{name}: the guard skips unchanged rows"
        );
        assert_eq!(
            text(&mut c, "SELECT id, v FROM item ORDER BY id").await,
            ["1|a", "2|B", "3|c", "4|d"],
            "{name}"
        );
        assert_eq!(
            text(&mut c, "SELECT stream_id, snapshot_id, mode, pending_snapshot, snapshot_time FROM _atlatl_cursors").await,
            ["s|6|cdc|NULL|2023-11-14T22:13:20.000000Z"],
            "{name}"
        );

        let e = apply(
            &mut c,
            &t,
            vec![plan("s", Some(5), 7, CursorMode::Cdc)],
            Some(rows(&[1], &["moved"])),
            None,
        )
        .await
        .expect_err("stale cursor");
        assert_eq!(
            (e.class, e.code.as_deref()),
            (ErrorClass::Conflict, Some("cursor_moved")),
            "{name}: {e}"
        );
        if name != "d1" {
            assert_eq!(
                text(&mut c, "SELECT v FROM item WHERE id = 1").await,
                ["a"],
                "{name}: rolled back with the cursor"
            );
        }
        let e = apply(
            &mut c,
            &t,
            vec![plan("s", None, 7, CursorMode::Bootstrap)],
            None,
            None,
        )
        .await
        .expect_err("exists");
        assert_eq!(
            e.class,
            ErrorClass::Conflict,
            "{name}: a first cursor must not exist yet"
        );
        assert_eq!(
            c.read_cursors(&["s", "nope"])
                .await
                .expect("read")
                .iter()
                .map(|c| c.snapshot_id)
                .collect::<Vec<_>>(),
            [6]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_deletes_and_restore() {
    for b in Backend::all().await {
        let name = b.name();
        let mut c = b.client().await;
        let t = target("item", soft());
        c.prepare(&t, &rows(&[], &[]).schema())
            .await
            .expect("create");
        apply(
            &mut c,
            &t,
            vec![plan("s", None, 1, CursorMode::Bootstrap)],
            Some(rows(&[1, 2], &["a", "b"])),
            None,
        )
        .await
        .expect(name);
        let (_, d) = apply(
            &mut c,
            &t,
            vec![plan("s", Some(1), 2, CursorMode::Cdc)],
            None,
            Some(keys(&[1, 3], 1_700_000_000_000_000)),
        )
        .await
        .expect(name);
        assert_eq!(d.rows_deleted, 1, "{name}: unknown keys are no-ops");
        let (_, d) = apply(
            &mut c,
            &t,
            vec![plan("s", Some(2), 3, CursorMode::Cdc)],
            None,
            Some(keys(&[1], 1_800_000_000_000_000)),
        )
        .await
        .expect(name);
        assert_eq!(d.rows_deleted, 0, "{name}: already deleted keeps its stamp");
        assert_eq!(
            text(&mut c, "SELECT id, v, deleted_at FROM item ORDER BY id").await,
            ["1|a|2023-11-14T22:13:20.000000Z", "2|b|NULL"],
            "{name}"
        );
        let (u, _) = apply(
            &mut c,
            &t,
            vec![plan("s", Some(3), 4, CursorMode::Cdc)],
            Some(rows(&[1, 2], &["a", "b"])),
            None,
        )
        .await
        .expect(name);
        assert_eq!(
            counts(u),
            (0, 1, 1, 0),
            "{name}: restoring counts as an update"
        );
        assert_eq!(
            text(&mut c, "SELECT count(*) FROM item WHERE deleted_at IS NULL").await,
            ["2"],
            "{name}"
        );

        let flag = Target {
            deletes: Deletes::Soft {
                column: "gone".into(),
                value: DeleteValue::True,
                restore: true,
            },
            ..target("flagged", Deletes::Hard)
        };
        c.prepare(&flag, &rows(&[], &[]).schema())
            .await
            .expect("create");
        apply(
            &mut c,
            &flag,
            vec![plan("f", None, 1, CursorMode::Bootstrap)],
            Some(rows(&[1], &["x"])),
            None,
        )
        .await
        .expect(name);
        let (_, d) = apply(
            &mut c,
            &flag,
            vec![plan("f", Some(1), 2, CursorMode::Cdc)],
            None,
            Some(rows(&[1], &["x"])),
        )
        .await
        .expect(name);
        assert_eq!(d.rows_deleted, 1);
        assert_eq!(
            text(&mut c, "SELECT gone FROM flagged").await,
            ["1"],
            "{name}"
        );
        apply(
            &mut c,
            &flag,
            vec![plan("f", Some(2), 3, CursorMode::Cdc)],
            Some(rows(&[1], &["x"])),
            None,
        )
        .await
        .expect(name);
        assert_eq!(
            text(&mut c, "SELECT gone FROM flagged").await,
            ["0"],
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_deletes_child_first() {
    for b in Backend::all().await {
        let name = b.name();
        let mut c = b.client().await;
        c.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, v TEXT)")
            .await
            .expect("ddl");
        c.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, v TEXT REFERENCES parent (id))")
            .await
            .expect("ddl");
        let parent = target("parent", Deletes::Hard);
        let child = Target {
            columns: vec!["id".into(), "v".into()],
            ..target("child", Deletes::Hard)
        };
        let child_rows = || {
            RecordBatch::try_from_iter([
                ("id", Arc::new(Int64Array::from(vec![10])) as ArrayRef),
                ("v", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ])
            .expect("batch")
        };
        let plans = |e: Option<i64>, to| {
            vec![
                plan("p", e, to, CursorMode::Cdc),
                plan("c", e, to, CursorMode::Cdc),
            ]
        };
        let mut tx = c.begin(plans(None, 1), 2).await.expect(name);
        tx.upsert(&parent, one(rows(&[1], &["p"])))
            .await
            .expect(name);
        tx.upsert(&child, one(child_rows())).await.expect(name);
        tx.commit().await.expect(name);

        let mut tx = c.begin(plans(Some(1), 2), 2).await.expect(name);
        let d = tx
            .delete(&child, one(rows(&[10], &[""])))
            .await
            .expect(name);
        assert_eq!(d.rows_deleted, 1);
        let e = tx
            .upsert(&parent, one(rows(&[2], &["late"])))
            .await
            .expect_err("phase");
        assert_eq!(e.class, ErrorClass::Internal, "{name}");
        drop(tx);
        let mut c = b.client().await;
        let mut tx = c.begin(plans(Some(1), 2), 2).await.expect(name);
        tx.delete(&child, one(rows(&[10], &[""])))
            .await
            .expect(name);
        tx.delete(&parent, one(rows(&[1], &[""])))
            .await
            .expect(name);
        tx.commit().await.expect(name);
        assert_eq!(
            text(&mut c, "SELECT count(*) FROM parent").await,
            ["0"],
            "{name}"
        );
        assert_eq!(
            text(&mut c, "SELECT count(*) FROM child").await,
            ["0"],
            "{name}"
        );
        if name != "turso" {
            let mut tx = c.begin(plans(Some(2), 3), 2).await.expect(name);
            tx.upsert(&parent, one(rows(&[1], &["p"])))
                .await
                .expect(name);
            tx.upsert(&child, one(child_rows())).await.expect(name);
            tx.commit().await.expect(name);
            let mut tx = c.begin(plans(Some(3), 4), 2).await.expect(name);
            let e = tx
                .delete(&parent, one(rows(&[1], &[""])))
                .await
                .expect_err("parent first violates the FK");
            assert_eq!(e.class, ErrorClass::Data, "{name}: {e}");
        }
    }
}

fn typed() -> RecordBatch {
    let list =
        ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), Some(2)]), None]);
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![Some(123_456), None])
                    .with_precision_and_scale(18, 4)
                    .expect("dec"),
            ) as ArrayRef,
        ),
        (
            "price",
            Arc::new(
                Decimal128Array::from(vec![150, 5])
                    .with_precision_and_scale(10, 2)
                    .expect("dec"),
            ) as ArrayRef,
        ),
        (
            "at",
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_700_000_000_123_456, 0])
                    .with_timezone("UTC"),
            ) as ArrayRef,
        ),
        (
            "at_ms",
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_700_000_000_123_456, 0])
                    .with_timezone("UTC"),
            ) as ArrayRef,
        ),
        (
            "ok",
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ),
        (
            "x",
            Arc::new(Float64Array::from(vec![0.5, f64::NAN])) as ArrayRef,
        ),
        ("tags", Arc::new(list) as ArrayRef),
    ])
    .expect("batch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_tables_and_type_mapping() {
    for b in Backend::all().await {
        let name = b.name();
        let mut c = b.client().await;
        let batch = typed();
        let cols: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let t = Target {
            table: "typed".into(),
            primary_key: vec!["id".into()],
            columns: cols,
            target_types: [
                ("price".to_owned(), "real".to_owned()),
                ("at_ms".to_owned(), "epoch_ms".to_owned()),
            ]
            .into(),
            deletes: soft(),
        };
        let info = c.prepare(&t, &batch.schema()).await.expect(name);
        let decls: Vec<String> = info
            .columns
            .iter()
            .map(|(n, d)| format!("{n}:{d}"))
            .collect();
        assert_eq!(
            decls,
            [
                "id:INTEGER",
                "amount:TEXT",
                "price:REAL",
                "at:TEXT",
                "at_ms:INTEGER",
                "ok:INTEGER",
                "x:REAL",
                "tags:TEXT",
                "deleted_at:TEXT"
            ],
            "{name}"
        );
        assert_eq!(info.primary_key, ["id"]);
        let (u, _) = apply(
            &mut c,
            &t,
            vec![plan("s", None, 1, CursorMode::Bootstrap)],
            Some(batch.clone()),
            None,
        )
        .await
        .expect(name);
        assert_eq!(u.rows_inserted, 2);
        assert_eq!(
            text(&mut c, "SELECT amount, typeof(amount), price, at, at_ms, ok, x, tags FROM typed ORDER BY id").await,
            [
                "12.3456|text|1.5|2023-11-14T22:13:20.123456Z|1700000000123|1|0.5|[1,2]",
                "NULL|null|0.05|1970-01-01T00:00:00.000000Z|0|0|NULL|NULL"
            ],
            "{name}"
        );
        let (u, _) = apply(
            &mut c,
            &t,
            vec![plan("s", Some(1), 2, CursorMode::Cdc)],
            Some(batch),
            None,
        )
        .await
        .expect(name);
        assert_eq!(
            counts(u),
            (0, 0, 2, 0),
            "{name}: re-applying identical rows writes nothing"
        );

        let e = c
            .prepare(
                &Target {
                    columns: vec!["id".into(), "nope".into()],
                    ..target("typed", Deletes::Hard)
                },
                &rows(&[], &[]).schema(),
            )
            .await
            .expect_err("col");
        assert_eq!(e.class, ErrorClass::Config, "{name}");
        c.execute("CREATE TABLE nokey (id INTEGER, v TEXT)")
            .await
            .expect("ddl");
        let e = c
            .prepare(&target("nokey", Deletes::Hard), &rows(&[], &[]).schema())
            .await
            .expect_err("pk");
        assert!(e.message.contains("unique index"), "{name}: {e}");
        c.execute("CREATE UNIQUE INDEX nokey_id ON nokey (id)")
            .await
            .expect("ddl");
        c.prepare(&target("nokey", Deletes::Hard), &rows(&[], &[]).schema())
            .await
            .expect("a unique index covers the key");
        let e = c
            .prepare(&target("nokey", soft()), &rows(&[], &[]).schema())
            .await
            .expect_err("soft col");
        assert!(e.message.contains("deleted_at"), "{name}: {e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_tables_off() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Client::open(&conn(ConnectionKind::Sqlite(SqliteConn {
        path: dir.path().join("x.db").display().to_string(),
        busy_timeout: None,
        create_tables: Some(false),
    })))
    .await
    .expect("open");
    let e = c
        .prepare(&target("item", Deletes::Hard), &rows(&[], &[]).schema())
        .await
        .expect_err("missing");
    assert!(e.message.contains("create_tables"), "{e}");
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Duration(TimeUnit::Second), true),
    ]);
    let e = c
        .prepare(&target("item", Deletes::Hard), &schema)
        .await
        .expect_err("type");
    assert!(e.message.contains("no SQLite mapping"), "{e}");
}

/// A trigger fails the apply on the row `v = 'boom'`, after earlier batches landed.
async fn arm(c: &mut Client) {
    c.execute("CREATE TRIGGER boom BEFORE INSERT ON item WHEN NEW.v = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END")
        .await
        .expect("trigger");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rerun_after_mid_window_failure() {
    for b in Backend::all().await {
        let name = b.name();
        let mut c = b.client_with(2).await;
        c.set_max_rows(2);
        let t = target("item", Deletes::Hard);
        c.prepare(&t, &rows(&[], &[]).schema())
            .await
            .expect("create");
        apply(
            &mut c,
            &t,
            vec![plan("s", None, 1, CursorMode::Bootstrap)],
            Some(rows(&[1], &["a"])),
            None,
        )
        .await
        .expect(name);
        arm(&mut c).await;
        let window = rows(&[1, 2, 3, 4, 5, 6], &["A", "b", "c", "d", "boom", "f"]);
        let e = apply(
            &mut c,
            &t,
            vec![plan("s", Some(1), 9, CursorMode::Cdc)],
            Some(window.clone()),
            Some(rows(&[1], &[""])),
        )
        .await
        .expect_err("boom");
        assert_eq!(e.class, ErrorClass::Data, "{name}: {e}");
        let cur = c.read_cursors(&["s"]).await.expect("read").remove(0);
        assert_eq!(
            cur.snapshot_id, 1,
            "{name}: the cursor stays until the last batch"
        );
        let landed = text(
            &mut c,
            "SELECT group_concat(id) FROM (SELECT id FROM item ORDER BY id)",
        )
        .await;
        match name {
            "sqlite" => {
                assert_eq!(cur.pending, None);
                assert_eq!(landed, ["1"], "one transaction: nothing landed");
            }
            _ => {
                assert_eq!(
                    cur.pending.as_ref().map(|p| p.snapshot_id),
                    Some(9),
                    "{name}: pending marks the window"
                );
                assert_eq!(landed, ["1,2,3,4"], "{name}: earlier batches landed");
            }
        }
        if name != "sqlite" {
            let e = apply(
                &mut c,
                &t,
                vec![plan("s", Some(1), 10, CursorMode::Cdc)],
                Some(rows(&[7], &["g"])),
                None,
            )
            .await
            .expect_err("other window");
            assert_eq!(
                e.class,
                ErrorClass::Conflict,
                "{name}: a different window can't start while one is pending"
            );
        }
        c.execute("DROP TRIGGER boom").await.expect("drop");
        let (u, d) = apply(
            &mut c,
            &t,
            vec![plan("s", Some(1), 9, CursorMode::Cdc)],
            Some(window),
            Some(rows(&[1], &[""])),
        )
        .await
        .expect(name);
        assert_eq!(u.upserts_sent, 6, "{name}");
        assert_eq!(d.rows_deleted, 1, "{name}");
        assert_eq!(
            text(
                &mut c,
                "SELECT group_concat(id || v) FROM (SELECT id, v FROM item ORDER BY id)"
            )
            .await,
            ["2b,3c,4d,5boom,6f"],
            "{name}"
        );
        let cur = c.read_cursors(&["s"]).await.expect("read").remove(0);
        assert_eq!((cur.snapshot_id, cur.pending), (9, None), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turso_small_windows_are_one_transaction() {
    let l = tokio::task::spawn_blocking(libsql).await.expect("libsql");
    let b = Backend::Turso(l);
    let mut c = b.client_with(100).await;
    c.set_max_rows(2);
    let t = target("item", Deletes::Hard);
    c.prepare(&t, &rows(&[], &[]).schema())
        .await
        .expect("create");
    apply(
        &mut c,
        &t,
        vec![plan("s", None, 1, CursorMode::Bootstrap)],
        Some(rows(&[1], &["a"])),
        None,
    )
    .await
    .expect("seed");
    arm(&mut c).await;
    let e = apply(
        &mut c,
        &t,
        vec![plan("s", Some(1), 2, CursorMode::Cdc)],
        Some(rows(&[2, 3, 4], &["b", "c", "boom"])),
        None,
    )
    .await
    .expect_err("boom");
    assert_eq!(e.class, ErrorClass::Data);
    assert_eq!(
        text(&mut c, "SELECT count(*) FROM item").await,
        ["1"],
        "rolled back"
    );
    assert_eq!(
        c.read_cursors(&["s"]).await.expect("read")[0].pending,
        None,
        "no pending marker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d1_http_contract() {
    let f = Fake::start().await;
    f.state().other_databases = 45;
    let b = Backend::D1(f.clone());
    let mut cfg = b.connection(0);
    if let ConnectionKind::D1(d) = &mut cfg.kind {
        d.database_id = None;
        d.database = Some(d1_fake::DATABASE_NAME.into());
        d.statements_per_request = Some(3);
    }
    let mut c = Client::open(&cfg).await.expect("resolved by name");
    let pages: Vec<String> = f
        .calls()
        .iter()
        .filter(|c| c.method == "GET")
        .map(|c| c.path.clone())
        .collect();
    assert_eq!(pages.len(), 3, "20 per page: the name is on page 3");
    c.ensure_state().await.expect("state");

    let t = Target {
        columns: (0..30)
            .map(|i| {
                if i == 0 {
                    "id".to_owned()
                } else {
                    format!("c{i}")
                }
            })
            .collect(),
        ..target("wide", Deletes::Hard)
    };
    let batch = RecordBatch::try_from_iter(t.columns.iter().map(|n| {
        (
            n.as_str(),
            Arc::new(Int64Array::from((0..10).collect::<Vec<i64>>())) as ArrayRef,
        )
    }))
    .expect("batch");
    c.prepare(&t, &batch.schema()).await.expect("create");
    let before = f.calls().len();
    let (u, _) = apply(
        &mut c,
        &t,
        vec![plan("w", None, 1, CursorMode::Bootstrap)],
        Some(batch),
        None,
    )
    .await
    .expect("apply");
    assert_eq!(u.rows_inserted, 10);
    let bodies: Vec<serde_json::Value> =
        f.calls()[before..].iter().map(|c| c.body.clone()).collect();
    let stmts: Vec<&serde_json::Value> = bodies
        .iter()
        .flat_map(|b| {
            b.get("batch")
                .and_then(|x| x.as_array())
                .map_or_else(|| vec![b], |a| a.iter().collect())
        })
        .collect();
    assert!(
        stmts
            .iter()
            .all(|s| s["params"].as_array().is_some_and(|p| p.len() <= 100)),
        "≤ 100 bound parameters per statement"
    );
    assert!(
        stmts
            .iter()
            .any(|s| s["params"].as_array().is_some_and(|p| p.len() == 90)),
        "floor(100 / 30) = 3 rows per upsert"
    );
    assert!(
        bodies.iter().any(|b| b
            .get("batch")
            .is_some_and(|x| x.as_array().is_some_and(|a| a.len() > 1))),
        "several statements per request"
    );

    f.state()
        .inject
        .push_back((429, "{}".into(), vec![("retry-after".into(), "0".into())]));
    f.state()
        .inject
        .push_back((200, "<html>oops</html>".into(), vec![]));
    f.state().inject.push_back((503, "{}".into(), vec![]));
    assert_eq!(
        text(&mut c, "SELECT count(*) FROM wide").await,
        ["10"],
        "throttled, non-JSON and 5xx are retried"
    );
    for _ in 0..4 {
        f.state().inject.push_back((500, "{}".into(), vec![]));
    }
    let e = c.query("SELECT 1", vec![]).await.expect_err("gives up");
    assert_eq!(e.class, ErrorClass::Transient);
    let e = c
        .query("SELECT * FROM missing", vec![])
        .await
        .expect_err("sql");
    assert_eq!(e.class, ErrorClass::Config, "{e}");

    f.state().lose_responses = 1;
    let t = target("item", Deletes::Hard);
    c.prepare(&t, &rows(&[], &[]).schema())
        .await
        .expect("create");
    apply(
        &mut c,
        &t,
        vec![plan("s", None, 1, CursorMode::Bootstrap)],
        Some(rows(&[1], &["a"])),
        None,
    )
    .await
    .expect("seed");
    let n = f.calls().len();
    let mut tx = c
        .begin(vec![plan("s", Some(1), 2, CursorMode::Cdc)], 1)
        .await
        .expect("begin");
    tx.upsert(&t, one(rows(&[1], &["b"])))
        .await
        .expect("upsert");
    f.state().lose_responses = 1;
    tx.commit()
        .await
        .expect("a lost response to the final flip is verified, not a conflict");
    assert!(f.calls().len() > n);
    assert_eq!(f.rows("SELECT snapshot_id || ':' || coalesce(pending_snapshot, '-') FROM _atlatl_cursors WHERE stream_id = 's'"), ["2:-"]);

    let mut bad = b.connection(0);
    if let ConnectionKind::D1(d) = &mut bad.kind {
        d.api_token = SecretValue::Literal("wrong".into());
    }
    let mut c = Client::open(&bad).await.expect("open by id needs no call");
    assert_eq!(
        c.query("SELECT 1", vec![]).await.expect_err("auth").class,
        ErrorClass::Auth
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d1_atomic_batches_and_limits() {
    let f = Fake::start().await;
    f.state().atomic_batches = true;
    let b = Backend::D1(f.clone());
    let mut cfg = b.connection(0);
    if let ConnectionKind::D1(d) = &mut cfg.kind {
        d.statements_per_request = Some(10);
    }
    let mut c = Client::open(&cfg).await.expect("open");
    c.ensure_state().await.expect("state");
    let t = Target {
        columns: (0..101)
            .map(|i| {
                if i == 0 {
                    "id".to_owned()
                } else {
                    format!("c{i}")
                }
            })
            .collect(),
        ..target("too_wide", Deletes::Hard)
    };
    let schema = Schema::new(
        t.columns
            .iter()
            .map(|n| Field::new(n, DataType::Int64, true))
            .collect::<Vec<_>>(),
    );
    let e = c.prepare(&t, &schema).await.expect_err("wide");
    assert!(e.message.contains("100 bound parameters"), "{e}");
    let blob = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Binary, true),
    ]);
    assert!(
        c.prepare(&target("b", Deletes::Hard), &blob)
            .await
            .expect_err("blob")
            .message
            .contains("BLOB")
    );
    let e = c
        .query(&format!("SELECT '{}'", "x".repeat(100_001)), vec![])
        .await
        .expect_err("size");
    assert_eq!(e.class, ErrorClass::Data);
}
