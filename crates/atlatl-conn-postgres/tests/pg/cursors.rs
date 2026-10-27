use std::time::{Duration, SystemTime};

use atlatl_conn_postgres::cursors::CURSOR_CONFLICT_CODE;
use atlatl_conn_postgres::{
    ConnectOptions, Cursor, CursorMode, CursorWrite, Session, advance_cursor, connect,
    ensure_state_schema, read_cursor, read_cursors,
};
use atlatl_core::ErrorClass;
use atlatl_testkit::Postgres;

use crate::fresh;

on_both!(
    creates_schema_idempotently,
    concurrent_creation,
    unmanaged_schema_must_exist,
    unmanaged_schema_is_checked_for_columns,
    first_write_then_advance,
    stale_expectation_is_conflict,
    concurrent_appliers_conflict,
    rolled_back_apply_leaves_cursor,
    reads_several,
);

async fn session(o: &ConnectOptions) -> Session {
    connect(o).await.expect("connect")
}

async fn ready(pg: &Postgres) -> (ConnectOptions, Session) {
    let o = fresh(pg);
    let mut s = session(&o).await;
    ensure_state_schema(s.client_mut(), true)
        .await
        .expect("ensure");
    (o, s)
}

fn t(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000 + secs)
}

fn write(stream: &str, snapshot: i64) -> CursorWrite<'_> {
    CursorWrite {
        stream_id: stream,
        source_table: "au.org_event",
        source_table_id: 17,
        snapshot_id: snapshot,
        snapshot_time: Some(t(snapshot as u64)),
        mode: CursorMode::Cdc,
        config_hash: "h1",
    }
}

async fn creates_schema_idempotently(pg: &'static Postgres) {
    let (_, mut s) = ready(pg).await;
    ensure_state_schema(s.client_mut(), true)
        .await
        .expect("again");
    ensure_state_schema(s.client_mut(), false)
        .await
        .expect("exists");
    let cols: Vec<(String, String, bool)> = s
        .client()
        .query(
            "SELECT column_name::text, data_type::text, is_nullable = 'YES' \
             FROM information_schema.columns \
             WHERE table_schema = 'atlatl' AND table_name = 'cursors' ORDER BY ordinal_position",
            &[],
        )
        .await
        .expect("columns")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let c = |n: &str, ty: &str, null: bool| (n.to_owned(), ty.to_owned(), null);
    assert_eq!(
        cols,
        vec![
            c("stream_id", "text", false),
            c("source_table", "text", false),
            c("source_table_id", "bigint", false),
            c("snapshot_id", "bigint", false),
            c("snapshot_time", "timestamp with time zone", true),
            c("mode", "text", false),
            c("config_hash", "text", false),
            c("updated_at", "timestamp with time zone", false),
        ]
    );
    assert!(
        read_cursor(s.client(), "nope")
            .await
            .expect("read")
            .is_none()
    );
}

async fn concurrent_creation(pg: &'static Postgres) {
    let o = fresh(pg);
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let o = o.clone();
            tokio::spawn(async move {
                let mut s = connect(&o).await?;
                ensure_state_schema(s.client_mut(), true).await
            })
        })
        .collect();
    for t in tasks {
        t.await.expect("join").expect("ensure");
    }
}

async fn unmanaged_schema_must_exist(pg: &'static Postgres) {
    let mut s = session(&fresh(pg)).await;
    let e = ensure_state_schema(s.client_mut(), false)
        .await
        .expect_err("missing");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(e.message.contains("manage_state_schema"), "{e}");
    let exists: bool = s
        .client()
        .query_one("SELECT to_regnamespace('atlatl') IS NOT NULL", &[])
        .await
        .expect("q")
        .get(0);
    assert!(!exists, "nothing is created when unmanaged");
}

async fn unmanaged_schema_is_checked_for_columns(pg: &'static Postgres) {
    let mut s = session(&fresh(pg)).await;
    s.client()
        .batch_execute(
            "CREATE SCHEMA atlatl;
             CREATE TABLE atlatl.cursors (
               stream_id text PRIMARY KEY, source_table text NOT NULL, snapshot_id bigint NOT NULL,
               snapshot_time timestamptz, mode text NOT NULL, config_hash text NOT NULL,
               updated_at timestamptz NOT NULL DEFAULT now())",
        )
        .await
        .expect("old ddl");
    for manage in [false, true] {
        let e = ensure_state_schema(s.client_mut(), manage)
            .await
            .expect_err("missing column");
        assert_eq!(e.class, ErrorClass::Config, "{e}");
        assert!(e.message.contains("source_table_id"), "{e}");
    }
}

async fn first_write_then_advance(pg: &'static Postgres) {
    let (_, mut s) = ready(pg).await;
    let tx = s.client_mut().transaction().await.expect("tx");
    advance_cursor(&tx, None, &write("cdc/lee", 5))
        .await
        .expect("first");
    tx.commit().await.expect("commit");

    let c = read_cursor(s.client(), "cdc/lee")
        .await
        .expect("read")
        .expect("cursor");
    let updated = c.updated_at.expect("updated_at");
    assert_eq!(
        c,
        Cursor {
            stream_id: "cdc/lee".into(),
            source_table: "au.org_event".into(),
            source_table_id: 17,
            snapshot_id: 5,
            snapshot_time: Some(t(5)),
            mode: CursorMode::Cdc,
            config_hash: "h1".into(),
            updated_at: Some(updated),
        }
    );

    let mut w = write("cdc/lee", 9);
    w.source_table_id = 18;
    w.mode = CursorMode::Bootstrap;
    w.snapshot_time = None;
    w.config_hash = "h2";
    advance_cursor(s.client(), Some(5), &w)
        .await
        .expect("advance");
    let c = read_cursor(s.client(), "cdc/lee")
        .await
        .expect("read")
        .expect("cursor");
    assert_eq!(
        (
            c.snapshot_id,
            c.source_table_id,
            c.mode,
            c.snapshot_time,
            c.config_hash.as_str()
        ),
        (9, 18, CursorMode::Bootstrap, None, "h2")
    );
    assert!(c.updated_at.expect("updated_at") >= updated);
}

async fn stale_expectation_is_conflict(pg: &'static Postgres) {
    let (_, s) = ready(pg).await;
    advance_cursor(s.client(), None, &write("a", 3))
        .await
        .expect("first");
    for expected in [None, Some(2), Some(4)] {
        let e = advance_cursor(s.client(), expected, &write("a", 7))
            .await
            .expect_err("stale");
        assert_eq!(e.class, ErrorClass::Conflict, "{e}");
        assert_eq!(e.code.as_deref(), Some(CURSOR_CONFLICT_CODE));
        assert!(e.message.contains("`a`"), "{e}");
    }
    let e = advance_cursor(s.client(), Some(3), &write("missing", 4))
        .await
        .expect_err("no row");
    assert_eq!(e.class, ErrorClass::Conflict, "{e}");
    let c = read_cursor(s.client(), "a")
        .await
        .expect("read")
        .expect("a");
    assert_eq!(c.snapshot_id, 3);
    assert!(
        read_cursor(s.client(), "missing")
            .await
            .expect("read")
            .is_none()
    );
}

/// Two appliers read the same cursor; the second blocks on the row, then sees it moved.
async fn concurrent_appliers_conflict(pg: &'static Postgres) {
    let (o, s) = ready(pg).await;
    advance_cursor(s.client(), None, &write("x", 1))
        .await
        .expect("seed");
    let mut a = session(&o).await;
    let mut b = session(&o).await;
    let ta = a.client_mut().transaction().await.expect("a tx");
    advance_cursor(&ta, Some(1), &write("x", 2))
        .await
        .expect("a advances");
    let pending = tokio::spawn(async move {
        let tb = b
            .client_mut()
            .transaction()
            .await
            .map_err(atlatl_conn_postgres::wrap)?;
        let r = advance_cursor(&tb, Some(1), &write("x", 3)).await;
        tb.rollback().await.ok();
        r
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!pending.is_finished(), "b waits on a's row lock");
    ta.commit().await.expect("a commits");
    let e = pending.await.expect("join").expect_err("b conflicts");
    assert_eq!(e.class, ErrorClass::Conflict, "{e}");
    let c = read_cursor(s.client(), "x")
        .await
        .expect("read")
        .expect("x");
    assert_eq!(c.snapshot_id, 2);
}

async fn rolled_back_apply_leaves_cursor(pg: &'static Postgres) {
    let (_, mut s) = ready(pg).await;
    advance_cursor(s.client(), None, &write("r", 1))
        .await
        .expect("seed");
    let tx = s.client_mut().transaction().await.expect("tx");
    advance_cursor(&tx, Some(1), &write("r", 2))
        .await
        .expect("advance");
    tx.rollback().await.expect("rollback");
    let c = read_cursor(s.client(), "r")
        .await
        .expect("read")
        .expect("r");
    assert_eq!(c.snapshot_id, 1);
}

async fn reads_several(pg: &'static Postgres) {
    let (_, s) = ready(pg).await;
    for (id, snap) in [("c", 3), ("a", 1), ("b", 2)] {
        advance_cursor(s.client(), None, &write(id, snap))
            .await
            .expect("seed");
    }
    let got: Vec<(String, i64)> = read_cursors(s.client(), &["c", "a", "zzz"])
        .await
        .expect("read")
        .into_iter()
        .map(|c| (c.stream_id, c.snapshot_id))
        .collect();
    assert_eq!(got, vec![("a".to_owned(), 1), ("c".to_owned(), 3)]);
    s.client()
        .batch_execute("UPDATE atlatl.cursors SET mode = 'weird' WHERE stream_id = 'b'")
        .await
        .expect("corrupt");
    let e = read_cursor(s.client(), "b").await.expect_err("bad mode");
    assert_eq!(e.class, ErrorClass::Data, "{e}");
}
