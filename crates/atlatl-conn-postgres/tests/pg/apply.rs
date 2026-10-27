use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{
    ArrayRef, Date32Array, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use atlatl_conn_postgres::cursors::CURSOR_CONFLICT_CODE;
use atlatl_conn_postgres::{
    ApplyCounts, ApplyOptions, CursorMode, CursorWrite, Deletes, PgApplier, Session, StreamTarget,
    check_target, connect, ensure_state_schema, read_cursor,
};
use atlatl_core::config::model::DeleteValue;
use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_testkit::Postgres;
use futures::Stream;

use crate::fresh;

on_both!(
    counts_and_change_guard,
    columns_outside_include_untouched,
    enum_casts_and_bad_label_rolls_back,
    arrays_json_and_timestamptz,
    soft_delete_stamp_idempotent_restore,
    soft_delete_now_and_boolean,
    soft_column_served_from_source,
    hard_deletes_child_first,
    upserts_parent_first,
    failure_rolls_back_every_stream_and_cursor,
    cursor_conflict_aborts,
    upstream_error_aborts,
    deletes_ignored,
    plan_time_checks,
);

const DDL: &str = "
CREATE SCHEMA app;
CREATE TYPE app.event_type AS ENUM ('LISTING', 'REPORT', 'FUNDING', 'RATING');
CREATE TYPE app.rating AS ENUM ('BUY', 'HOLD', 'SELL', 'STRONG_SELL');
CREATE TABLE app.org_event (
  object_id bigint PRIMARY KEY,
  org_id bigint NOT NULL,
  type app.event_type NOT NULL,
  date date NOT NULL,
  inserted_at timestamp NOT NULL DEFAULT now(),
  updated_at timestamp NOT NULL DEFAULT now(),
  deleted_at timestamp
);
CREATE TABLE app.org_event_rating (
  event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  firm text,
  rating_current app.rating,
  inserted_at timestamp NOT NULL DEFAULT now(),
  updated_at timestamp NOT NULL,
  deleted_at timestamp
);
CREATE TABLE app.org_event_funding (
  event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  name text NOT NULL,
  unlinked_names text[],
  meta jsonb,
  raw json,
  announced_at timestamptz,
  updated_at timestamp NOT NULL DEFAULT now(),
  deleted_at timestamp
);
CREATE TABLE app.flagged (
  id bigint PRIMARY KEY,
  v text,
  is_deleted boolean NOT NULL DEFAULT false
);
";

const BASE: i64 = 1_700_000_000;

async fn setup(pg: &Postgres) -> Session {
    let mut s = connect(&fresh(pg)).await.expect("connect");
    s.client().batch_execute(DDL).await.expect("ddl");
    ensure_state_schema(s.client_mut(), true)
        .await
        .expect("state schema");
    s
}

fn at(secs: i64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs((BASE + secs) as u64)
}

fn us(secs: i64) -> i64 {
    (BASE + secs) * 1_000_000
}

fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(cols).expect("batch")
}

fn ok(bs: Vec<RecordBatch>) -> impl Stream<Item = Result<RecordBatch>> {
    futures::stream::iter(bs.into_iter().map(Ok))
}

fn none() -> impl Stream<Item = Result<RecordBatch>> {
    ok(Vec::new())
}

fn ts(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(v))
}

fn tstz(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC"))
}

fn ints(v: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(v.to_vec()))
}

/// Hub rows: `(object_id, type, updated_at)`.
fn hub(rows: &[(i64, &str, i64)]) -> RecordBatch {
    batch(vec![
        (
            "object_id",
            ints(&rows.iter().map(|r| r.0).collect::<Vec<_>>()),
        ),
        ("org_id", ints(&vec![1; rows.len()])),
        (
            "type",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ),
        (
            "date",
            Arc::new(Date32Array::from(
                rows.iter().map(|r| 19_000 + r.0 as i32).collect::<Vec<_>>(),
            )),
        ),
        (
            "updated_at",
            ts(rows.iter().map(|r| Some(us(r.2))).collect()),
        ),
    ])
}

/// Rating rows: `(event_id, firm, rating_current)`.
fn rating(rows: &[(i64, &str, Option<&str>)]) -> RecordBatch {
    batch(vec![
        (
            "event_id",
            ints(&rows.iter().map(|r| r.0).collect::<Vec<_>>()),
        ),
        (
            "rating_current",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ),
        (
            "firm",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ),
        ("updated_at", ts(vec![Some(us(0)); rows.len()])),
    ])
}

struct Fin<'a> {
    id: i64,
    participants: &'a [&'a str],
    meta: &'a str,
    deleted_at: Option<i64>,
}

fn funding(rows: &[Fin<'_>]) -> RecordBatch {
    let mut parts = ListBuilder::new(StringBuilder::new());
    for r in rows {
        for p in r.participants {
            parts.values().append_value(p);
        }
        parts.append(true);
    }
    let meta: Vec<&str> = rows.iter().map(|r| r.meta).collect();
    batch(vec![
        (
            "event_id",
            ints(&rows.iter().map(|r| r.id).collect::<Vec<_>>()),
        ),
        (
            "name",
            Arc::new(StringArray::from(vec!["seed"; rows.len()])),
        ),
        ("unlinked_names", Arc::new(parts.finish())),
        ("meta", Arc::new(StringArray::from(meta.clone()))),
        ("raw", Arc::new(StringArray::from(meta))),
        ("announced_at", tstz(vec![Some(us(42)); rows.len()])),
        ("updated_at", ts(vec![Some(us(0)); rows.len()])),
        (
            "deleted_at",
            ts(rows.iter().map(|r| r.deleted_at.map(us)).collect()),
        ),
    ])
}

/// Reader-shaped delete rows: the PK, `_snapshot` and `_ts`.
fn dels(pk: &str, rows: &[(i64, i64)]) -> RecordBatch {
    batch(vec![
        (pk, ints(&rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        ("_snapshot", ints(&vec![9; rows.len()])),
        ("_ts", tstz(rows.iter().map(|r| Some(us(r.1))).collect())),
    ])
}

fn soft(restore: bool) -> Deletes {
    Deletes::Soft {
        column: "deleted_at".into(),
        value: DeleteValue::SnapshotTime,
        restore,
    }
}

fn target(
    table: &str,
    pk: &str,
    cols: &[&str],
    casts: &[(&str, &str)],
    d: Deletes,
) -> StreamTarget {
    StreamTarget {
        schema: "app".into(),
        table: table.into(),
        primary_key: vec![pk.into()],
        columns: cols.iter().map(|c| (*c).into()).collect(),
        target_types: casts
            .iter()
            .map(|(c, t)| ((*c).into(), (*t).into()))
            .collect::<BTreeMap<_, _>>(),
        deletes: d,
    }
}

fn hub_t(d: Deletes) -> StreamTarget {
    target(
        "org_event",
        "object_id",
        &["object_id", "org_id", "type", "date", "updated_at"],
        &[("type", "app.event_type")],
        d,
    )
}

fn rating_t(d: Deletes) -> StreamTarget {
    target(
        "org_event_rating",
        "event_id",
        &["event_id", "firm", "rating_current", "updated_at"],
        &[("rating_current", "app.rating")],
        d,
    )
}

fn fin_t() -> StreamTarget {
    target(
        "org_event_funding",
        "event_id",
        &[
            "event_id",
            "name",
            "unlinked_names",
            "meta",
            "raw",
            "announced_at",
            "updated_at",
            "deleted_at",
        ],
        &[],
        soft(true),
    )
}

fn cursor(stream: &str, snapshot: i64) -> CursorWrite<'_> {
    CursorWrite {
        stream_id: stream,
        source_table: "au.org_event",
        source_table_id: 3,
        snapshot_id: snapshot,
        snapshot_time: Some(at(snapshot)),
        mode: CursorMode::Cdc,
        config_hash: "h",
    }
}

fn counts(sent: u64, ins: u64, upd: u64, same: u64) -> ApplyCounts {
    ApplyCounts {
        upserts_sent: sent,
        rows_inserted: ins,
        rows_updated: upd,
        rows_unchanged: same,
        ..ApplyCounts::default()
    }
}

fn deleted(n: u64) -> ApplyCounts {
    ApplyCounts {
        rows_deleted: n,
        ..ApplyCounts::default()
    }
}

async fn upsert_all(s: &mut Session, steps: Vec<(StreamTarget, RecordBatch)>) {
    let mut tx = PgApplier::begin(s, &ApplyOptions::default())
        .await
        .expect("begin");
    for (t, b) in steps {
        tx.upsert(&t, ok(vec![b])).await.expect("upsert");
    }
    tx.commit(&[]).await.expect("commit");
}

async fn count(s: &Session, table: &str) -> i64 {
    s.client()
        .query_one(&format!("SELECT count(*) FROM app.{table}"), &[])
        .await
        .expect("count")
        .get(0)
}

async fn xmins(s: &Session) -> Vec<i64> {
    s.client()
        .query(
            "SELECT xmin::text::bigint FROM app.org_event ORDER BY object_id",
            &[],
        )
        .await
        .expect("xmin")
        .iter()
        .map(|r| r.get(0))
        .collect()
}

fn class(e: &AtlatlError, c: ErrorClass, code: Option<&str>) {
    assert_eq!(e.class, c, "{e}");
    if code.is_some() {
        assert_eq!(e.code.as_deref(), code, "{e}");
    }
}

async fn counts_and_change_guard(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let (send, mut recv) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(1);
    tokio::spawn(async move {
        for b in [
            hub(&[(1, "LISTING", 0), (2, "LISTING", 0)]),
            hub(&[(3, "REPORT", 0)]),
        ] {
            send.send(Ok(b)).await.expect("send");
        }
    });
    let feed = futures::stream::poll_fn(move |cx| recv.poll_recv(cx));
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let t = hub_t(soft(true));
    assert_eq!(tx.upsert(&t, feed).await.expect("up"), counts(3, 3, 0, 0));
    assert_eq!(
        tx.upsert(&t, none()).await.expect("empty"),
        counts(0, 0, 0, 0)
    );
    tx.commit(&[]).await.expect("commit");
    let before = xmins(&s).await;
    let inserted: SystemTime = s
        .client()
        .query_one(
            "SELECT inserted_at FROM app.org_event WHERE object_id = 2",
            &[],
        )
        .await
        .expect("inserted_at")
        .get(0);

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let a = tx
        .upsert(
            &t,
            ok(vec![hub(&[
                (1, "LISTING", 0),
                (2, "FUNDING", 5),
                (3, "REPORT", 0),
            ])]),
        )
        .await
        .expect("up");
    let b = tx
        .upsert(&t, ok(vec![hub(&[(4, "RATING", 0)])]))
        .await
        .expect("second chunk");
    let mut total = a;
    total += b;
    assert_eq!(a, counts(3, 0, 1, 2));
    assert_eq!(total, counts(4, 1, 1, 2));
    tx.commit(&[]).await.expect("commit");

    let after = xmins(&s).await;
    assert_eq!(after[0], before[0], "identical row 1 must not be rewritten");
    assert_eq!(after[2], before[2], "identical row 3 must not be rewritten");
    assert_ne!(after[1], before[1]);
    let r = s
        .client()
        .query_one(
            "SELECT type::text, updated_at, inserted_at FROM app.org_event \
             WHERE object_id = 2",
            &[],
        )
        .await
        .expect("row 2");
    assert_eq!(r.get::<_, String>(0), "FUNDING");
    assert_eq!(r.get::<_, SystemTime>(1), at(5));
    assert_eq!(
        r.get::<_, SystemTime>(2),
        inserted,
        "inserted_at is not served"
    );
    let r = s
        .client()
        .query_one(
            "SELECT inserted_at IS NOT NULL FROM app.org_event WHERE object_id = 4",
            &[],
        )
        .await
        .expect("row 4");
    assert!(r.get::<_, bool>(0), "the target default applies on insert");
}

async fn columns_outside_include_untouched(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let rt = rating_t(soft(false));
    upsert_all(
        &mut s,
        vec![
            (hub_t(soft(true)), hub(&[(1, "RATING", 0)])),
            (rt.clone(), rating(&[(1, "gs", Some("BUY"))])),
        ],
    )
    .await;
    s.client()
        .batch_execute(
            "UPDATE app.org_event_rating \
             SET inserted_at = '2001-01-01', deleted_at = '2002-02-02'",
        )
        .await
        .expect("mark");
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .upsert(&rt, ok(vec![rating(&[(1, "ms", Some("BUY"))])]))
        .await
        .expect("up");
    assert_eq!(c, counts(1, 0, 1, 0));
    let c = tx
        .upsert(&rt, ok(vec![rating(&[(1, "ms", Some("BUY"))])]))
        .await
        .expect("again");
    assert_eq!(
        c,
        counts(1, 0, 0, 1),
        "without restore the soft column is not in the guard"
    );
    tx.commit(&[]).await.expect("commit");
    let r = s
        .client()
        .query_one(
            "SELECT firm, inserted_at::text, deleted_at::text \
             FROM app.org_event_rating",
            &[],
        )
        .await
        .expect("row");
    assert_eq!(r.get::<_, String>(0), "ms");
    assert_eq!(r.get::<_, String>(1), "2001-01-01 00:00:00");
    assert_eq!(r.get::<_, String>(2), "2002-02-02 00:00:00");
}

async fn enum_casts_and_bad_label_rolls_back(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    upsert_all(
        &mut s,
        vec![
            (
                hub_t(soft(true)),
                hub(&[(1, "RATING", 0), (2, "LISTING", 0)]),
            ),
            (
                rating_t(soft(true)),
                rating(&[(1, "gs", Some("STRONG_SELL")), (2, "ms", None)]),
            ),
        ],
    )
    .await;
    let got: Vec<(String, Option<String>)> = s
        .client()
        .query(
            "SELECT e.type::text, r.rating_current::text FROM app.org_event e \
             JOIN app.org_event_rating r ON r.event_id = e.object_id \
             ORDER BY e.object_id",
            &[],
        )
        .await
        .expect("read")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        got,
        vec![
            ("RATING".into(), Some("STRONG_SELL".into())),
            ("LISTING".into(), None)
        ]
    );

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    tx.upsert(&hub_t(soft(true)), ok(vec![hub(&[(3, "LISTING", 0)])]))
        .await
        .expect("hub");
    let e = tx
        .upsert(
            &rating_t(soft(true)),
            ok(vec![rating(&[(3, "gs", Some("STRONG_BUY"))])]),
        )
        .await
        .expect_err("bad label");
    class(&e, ErrorClass::Data, Some("22P02"));
    let e = tx
        .commit(&[(None, cursor("hub", 5))])
        .await
        .expect_err("a failed transaction must not commit");
    class(&e, ErrorClass::Internal, None);
    assert_eq!(count(&s, "org_event").await, 2);
    assert!(
        read_cursor(s.client(), "hub")
            .await
            .expect("read")
            .is_none()
    );
}

async fn arrays_json_and_timestamptz(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let parts: &[&str] = &["a", "b,c", "d \"q\""];
    let rows = |meta: &'static str| {
        funding(&[
            Fin {
                id: 1,
                participants: parts,
                meta,
                deleted_at: None,
            },
            Fin {
                id: 2,
                participants: &[],
                meta: "null",
                deleted_at: None,
            },
        ])
    };
    upsert_all(
        &mut s,
        vec![
            (
                hub_t(soft(true)),
                hub(&[(1, "FUNDING", 0), (2, "FUNDING", 0)]),
            ),
            (fin_t(), rows(r#"{"a": 1, "b": [1, 2]}"#)),
        ],
    )
    .await;
    let r = s
        .client()
        .query_one(
            "SELECT unlinked_names, meta::text, raw::text, announced_at \
             FROM app.org_event_funding WHERE event_id = 1",
            &[],
        )
        .await
        .expect("row");
    assert_eq!(r.get::<_, Vec<String>>(0), parts);
    assert_eq!(r.get::<_, String>(1), r#"{"a": 1, "b": [1, 2]}"#);
    assert_eq!(r.get::<_, String>(2), r#"{"a": 1, "b": [1, 2]}"#);
    assert_eq!(r.get::<_, SystemTime>(3), at(42));

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .upsert(&fin_t(), ok(vec![rows(r#"{"b": [1, 2], "a": 1}"#)]))
        .await
        .expect("reordered keys");
    assert_eq!(c, counts(2, 0, 0, 2), "json and jsonb compare semantically");
    let c = tx
        .upsert(&fin_t(), ok(vec![rows(r#"{"a": 2, "b": [1, 2]}"#)]))
        .await
        .expect("changed");
    assert_eq!(c, counts(2, 0, 1, 1));
    tx.commit(&[]).await.expect("commit");
}

async fn deleted_at(s: &Session, table: &str, pk: &str, id: i64) -> Option<SystemTime> {
    s.client()
        .query_one(
            &format!("SELECT deleted_at FROM app.{table} WHERE {pk} = $1"),
            &[&id],
        )
        .await
        .expect("deleted_at")
        .get(0)
}

async fn hub_deleted(s: &Session, id: i64) -> Option<SystemTime> {
    deleted_at(s, "org_event", "object_id", id).await
}

async fn fin_deleted(s: &Session, id: i64) -> Option<SystemTime> {
    deleted_at(s, "org_event_funding", "event_id", id).await
}

async fn flag(s: &Session) -> bool {
    s.client()
        .query_one("SELECT is_deleted FROM app.flagged", &[])
        .await
        .expect("flag")
        .get(0)
}

async fn soft_delete_stamp_idempotent_restore(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let t = hub_t(soft(true));
    upsert_all(
        &mut s,
        vec![(t.clone(), hub(&[(1, "LISTING", 0), (2, "LISTING", 0)]))],
    )
    .await;

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .delete(&t, ok(vec![dels("object_id", &[(1, 100)])]))
        .await
        .expect("delete");
    assert_eq!(c, deleted(1));
    tx.commit(&[]).await.expect("commit");
    assert_eq!(
        hub_deleted(&s, 1).await,
        Some(at(100)),
        "stamped with the snapshot time"
    );
    assert_eq!(hub_deleted(&s, 2).await, None);

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .delete(&t, ok(vec![dels("object_id", &[(1, 200), (2, 200)])]))
        .await
        .expect("delete again");
    assert_eq!(c, deleted(1));
    tx.commit(&[]).await.expect("commit");
    assert_eq!(
        hub_deleted(&s, 1).await,
        Some(at(100)),
        "the original stamp is kept"
    );
    assert_eq!(hub_deleted(&s, 2).await, Some(at(200)));
    assert_eq!(count(&s, "org_event").await, 2, "rows stay");

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .upsert(&t, ok(vec![hub(&[(1, "LISTING", 0)])]))
        .await
        .expect("restore");
    assert_eq!(c, counts(1, 0, 1, 0), "an identical payload still restores");
    let c = tx
        .upsert(&t, ok(vec![hub(&[(1, "LISTING", 0)])]))
        .await
        .expect("again");
    assert_eq!(c, counts(1, 0, 0, 1));
    tx.commit(&[]).await.expect("commit");
    assert_eq!(hub_deleted(&s, 1).await, None);
    assert_eq!(hub_deleted(&s, 2).await, Some(at(200)));
}

async fn soft_delete_now_and_boolean(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let now_t = hub_t(Deletes::Soft {
        column: "deleted_at".into(),
        value: DeleteValue::Now,
        restore: true,
    });
    upsert_all(&mut s, vec![(now_t.clone(), hub(&[(1, "LISTING", 0)]))]).await;
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let pk_only = batch(vec![("object_id", ints(&[1]))]);
    let c = tx
        .delete(&now_t, ok(vec![pk_only]))
        .await
        .expect("delete without _ts");
    assert_eq!(c, deleted(1));
    tx.commit(&[]).await.expect("commit");
    let stamp = deleted_at(&s, "org_event", "object_id", 1)
        .await
        .expect("stamped");
    let age = SystemTime::now()
        .duration_since(stamp)
        .unwrap_or_else(|e| e.duration());
    assert!(age < Duration::from_secs(300), "{age:?}");

    let flag_t = target(
        "flagged",
        "id",
        &["id", "v"],
        &[],
        Deletes::Soft {
            column: "is_deleted".into(),
            value: DeleteValue::True,
            restore: true,
        },
    );
    let row = || {
        batch(vec![
            ("id", ints(&[1])),
            ("v", Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
        ])
    };
    upsert_all(&mut s, vec![(flag_t.clone(), row())]).await;
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .delete(&flag_t, ok(vec![dels("id", &[(1, 0)])]))
        .await
        .expect("delete");
    assert_eq!(c, deleted(1));
    let c = tx
        .delete(&flag_t, ok(vec![dels("id", &[(1, 0)])]))
        .await
        .expect("again");
    assert_eq!(c, deleted(0));
    tx.commit(&[]).await.expect("commit");
    assert!(flag(&s).await);
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx.upsert(&flag_t, ok(vec![row()])).await.expect("restore");
    assert_eq!(c, counts(1, 0, 1, 0));
    tx.commit(&[]).await.expect("commit");
    assert!(!flag(&s).await);
}

async fn soft_column_served_from_source(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let fin = |id, deleted_at| Fin {
        id,
        participants: &[],
        meta: "{}",
        deleted_at,
    };
    upsert_all(
        &mut s,
        vec![
            (
                hub_t(soft(true)),
                hub(&[(1, "FUNDING", 0), (2, "FUNDING", 0)]),
            ),
            (fin_t(), funding(&[fin(1, Some(7)), fin(2, None)])),
        ],
    )
    .await;
    assert_eq!(
        fin_deleted(&s, 1).await,
        Some(at(7)),
        "the lake's value is written"
    );
    assert_eq!(fin_deleted(&s, 2).await, None);

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .delete(&fin_t(), ok(vec![dels("event_id", &[(1, 50), (2, 50)])]))
        .await
        .expect("delete");
    assert_eq!(c, deleted(1));
    tx.commit(&[]).await.expect("commit");
    assert_eq!(fin_deleted(&s, 1).await, Some(at(7)));
    assert_eq!(fin_deleted(&s, 2).await, Some(at(50)));

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .upsert(&fin_t(), ok(vec![funding(&[fin(2, None)])]))
        .await
        .expect("reappear");
    assert_eq!(c, counts(1, 0, 1, 0));
    tx.commit(&[]).await.expect("commit");
    assert_eq!(fin_deleted(&s, 2).await, None, "a NULL lake value restores");
}

async fn hard_deletes_child_first(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let (ht, rt) = (hub_t(Deletes::Hard), rating_t(Deletes::Hard));
    upsert_all(
        &mut s,
        vec![
            (ht.clone(), hub(&[(1, "RATING", 0), (2, "LISTING", 0)])),
            (rt.clone(), rating(&[(1, "gs", None)])),
        ],
    )
    .await;

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let e = tx
        .delete(&ht, ok(vec![dels("object_id", &[(1, 0), (2, 0)])]))
        .await
        .expect_err("parent before child");
    class(&e, ErrorClass::Data, Some("23503"));
    tx.rollback().await.expect("rollback");
    assert_eq!(count(&s, "org_event").await, 2);

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .delete(&rt, ok(vec![dels("event_id", &[(1, 0)])]))
        .await
        .expect("child");
    assert_eq!(c, deleted(1));
    let c = tx
        .delete(&ht, ok(vec![dels("object_id", &[(1, 0), (2, 0), (3, 0)])]))
        .await
        .expect("parent");
    assert_eq!(c, deleted(2));
    let e = tx
        .upsert(&ht, ok(vec![hub(&[(9, "LISTING", 0)])]))
        .await
        .expect_err("upsert after delete");
    class(&e, ErrorClass::Internal, None);
    tx.commit(&[]).await.expect("commit");
    assert_eq!(count(&s, "org_event").await, 0);
    assert_eq!(count(&s, "org_event_rating").await, 0);
}

async fn upserts_parent_first(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let (ht, rt) = (hub_t(soft(true)), rating_t(soft(true)));
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let e = tx
        .upsert(&rt, ok(vec![rating(&[(1, "gs", None)])]))
        .await
        .expect_err("child before parent");
    class(&e, ErrorClass::Data, Some("23503"));
    drop(tx);
    upsert_all(
        &mut s,
        vec![
            (ht, hub(&[(1, "RATING", 0)])),
            (rt, rating(&[(1, "gs", None)])),
        ],
    )
    .await;
    assert_eq!(count(&s, "org_event_rating").await, 1);
}

async fn failure_rolls_back_every_stream_and_cursor(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let (ht, rt) = (hub_t(soft(true)), rating_t(soft(true)));
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    tx.upsert(&ht, ok(vec![hub(&[(1, "LISTING", 0)])]))
        .await
        .expect("hub");
    tx.commit(&[(None, cursor("hub", 5)), (None, cursor("rating", 5))])
        .await
        .expect("commit");

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    tx.upsert(&ht, ok(vec![hub(&[(2, "LISTING", 0)])]))
        .await
        .expect("hub");
    tx.delete(&ht, ok(vec![dels("object_id", &[(1, 60)])]))
        .await
        .expect("hub delete");
    let e = tx
        .delete(&rt, ok(vec![batch(vec![("event_id", ints(&[1]))])]))
        .await
        .expect_err("no _ts");
    class(&e, ErrorClass::Internal, None);
    drop(tx);

    assert_eq!(count(&s, "org_event").await, 1);
    assert_eq!(deleted_at(&s, "org_event", "object_id", 1).await, None);
    for id in ["hub", "rating"] {
        let c = read_cursor(s.client(), id)
            .await
            .expect("read")
            .expect("cursor");
        assert_eq!(c.snapshot_id, 5, "{id}");
    }
}

async fn cursor_conflict_aborts(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    tx.commit(&[(None, cursor("rating", 7))])
        .await
        .expect("seed");

    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    tx.upsert(&hub_t(soft(true)), ok(vec![hub(&[(1, "LISTING", 0)])]))
        .await
        .expect("hub");
    let e = tx
        .commit(&[(None, cursor("hub", 8)), (Some(6), cursor("rating", 8))])
        .await
        .expect_err("stale expectation");
    class(&e, ErrorClass::Conflict, Some(CURSOR_CONFLICT_CODE));
    assert_eq!(count(&s, "org_event").await, 0);
    assert!(
        read_cursor(s.client(), "hub")
            .await
            .expect("read")
            .is_none()
    );
    let c = read_cursor(s.client(), "rating")
        .await
        .expect("read")
        .expect("cursor");
    assert_eq!(c.snapshot_id, 7);

    let tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("session is reusable");
    tx.commit(&[(Some(7), cursor("rating", 8))])
        .await
        .expect("commit");
}

async fn upstream_error_aborts(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let opts = ApplyOptions {
        lock_timeout: Some(Duration::from_secs(5)),
        statement_timeout: Some(Duration::from_secs(60)),
        idle_in_transaction_session_timeout: Some(Duration::from_secs(60)),
    };
    let mut tx = PgApplier::begin(&mut s, &opts).await.expect("begin");
    let r = tx
        .transaction()
        .query_one(
            "SELECT current_setting('lock_timeout'), current_setting('statement_timeout'), \
             current_setting('idle_in_transaction_session_timeout'), current_setting('TimeZone')",
            &[],
        )
        .await
        .expect("settings");
    let got: Vec<String> = (0..4).map(|i| r.get(i)).collect();
    assert_eq!(got, ["5s", "1min", "1min", "UTC"]);
    let feed = futures::stream::iter(vec![
        Ok(hub(&[(1, "LISTING", 0)])),
        Err(AtlatlError::transient("reader died")),
    ]);
    let e = tx
        .upsert(&hub_t(soft(true)), feed)
        .await
        .expect_err("upstream");
    class(&e, ErrorClass::Transient, None);
    tx.rollback().await.expect("rollback");
    assert_eq!(count(&s, "org_event").await, 0);
}

async fn deletes_ignored(pg: &'static Postgres) {
    let mut s = setup(pg).await;
    let t = hub_t(Deletes::Ignore);
    upsert_all(&mut s, vec![(t.clone(), hub(&[(1, "LISTING", 0)]))]).await;
    let mut tx = PgApplier::begin(&mut s, &ApplyOptions::default())
        .await
        .expect("begin");
    let c = tx
        .delete(
            &t,
            ok(vec![
                dels("object_id", &[(1, 0)]),
                dels("object_id", &[(2, 0)]),
            ]),
        )
        .await
        .expect("ignore");
    assert_eq!(
        c,
        ApplyCounts {
            deletes_ignored: 2,
            ..ApplyCounts::default()
        }
    );
    tx.commit(&[]).await.expect("commit");
    assert_eq!(count(&s, "org_event").await, 1);
    assert_eq!(deleted_at(&s, "org_event", "object_id", 1).await, None);
}

async fn plan_time_checks(pg: &'static Postgres) {
    let s = setup(pg).await;
    s.client()
        .batch_execute(
            "CREATE TABLE app.no_pk (id bigint, v text);
             CREATE TABLE app.wide (a bigint, b bigint, v text, UNIQUE (a, b));
             CREATE TABLE app.partial (id bigint, v text);
             CREATE UNIQUE INDEX ON app.partial (id) WHERE v IS NOT NULL;
             CREATE TABLE app.deferred (id bigint, v text, UNIQUE (id) DEFERRABLE);
             CREATE TABLE app.int_deleted (id bigint PRIMARY KEY, v text, deleted_at int);
             CREATE VIEW app.a_view AS SELECT 1 AS id;",
        )
        .await
        .expect("ddl");
    let c = s.client();
    for t in [
        hub_t(soft(true)),
        hub_t(Deletes::Hard),
        rating_t(Deletes::Ignore),
        fin_t(),
        target(
            "org_event_funding",
            "event_id",
            &["event_id", "name", "unlinked_names"],
            &[("unlinked_names", "text[]")],
            soft(false),
        ),
    ] {
        check_target(c, &t).await.expect("valid target");
    }

    let simple = |table: &str, d: Deletes| target(table, "id", &["id", "v"], &[], d);
    let mut missing_col = hub_t(soft(true));
    missing_col.columns.push("nope".into());
    let mut unknown_type = hub_t(soft(true));
    unknown_type
        .target_types
        .insert("type".into(), "app.nope_enum".into());
    let mut pk_not_served = hub_t(soft(true));
    pk_not_served.columns.retain(|c| c != "object_id");
    let mut cast_not_served = hub_t(soft(true));
    cast_not_served
        .target_types
        .insert("inserted_at".into(), "timestamptz".into());
    let mut soft_in_pk = simple("int_deleted", Deletes::Hard);
    soft_in_pk.deletes = Deletes::Soft {
        column: "id".into(),
        value: DeleteValue::Now,
        restore: false,
    };
    let bool_value = hub_t(Deletes::Soft {
        column: "deleted_at".into(),
        value: DeleteValue::True,
        restore: false,
    });
    let cases = [
        (simple("nope", Deletes::Hard), "does not exist"),
        (simple("a_view", Deletes::Hard), "not a table"),
        (
            missing_col,
            "columns.include has columns the table does not: nope",
        ),
        (simple("no_pk", Deletes::Hard), "no unique index"),
        (
            target("wide", "a", &["a", "v"], &[], Deletes::Hard),
            "no unique index",
        ),
        (simple("partial", Deletes::Hard), "no unique index"),
        (simple("deferred", Deletes::Hard), "no unique index"),
        (
            hub_t(Deletes::Soft {
                column: "removed_at".into(),
                value: DeleteValue::SnapshotTime,
                restore: true,
            }),
            "soft-delete column `removed_at` does not exist",
        ),
        (
            simple("int_deleted", soft(true)),
            "needs timestamp or timestamptz",
        ),
        (bool_value, "needs boolean"),
        (unknown_type, "unknown target_type `app.nope_enum`"),
        (
            pk_not_served,
            "primary key column `object_id` is not in columns.include",
        ),
        (
            cast_not_served,
            "has a target_type but is not in columns.include",
        ),
        (soft_in_pk, "is part of the primary key"),
    ];
    for (t, want) in cases {
        let e = check_target(c, &t).await.expect_err(want);
        class(&e, ErrorClass::Config, None);
        assert!(e.message.contains(want), "{want}: {e}");
    }
}
