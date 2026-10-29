//! REST → lake extract (07, 04 §3/§5.2): runs an endpoint through [`atlatl_http::Driver`], lands its
//! records as JSON staging, and projects them into a typed DuckDB TEMP table. Queues (07 §7) are
//! TEMP tables in the unit's session holding exact JSON values; `for_each.over.query` (M7.1) reads
//! a lake or Postgres connection. State and rejects are returned for the caller's commit.

use std::collections::HashSet;
use std::sync::Arc;

use atlatl_core::config::load::Stream;
use atlatl_core::config::model::{ConnectionKind, ErrorRetry, Infer, Naming, RestConn};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::obs::report::{HttpStats as ReportHttp, SkippedItem as ReportSkip};
use atlatl_core::plan::{MemberPlan, ReplicationPlan, StreamPlan, StreamRole};
use atlatl_core::retry::RetryPolicy;
use atlatl_engine::Session;
use atlatl_http::cassette;
use atlatl_http::iterate::{Chunk, OpenReject};
use atlatl_http::json::JValue;
use atlatl_http::spec::{DecodeRetry, Over};
use atlatl_http::{
    ApiClient, ApiSpec, Cassette, ClientOptions, DriveOptions, Driver, EndpointStats,
    RangeOverride, RunVars,
};
use atlatl_ingest::sql::{db, ident};
use atlatl_ingest::{metadata, naming, staging};
use atlatl_state::{Reject, StateKind, StateRow};
use duckdb::Connection;
use indexmap::IndexMap;

use crate::lake::split_object;
use crate::runtime::Runtime;

/// One REST member, extracted and staged (or only fed to queues, for a feed-only producer).
#[derive(Clone, Debug, Default)]
pub struct RestStaged {
    /// The TEMP staging table, or `None` for a feed-only producer (nothing to apply).
    pub staging: Option<String>,
    pub rows: u64,
    /// No records and no known column shape (new table, nothing declared): skip the apply.
    pub empty: bool,
    /// State and reject member key (`""`: REST streams have one member).
    pub member: String,
    /// `sync` values to `LakeState::put` inside the commit.
    pub state: Option<StateRow>,
    pub state_before: Option<serde_json::Value>,
    /// New rejects, or open ones whose error changed: `LakeState::upsert_reject` inside the commit.
    pub rejects: Vec<Reject>,
    /// Open rejects that failed again with the same error: upserted only when the replication
    /// commits anyway, so on their own they don't force a commit (03 §7).
    pub refreshed: Vec<Reject>,
    /// Reject keys to `LakeState::resolve_rejects` inside the commit.
    pub resolved: Vec<String>,
    /// Open rejects past `reject_ttl`: deleted in the commit (not retried).
    pub expired: Vec<String>,
    /// Queue tables this producer created; drop them (like staging) after the unit.
    pub temp_tables: Vec<String>,
    pub stats: EndpointStats,
}

/// Replays every REST connection's traffic from a cassette (07 §13): a name
/// (`apis/<api>/cassettes/<name>.json`) or a path. Testing and dogfooding only.
pub const CASSETTE_ENV: &str = "ATLATL_CASSETTE";
/// Records it instead, like `atlatl api test --record`.
pub const CASSETTE_RECORD_ENV: &str = "ATLATL_CASSETTE_RECORD";

/// The cassette `ATLATL_CASSETTE` / `ATLATL_CASSETTE_RECORD` select for this spec, if any.
fn cassette(rt: &Runtime, spec_path: &str, spec: &ApiSpec) -> Result<Option<Cassette>> {
    let var = |k: &str| rt.vars.var(k).filter(|v| !v.is_empty());
    match (var(CASSETTE_ENV), var(CASSETTE_RECORD_ENV)) {
        (Some(_), Some(_)) => Err(AtlatlError::config(format!(
            "set only one of `{CASSETTE_ENV}` and `{CASSETTE_RECORD_ENV}`"
        ))),
        (Some(r), None) => {
            Cassette::replay(cassette::resolve(&rt.project.root, spec_path, &r)).map(Some)
        }
        (None, Some(r)) => Ok(Some(
            Cassette::record(cassette::resolve(&rt.project.root, spec_path, &r))
                .api(cassette::api_dir(spec_path))
                .ignore_query(cassette::volatile_params(spec)),
        )),
        (None, None) => Ok(None),
    }
}

/// One client (limiter, tokens, decode window) per connection per run (07 §8).
fn client(rt: &Runtime, conn_name: &str) -> Result<ApiClient> {
    // One lock for lookup and insert: concurrent units on a connection share one limiter.
    let mut cache = rt.rest_clients.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((c, _)) = cache.get(conn_name) {
        return Ok(c.clone());
    }
    let conn = rt.connection(conn_name)?;
    let ConnectionKind::Rest(RestConn { spec, secrets }) = &conn.kind else {
        return Err(AtlatlError::config(format!(
            "connection `{conn_name}` is not a REST connection"
        )));
    };
    let parsed = ApiSpec::load(&rt.project.root, &spec.value)?;
    let secrets: IndexMap<String, String> = secrets
        .iter()
        .map(|(k, v)| match v {
            SecretValue::Literal(s) => Ok((k.clone(), s.clone())),
            SecretValue::Ref(r) => Err(AtlatlError::internal(format!(
                "secret `{k}` ({r}) was not resolved"
            ))),
        })
        .collect::<Result<_>>()?;
    // The connection's own `retry`; replications without one pass theirs per run (`RunVars.retry`).
    let retry = match &conn.common.retry {
        Some(r) => RetryPolicy::from_config(r).map_err(AtlatlError::config)?,
        None => RetryPolicy::default(),
    };
    let mut opts = ClientOptions {
        retry,
        env: rt.vars.clone(),
        ..Default::default()
    };
    if let Some(t) = conn.common.timeouts.as_ref() {
        if let Some(c) = t.connect {
            opts.connect_timeout = Some(c.0);
        }
        if let Some(s) = t.statement {
            opts.timeout = Some(s.0);
        }
    }
    opts.cassette = cassette(rt, &spec.value, &parsed)?;
    let recording = opts.cassette.clone().filter(|c| !c.is_replay());
    let c = ApiClient::new(Arc::new(parsed), secrets, opts)?;
    cache.insert(conn_name.to_owned(), (c.clone(), recording));
    Ok(c)
}

/// The replication's `retry` when its connection has none (the connection's wins, as for the
/// client itself).
fn replication_retry(rt: &Runtime, rep: &ReplicationPlan) -> Result<Option<RetryPolicy>> {
    if rt.connection(&rep.source.name)?.common.retry.is_some() {
        return Ok(None);
    }
    Ok(rt
        .project
        .replication(rep.config_name())
        .map(|r| RetryPolicy::from_config(&r.retry).unwrap_or_default()))
}

fn endpoint_of(sp: &StreamPlan, m: &MemberPlan) -> String {
    sp.from.endpoint.clone().unwrap_or_else(|| m.name.clone())
}

/// Streams in producer-first order (queue producers and `depends_on` before consumers), ties by
/// declaration. Stage REST streams in this order.
pub fn extract_order(rt: &Runtime, rep: &ReplicationPlan) -> Result<Vec<usize>> {
    let c = client(rt, &rep.source.name)?;
    let spec = c.spec();
    let eps: Vec<Option<String>> = rep
        .streams
        .iter()
        .map(|s| s.from.endpoint.clone())
        .collect();
    let producers = |ep: &str| -> Vec<String> {
        let Some(e) = spec.endpoint(ep) else {
            return Vec::new();
        };
        let mut out: Vec<String> = e.depends_on.clone();
        if let Some(Over::Queue { queue, .. }) = e.for_each.as_ref().map(|f| &f.over) {
            out.extend(
                spec.endpoints
                    .iter()
                    .filter(|(_, o)| o.emit.contains_key(queue))
                    .map(|(n, _)| n.clone()),
            );
        }
        out
    };
    let mut done: Vec<usize> = Vec::new();
    while done.len() < eps.len() {
        let before = done.len();
        for (i, e) in eps.iter().enumerate() {
            if done.contains(&i) {
                continue;
            }
            let ready = e.as_deref().is_none_or(|e| {
                producers(e).iter().all(|p| {
                    eps.iter()
                        .enumerate()
                        .all(|(j, x)| x.as_deref() != Some(p.as_str()) || done.contains(&j))
                })
            });
            if ready {
                done.push(i);
            }
        }
        if done.len() == before {
            return Err(AtlatlError::config(format!(
                "replication `{}`: endpoint dependency cycle",
                rep.name
            )));
        }
    }
    Ok(done)
}

fn short(run_id: &str) -> String {
    let s: String = run_id
        .chars()
        .rev()
        .take(8)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    s.to_lowercase()
}

fn safe_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// The TEMP table holding queue `queue` for one replication of one run.
pub fn queue_table(run_id: &str, replication: &str, queue: &str) -> String {
    let h = atlatl_http::funcs::hex(&atlatl_http::funcs::sha256(replication.as_bytes()));
    format!("q_{}_{}_{}", short(run_id), &h[..8], safe_name(queue))
}

fn temp_exists(c: &Connection, table: &str) -> Result<bool> {
    let n: i64 = c
        .query_row(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'temp' AND table_name = ?",
            [table],
            |r| r.get(0),
        )
        .map_err(db)?;
    Ok(n > 0)
}

fn read_queue(c: &Connection, table: &str, queue: &str, unique: bool) -> Result<Vec<JValue>> {
    if !temp_exists(c, table)? {
        return Err(AtlatlError::config(format!(
            "queue `{queue}` has no items yet: its producer must be extracted first in this replication (see `rest::extract_order`)"
        )));
    }
    let sql = if unique {
        format!(
            "SELECT value FROM temp.main.{} QUALIFY row_number() OVER (PARTITION BY value ORDER BY seq) = 1 ORDER BY seq",
            ident(table)
        )
    } else {
        format!("SELECT value FROM temp.main.{} ORDER BY seq", ident(table))
    };
    let mut s = c.prepare(&sql).map_err(db)?;
    let rows: Vec<String> = s
        .query_map([], |r| r.get(0))
        .map_err(db)?
        .collect::<std::result::Result<_, _>>()
        .map_err(db)?;
    rows.iter()
        .map(|t| {
            atlatl_http::json::parse_str(t)
                .map_err(|e| AtlatlError::internal(format!("queue `{queue}`: {e}")))
        })
        .collect()
}

/// A stream's configured `vars` as JSON (`null` when it has none).
pub(crate) fn config_vars(s: &atlatl_core::config::load::Stream) -> serde_json::Value {
    s.config
        .vars
        .as_ref()
        .map(|v| serde_json::to_value(v).unwrap_or_default())
        .unwrap_or_default()
}

fn row_item(text: &str) -> Result<JValue> {
    let v = atlatl_http::json::parse_str(text)
        .map_err(|e| AtlatlError::internal(format!("query row: {e}")))?;
    Ok(match v {
        JValue::Object(m) if m.len() == 1 => m.into_values().next().unwrap_or_default(),
        other => other,
    })
}

fn item_key(item: &JValue, cols: &[String]) -> Vec<Option<String>> {
    cols.iter()
        .map(|c| {
            match item.get(c).or(if cols.len() == 1 && !item.is_object() {
                Some(item)
            } else {
                None
            }) {
                None | Some(JValue::Null) => None,
                Some(JValue::String(s)) => Some(s.clone()),
                Some(v) => Some(v.to_string()),
            }
        })
        .collect()
}

/// `for_each.over.query` (07 §7, M7.1): rows from a lake (this session) or a Postgres connection,
/// minus the keys already landed in the target when `except_landed` is set.
pub(crate) async fn query_items(
    rt: &Runtime,
    session: &mut Session,
    sql: &str,
    conn: &str,
    except_landed: &[String],
    target: Option<(String, String, String)>,
) -> Result<Vec<JValue>> {
    let texts = query_rows(rt, session, sql, conn).await?;
    let items: Vec<JValue> = texts.iter().map(|t| row_item(t)).collect::<Result<_>>()?;
    except_landed_items(session, items, except_landed, target).await
}

/// `sql`'s rows as JSON objects, unparsed, from a lake (this session), DuckDB file or Postgres
/// connection.
pub(crate) async fn query_rows(
    rt: &Runtime,
    session: &mut Session,
    sql: &str,
    conn: &str,
) -> Result<Vec<String>> {
    Ok(if let Some(lake) = rt.lakes.get(conn) {
        let q = format!("SELECT to_json(q)::VARCHAR FROM ({sql}) q");
        let read = move |c: &mut duckdb::Connection| {
            let mut s = c.prepare(&q).map_err(db)?;
            let rows = s.query_map([], |r| r.get::<_, String>(0)).map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        };
        if lake.is_plain() {
            // Inside the file, as when it is opened on its own: `USE` on a throwaway connection
            // (closed when dropped), so no shared session is left resolving names in the file.
            let alias = atlatl_ingest::sql::ident(&lake.alias);
            // Interrupted with the unit, like the session it stands in for.
            let mut own = match session.cancel_token() {
                Some(c) => rt.engine.session_with_cancel(c.clone())?,
                None => rt.engine.session()?,
            };
            own.run(move |c| {
                c.execute_batch(&format!("USE {alias}")).map_err(db)?;
                read(c)
            })
            .await?
        } else {
            session.run(read).await?
        }
    } else {
        let c = rt.connections.get(conn).ok_or_else(|| {
            AtlatlError::config(format!(
                "`for_each.over.connection: {conn}` is not a resolved connection"
            ))
        })?;
        if let ConnectionKind::Duckdb(d) = &c.kind {
            duckdb_items(
                d.path.clone(),
                format!("SELECT to_json(q)::VARCHAR FROM ({sql}) q"),
            )
            .await?
        } else {
            if !matches!(c.kind, ConnectionKind::Postgres(_)) {
                return Err(AtlatlError::config(format!(
                    "`for_each.over.connection: {conn}` must be a lake, postgres or duckdb connection, not {}",
                    c.type_name()
                )));
            }
            let opts = atlatl_conn_postgres::ConnectOptions::from_connection(c)?;
            let pg = atlatl_conn_postgres::connect(&opts).await?;
            let rows = pg
                .client()
                .query(&format!("SELECT to_json(q)::text FROM ({sql}) q"), &[])
                .await
                .map_err(atlatl_conn_postgres::wrap)?;
            rows.iter()
                .map(|r| {
                    r.try_get::<_, String>(0)
                        .map_err(atlatl_conn_postgres::wrap)
                })
                .collect::<Result<_>>()?
        }
    })
}

/// `items` minus those whose `except_landed` values (as text) are already in the stream's target
/// table; a missing target excludes nothing (07 §7).
async fn except_landed_items(
    session: &mut Session,
    items: Vec<JValue>,
    except_landed: &[String],
    target: Option<(String, String, String)>,
) -> Result<Vec<JValue>> {
    let (Some((alias, schema, table)), false) = (target, except_landed.is_empty()) else {
        return Ok(items);
    };
    let cols = except_landed.to_vec();
    let landed: HashSet<Vec<Option<String>>> = session
        .run(move |c| {
            if staging::table_columns(c, &alias, &schema, &table)?.is_none() {
                return Ok(HashSet::new());
            }
            let sel = cols
                .iter()
                .map(|k| format!("CAST({} AS VARCHAR)", ident(k)))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT DISTINCT {sel} FROM {}.{}.{}",
                ident(&alias),
                ident(&schema),
                ident(&table)
            );
            let mut s = c.prepare(&sql).map_err(db)?;
            let n = cols.len();
            let rows = s
                .query_map([], |r| {
                    (0..n)
                        .map(|i| r.get::<_, Option<String>>(i))
                        .collect::<std::result::Result<Vec<_>, _>>()
                })
                .map_err(db)?;
            rows.collect::<std::result::Result<HashSet<_>, _>>()
                .map_err(db)
        })
        .await?;
    Ok(items
        .into_iter()
        .filter(|i| !landed.contains(&item_key(i, except_landed)))
        .collect())
}

/// Rows of `q` from a DuckDB file opened read-only in its own database, so a writer elsewhere
/// fails the open rather than racing it.
async fn duckdb_items(path: String, q: String) -> Result<Vec<String>> {
    tokio::task::spawn_blocking(move || {
        if !std::path::Path::new(&path).is_file() {
            return Err(AtlatlError::config(format!(
                "duckdb connection: `{path}` does not exist"
            )));
        }
        let cfg = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .and_then(|c| c.enable_autoload_extension(false))
            .map_err(db)?;
        let c = duckdb::Connection::open_with_flags(&path, cfg).map_err(db)?;
        let mut s = c.prepare(&q).map_err(db)?;
        let rows = s.query_map([], |r| r.get::<_, String>(0)).map_err(db)?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
    })
    .await
    .map_err(|e| AtlatlError::internal(format!("duckdb query task: {e}")))?
}

/// Per-column profile of the staged values, for `infer: safe` (04 §5.1).
#[derive(Clone, Debug, Default)]
struct Col {
    name: String,
    bools: u64,
    ints: u64,
    decimals: u64,
    floats: u64,
    strings: u64,
    nested: u64,
    scale: u32,
    int_digits: u32,
    /// Common ISO layout of every string value: `date`, `ts`, `tstz`, or none.
    layout: Option<&'static str>,
    layout_broken: bool,
}

fn layout_of(s: &str) -> Option<&'static str> {
    let b = s.as_bytes();
    let digits = |r: std::ops::Range<usize>| {
        b.get(r.clone())
            .is_some_and(|x| x.iter().all(u8::is_ascii_digit))
    };
    let date = b.len() >= 10
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10);
    if !date {
        return None;
    }
    if b.len() == 10 {
        return Some("date");
    }
    let t = &s[10..];
    let tb = t.as_bytes();
    if tb.len() < 9
        || !(tb[0] == b'T' || tb[0] == b' ')
        || !(tb[1..3].iter().all(u8::is_ascii_digit)
            && tb[3] == b':'
            && tb[4..6].iter().all(u8::is_ascii_digit)
            && tb[6] == b':'
            && tb[7..9].iter().all(u8::is_ascii_digit))
    {
        return None;
    }
    let mut rest = &t[9..];
    if let Some(f) = rest.strip_prefix('.') {
        let n = f.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            return None;
        }
        rest = &f[n..];
    }
    match rest {
        "" => Some("ts"),
        "Z" => Some("tstz"),
        z if (z.starts_with('+') || z.starts_with('-'))
            && z.len() == 6
            && z.as_bytes()[3] == b':' =>
        {
            Some("tstz")
        }
        _ => None,
    }
}

impl Col {
    fn see(&mut self, v: &JValue) -> Option<String> {
        match v {
            JValue::Null => None,
            JValue::Bool(b) => {
                self.bools += 1;
                Some(b.to_string())
            }
            JValue::Number(n) => {
                let t = n.as_str();
                if t.contains(['e', 'E']) {
                    self.floats += 1;
                } else if let Some((i, f)) = t.split_once('.') {
                    self.decimals += 1;
                    self.scale = self.scale.max(f.len() as u32);
                    self.int_digits = self.int_digits.max(i.trim_start_matches('-').len() as u32);
                } else if n.as_i64().is_some() {
                    self.ints += 1;
                } else {
                    self.decimals += 1;
                    self.int_digits = self.int_digits.max(t.trim_start_matches('-').len() as u32);
                }
                Some(t.to_owned())
            }
            JValue::String(s) => {
                self.strings += 1;
                match (layout_of(s), self.layout) {
                    (Some(l), None) if !self.layout_broken => self.layout = Some(l),
                    (Some(l), Some(p)) if l == p => {}
                    _ => {
                        self.layout = None;
                        self.layout_broken = true;
                    }
                }
                Some(s.clone())
            }
            nested => {
                self.nested += 1;
                Some(nested.to_string())
            }
        }
    }

    /// The DuckDB type `infer: safe` gives this column.
    fn safe_type(&self) -> &'static str {
        let kinds = [
            self.bools > 0,
            self.ints + self.decimals + self.floats > 0,
            self.strings > 0,
            self.nested > 0,
        ];
        match kinds {
            [true, false, false, false] => "BOOLEAN",
            [false, true, false, false] if self.decimals == 0 && self.floats == 0 => "BIGINT",
            [false, true, false, false]
                if self.floats == 0 && self.int_digits + self.scale <= 38 =>
            {
                "DECIMAL"
            }
            [false, true, false, false] => "DOUBLE",
            [false, false, true, false] => match self.layout {
                Some("date") => "DATE",
                Some("ts") => "TIMESTAMP",
                Some("tstz") => "TIMESTAMPTZ",
                _ => "VARCHAR",
            },
            _ => "VARCHAR",
        }
    }
}

fn column_name(n: &str, naming_mode: Option<Naming>) -> String {
    match naming_mode {
        Some(Naming::Lower) => n.to_lowercase(),
        Some(Naming::Snake) => naming::snake(n),
        Some(Naming::CleanCsv) => naming::clean_csv(&[n])
            .pop()
            .unwrap_or_else(|| n.to_owned()),
        _ => n.to_owned(),
    }
}

/// Collects rows for the raw staging table: each record becomes a JSON array of text values
/// aligned with first-seen column order.
#[derive(Default)]
struct Shaper {
    cols: IndexMap<String, Col>,
}

impl Shaper {
    fn row(&mut self, rec: &JValue) -> String {
        let mut vals: Vec<JValue> = Vec::new();
        if let Some(m) = rec.as_object() {
            for (k, v) in m {
                let n = self.cols.len();
                let i = self.cols.get_index_of(k).unwrap_or(n);
                let col = self.cols.entry(k.clone()).or_insert_with(|| Col {
                    name: k.clone(),
                    ..Default::default()
                });
                let text = col.see(v);
                if vals.len() <= i {
                    vals.resize(i + 1, JValue::Null);
                }
                vals[i] = text.map(JValue::String).unwrap_or(JValue::Null);
            }
        }
        JValue::Array(vals).to_string()
    }
}

fn threshold(s: &Stream, ep: &atlatl_http::spec::Endpoint) -> (f64, u64) {
    let st = s
        .config
        .on_decode_error
        .as_ref()
        .and_then(|o| o.reject_threshold);
    let ep_t = ep.on_decode_error.reject_threshold.as_ref();
    let ratio = st
        .and_then(|t| t.ratio)
        .or(ep_t.map(|t| t.ratio))
        .unwrap_or(0.0);
    let min = st
        .and_then(|t| t.min_count)
        .or(ep_t.map(|t| t.min_count))
        .unwrap_or(1);
    (ratio, min)
}

/// A stream's `on_decode_error.reject_ttl`, over the endpoint's.
fn reject_ttl(s: &Stream, ep: &atlatl_http::spec::Endpoint) -> Option<std::time::Duration> {
    s.config
        .on_decode_error
        .as_ref()
        .and_then(|o| o.reject_ttl)
        .or(ep.on_decode_error.reject_ttl)
        .map(|d| d.0)
}

fn to_serde(v: &JValue) -> serde_json::Value {
    v.to_serde()
}

/// Extracts and stages one REST member. Call for every REST stream of the unit in
/// [`extract_order`], feed-only producers included, on the unit's one session (queues live there).
///
/// In the commit the caller must `put` [`RestStaged::state`], delete [`RestStaged::expired`],
/// `upsert_reject` each of [`RestStaged::rejects`] (and [`RestStaged::refreshed`], which alone
/// don't warrant a commit) and `resolve_rejects(stream, member, resolved)`; afterwards drop the
/// staging and [`RestStaged::temp_tables`].
#[allow(clippy::too_many_arguments)]
pub async fn stage(
    rt: &Runtime,
    session: &mut Session,
    rep: &ReplicationPlan,
    sp: &StreamPlan,
    s: &Stream,
    member: &MemberPlan,
    staging_name: &str,
) -> Result<RestStaged> {
    let feed_only = sp.role == StreamRole::FeedOnly;
    let c = client(rt, &rep.source.name)?;
    let endpoint = endpoint_of(sp, member);
    let ep = c.spec().endpoint(&endpoint).cloned().ok_or_else(|| {
        AtlatlError::config(format!(
            "stream `{}`: API spec has no endpoint `{endpoint}`",
            sp.id
        ))
    })?;
    let state_member = String::new();
    let tgt_alias = rt.lake(&rep.target.name).map(|l| l.alias.clone()).ok();
    let target = match (&member.object, &tgt_alias) {
        (Some(o), Some(a)) => {
            let (sc, t) = split_object(o);
            Some((a.clone(), sc, t))
        }
        _ => None,
    };

    // Stored sync values and open rejects (read before the run; written only in the commit).
    let mut state_before = None;
    let mut open: Vec<Reject> = Vec::new();
    let except_skipped = ep
        .for_each
        .as_ref()
        .and_then(|f| f.except_skipped)
        .map(|d| d.0);
    let mut recent_skips: HashSet<String> = HashSet::new();
    let mut expired: Vec<String> = Vec::new();
    if let Some(st) = rt.state.clone() {
        let (id, h, st2) = (sp.state_id().to_owned(), sp.config_hash.clone(), st.clone());
        if !ep.sync.is_empty() {
            let row = session
                .run(move |conn| st2.get_checked(conn, &id, "", StateKind::Sync, &h))
                .await?;
            state_before = row.map(|r| r.value);
        }
        if !feed_only {
            let (id, st) = (sp.state_id().to_owned(), st.clone());
            open = session
                .run(move |conn| st.open_rejects(conn, &id))
                .await?
                .into_iter()
                .filter(|r| r.member.is_empty())
                .collect();
        }
        if let Some(ttl) = reject_ttl(s, &ep).filter(|_| !feed_only) {
            let (st, id) = (st.clone(), sp.state_id().to_owned());
            expired = session
                .run(move |conn| st.expired_rejects(conn, &id, "", ttl))
                .await?;
            open.retain(|r| !expired.contains(&r.reject_key));
        }
        if let Some(ttl) = except_skipped.filter(|_| !feed_only) {
            let (st, id) = (st.clone(), sp.state_id().to_owned());
            recent_skips = session
                .run(move |conn| st.recent_skips(conn, &id, "", ttl))
                .await?
                .into_iter()
                .collect();
        }
    } else if !feed_only
        && (!ep.sync.is_empty()
            || ep.record_skips
            || ep.on_decode_error.retry == DecodeRetry::Row
            || s.config.on_decode_error.as_ref().and_then(|o| o.retry) == Some(ErrorRetry::Row))
    {
        // Sync values and rejects persist in the lake's `_atlatl` tables.
        rt.state()?;
    }

    // `rejects retry`: only the open rejects are re-issued; nothing to do without any.
    let retry_only = rt.retry_rejects && !feed_only;
    if retry_only && open.is_empty() {
        let n = expired.len() as u64;
        rt.report.member(&rep.name, &sp.id, &member.name, |r| {
            r.rejects.expired = n;
        });
        return Ok(RestStaged {
            empty: true,
            member: state_member,
            state_before,
            expired,
            ..Default::default()
        });
    }

    let vars_json =
        atlatl_core::plan::stream_vars(crate::rest::config_vars(s), &rt.plan.selection.vars);
    let mut vars = RunVars {
        retry: replication_retry(rt, rep)?,
        ..Default::default()
    };
    let jv = |v: &serde_json::Value| JValue::from(v);
    vars.globals.insert("vars".into(), jv(&vars_json));
    vars.globals.insert(
        "stream".into(),
        jv(&serde_json::json!({"id": sp.id, "vars": vars_json})),
    );
    vars.globals.insert("run".into(), jv(&serde_json::json!({"id": rt.run_id, "started_at": atlatl_core::ids::rfc3339(rt.started)})));
    if let Some(i) = &rep.instance {
        vars.globals.insert(i.as_name.clone(), jv(&i.item));
    }
    if let Some(serde_json::Value::Object(m)) = &state_before {
        for (k, v) in m {
            vars.sync.insert(k.clone(), JValue::from_serde_exact(v));
        }
    }

    let items = match ep.for_each.as_ref().map(|f| &f.over) {
        _ if retry_only => None,
        Some(Over::Queue {
            queue,
            unique,
            except_landed,
        }) => {
            let (t, q, u) = (
                queue_table(&rt.run_id, &rep.name, queue),
                queue.clone(),
                *unique,
            );
            let items = session.run(move |conn| read_queue(conn, &t, &q, u)).await?;
            Some(except_landed_items(session, items, except_landed, target.clone()).await?)
        }
        Some(Over::Query {
            query,
            connection,
            except_landed,
        }) => Some(
            query_items(
                rt,
                session,
                query,
                connection,
                except_landed,
                target.clone(),
            )
            .await?,
        ),
        _ => None,
    };
    let range = match (&rt.plan.selection.range, feed_only) {
        (Some(r), false) if !retry_only => Some(RangeOverride::parse(r)?),
        _ => None,
    };
    let decode_retry = s
        .config
        .on_decode_error
        .as_ref()
        .and_then(|o| o.retry)
        .map(|r| match r {
            ErrorRetry::Request => DecodeRetry::Request,
            ErrorRetry::Row => DecodeRetry::Row,
            ErrorRetry::None => DecodeRetry::None,
        });
    let open_rejects = open
        .iter()
        .map(|r| OpenReject {
            key: r.reject_key.clone(),
            request: JValue::from_serde_exact(&r.request),
            class: r.error_class.clone(),
        })
        .collect();
    let tracked = rt.progress.track(&rep.name, &sp.id, &member.name);
    vars.live = Some(tracked.live.clone());
    let mut driver = Driver::start(
        &c,
        &endpoint,
        DriveOptions {
            vars,
            items,
            range: range.clone(),
            decode_retry,
            open_rejects,
            rejects_only: retry_only,
            recent_skips,
        },
    )
    .await?;

    // Queue tables for this producer's emits, and the raw staging table.
    let queues: IndexMap<String, String> = ep
        .emit
        .keys()
        .map(|q| (q.clone(), queue_table(&rt.run_id, &rep.name, q)))
        .collect();
    let raw = format!("{staging_name}__raw");
    let (qs, raw_c, stage_rows) = (
        queues.values().cloned().collect::<Vec<_>>(),
        raw.clone(),
        !feed_only,
    );
    let mut seq: i64 = session
        .run(move |conn| {
            let mut max = -1i64;
            for q in &qs {
                conn.execute_batch(&format!(
                    "CREATE TEMP TABLE IF NOT EXISTS {} (seq BIGINT, value VARCHAR)",
                    ident(q)
                ))
                .map_err(db)?;
                let m: Option<i64> = conn
                    .query_row(
                        &format!("SELECT max(seq) FROM temp.main.{}", ident(q)),
                        [],
                        |r| r.get(0),
                    )
                    .map_err(db)?;
                max = max.max(m.unwrap_or(-1));
            }
            if stage_rows {
                conn.execute_batch(&format!(
                    "CREATE OR REPLACE TEMP TABLE {} (ordinal BIGINT, record VARCHAR)",
                    ident(&raw_c)
                ))
                .map_err(db)?;
            }
            Ok(max + 1)
        })
        .await?;

    let mut shaper = Shaper::default();
    let mut rows = 0u64;
    while let Some(Chunk { records, emits }) = driver.next().await? {
        rows += records.len() as u64;
        let recs: Vec<(i64, String)> = if feed_only {
            Vec::new()
        } else {
            records
                .iter()
                .map(|r| (r.ordinal, shaper.row(&r.record)))
                .collect()
        };
        let mut qrows: Vec<(String, i64, String)> = Vec::new();
        for (q, v) in emits {
            if let Some(t) = queues.get(&q) {
                qrows.push((t.clone(), seq, v.to_string()));
                seq += 1;
            }
        }
        if recs.is_empty() && qrows.is_empty() {
            continue;
        }
        let raw_c = raw.clone();
        session
            .run(move |conn| {
                if !recs.is_empty() {
                    let mut app = conn
                        .appender_to_catalog_and_db(&raw_c, "temp", "main")
                        .map_err(db)?;
                    for (o, r) in &recs {
                        app.append_row(duckdb::params![o, r]).map_err(db)?;
                    }
                    app.flush().map_err(db)?;
                }
                let mut by_table: IndexMap<&str, Vec<(i64, &str)>> = IndexMap::new();
                for (t, sq, v) in &qrows {
                    by_table
                        .entry(t.as_str())
                        .or_default()
                        .push((*sq, v.as_str()));
                }
                for (t, vs) in by_table {
                    let mut app = conn
                        .appender_to_catalog_and_db(t, "temp", "main")
                        .map_err(db)?;
                    for (sq, v) in vs {
                        app.append_row(duckdb::params![sq, v]).map_err(db)?;
                    }
                    app.flush().map_err(db)?;
                }
                Ok(())
            })
            .await?;
    }
    let out = driver.finish()?;
    let rec = rt
        .rest_clients
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&rep.source.name)
        .and_then(|(_, r)| r.clone());
    if let Some(cas) = rec {
        cas.save()?;
    }

    // Rejects: threshold first (03 §6.1); nothing is persisted if the replication fails.
    let open_keys: HashSet<&str> = open.iter().map(|r| r.reject_key.as_str()).collect();
    let decode_rejects = out.rejects.iter().filter(|r| r.class == "decode").count() as u64;
    let samples: Vec<serde_json::Value> = out.rejects.iter().take(5).map(|r| serde_json::json!({"key": r.key, "class": r.class, "error": r.error, "raw": r.raw, "request": to_serde(&r.request)})).collect();
    let new = out
        .rejects
        .iter()
        .filter(|r| !open_keys.contains(r.key.as_str()))
        .count() as u64;
    let open_after = (open.len() as u64).saturating_sub(out.resolved.len() as u64) + new;
    let http = ReportHttp {
        requests: out.stats.http.requests,
        retries: out.stats.http.retries,
        throttle_wait_ms: out.stats.http.throttle_wait_ms,
        status_counts: out.stats.http.status_counts.clone(),
        items_deferred: out.items_deferred,
        items_held: out.items_held,
        skipped_items: out
            .skipped
            .iter()
            .map(|s| ReportSkip {
                key: s.request_key.clone(),
                reason: s.reason.clone(),
            })
            .collect(),
    };
    let range_unused = range.is_some() && !out.range_applied;
    let deferred = (out.items_deferred > 0).then(|| {
        format!(
            "endpoint `{endpoint}`: {} item(s) deferred to later runs by `for_each.max_items` ({} run)",
            out.items_deferred, out.items
        )
    });
    if let Some(m) = &deferred {
        tracing::warn!(replication = %rep.name, stream_id = %sp.id, "{m}");
        rt.events.emit(atlatl_core::obs::events::EventBody::Warning(
            atlatl_core::obs::events::Warning {
                replication: Some(rep.name.clone()),
                stream_id: Some(sp.id.clone()),
                code: "items_deferred".into(),
                message: m.clone(),
            },
        ));
    }
    rt.report.member(&rep.name, &sp.id, &member.name, |r| {
        r.http = Some(http);
        r.bytes.read += out.stats.bytes_read;
        r.rows.rejected = decode_rejects;
        r.rejects.new = new;
        r.rejects.resolved = out.resolved.len() as u64;
        r.rejects.expired = expired.len() as u64;
        r.rejects.open = open_after;
        r.rejects.samples = samples.clone();
        for a in &out.stats.attempts {
            r.record_retry(a);
        }
        if let Some(m) = &deferred {
            r.warn("items_deferred", m.clone());
        }
        if range_unused {
            r.warn("range_unused", format!("`--range` has no effect on endpoint `{endpoint}`: its `for_each.over` doesn't call `range()`"));
        }
    });
    // A retry replays only the rejects: its ratio says nothing, and nothing gets worse.
    let (ratio, min_count) = threshold(s, &ep);
    let total = rows + decode_rejects;
    if !retry_only
        && decode_rejects > 0
        && decode_rejects >= min_count
        && (decode_rejects as f64 / total.max(1) as f64) > ratio
    {
        return Err(AtlatlError::new(ErrorClass::Data, format!("stream `{}`: {decode_rejects} rejected row(s) of {total}, over `reject_threshold` (ratio {ratio}, min_count {min_count}); first: {}", sp.id, out.rejects.iter().find(|r| r.class == "decode").map(|r| r.error.as_str()).unwrap_or_default())).code("reject_threshold"));
    }

    let temp_tables: Vec<String> = queues.values().cloned().collect();
    if feed_only {
        return Ok(RestStaged {
            staging: None,
            rows,
            member: state_member,
            temp_tables,
            stats: out.stats,
            ..Default::default()
        });
    }

    // Project the JSON staging into typed columns (04 §5.2): first-seen key order, then declared
    // columns not seen; nested values are JSON text; `infer: text` (default) keeps VARCHAR.
    let cfg = &s.config;
    let safe = cfg.infer == Some(Infer::Safe);
    let include = cfg.columns.as_ref().and_then(|c| c.include.clone());
    let exclude = cfg
        .columns
        .as_ref()
        .and_then(|c| c.exclude.clone())
        .unwrap_or_default();
    let keep = |n: &str| {
        include.as_ref().is_none_or(|i| i.iter().any(|x| x == n)) && !exclude.iter().any(|x| x == n)
    };
    let mut exprs: Vec<(String, String)> = Vec::new();
    for (i, (_, col)) in shaper.cols.iter().enumerate() {
        if !keep(&col.name) {
            continue;
        }
        let base = format!("json_extract_string(record, '$[{i}]')");
        let e = match (safe, col.safe_type()) {
            (true, "DECIMAL") => format!("TRY_CAST({base} AS DECIMAL(38, {}))", col.scale),
            (true, t) if t != "VARCHAR" => format!("TRY_CAST({base} AS {t})"),
            _ => base,
        };
        exprs.push((column_name(&col.name, cfg.naming), e));
    }
    let seen: HashSet<String> = exprs.iter().map(|(n, _)| n.clone()).collect();
    let declared: Vec<String> = cfg
        .columns
        .as_ref()
        .map(|c| {
            c.columns
                .keys()
                .filter(|k| k.as_str() != "*")
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let mut empty = false;
    if rows == 0 && exprs.is_empty() {
        let tcols = match &target {
            Some((a, sc, t)) => {
                let (a, sc, t) = (a.clone(), sc.clone(), t.clone());
                session
                    .run(move |conn| staging::table_columns(conn, &a, &sc, &t))
                    .await?
            }
            None => None,
        };
        let meta_names: Vec<String> =
            metadata::columns(cfg.metadata.as_ref().map(|m| &m.value), 0, "", "")
                .into_iter()
                .map(|(n, _, _)| n)
                .collect();
        match tcols {
            Some(tc) => exprs.extend(
                tc.into_iter()
                    .filter(|c| !meta_names.contains(&c.name))
                    .map(|c| (c.name, format!("CAST(NULL AS {})", c.kind))),
            ),
            None => {
                let pk = sp.primary_key.clone().unwrap_or_default();
                empty = declared.is_empty();
                exprs.extend(
                    pk.into_iter()
                        .map(|k| (k, "CAST(NULL AS VARCHAR)".to_owned())),
                );
            }
        }
    }
    for d in declared.iter().filter(|d| !seen.contains(*d) && keep(d)) {
        if !exprs.iter().any(|(n, _)| n == d) {
            exprs.push((d.clone(), "CAST(NULL AS VARCHAR)".into()));
        }
    }
    if exprs.is_empty() {
        exprs.push(("_atlatl_empty".into(), "CAST(NULL AS VARCHAR)".into()));
        empty = true;
    }
    let proj = exprs
        .iter()
        .map(|(n, e)| format!("{e} AS {}", ident(n)))
        .collect::<Vec<_>>()
        .join(", ");
    let meta = metadata::columns(
        cfg.metadata.as_ref().map(|m| &m.value),
        rt.started_epoch_s(),
        &rt.run_id,
        &format!("{}:{endpoint}", rep.source.name),
    );
    let select = metadata::with_columns(
        &format!(
            "(SELECT {proj} FROM temp.main.{} ORDER BY ordinal)",
            ident(&raw)
        ),
        &meta,
    );
    let (stg, raw_c) = (staging_name.to_owned(), raw.clone());
    let staged = session
        .run(move |conn| {
            let n = staging::stage_query(conn, &stg, &select)?;
            staging::drop(conn, &raw_c)?;
            Ok(n)
        })
        .await?;
    if staged != rows {
        return Err(AtlatlError::internal(format!(
            "stream `{}`: extracted {rows} records but staged {staged}",
            sp.id
        )));
    }

    // A retry's records don't move `sync` values: they're folded only over the replays.
    let state = (!out.sync.is_empty() && !retry_only).then(|| StateRow {
        stream_id: sp.state_id().to_owned(),
        member: state_member.clone(),
        kind: StateKind::Sync,
        value: serde_json::Value::Object(
            out.sync
                .iter()
                .map(|(k, v)| (k.clone(), v.to_serde_exact()))
                .collect(),
        ),
        config_hash: sp.config_hash.clone(),
        run_id: rt.run_id.clone(),
        lake_snapshot: None,
        updated_at: None,
    });
    let (refreshed, rejects): (Vec<Reject>, Vec<Reject>) = out
        .rejects
        .iter()
        .map(|r| Reject {
            stream_id: sp.state_id().to_owned(),
            member: state_member.clone(),
            reject_key: r.key.clone(),
            request: r.request.to_serde_exact(),
            raw: r.raw.clone(),
            error_class: r.class.clone(),
            error: atlatl_core::redact::redact(&r.error).into_owned(),
            run_id: rt.run_id.clone(),
            attempts: 0,
        })
        // Under `except_skipped` a skip seen again is a retry its TTL allowed: recording it
        // restarts the TTL, so it's a change even when the error is the same.
        .partition(|r| {
            !(except_skipped.is_some() && r.error_class == "skipped")
                && open.iter().any(|o| {
                    o.reject_key == r.reject_key
                        && o.error_class == r.error_class
                        && o.error == r.error
                })
        });
    Ok(RestStaged {
        staging: Some(staging_name.to_owned()),
        rows,
        empty,
        member: state_member,
        state,
        state_before,
        rejects,
        refreshed,
        resolved: out.resolved,
        expired,
        temp_tables,
        stats: out.stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_and_safe_types() {
        assert_eq!(layout_of("2025-01-31"), Some("date"));
        assert_eq!(layout_of("2025-01-31T10:00:00"), Some("ts"));
        assert_eq!(layout_of("2025-01-31 10:00:00.123"), Some("ts"));
        assert_eq!(layout_of("2025-01-31T10:00:00Z"), Some("tstz"));
        assert_eq!(layout_of("2025-01-31T10:00:00+02:00"), Some("tstz"));
        assert_eq!(layout_of("2025-1-31"), None);
        let j = |s: &str| atlatl_http::json::parse_str(s).expect("j");
        let prof = |vals: &[&str]| {
            let mut c = Col::default();
            for v in vals {
                c.see(&j(v));
            }
            c.safe_type()
        };
        assert_eq!(prof(&["1", "2", "null"]), "BIGINT");
        assert_eq!(prof(&["1.10", "2"]), "DECIMAL");
        assert_eq!(prof(&["1e3"]), "DOUBLE");
        assert_eq!(prof(&["true", "false"]), "BOOLEAN");
        assert_eq!(prof(&[r#""2025-01-01""#, r#""2025-01-02""#]), "DATE");
        assert_eq!(
            prof(&[r#""2025-01-01""#, r#""2025-01-02T00:00:00""#]),
            "VARCHAR"
        );
        assert_eq!(prof(&[r#""007""#]), "VARCHAR");
        assert_eq!(prof(&["1", r#""x""#]), "VARCHAR");
        assert_eq!(prof(&["123456789012345678901234567890"]), "DECIMAL");
    }

    #[test]
    fn shaper_aligns_columns() {
        let j = |s: &str| atlatl_http::json::parse_str(s).expect("j");
        let mut s = Shaper::default();
        assert_eq!(
            s.row(&j(r#"{"a": 1.10, "b": {"x": [1]}}"#)),
            r#"["1.10","{\"x\":[1]}"]"#
        );
        assert_eq!(
            s.row(&j(r#"{"c": true, "a": null}"#)),
            r#"[null,null,"true"]"#
        );
        assert_eq!(
            s.cols.keys().cloned().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn queue_names_are_scoped() {
        let a = queue_table("01J9ZZABCDEFGHJK", "rep", "companies");
        assert!(
            a.starts_with("q_cdefghjk_") && a.ends_with("_companies"),
            "{a}"
        );
        assert_ne!(a, queue_table("01J9ZZABCDEFGHJK", "rep2", "companies"));
    }
}
