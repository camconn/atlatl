//! Lake → lake replications (04, M0.17/M0.18): stage every member outside the transaction, validate
//! and plan table drift, lock the targets, then one `BEGIN … COMMIT` for all streams and their state.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use atlatl_conn_clickhouse::bind::Lit;
use atlatl_conn_redshift::{Redshift, UnloadRequest, Unloaded};
use atlatl_core::config::connection::Connection as ConnConfig;
use atlatl_core::config::load::Stream;
use atlatl_core::config::model::{
    ChunkBy, ConnectionKind, DeltaConn, FilesConn, Mode, NullKeys, OnEmpty,
};
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::lock::LockKey;
use atlatl_core::obs::report::{DiffReport, StreamStatus};
use atlatl_core::plan::{InstancePlan, MemberPlan, ReplicationPlan, StreamPlan, StreamRole};
use atlatl_core::retry::{FixedJitter, RandomJitter, RetryLevel, RetryPolicy, retry};
use atlatl_core::template::Context;
use atlatl_engine::Session;
use atlatl_ingest::drift::{self, Adoption, Declared, Policy};
use atlatl_ingest::modes::{Apply, Counts, Kind, staged_max, update_key_filter};
use atlatl_ingest::sql::ident;
use atlatl_ingest::{chunk, metadata, staging, validate};
use atlatl_state::{StateKind, StateRow};
use duckdb::Connection;
use tokio_util::sync::CancellationToken;

use crate::runtime::Runtime;

/// The runner's output for one unit.
#[derive(Debug, Default)]
pub struct LakeOutcome {
    pub lake_snapshot: Option<i64>,
    pub changed: bool,
}

/// Where a lake-bound unit reads from: a lake pinned at a snapshot, or Delta tables pinned per member.
enum Src {
    Lake {
        alias: String,
        snapshot: i64,
    },
    Delta(Box<DeltaConn>),
    Files(Box<FilesConn>),
    Redshift(Arc<Redshift>),
    Postgres(Arc<crate::pgsrc::PgSrc>),
    /// REST endpoints; staged by `crate::rest::stage`.
    Rest,
    Clickhouse(Arc<atlatl_conn_clickhouse::ClickHouse>),
}

/// UNLOADs of a unit, removed from S3 once it is over (not on kill: 03 §9).
type Unloads = std::sync::Mutex<Vec<(Arc<Redshift>, Unloaded)>>;

/// One member's staged work, ready for the commit.
#[derive(Clone, Debug)]
struct Work {
    replication: String,
    stream_id: String,
    /// The key for state and rejects (the stream id, or `<id>@<key>` for a `for_each` instance).
    state_id: String,
    member: String,
    target: String,
    target_schema: String,
    target_table: String,
    kind: Kind,
    primary_key: Vec<String>,
    null_safe: bool,
    /// Run-varying metadata columns the change guard skips (04 §8).
    unguarded: Vec<String>,
    adoption: Adoption,
    /// Re-plans `adoption` when the target appeared after staging (a concurrent creator, e.g. a
    /// sibling `for_each` instance): the target's catalog alias, drift policy and declared types.
    replan: (String, Policy, Declared),
    /// Staging table, or the pinned source relation for chunked streams.
    source: String,
    /// Chunk relations, applied in order inside the one transaction.
    chunks: Vec<String>,
    staged: u64,
    skip: bool,
    state: Option<StateRow>,
    state_before: Option<serde_json::Value>,
    pre_hooks: Vec<String>,
    post_hooks: Vec<String>,
    /// Delta pin to detach once the unit is done.
    pinned: Option<String>,
    /// Downloaded files (ClickHouse results) to remove once the unit is done.
    scratch: Option<std::path::PathBuf>,
    /// SQL to run once the unit is done (Redshift raw staging, read-back secret).
    after: Vec<String>,
    /// REST rejects to record, and `(member, keys)` of rejects that now succeeded.
    rejects: Vec<atlatl_state::Reject>,
    /// Still failing with the same error: bumped only if the commit happens anyway.
    refreshed: Vec<atlatl_state::Reject>,
    /// Past `reject_ttl`: deleted in the commit.
    expired: Vec<String>,
    resolved: Option<(String, Vec<String>)>,
}

pub fn split_object(object: &str) -> (String, String) {
    let parts: Vec<&str> = object.split('.').collect();
    match parts.as_slice() {
        [t] => ("main".into(), (*t).to_owned()),
        [.., s, t] => ((*s).to_owned(), (*t).to_owned()),
        [] => ("main".into(), String::new()),
    }
}

fn glob_to_like(g: &str) -> String {
    g.replace('%', "\\%")
        .replace('_', "\\_")
        .replace('*', "%")
        .replace('?', "_")
}

/// Sorted `schema.table` members of a lake matching a `from.tables` glob (views excluded).
fn discover(c: &Connection, alias: &str, pattern: &str, exclude: &[String]) -> Result<Vec<String>> {
    let mut s = c
        .prepare(
            "SELECT schema_name || '.' || table_name FROM duckdb_tables()
             WHERE database_name = ? AND (schema_name || '.' || table_name) LIKE ? ESCAPE '\\' ORDER BY 1",
        )
        .map_err(atlatl_ingest::sql::db)?;
    let names: Vec<String> = s
        .query_map([alias, &glob_to_like(pattern)], |r| r.get(0))
        .map_err(atlatl_ingest::sql::db)?
        .collect::<std::result::Result<_, _>>()
        .map_err(atlatl_ingest::sql::db)?;
    Ok(names
        .into_iter()
        .filter(|n| !exclude.iter().any(|e| n.as_str() == e || like_match(n, e)))
        .collect())
}

fn like_match(name: &str, glob: &str) -> bool {
    fn m(n: &[u8], g: &[u8]) -> bool {
        match (g.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => m(n, &g[1..]) || (!n.is_empty() && m(&n[1..], g)),
            (Some(b'?'), Some(_)) => m(&n[1..], &g[1..]),
            (Some(a), Some(b)) if a == b => m(&n[1..], &g[1..]),
            _ => false,
        }
    }
    m(name.as_bytes(), glob.as_bytes())
}

fn kind_of(mode: &str) -> Result<Kind> {
    match mode {
        "full-refresh" => Ok(Kind::FullRefresh),
        "incremental" => Ok(Kind::Incremental),
        "snapshot" => Ok(Kind::Snapshot),
        m => Err(AtlatlError::config(format!(
            "mode `{m}` isn't a lake write mode"
        ))),
    }
}

fn render_member(
    rt: &Runtime,
    s: &Stream,
    sp: &StreamPlan,
    inst: Option<&InstancePlan>,
    name: &str,
) -> Result<(Option<String>, Option<String>)> {
    let (schema, table) = split_object(name);
    let member = minijinja::Value::from_pairs([
        ("name", name.to_owned()),
        ("schema", schema),
        ("table", table),
    ]);
    let vars = atlatl_core::plan::stream_vars(crate::rest::config_vars(s), &rt.plan.selection.vars);
    let ctx = Context::load(rt.vars.clone(), &rt.project.root, rt.plan.env.as_deref())
        .with("member", member)
        .with("vars", atlatl_core::template::to_value(&vars))
        .with(
            "stream",
            minijinja::Value::from_pairs([
                ("id", minijinja::Value::from(sp.id.clone())),
                ("vars", atlatl_core::template::to_value(&vars)),
            ]),
        )
        .with(
            "run",
            minijinja::Value::from_pairs([
                ("id", rt.run_id.clone()),
                ("started_at", atlatl_core::ids::rfc3339(rt.started)),
            ]),
        );
    let ctx = match inst {
        Some(i) => ctx.with(&i.as_name, atlatl_core::template::to_value(&i.item)),
        None => ctx,
    };
    let r = |t: Option<&String>| {
        t.map(|t| {
            rt.templates
                .render(t, &ctx)
                .map_err(|e| AtlatlError::config(e.message))
        })
        .transpose()
    };
    Ok((
        r(Some(&sp.object))?,
        r(s.config.window.as_ref().map(|w| &w.value))?,
    ))
}

/// Resolved members of a stream: the plan's rendered members, or wildcard expansion now.
#[allow(clippy::too_many_arguments)]
async fn members(
    rt: &Runtime,
    session: &mut Session,
    alias: &str,
    rs: Option<&Redshift>,
    pg: Option<&crate::pgsrc::PgSrc>,
    cancel: &CancellationToken,
    s: &Stream,
    sp: &StreamPlan,
    inst: Option<&InstancePlan>,
) -> Result<Vec<MemberPlan>> {
    let mut out = Vec::new();
    for m in &sp.members {
        if !m.pattern {
            out.push(m.clone());
            continue;
        }
        let (a, pat) = (alias.to_owned(), m.name.clone());
        let excl = s
            .config
            .from
            .as_ref()
            .and_then(|f| f.exclude.clone())
            .unwrap_or_default();
        let views = s
            .config
            .from
            .as_ref()
            .and_then(|f| f.include_views)
            .unwrap_or(false);
        let found = match (rs, pg) {
            (Some(rs), _) => rs.discover(&pat, &excl, views, cancel).await?,
            (None, Some(pg)) => pg.discover(&pat, &excl, views).await?,
            (None, None) => session.run(move |c| discover(c, &a, &pat, &excl)).await?,
        };
        if found.is_empty() {
            let warn = s.config.from.as_ref().and_then(|f| f.on_no_match)
                == Some(atlatl_core::config::model::OnNoMatch::Warn);
            if !warn {
                return Err(AtlatlError::config(format!(
                    "`from.tables: {}` matched no tables",
                    m.name
                ))
                .code("no_match"));
            }
        }
        for name in found {
            let (object, window) = render_member(rt, s, sp, inst, &name)?;
            let (schema, table) = split_object(&name);
            let (primary_key, primary_key_from) =
                crate::pgsrc::member_key(pg.map(|p| &p.source), s, &name).await?;
            out.push(MemberPlan {
                name,
                schema: Some(schema),
                table: Some(table),
                pattern: false,
                object,
                window,
                query: None,
                url: None,
                primary_key,
                primary_key_from,
            });
        }
    }
    Ok(out)
}

struct StagedFiles {
    set: atlatl_conn_files::FileSet,
    select: String,
}

/// Lists (and fetches) a files member; the set is empty only under `on_no_match: warn`.
async fn files_source(
    rt: &Runtime,
    rep: &ReplicationPlan,
    s: &Stream,
    m: &MemberPlan,
    conn: &FilesConn,
    seq: (usize, usize),
) -> Result<StagedFiles> {
    use atlatl_conn_files::{Downloader, FilesRead, FilesSource, render_selector};
    let mut read = FilesRead::from_stream(&s.config, Some(conn))?;
    let ctx = Context::load(rt.vars.clone(), &rt.project.root, rt.plan.env.as_deref());
    let url = m.url.clone().unwrap_or_else(|| read.selector.url.clone());
    let vars: indexmap::IndexMap<String, serde_json::Value> = serde_json::from_value(
        atlatl_core::plan::stream_vars(crate::rest::config_vars(s), &rt.plan.selection.vars),
    )
    .unwrap_or_default();
    read.selector = render_selector(&read.selector, &rt.templates, &ctx, Some(&vars))?;
    read.selector.url = url;
    if read.root.is_none() {
        read.root = Some(rt.project.root.clone());
    }
    let mut src = FilesSource::new(read)?;
    static UNIT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = UNIT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let workdir = rt.workdir().join(format!("m{n}_{}_{}", seq.0, seq.1));
    std::fs::create_dir_all(&workdir)
        .map_err(|e| AtlatlError::internal(format!("creating {}: {e}", workdir.display())))?;
    if src.needs_fetch() {
        let policy = rt
            .project
            .replication(rep.config_name())
            .map(|r| RetryPolicy::from_config(&r.retry).unwrap_or_default())
            .unwrap_or_default();
        let headers: Vec<(String, String)> = conn
            .headers
            .iter()
            .flatten()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        src.fetch(&Downloader::with_headers(policy, &headers)?, &workdir)
            .await?;
    }
    let mut session = rt.engine.session()?;
    let set = session.run(move |c| src.discover(c, &workdir)).await?;
    let select = if set.is_empty() {
        "(SELECT NULL AS x WHERE false)".into()
    } else {
        format!("({})", set.select()?)
    };
    Ok(StagedFiles { set, select })
}

/// Stages one member and plans its apply.
#[allow(clippy::too_many_arguments)]
async fn prepare(
    rt: &Runtime,
    session: &mut Session,
    rep: &ReplicationPlan,
    sp: &StreamPlan,
    s: &Stream,
    m: &MemberPlan,
    seq: (usize, usize),
    source: &Src,
    tgt_alias: &str,
    attempt: u32,
    cancel: &CancellationToken,
    unloads: &Unloads,
    mut admitted: Option<Admitted>,
) -> Result<Work> {
    let c = &s.config;
    // `rejects retry` lands only the replayed rows: merged by key (appended without one), never
    // mirrored, so a full-refresh target keeps the rest.
    let kind = match kind_of(&sp.mode)? {
        Kind::FullRefresh if rt.retry_rejects => Kind::Incremental,
        k => k,
    };
    let object = m
        .object
        .clone()
        .ok_or_else(|| AtlatlError::config(format!("stream `{}` has no rendered object", sp.id)))?;
    let (t_schema, t_table) = split_object(&object);
    let target = format!(
        "{}.{}.{}",
        ident(tgt_alias),
        ident(&t_schema),
        ident(&t_table)
    );
    let mut pinned = None;
    let mut scratch = None;
    let mut file_set: Option<StagedFiles> = None;
    let is_rs = matches!(source, Src::Redshift(_));
    let is_pg = matches!(source, Src::Postgres(_));
    let is_rest = matches!(source, Src::Rest);
    let mut rs_cols = None;
    let mut rest: Option<crate::rest::RestStaged> = None;
    let state_member = if s.config.from.as_ref().is_some_and(|f| f.tables.is_some()) {
        m.name.clone()
    } else {
        String::new()
    };
    let config_hash = sp.config_hash.clone();
    let mut filters: Vec<String> = m.window.iter().cloned().collect();
    let mut pg_cursor: Option<(usize, atlatl_conn_postgres::source::Cursor)> = None;
    let mut state_before = None;
    let mut ch_mark = None;
    let update_key = c.update_key.as_ref().map(|u| u.value.clone());
    if let Some(uk) = &update_key
        && kind == Kind::Incremental
        && !is_rest
    {
        let st = rt.state()?.clone();
        let (id, mem, h) = (
            sp.state_id().to_owned(),
            state_member.clone(),
            config_hash.clone(),
        );
        let row = session
            .run(move |conn| st.get_checked(conn, &id, &mem, StateKind::UpdateKey, &h))
            .await?;
        let stored = row.as_ref().and_then(|r| {
            r.value
                .get("value")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        });
        let stored_ty = row.as_ref().and_then(|r| {
            r.value
                .get("type")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        });
        state_before = row.map(|r| r.value);
        let lookback_us = lookback_us(uk.lookback);
        let lookback = lookback_us.map(atlatl_core::config::types::interval_text);
        let include_nulls = uk.nulls == Some(atlatl_core::config::model::Nulls::Include);
        let f = if is_rs {
            atlatl_conn_redshift::catalog::update_key_filter(
                &uk.column,
                stored.as_deref(),
                stored_ty.as_deref(),
                lookback.as_deref(),
                uk.inclusive.unwrap_or(true),
                include_nulls,
            )
        } else {
            update_key_filter(
                &uk.column,
                stored.as_deref(),
                stored_ty.as_deref(),
                lookback.as_deref(),
                uk.inclusive.unwrap_or(true),
                include_nulls,
            )
        };
        if let Some(f) = f {
            if is_pg {
                let cur = atlatl_conn_postgres::source::Cursor {
                    column: uk.column.clone(),
                    stored: stored.clone(),
                    lookback_us,
                    inclusive: uk.inclusive.unwrap_or(true),
                    include_nulls,
                };
                pg_cursor = Some((filters.len(), cur));
            }
            filters.push(f);
        }
        if matches!(source, Src::Clickhouse(_)) {
            let value = match stored.as_deref() {
                Some(v) => {
                    Some(clickhouse_watermark(session, v, stored_ty.as_deref(), lookback_us).await?)
                }
                None => None,
            };
            ch_mark = Some(atlatl_conn_clickhouse::incremental::Watermark {
                column: uk.column.clone(),
                value,
                inclusive: uk.inclusive.unwrap_or(true),
                include_nulls,
            });
        }
    }
    let stg = staging::staging_name(&rt.run_id, seq.0, seq.1);
    let src = match (source, &m.table, &m.query) {
        (Src::Rest, ..) => {
            rest = Some(crate::rest::stage(rt, session, rep, sp, s, m, &stg).await?);
            String::new()
        }
        (Src::Delta(d), ..) => {
            let from = c.from.as_ref();
            let table = from.and_then(|f| f.table.clone()).ok_or_else(|| {
                AtlatlError::config(format!(
                    "stream `{}`: delta sources read `from.table`",
                    sp.id
                ))
            })?;
            let version = from.and_then(|f| f.version);
            let (d, alias) = (d.clone(), atlatl_conn_delta::pin_alias(seq.0, seq.1));
            let p = session
                .run(move |conn| {
                    let t = atlatl_conn_delta::discover(conn, &d, &table)?;
                    atlatl_conn_delta::pin(conn, &t.uri, version.unwrap_or(t.head), &alias)
                })
                .await?;
            pinned = Some(p.alias.clone());
            format!("(SELECT * FROM {})", p.relation())
        }
        (Src::Files(fc), ..) => {
            let files = files_source(rt, rep, s, m, fc, seq).await?;
            let src = files.select.clone();
            file_set = Some(files);
            src
        }
        // Read once the cursor is known (it may be evaluated in Postgres).
        (Src::Postgres(_), ..) => String::new(),
        (Src::Clickhouse(ch), ..) => {
            let first_run = if s
                .config
                .bind
                .iter()
                .flatten()
                .any(|(_, b)| b.fallback.is_some())
            {
                let (a, sc, t) = (tgt_alias.to_owned(), t_schema.clone(), t_table.clone());
                session
                    .run(move |c| staging::table_columns(c, &a, &sc, &t))
                    .await?
                    .is_none()
            } else {
                false
            };
            let (rel, dir) = clickhouse_member(
                rt,
                session,
                rep,
                sp,
                s,
                m,
                seq,
                ch,
                ch_mark.as_ref(),
                first_run,
                match admitted.take() {
                    Some(Admitted::Clickhouse(d)) => Some(d),
                    _ => None,
                },
                cancel,
            )
            .await?;
            scratch = Some(dir);
            rel
        }
        (Src::Redshift(rs), Some(_), _) => {
            let object = atlatl_conn_redshift::Object::parse(&m.name);
            let cols = match admitted.take() {
                Some(Admitted::Redshift(cols)) => cols,
                _ => redshift_admitted(&s.config, rs.columns(&m.name, cancel).await?)
                    .map_err(|l| crate::pgsrc::source_types(&m.name, l))?,
            };
            rs_cols = Some((rs.clone(), object, cols));
            String::new()
        }
        (Src::Redshift(_), None, _) => {
            return Err(AtlatlError::config(format!(
                "stream `{}`: redshift sources read `from.table` or `from.tables`",
                sp.id
            )));
        }
        (Src::Lake { .. }, _, Some(q)) => format!("({q})"),
        (Src::Lake { alias, snapshot }, Some(t), _) => {
            let schema = m.schema.clone().unwrap_or_else(|| "main".into());
            format!(
                "(SELECT * FROM {}.{}.{} AT (VERSION => {snapshot}))",
                ident(alias),
                ident(&schema),
                ident(t)
            )
        }
        _ => {
            return Err(AtlatlError::config(format!(
                "stream `{}`: lake sources read `from.table`, `from.tables` or `from.query`",
                sp.id
            )));
        }
    };

    let src = match source {
        Src::Postgres(pg) => {
            let cols = match admitted.take() {
                Some(Admitted::Postgres(cols)) => Some(cols),
                _ => None,
            };
            let r = pg
                .relation(s, sp, m, cols, pg_cursor.as_ref().map(|(_, c)| c), &filters)
                .await?;
            if let (true, Some((i, _))) = (r.pushed, &pg_cursor) {
                filters.remove(*i);
            }
            r.sql
        }
        _ => src,
    };
    let mut after: Vec<String> = Vec::new();
    let mut rs_chunks: Option<Vec<String>> = None;
    let mut rs_expected: Option<u64> = None;
    let src = match rs_cols {
        Some((rs, object, cols)) => {
            let r = if rs.unloads() {
                unload_member(
                    rt,
                    session,
                    rep,
                    sp,
                    m,
                    seq,
                    (&rs, &object, cols),
                    &filters,
                    attempt,
                    cancel,
                    unloads,
                )
                .await?
            } else {
                export_member(
                    rt,
                    session,
                    rep,
                    sp,
                    m,
                    seq,
                    (&rs, object, cols),
                    &filters,
                    cancel,
                )
                .await?
            };
            // Filters and columns went into the UNLOAD; nothing more to apply engine-side.
            filters.clear();
            after = r.after;
            rs_expected = r.chunks.as_ref().and(r.expected_rows);
            rs_chunks = r.chunks;
            r.source
        }
        None => src,
    };
    // Redshift and postgres narrow `columns` at the source; ClickHouse drops excluded ones.
    // A files column's `on_cast_error.set_null` flag goes with it.
    let nulled = file_set
        .as_ref()
        .map(|f| f.set.nulled())
        .unwrap_or_default();
    let with_flags = |names: Vec<String>| {
        let flags: Vec<String> = names
            .iter()
            .filter_map(|n| nulled.iter().find(|a| a.eq_ignore_ascii_case(n)))
            .map(|a| format!("{}{a}", atlatl_core::config::model::CAST_NULLED))
            .collect();
        names
            .into_iter()
            .chain(flags)
            .map(|i| ident(&i))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let projection = if is_rs || is_pg {
        "*".to_owned()
    } else {
        match c.columns.as_ref().and_then(|cc| cc.include.clone()) {
            Some(inc) => with_flags(inc),
            None => match c.columns.as_ref().and_then(|cc| cc.exclude.clone()) {
                Some(ex) if !matches!(source, Src::Clickhouse(_)) => {
                    format!("* EXCLUDE ({})", with_flags(ex))
                }
                _ => "*".into(),
            },
        }
    };
    let where_ = if filters.is_empty() {
        String::new()
    } else {
        format!(
            " WHERE {}",
            filters
                .iter()
                .map(|f| format!("({f})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    };
    let meta = metadata::columns(
        c.metadata.as_ref().map(|m| &m.value),
        rt.started_epoch_s(),
        &rt.run_id,
        &m.name,
    );
    let wrap = |src: &str| {
        metadata::with_columns(
            &format!("(SELECT {projection} FROM {src} x{where_})"),
            &meta,
        )
    };
    let select = wrap(&src);
    let file_chunks: Option<Vec<String>> = match (&file_set, &c.chunk) {
        (Some(f), Some(ch)) if ch.by == ChunkBy::Files => Some(
            f.set
                .chunks(ch)?
                .iter()
                .map(|k| format!("({})", wrap(&format!("({})", k.select))))
                .collect(),
        ),
        _ => None,
    }
    .or_else(|| {
        rs_chunks.map(|v| {
            v.iter()
                .map(|k| format!("({})", wrap(&format!("({k})"))))
                .collect()
        })
    });
    let empty_files = file_set.as_ref().is_some_and(|f| f.set.is_empty());
    let expected_rows = file_set
        .as_ref()
        .and_then(|f| f.set.expected_rows())
        .or(rs_expected);
    if let Some(f) = &file_set {
        let bytes: u64 = f.set.files.iter().filter_map(|e| e.size).sum();
        rt.report.member(&rep.name, &sp.id, &m.name, |r| {
            r.bytes.read = bytes;
            for w in &f.set.warnings {
                r.warn("files", w.clone());
            }
        });
    }

    let pk = m
        .primary_key
        .clone()
        .or_else(|| sp.primary_key.clone())
        .unwrap_or_default();
    let null_keys = c.null_keys.unwrap_or(NullKeys::Fail);
    let mut declared: Declared = c
        .columns
        .as_ref()
        .map(|cc| {
            cc.columns
                .iter()
                .filter_map(|(n, cfg)| cfg.kind.clone().map(|k| (n.clone(), k)))
                .collect()
        })
        .unwrap_or_default();
    // `"*"` types source columns; metadata columns keep their own type unless named explicitly.
    for (n, _, t) in &meta {
        declared.entry(n.clone()).or_insert_with(|| t.clone());
    }
    let policy = Policy::from(c.table_drift.as_ref());
    let set_null = c.set_null();
    let nulled = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let nulled_n = nulled.clone();
    let duplicates = c
        .duplicates
        .unwrap_or(atlatl_core::config::model::Duplicates::Fail);
    let order_by = c.order_by.clone().unwrap_or_default();
    let on_empty = c
        .on_empty
        .as_ref()
        .map(|o| o.value)
        .unwrap_or(OnEmpty::Mirror);
    let guard = c.guard;
    let hooks = c.hooks.clone().unwrap_or_default();
    let chunk_cfg = c
        .chunk
        .clone()
        .filter(|ch| !(is_rest || is_rs && ch.by == ChunkBy::Files));
    let rest_empty = rest
        .as_ref()
        .is_some_and(|r| r.empty || r.staging.is_none());
    let (tgt_alias_o, t_schema_o, t_table_o, target_o) = (
        tgt_alias.to_owned(),
        t_schema.clone(),
        t_table.clone(),
        target.clone(),
    );
    let required: Vec<(String, String)> = pk
        .iter()
        .map(|k| ("primary_key".to_owned(), k.clone()))
        .chain(
            update_key
                .iter()
                .map(|u| ("update_key".to_owned(), u.column.clone())),
        )
        .collect();
    let pk_o = pk.clone();

    let pg_snap: (String, Option<String>) = match source {
        Src::Postgres(p) => (p.alias.clone(), p.snapshot().map(str::to_owned)),
        _ => (String::new(), None),
    };
    let replan = (tgt_alias.to_owned(), policy, declared.clone());
    let schema_rel = if file_chunks.is_some() || chunk_cfg.is_some() {
        format!("({select})")
    } else {
        stg.clone()
    };
    let checked = session
        .run(move |conn| {
            let target_cols = staging::table_columns(conn, &tgt_alias_o, &t_schema_o, &t_table_o)?;
            let req: Vec<(&str, &str)> = required
                .iter()
                .map(|(a, b)| (a.as_str(), b.as_str()))
                .collect();
            if empty_files || rest_empty {
                return Ok((
                    Adoption::default(),
                    String::new(),
                    Vec::new(),
                    0,
                    true,
                    None,
                ));
            }
            // Chunks merge from the pinned source; their invariants run over it before the commit.
            let unstaged = |conn: &Connection, rel: &str, a: &Adoption| {
                let full = kind == Kind::FullRefresh;
                let t_rows = if full && target_cols.is_some() {
                    Some(drift::target_rows(conn, &target_o)?)
                } else {
                    None
                };
                chunk::Unstaged {
                    primary_key: &pk_o,
                    null_keys,
                    duplicates,
                    set_null,
                    expected_rows,
                    full_refresh: full.then_some((on_empty, guard.as_ref(), t_rows)),
                    update_key: update_key.as_ref().map(|u| u.column.as_str()),
                }
                .check(conn, rel, a)
            };
            if let Some(rels) = file_chunks {
                let rel = format!("({select})");
                let cols = staging::describe(conn, &rel)?;
                validate::require_columns(&cols, &req)?;
                let a = drift::plan(&cols, target_cols.as_deref(), policy, &declared)?;
                let o = unstaged(conn, &rel, &a)?;
                return Ok((a, rel, o.filter(rels), o.rows, o.skip, o.max));
            }
            if let Some(ch) = &chunk_cfg {
                // Chunked: no staging copy inside the transaction; chunks merge from the pinned source.
                let rel = format!("({select})");
                let cols = staging::describe(conn, &rel)?;
                validate::require_columns(&cols, &req)?;
                let a = drift::plan(&cols, target_cols.as_deref(), policy, &declared)?;
                let o = unstaged(conn, &rel, &a)?;
                let preds = chunk::predicates(conn, &rel, ch)?;
                let rels = preds.iter().map(|p| chunk::relation(&rel, p)).collect();
                return Ok((a, rel, o.filter(rels), o.rows, o.skip, o.max));
            }
            // REST staging is already shaped (columns, naming, metadata, infer) by `rest::stage`.
            let n = if is_rest {
                staging::row_count(conn, &stg)?
            } else {
                crate::pgsrc::PgSrc::read_in_snapshot(
                    &pg_snap.0,
                    pg_snap.1.as_deref(),
                    conn,
                    |c| staging::stage_query(c, &stg, &select),
                )?
            };
            if set_null {
                nulled_n.fetch_add(
                    drift::take_nulled(conn, &stg)?,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            if let Some(want) = expected_rows
                && want != n
            {
                return Err(AtlatlError::new(
                    ErrorClass::Decode,
                    format!("staged {n} rows but the manifest lists {want}"),
                ));
            }
            let cols = staging::describe(conn, &stg)?;
            validate::require_columns(&cols, &req)?;
            let skipped = validate::null_keys(conn, &stg, &pk_o, null_keys)?;
            let deduped = validate::duplicates(conn, &stg, &pk_o, duplicates, &order_by)?;
            let n = n - skipped - deduped;
            let a = drift::plan(&cols, target_cols.as_deref(), policy, &declared)?;
            nulled_n.fetch_add(
                drift::check_casts(conn, &stg, &a, set_null)?,
                std::sync::atomic::Ordering::Relaxed,
            );
            let mut skip = false;
            if kind == Kind::FullRefresh {
                let t_rows = if target_cols.is_some() {
                    Some(drift::target_rows(conn, &target_o)?)
                } else {
                    None
                };
                skip = validate::empty_and_guard(n, t_rows, on_empty, guard.as_ref())?
                    == validate::EmptyOutcome::Skip;
            }
            let max = match &update_key {
                Some(uk) => staged_max(conn, &stg, &uk.column)?,
                None => None,
            };
            Ok((a, stg, Vec::new(), n, skip, max))
        })
        .await
        .map_err(|e| {
            if is_pg {
                atlatl_conn_postgres::source::reclassify(e)
            } else {
                e
            }
        });
    let who = (rep.name.as_str(), sp.id.as_str(), m.name.as_str());
    let exclude: Vec<String> = meta.iter().map(|(n, _, _)| n.clone()).collect();
    // A key column missing from the source is upstream drift: the source schema reports it first.
    if let (Err(e), Some(doc)) = (&checked, &s.schema)
        && e.code.as_deref() == Some("missing_column")
    {
        check_source_schema(rt, session, who, doc, exclude.clone(), schema_rel).await?;
    }
    let (adoption, source, chunks, staged, skip, new_max) = checked?;
    let n = nulled.load(std::sync::atomic::Ordering::Relaxed);
    if n > 0 {
        rt.report.member(&rep.name, &sp.id, &m.name, |r| {
            r.warn(
                "cast_set_null",
                format!("{n} values didn't fit their types exactly and landed as NULL (`on_cast_error.set_null`)"),
            );
        });
    }
    if let Some(doc) = &s.schema
        && !skip
        && chunks.is_empty()
    {
        check_source_schema(rt, session, who, doc, exclude, source.clone()).await?;
    }
    let state = match (&c.update_key, new_max) {
        (Some(uk), Some((v, ty))) if kind == Kind::Incremental => Some(StateRow {
            stream_id: sp.state_id().to_owned(),
            member: state_member,
            kind: StateKind::UpdateKey,
            value: serde_json::json!({"column": uk.value.column, "type": ty, "value": v}),
            config_hash,
            run_id: rt.run_id.clone(),
            lake_snapshot: None,
            updated_at: None,
        }),
        _ => None,
    };
    let (state, state_before, rejects, refreshed, expired, resolved) = match rest {
        Some(r) => (
            r.state,
            r.state_before,
            r.rejects,
            r.refreshed,
            r.expired,
            Some((r.member, r.resolved)),
        ),
        None => (
            state,
            state_before,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        ),
    };
    let hooks = if rt.retry_rejects {
        if hooks.pre_apply.is_some() || hooks.post_apply.is_some() {
            rt.report.member(&rep.name, &sp.id, &m.name, |r| {
                r.warn(
                    "rejects_retry_hooks",
                    "hooks are not run by `rejects retry`",
                );
            });
        }
        Default::default()
    } else {
        hooks
    };
    let base = Context::load(rt.vars.clone(), &rt.project.root, rt.plan.env.as_deref());
    let render_hooks =
        |hs: &Option<Vec<atlatl_core::config::de::Spanned<String>>>| -> Result<Vec<String>> {
            let hs: Vec<String> = hs.iter().flatten().map(|h| h.value.clone()).collect();
            metadata::render_hooks(&rt.templates, &base, &hs, &source, &target)
        };
    Ok(Work {
        replication: rep.name.clone(),
        stream_id: sp.id.clone(),
        state_id: sp.state_id().to_owned(),
        member: m.name.clone(),
        target: target.clone(),
        target_schema: t_schema,
        target_table: t_table,
        kind,
        primary_key: pk,
        null_safe: null_keys == NullKeys::NullSafe,
        unguarded: metadata::run_varying(c.metadata.as_ref().map(|m| &m.value)),
        pre_hooks: render_hooks(&hooks.pre_apply)?,
        post_hooks: render_hooks(&hooks.post_apply)?,
        adoption,
        replan,
        source,
        chunks,
        staged,
        skip,
        state,
        state_before,
        pinned,
        scratch,
        after,
        rejects,
        refreshed,
        expired,
        resolved,
    })
}

struct UnloadedMember {
    source: String,
    chunks: Option<Vec<String>>,
    after: Vec<String>,
    expected_rows: Option<u64>,
}

async fn check_source_schema(
    rt: &Runtime,
    session: &mut Session,
    (rep, stream, member): (&str, &str, &str),
    doc: &atlatl_core::config::load::SchemaDoc,
    exclude: Vec<String>,
    rel: String,
) -> Result<()> {
    let sch = doc.schema.value.clone();
    let ev = session
        .run(move |conn| {
            let ex: Vec<&str> = exclude.iter().map(String::as_str).collect();
            atlatl_ingest::schema::evaluate(
                conn,
                &atlatl_ingest::schema::Staged::Columns {
                    relation: &rel,
                    exclude: &ex,
                    ordinal: None,
                },
                &sch,
            )
        })
        .await?;
    rt.report
        .member(rep, stream, member, |r| r.schema = Some(ev.report.clone()));
    if ev.drifted() {
        rt.events
            .emit(atlatl_core::obs::events::EventBody::SchemaDrift(
                atlatl_core::obs::events::SchemaDrift::new(
                    rep,
                    stream,
                    Some(member.to_owned()),
                    &ev.report,
                ),
            ));
    }
    ev.failure.map_or(Ok(()), Err)
}

/// Redshift: UNLOAD the member (columns and filters pushed down), then stage it per object into a
/// raw table, or return whole-file-group chunk selects for `chunk: {by: files}`.
#[allow(clippy::too_many_arguments)]
async fn unload_member(
    rt: &Runtime,
    session: &mut Session,
    rep: &ReplicationPlan,
    sp: &StreamPlan,
    m: &MemberPlan,
    seq: (usize, usize),
    (rs, object, cols): (
        &Arc<Redshift>,
        &atlatl_conn_redshift::Object,
        Vec<atlatl_conn_redshift::SourceColumn>,
    ),
    filters: &[String],
    attempt: u32,
    cancel: &CancellationToken,
    unloads: &Unloads,
) -> Result<UnloadedMember> {
    let c = &rt.stream(&sp.id)?.config;
    let select = atlatl_conn_redshift::unload::select_sql(object, &cols, filters, None);
    let req = UnloadRequest {
        run_id: rt.run_id.clone(),
        stream: sp.id.clone(),
        member: m.name.clone(),
        attempt,
        select,
        columns: cols,
    };
    let u = rs.unload(&req, cancel).await?;
    unloads
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((rs.clone(), u.clone()));
    let raw = format!("{}_raw", staging::staging_name(&rt.run_id, seq.0, seq.1));
    let after = vec![
        format!("DROP TABLE IF EXISTS temp.{}", ident(&raw)),
        format!("DROP SECRET IF EXISTS {}", ident(&u.secret_name())),
    ];
    let bytes: u64 = u.files.iter().filter_map(|f| f.size).sum();
    let by_files = c
        .chunk
        .as_ref()
        .filter(|ch| ch.by == ChunkBy::Files)
        .map(|ch| ch.size.map(|s| s.0).unwrap_or(u64::MAX));
    let policy = rs.options().object_retry.clone();
    let (u2, raw2) = (u.clone(), raw.clone());
    let (chunks, retries) = session
        .run(move |conn| {
            u2.prepare(conn)?;
            if let Some(size) = by_files
                && !u2.is_empty()
            {
                return Ok((
                    Some(
                        u2.chunks(size)?
                            .into_iter()
                            .map(|k| k.select)
                            .collect::<Vec<_>>(),
                    ),
                    Vec::new(),
                ));
            }
            let mut recs = Vec::new();
            u2.stage(conn, &raw2, &policy, &mut |a| recs.push(a.clone()))?;
            Ok((None, recs))
        })
        .await?;
    rt.report.member(&rep.name, &sp.id, &m.name, |r| {
        r.bytes.read = bytes;
        for a in &retries {
            r.record_retry(a);
        }
    });
    let source = match &chunks {
        Some(_) => format!("({})", u.select_for(&u.files)?),
        None => format!("(SELECT * FROM {})", ident(&raw)),
    };
    Ok(UnloadedMember {
        source,
        chunks,
        after,
        expected_rows: u.expected_rows,
    })
}

/// `columns.include`/`exclude` applied to the source columns (pushed into the Redshift select).
/// A Redshift member's columns narrowed by `columns`, `super` opted in by its declared type; the
/// lines name every column still refused.
fn redshift_admitted(
    c: &atlatl_core::config::model::StreamConfig,
    cols: Vec<atlatl_conn_redshift::SourceColumn>,
) -> std::result::Result<Vec<atlatl_conn_redshift::SourceColumn>, Vec<String>> {
    let cc = c.columns.as_ref();
    atlatl_conn_redshift::catalog::admit(narrow_columns(c, cols), &|n| {
        cc.and_then(|c| c.declared(n)).map(str::to_owned)
    })
}

/// A member's admitted source columns from the early check, reused when it's staged.
enum Admitted {
    Redshift(Vec<atlatl_conn_redshift::SourceColumn>),
    Postgres(Vec<atlatl_conn_postgres::source::SourceColumn>),
    Clickhouse(atlatl_conn_clickhouse::Described),
}

type Resolved = std::collections::HashMap<(usize, usize), Vec<(MemberPlan, Option<Admitted>)>>;

/// Every Postgres, Redshift and ClickHouse member's columns, checked against what the target holds
/// once the source catalog is known and before anything is read: one `config` error names every
/// refused column across the unit. Returns the resolved members with their admitted columns, so
/// discovery and each catalog lookup run once per attempt. ClickHouse members whose columns hang
/// on runtime values (`clickhouse_select`) are checked as they're read.
async fn check_source_types(
    rt: &Runtime,
    session: &mut Session,
    unit: &[&ReplicationPlan],
    src: &Src,
    cancel: &CancellationToken,
) -> Result<Resolved> {
    let mut resolved = std::collections::HashMap::new();
    let (rs, pg, ch) = match src {
        Src::Redshift(r) => (Some(r.as_ref()), None, None),
        Src::Postgres(p) => (None, Some(p.as_ref()), None),
        Src::Clickhouse(c) => (None, None, Some(c.as_ref())),
        _ => return Ok(resolved),
    };
    let mut found: Vec<(String, String)> = Vec::new();
    for (ri, rep) in unit.iter().enumerate() {
        for (si, sp) in rep.streams.iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(AtlatlError::new(ErrorClass::Killed, "cancelled"));
            }
            let s = rt.stream(&sp.id)?;
            if sp.role == StreamRole::FeedOnly
                || matches!(s.config.mode.as_ref().map(|m| m.value), Some(Mode::Cdc))
            {
                continue;
            }
            let ms = members(
                rt,
                session,
                "",
                rs,
                pg,
                cancel,
                s,
                sp,
                rep.instance.as_ref(),
            )
            .await?;
            let mut out = Vec::with_capacity(ms.len());
            for m in ms {
                let (label, cols) = match (rs, pg, ch) {
                    (_, _, Some(ch)) => {
                        match clickhouse_admitted(ch, &s.config, sp, &m, cancel).await? {
                            Some((label, cols)) => {
                                (label, cols.map(|d| d.map(Admitted::Clickhouse)))
                            }
                            None => {
                                out.push((m, None));
                                continue;
                            }
                        }
                    }
                    (Some(rs), ..) if m.table.is_some() => (
                        m.name.clone(),
                        redshift_admitted(&s.config, rs.columns(&m.name, cancel).await?)
                            .map(|c| Some(Admitted::Redshift(c))),
                    ),
                    (_, Some(pg), _) => {
                        let (label, cols) = crate::pgsrc::admitted(&pg.source, s, sp, &m).await?;
                        (label, cols.map(|c| Some(Admitted::Postgres(c))))
                    }
                    _ => {
                        out.push((m, None));
                        continue;
                    }
                };
                match cols {
                    Ok(c) => out.push((m, c)),
                    Err(lines) => {
                        found.extend(lines.into_iter().map(|l| (label.clone(), l)));
                        out.push((m, None));
                    }
                }
            }
            resolved.insert((ri, si), out);
        }
    }
    if found.is_empty() {
        Ok(resolved)
    } else {
        Err(atlatl_conn_postgres::types::refused(&found))
    }
}

/// What a clickhouse member's refusals name it by.
fn clickhouse_label(sp: &StreamPlan, m: &MemberPlan) -> String {
    match &m.query {
        Some(_) => format!("from.query of {}", sp.id),
        None => m.name.clone(),
    }
}

/// The select a clickhouse member's columns can be described from before anything is read, and
/// whether it stands in for the watermark; `None` when they hang on `bind` results, or `from.query`
/// sets a setting atlatl owns (refused when it's read). The watermark condition filters rows only,
/// so it reads as `1=1`; `{incremental_value}` reads as `NULL`, as before the first watermark.
fn clickhouse_select(
    c: &atlatl_core::config::model::StreamConfig,
    m: &MemberPlan,
) -> Option<(String, bool)> {
    use atlatl_core::template::{INCREMENTAL_VALUE, INCREMENTAL_WHERE_COND};
    if c.bind.as_ref().is_some_and(|b| !b.is_empty()) {
        return None;
    }
    match (&m.query, &m.table) {
        (Some(q), _) if atlatl_conn_clickhouse::sets_owned_setting(q).is_some() => None,
        (Some(q), _) => Some((
            q.replace(INCREMENTAL_WHERE_COND, "1=1")
                .replace(INCREMENTAL_VALUE, "NULL"),
            q.contains(INCREMENTAL_VALUE),
        )),
        (None, Some(_)) => Some((
            format!(
                "SELECT * FROM {}",
                atlatl_conn_clickhouse::table_name(&m.name)
            ),
            false,
        )),
        (None, None) => None,
    }
}

/// A clickhouse member's landing columns checked before anything is read: its label, and the
/// `DESCRIBE` to reuse when it's read or every refused column. `None` when its columns can't be
/// known up front. With `{incremental_value}` read as `NULL`, a column typed from it alone
/// (`Nothing` anywhere in its type) is left to the read, and the `DESCRIBE` isn't reused, since a
/// stored watermark may type other columns differently too.
pub(crate) async fn clickhouse_admitted(
    ch: &atlatl_conn_clickhouse::ClickHouse,
    c: &atlatl_core::config::model::StreamConfig,
    sp: &StreamPlan,
    m: &MemberPlan,
    cancel: &CancellationToken,
) -> Result<
    Option<(
        String,
        std::result::Result<Option<atlatl_conn_clickhouse::Described>, Vec<String>>,
    )>,
> {
    let Some((sql, stand_in)) = clickhouse_select(c, m) else {
        return Ok(None);
    };
    let d = ch.describe(&sql, cancel).await?;
    let landing: Vec<_> = d
        .columns
        .iter()
        .filter(|(n, t)| lands(c, n) && !(stand_in && t.contains("Nothing")))
        .cloned()
        .collect();
    let cols = atlatl_conn_clickhouse::types::check(&landing).map(|_| (!stand_in).then_some(d));
    Ok(Some((clickhouse_label(sp, m), cols)))
}

fn narrow_columns(
    c: &atlatl_core::config::model::StreamConfig,
    cols: Vec<atlatl_conn_redshift::SourceColumn>,
) -> Vec<atlatl_conn_redshift::SourceColumn> {
    cols.into_iter().filter(|col| lands(c, &col.name)).collect()
}

/// Whether `columns.include`/`exclude` keep source column `name`.
fn lands(c: &atlatl_core::config::model::StreamConfig, name: &str) -> bool {
    let Some(cc) = c.columns.as_ref() else {
        return true;
    };
    cc.include
        .as_ref()
        .is_none_or(|i| i.iter().any(|n| n.eq_ignore_ascii_case(name)))
        && !cc
            .exclude
            .iter()
            .flatten()
            .any(|n| n.eq_ignore_ascii_case(name))
}

/// Appends export batches to a raw table on the unit's engine session.
struct EngineSink<'a> {
    session: &'a mut Session,
    raw: Arc<atlatl_conn_redshift::RawTable>,
}

impl atlatl_conn_redshift::Sink for EngineSink<'_> {
    async fn append(&mut self, batch: arrow::array::RecordBatch) -> Result<()> {
        let raw = self.raw.clone();
        self.session
            .run(move |c| raw.append(c, batch).map(drop))
            .await
    }

    async fn reset(&mut self) -> Result<()> {
        let raw = self.raw.clone();
        self.session.run(move |c| raw.reset(c)).await
    }
}

/// Redshift over the wire without `unload`: the member's select (columns and filters pushed
/// down) streamed into a raw table, ordered by its key so a dropped read resumes (06 §4).
#[allow(clippy::too_many_arguments)]
async fn export_member(
    rt: &Runtime,
    session: &mut Session,
    rep: &ReplicationPlan,
    sp: &StreamPlan,
    m: &MemberPlan,
    seq: (usize, usize),
    (rs, object, cols): (
        &Arc<Redshift>,
        atlatl_conn_redshift::Object,
        Vec<atlatl_conn_redshift::SourceColumn>,
    ),
    filters: &[String],
    cancel: &CancellationToken,
) -> Result<UnloadedMember> {
    let c = &rt.stream(&sp.id)?.config;
    if c.chunk.as_ref().is_some_and(|ch| ch.by == ChunkBy::Files) {
        return Err(AtlatlError::config(format!(
            "stream `{}`: `chunk: {{by: files}}` needs the connection's `unload`",
            sp.id
        )));
    }
    let key = m
        .primary_key
        .clone()
        .or_else(|| sp.primary_key.clone())
        .unwrap_or_default();
    let raw = Arc::new(atlatl_conn_redshift::RawTable::new(
        format!("{}_raw", staging::staging_name(&rt.run_id, seq.0, seq.1)),
        cols.clone(),
    ));
    let after = raw.drop_sql();
    let r2 = raw.clone();
    session.run(move |conn| r2.create(conn)).await?;
    let req = atlatl_conn_redshift::ExportRequest {
        object,
        columns: cols,
        filters: filters.to_vec(),
        key,
    };
    if req.key_positions().is_none() {
        tracing::info!(member = %m.name, "Redshift export has no key among its columns; a dropped read starts over");
    }
    let mut sink = EngineSink {
        session,
        raw: raw.clone(),
    };
    let out = rs.export(&req, &mut sink, cancel).await?;
    rt.report.member(&rep.name, &sp.id, &m.name, |r| {
        r.bytes.read = out.bytes;
        for a in &out.retries {
            r.record_retry(a);
        }
    });
    Ok(UnloadedMember {
        source: format!("(SELECT * FROM temp.{})", ident(&raw.name)),
        chunks: None,
        after,
        expected_rows: Some(out.rows),
    })
}

/// Runs a clickhouse member's queries (one per `bind` batch) into Parquet files in a directory of
/// its own under the run's workdir; returns the relation reading them all, and the directory.
#[allow(clippy::too_many_arguments)]
async fn clickhouse_member(
    rt: &Runtime,
    session: &mut Session,
    rep: &ReplicationPlan,
    sp: &StreamPlan,
    s: &Stream,
    m: &MemberPlan,
    seq: (usize, usize),
    ch: &atlatl_conn_clickhouse::ClickHouse,
    mark: Option<&atlatl_conn_clickhouse::incremental::Watermark>,
    first_run: bool,
    admitted: Option<atlatl_conn_clickhouse::Described>,
    cancel: &CancellationToken,
) -> Result<(String, std::path::PathBuf)> {
    use atlatl_conn_clickhouse::bind::{Bound, render};
    let sql = match (&m.query, &m.table, mark) {
        (Some(q), _, Some(w)) => w.render(q),
        (Some(q), _, None) => q.clone(),
        (None, Some(_), Some(w)) => format!(
            "SELECT * FROM {} WHERE {}",
            atlatl_conn_clickhouse::table_name(&m.name),
            w.condition()
        ),
        (None, Some(_), None) => format!(
            "SELECT * FROM {}",
            atlatl_conn_clickhouse::table_name(&m.name)
        ),
        (None, None, _) => {
            return Err(AtlatlError::config(format!(
                "stream `{}`: clickhouse sources read `from.table` or `from.query`",
                sp.id
            )));
        }
    };
    let mut whole = Vec::new();
    let mut batched = None;
    let mut concurrency = 1;
    for (name, b) in s.config.bind.iter().flatten() {
        let query = match &b.fallback {
            Some(f) if first_run => f,
            _ => &b.query,
        };
        let (types, texts) =
            crate::rest::query_typed(rt, session, query, &b.connection, true).await?;
        let (cols, items) = bind_cols(&types)
            .and_then(|cols| {
                let items = bind_rows(&texts)?;
                bind_fractions(&cols, &items)?;
                Ok((cols, items))
            })
            .map_err(|e| AtlatlError::config(format!("stream `{}`: `bind.{name}`: {e}", sp.id)))?;
        if items.is_empty() {
            rt.report.member(&rep.name, &sp.id, &m.name, |r| {
                r.warn(
                    "bind_empty",
                    format!(
                        "`bind.{name}` listed nothing and is spliced as NULL: `IN (NULL)` matches no rows, `NOT IN (NULL)` every row"
                    ),
                )
            });
        }
        let bound = Bound {
            name: name.clone(),
            cols,
            rows: items,
        };
        atlatl_conn_clickhouse::bind::check_values(&sql, &bound)
            .map_err(|e| AtlatlError::config(format!("stream `{}`: {e}", sp.id)))?;
        // Plan checks allow one batched list, with a batch of at least 1.
        match b
            .batch
            .and_then(|n| std::num::NonZeroUsize::new(n as usize))
        {
            Some(n) if batched.is_none() => {
                concurrency = b.concurrency.map_or(1, |c| c.max(1) as usize);
                batched = Some((bound, n));
            }
            _ => whole.push(bound),
        }
    }
    if let Some(k) = atlatl_conn_clickhouse::sets_owned_setting(&sql) {
        return Err(AtlatlError::config(format!(
            "stream `{}`: `from.query` sets `{k}`, which atlatl owns (it decides what types land and how watermarks compare); only Parquet layout settings (compression, row groups, pages, bloom filters) may be set, so convert columns in the select instead",
            sp.id
        )));
    }
    let queries = render(&sql, &whole, batched.as_ref().map(|(b, n)| (b, *n)));
    // Every batch has the first one's columns.
    let decimals: Vec<String> = whole
        .iter()
        .chain(batched.as_ref().map(|(b, _)| b))
        .flat_map(|b| {
            b.cols
                .iter()
                .filter(|c| matches!(c.num, atlatl_conn_clickhouse::bind::Num::Decimal(..)))
                .map(move |c| format!("`bind.{}` column `{}` ({})", b.name, c.name, c.ty))
        })
        .collect();
    let hint = |mut e: AtlatlError| {
        if e.code.as_deref() == Some("clickhouse_53") && !decimals.is_empty() {
            e.message.push_str(&format!(
                "\nhint: {} is sent as a typed `Decimal`, which ClickHouse won't compare with a float or integer column; cast it in the bind query to match (`CAST(x AS DOUBLE)` for a float column)",
                decimals.join(", ")
            ));
        }
        e
    };
    let described = match admitted {
        Some(d) => d,
        None => ch.describe(&queries[0], cancel).await.map_err(hint)?,
    };
    rt.report.member(&rep.name, &sp.id, &m.name, |r| {
        for a in &described.retries {
            r.record_retry(a);
        }
    });
    let landing: Vec<(String, String)> = described
        .columns
        .iter()
        .filter(|(n, _)| lands(&s.config, n))
        .cloned()
        .collect();
    if let Some(inc) = s.config.columns.as_ref().and_then(|cc| cc.include.as_ref())
        && let Some(miss) = inc
            .iter()
            .find(|i| !landing.iter().any(|(n, _)| n.eq_ignore_ascii_case(i)))
    {
        return Err(AtlatlError::config(format!(
            "stream `{}`: `columns.include` names `{miss}`, which the source doesn't return",
            sp.id
        )));
    }
    let wide = atlatl_conn_clickhouse::types::check(&landing)
        .map_err(|l| crate::pgsrc::source_types(&clickhouse_label(sp, m), l))?;
    let narrowed = (landing.len() < described.columns.len()).then(|| {
        landing
            .iter()
            .map(|(n, _)| atlatl_conn_clickhouse::ident(n))
            .collect::<Vec<_>>()
            .join(", ")
    });
    let queries: Vec<String> = queries
        .iter()
        .map(|q| {
            let q = match &narrowed {
                Some(cols) => format!(
                    "SELECT {cols} FROM ({}\n)",
                    atlatl_conn_clickhouse::types::trimmed(q)
                ),
                None => q.clone(),
            };
            atlatl_conn_clickhouse::types::stringify(&q, &wide)
        })
        .collect();
    static UNIT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = UNIT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = rt
        .workdir()
        .join(format!("clickhouse{n}_{}_{}", seq.0, seq.1));
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| AtlatlError::internal(format!("create {}: {e}", dir.display())))?;
    use futures::{StreamExt, TryStreamExt};
    let fetches: Vec<_> = queries
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let path = dir.join(format!("{i:05}.parquet"));
            async move {
                let got = ch.query_to_file(q, &path, cancel).await.map_err(hint)?;
                Ok::<_, AtlatlError>((path, got))
            }
        })
        .collect();
    let fetched: Vec<_> = futures::stream::iter(fetches)
        .buffered(concurrency)
        .try_collect()
        .await?;
    let mut files = Vec::with_capacity(fetched.len());
    for (path, got) in fetched {
        rt.report.member(&rep.name, &sp.id, &m.name, |r| {
            r.bytes.read += got.bytes;
            for a in &got.retries {
                r.record_retry(a);
            }
        });
        files.push(atlatl_ingest::sql::literal(&path.display().to_string()));
    }
    tracing::info!(stream = %sp.id, queries = queries.len(), "clickhouse member fetched");
    let rel = format!("read_parquet([{}])", files.join(", "));
    Ok((atlatl_conn_clickhouse::types::cast_back(&rel, &wide), dir))
}

/// A bind query's rows (JSON objects) as values in column order. Numbers keep their text, so wide
/// integers and decimals stay exact; a column name used twice is refused, since an object keeps
/// only one of them and the row would quietly lose a column.
fn bind_rows(texts: &[String]) -> std::result::Result<Vec<Vec<Lit>>, String> {
    use atlatl_http::json::JValue;
    if let Some(first) = texts.first() {
        let names = json_object_keys(first).map_err(|e| format!("row: {e}"))?;
        let mut seen = std::collections::HashSet::new();
        if let Some(d) = names.iter().find(|n| !seen.insert(n.as_str())) {
            return Err(format!(
                "the query returns two columns named `{d}`; alias one of them"
            ));
        }
    }
    texts
        .iter()
        .map(|t| {
            let v = atlatl_http::json::parse_str(t).map_err(|e| format!("row: {e}"))?;
            let vals: Vec<JValue> = match v {
                JValue::Object(m) => m.into_iter().map(|(_, v)| v).collect(),
                other => vec![other],
            };
            vals.into_iter()
                .map(|v| match v {
                    JValue::Null => Ok(Lit::Null),
                    JValue::Bool(b) => Ok(Lit::Bool(b)),
                    JValue::Number(n) => Ok(Lit::Number(n.as_str().to_owned())),
                    JValue::String(s) => Ok(Lit::String(s)),
                    v @ (JValue::Array(_) | JValue::Object(_)) => {
                        Err(format!("a bind value must be a scalar, got {v}"))
                    }
                })
                .collect()
        })
        .collect()
}

/// How each bind column's numbers are written, from its type in the bind query (DuckDB's names,
/// or Postgres's `format_type`).
fn bind_cols(
    types: &[(String, String)],
) -> std::result::Result<Vec<atlatl_conn_clickhouse::bind::Col>, String> {
    use atlatl_conn_clickhouse::bind::{Col, Num};
    types
        .iter()
        .map(|(name, ty)| {
            let t = ty.trim().to_ascii_uppercase();
            let num = if let Some(args) = ["DECIMAL(", "NUMERIC("]
                .iter()
                .find_map(|p| t.strip_prefix(p))
                .and_then(|r| r.strip_suffix(')'))
            {
                let ps: Vec<i64> = args
                    .split(',')
                    .map(|a| a.trim().parse::<i64>())
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|_| format!("column `{name}` is {ty}, which atlatl can't read"))?;
                match ps[..] {
                    [p, s] if (1..=76).contains(&p) && (0..=p).contains(&s) => {
                        Num::Decimal(p as u32, s as u32)
                    }
                    [p] if (1..=76).contains(&p) => Num::Decimal(p as u32, 0),
                    _ => {
                        return Err(format!(
                            "column `{name}` is {ty}, which no ClickHouse `Decimal` holds (up to 76 digits, a scale from 0 to the precision); cast it in the bind query (`CAST({name} AS DECIMAL(38, 4))`, or `CAST({name} AS DOUBLE)`)"
                        ));
                    }
                }
            } else {
                match t.as_str() {
                    "DECIMAL" | "NUMERIC" => {
                        return Err(format!(
                            "column `{name}` is {ty} without a precision and scale, so its values have no exact ClickHouse type; cast it in the bind query (`{name}::numeric(18,4)`, or `{name}::float8`)"
                        ));
                    }
                    "DOUBLE" | "FLOAT" | "REAL" | "DOUBLE PRECISION" | "FLOAT4" | "FLOAT8" => {
                        Num::Float
                    }
                    _ => Num::Other,
                }
            };
            Ok(Col {
                name: name.clone(),
                ty: ty.clone(),
                num,
            })
        })
        .collect()
}

/// Refuses a fraction in a column that's neither decimal nor float: there's no type to write it
/// as that ClickHouse wouldn't truncate to the compared column's.
fn bind_fractions(
    cols: &[atlatl_conn_clickhouse::bind::Col],
    rows: &[Vec<Lit>],
) -> std::result::Result<(), String> {
    use atlatl_conn_clickhouse::bind::Num;
    for r in rows {
        for (v, c) in r.iter().zip(cols) {
            if let Lit::Number(n) = v
                && c.num == Num::Other
                && n.contains(['.', 'e', 'E'])
            {
                return Err(format!(
                    "column `{}` is {}, and its value {n} has no exact ClickHouse type; cast it in the bind query (`CAST({} AS DECIMAL(18, 4))`, or `CAST({} AS DOUBLE)`)",
                    c.name, c.ty, c.name, c.name
                ));
            }
        }
    }
    Ok(())
}

/// A JSON object's keys in order, repeats included.
fn json_object_keys(text: &str) -> std::result::Result<Vec<String>, serde_json::Error> {
    use serde::de::{Deserializer, IgnoredAny, MapAccess, Visitor};
    struct Keys;
    impl<'de> Visitor<'de> for Keys {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<A: MapAccess<'de>>(
            self,
            mut a: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut keys = Vec::new();
            while let Some(k) = a.next_key::<String>()? {
                a.next_value::<IgnoredAny>()?;
                keys.push(k);
            }
            Ok(keys)
        }
    }
    serde_json::Deserializer::from_str(text).deserialize_map(Keys)
}

/// A nonzero `update_key.lookback` in whole microseconds.
fn lookback_us(lookback: Option<atlatl_core::config::types::Dur>) -> Option<u64> {
    lookback
        .map(|d| u64::try_from(d.0.as_micros()).unwrap_or(u64::MAX))
        .filter(|&n| n > 0)
}

/// The stored watermark less `lookback`, as a ClickHouse literal: numbers and booleans bare (wide
/// integers, decimals and `Float32` cast), timestamps a UTC `DateTime64(6)`, dates and strings
/// quoted.
async fn clickhouse_watermark(
    session: &mut Session,
    value: &str,
    ty: Option<&str>,
    lookback_us: Option<u64>,
) -> Result<String> {
    let (sql, lit) = clickhouse_watermark_sql(value, ty, lookback_us)?;
    let v = session
        .run(move |c| {
            c.query_row(&sql, [], |r| r.get::<_, String>(0))
                .map_err(atlatl_ingest::sql::db)
        })
        .await?;
    Ok(lit.render(v))
}

/// How a watermark's text becomes a ClickHouse literal.
#[derive(Debug, PartialEq)]
enum ChLit {
    Bare,
    Quoted,
    /// ClickHouse parses integers past 64 bits and decimal and float literals as `Float64`, so
    /// these go in as a typed cast of the text.
    Cast(String),
    /// A UTC instant: a bare string would be read in the column's (or server's) time zone.
    Utc,
}

impl ChLit {
    fn render(&self, v: String) -> String {
        let quoted = atlatl_conn_clickhouse::bind::quoted;
        match self {
            ChLit::Bare => v,
            ChLit::Quoted => quoted(&v),
            ChLit::Cast(t) => format!("CAST({} AS {t})", quoted(&v)),
            ChLit::Utc => format!("toDateTime64({}, 6, 'UTC')", quoted(&v)),
        }
    }
}

/// The DuckDB select rendering the watermark's text, and how ClickHouse takes it. Types are what
/// a clickhouse member lands as (`atlatl_conn_clickhouse::types`): integers keep their width
/// (128-bit ones as `HUGEINT`/`UHUGEINT`), `Decimal` up to 38 digits too (wider ones arrive as
/// `DOUBLE`), `Bool` is `BOOLEAN`, `Date32` is `DATE`, `DateTime`/`DateTime64` are
/// `TIMESTAMP WITH TIME ZONE`. Anything else (`UUID`, a `BLOB`) can't be a watermark.
fn clickhouse_watermark_sql(
    value: &str,
    ty: Option<&str>,
    lookback_us: Option<u64>,
) -> Result<(String, ChLit)> {
    let ty = ty.unwrap_or("VARCHAR").to_owned();
    let upper = ty.to_ascii_uppercase();
    let numeric = upper.starts_with("DECIMAL")
        || [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
        ]
        .contains(&upper.as_str());
    let temporal = upper == "DATE" || upper.starts_with("TIMESTAMP");
    if !numeric && !temporal && upper != "VARCHAR" {
        return Err(AtlatlError::config(format!(
            "update_key is {ty}, which a clickhouse watermark can't compare (cast it to a number, date, timestamp or string in `from.query`)"
        )));
    }
    let mut x = format!("CAST({} AS {ty})", atlatl_ingest::sql::literal(value));
    if let Some(us) = lookback_us {
        if !temporal {
            return Err(AtlatlError::config(format!(
                "update_key is {ty}; `lookback` needs a date or timestamp column"
            )));
        }
        x = format!(
            "CAST({x} - INTERVAL '{}' AS {ty})",
            atlatl_core::config::types::interval_text(us)
        );
    }
    let text = if upper == "DATE" {
        format!("strftime({x}, '%Y-%m-%d')")
    } else if temporal {
        let t = if upper.contains("TIME ZONE") {
            format!("timezone('UTC', {x})")
        } else {
            format!("CAST({x} AS TIMESTAMP)")
        };
        format!("strftime({t}, '%Y-%m-%d %H:%M:%S.%f')")
    } else {
        format!("({x})::VARCHAR")
    };
    let lit = match upper.as_str() {
        t if t.starts_with("TIMESTAMP") => ChLit::Utc,
        // Shortest text for a FLOAT is a Float64 literal to ClickHouse, a hair off the stored value.
        "FLOAT" => ChLit::Cast("Float32".into()),
        "HUGEINT" => ChLit::Cast("Int128".into()),
        "UHUGEINT" => ChLit::Cast("UInt128".into()),
        d if d.starts_with("DECIMAL") => ChLit::Cast(format!("Decimal{}", &ty["DECIMAL".len()..])),
        _ if numeric => ChLit::Bare,
        _ => ChLit::Quoted,
    };
    Ok((format!("SELECT {text}"), lit))
}

/// Query retries follow the replication's `retry`; the statement timeout the connection's
/// `timeouts.statement`, the connect timeout its `timeouts.connect`, and queries in flight its
/// `max_concurrency`, across the run.
fn clickhouse_options(
    rt: &Runtime,
    rep: &ReplicationPlan,
    conn: &ConnConfig,
) -> atlatl_conn_clickhouse::Options {
    let mut o = clickhouse_base_options(&rt.project, rep, conn);
    if let Some(n) = conn.common.max_concurrency {
        let mut slots = rt
            .clickhouse_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        o.slots = Some(
            slots
                .entry(conn.name.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new((n as usize).max(1))))
                .clone(),
        );
    }
    o
}

/// `clickhouse_options` without the run's shared slots.
pub(crate) fn clickhouse_base_options(
    project: &atlatl_core::config::load::Project,
    rep: &ReplicationPlan,
    conn: &ConnConfig,
) -> atlatl_conn_clickhouse::Options {
    let mut o = atlatl_conn_clickhouse::Options::default();
    if let Some(p) = project
        .replication(rep.config_name())
        .and_then(|r| RetryPolicy::from_config(&r.retry).ok())
    {
        o.retry = p;
    }
    if let Some(t) = conn.common.timeouts.as_ref() {
        if let Some(s) = t.statement {
            o.statement_timeout = s.0;
        }
        if let Some(c) = t.connect {
            o.connect_timeout = c.0;
        }
    }
    o
}

/// Query and object retries follow the replication's `retry`; statement timeout and sessions the
/// connection's `timeouts.statement` and `max_concurrency`, the sessions from one wire pool per
/// connection across the run.
fn redshift_options(
    rt: &Runtime,
    rep: &ReplicationPlan,
    conn: &ConnConfig,
) -> atlatl_conn_redshift::Options {
    let mut o = atlatl_conn_redshift::Options::default();
    if let Some(p) = rt
        .project
        .replication(rep.config_name())
        .and_then(|r| RetryPolicy::from_config(&r.retry).ok())
    {
        o.query_retry = p.clone();
        o.object_retry = p;
    }
    if let Some(t) = conn.common.timeouts.as_ref().and_then(|t| t.statement) {
        o.statement_timeout = t.0;
    }
    if let Some(n) = conn.common.max_concurrency {
        o.max_sessions = n as usize;
    }
    if let ConnectionKind::Redshift(r) = &conn.kind
        && r.access == Some(atlatl_core::config::model::RedshiftAccess::Wire)
        && let Some(w) = r.wire.as_deref()
    {
        let mut wires = rt.redshift_wires.lock().unwrap_or_else(|e| e.into_inner());
        o.wire = match wires.get(&conn.name) {
            Some(shared) => Some(shared.clone()),
            // A bad `wire` block is left for `Redshift::new` to report.
            None => atlatl_conn_redshift::wire::connect_options(w)
                .and_then(|c| {
                    atlatl_conn_redshift::Wire::new(c, o.max_sessions, o.statement_timeout)
                })
                .ok()
                .inspect(|shared| {
                    wires.insert(conn.name.clone(), shared.clone());
                }),
        };
    }
    o
}

/// DuckLake (seen on DuckDB v1.5.3, Postgres catalog) fails the next transaction on this connection that
/// reads a table whose commit just conflicted ("Current transaction is aborted"); a throwaway read
/// per table clears it, so the conflict retry can run.
fn clear_conflict(conn: &Connection, works: &[Work]) {
    for w in works {
        let _ = conn.execute_batch("BEGIN");
        let _ = conn.query_row(&format!("SELECT count(*) FROM {}", w.target), [], |r| {
            r.get::<_, i64>(0)
        });
        let _ = conn.execute_batch("ROLLBACK");
    }
}

/// One member's part of the commit.
#[derive(Clone, Copy, Debug, Default)]
struct Applied {
    counts: Counts,
    /// Anything written: rows, DDL, hooks, a moved state value, or a new, changed or resolved
    /// reject (not a still-failing one with the same error).
    wrote: bool,
}

/// The state row to write: `None` when its value is what's stored, so an unchanged rerun
/// commits nothing (05 §4).
fn state_to_write(w: &Work) -> Option<&StateRow> {
    w.state
        .as_ref()
        .filter(|s| w.state_before.as_ref() != Some(&s.value))
}

/// A member's reject changes; `true` if any counts as a write (03 §7).
fn write_rejects(conn: &Connection, st: &atlatl_state::LakeState, w: &Work) -> Result<bool> {
    let mut wrote = false;
    let member = w
        .resolved
        .as_ref()
        .map(|(m, _)| m.as_str())
        .unwrap_or_default();
    // Expired first: one that this run's extract hit again is recorded afresh below.
    if !w.expired.is_empty() {
        wrote |= st.resolve_rejects(conn, &w.state_id, member, &w.expired)? > 0;
    }
    for r in &w.rejects {
        st.upsert_reject(conn, r)?;
        wrote = true;
    }
    // Rolled back with the rest when nothing else writes.
    for r in &w.refreshed {
        st.upsert_reject(conn, r)?;
    }
    if let Some((m, keys)) = &w.resolved
        && !keys.is_empty()
    {
        wrote |= st.resolve_rejects(conn, &w.state_id, m, keys)? > 0;
    }
    Ok(wrote)
}

/// Everything inside the one transaction. Re-run whole on a commit conflict. When nothing was
/// written it rolls back instead of committing.
fn commit(
    conn: &Connection,
    lake: &atlatl_conn_ducklake::Lake,
    works: &[Work],
    state: Option<&atlatl_state::LakeState>,
    cancel: &CancellationToken,
) -> Result<Vec<Applied>> {
    let run = || -> Result<Vec<Applied>> {
        let mut out = Vec::new();
        conn.execute_batch("BEGIN")
            .map_err(atlatl_ingest::sql::db)?;
        for w in works {
            if w.skip {
                let wrote = match state {
                    Some(st) => write_rejects(conn, st, w)?,
                    None => false,
                };
                out.push(Applied {
                    wrote,
                    ..Applied::default()
                });
                continue;
            }
            let alias = w.target.split('.').next().unwrap_or_default();
            conn.execute_batch(&format!(
                "CREATE SCHEMA IF NOT EXISTS {alias}.{}",
                ident(&w.target_schema)
            ))
            .map_err(atlatl_ingest::sql::db)?;
            let (cat, policy, declared) = &w.replan;
            let now = if w.adoption.create {
                staging::table_columns(conn, cat, &w.target_schema, &w.target_table)?
            } else {
                None
            };
            let adoption = match now {
                Some(target_cols) => drift::plan(
                    &staging::describe(conn, &w.source)?,
                    Some(&target_cols),
                    *policy,
                    declared,
                )?,
                None => w.adoption.clone(),
            };
            let ddl = drift::ddl(&w.target, &adoption);
            let mut wrote = adoption.create || !ddl.is_empty() || !w.pre_hooks.is_empty();
            if adoption.create {
                conn.execute_batch(&drift::create_sql(&w.target, &adoption))
                    .map_err(atlatl_ingest::sql::db)?;
            }
            for d in ddl {
                conn.execute_batch(&d).map_err(atlatl_ingest::sql::db)?;
            }
            for h in &w.pre_hooks {
                conn.execute_batch(h).map_err(atlatl_ingest::sql::db)?;
            }
            let cols = drift::projection(&adoption);
            let base = Apply {
                kind: w.kind,
                target: &w.target,
                source: &w.source,
                columns: &cols,
                primary_key: &w.primary_key,
                null_safe: w.null_safe,
            };
            let counts = if w.chunks.is_empty() {
                base.run_guarded(conn, &w.unguarded)?
            } else {
                let mut total = Counts::default();
                let seen = format!("seen_{}", w.stream_id.replace(['/', '.', '-'], "_"));
                let keyed_full = w.kind == Kind::FullRefresh && !w.primary_key.is_empty();
                if keyed_full {
                    // Created before BEGIN would be better for memory; keys only, so it's small.
                    let keys = w
                        .primary_key
                        .iter()
                        .map(|k| ident(k))
                        .collect::<Vec<_>>()
                        .join(", ");
                    conn.execute_batch(&format!(
                        "CREATE OR REPLACE TEMP TABLE {} AS SELECT {keys} FROM {} WHERE false",
                        ident(&seen),
                        w.source
                    ))
                    .map_err(atlatl_ingest::sql::db)?;
                }
                for rel in &w.chunks {
                    let a = Apply {
                        source: rel,
                        ..base
                    };
                    total.add(if keyed_full {
                        a.run_chunk_guarded(conn, &seen, &w.unguarded)?
                    } else {
                        a.run_guarded(conn, &w.unguarded)?
                    });
                    // Freed allocator pages otherwise pile up across chunks (spike S3 §6.2).
                    atlatl_engine::alloc::release();
                }
                if keyed_full {
                    total.deleted += base.finish_chunked(conn, &seen)?;
                }
                total
            };
            wrote |= counts.changed() || !w.post_hooks.is_empty();
            for h in &w.post_hooks {
                conn.execute_batch(h).map_err(atlatl_ingest::sql::db)?;
            }
            if let (Some(st), Some(row)) = (state, state_to_write(w)) {
                st.put(conn, row)?;
                wrote = true;
            }
            if let Some(st) = state {
                wrote |= write_rejects(conn, st, w)?;
            }
            out.push(Applied { counts, wrote });
        }
        atlatl_core::fault::hit(
            "lake.commit",
            works
                .first()
                .map(|w| w.replication.as_str())
                .unwrap_or_default(),
        )?;
        // A kill that landed between statements must not commit (an interrupt only aborts a running one).
        if cancel.is_cancelled() {
            return Err(AtlatlError::new(ErrorClass::Killed, "killed before commit"));
        }
        if !out.iter().any(|a| a.wrote) {
            conn.execute_batch("ROLLBACK")
                .map_err(atlatl_ingest::sql::db)?;
            return Ok(out);
        }
        conn.execute_batch("COMMIT").map_err(|e| {
            let err = atlatl_engine::error::classify(&e);
            let _ = conn.execute_batch("ROLLBACK");
            if err.class == ErrorClass::Conflict {
                clear_conflict(conn, works);
                return lake.name_conflict(conn, err);
            }
            err
        })?;
        Ok(out)
    };
    run().inspect_err(|_| {
        let _ = conn.execute_batch("ROLLBACK");
    })
}

pub async fn run_unit(
    rt: &Runtime,
    unit: &[&ReplicationPlan],
    attempt: u32,
    cancel: CancellationToken,
) -> Result<LakeOutcome> {
    let unloads = Unloads::default();
    let r = run_unit_inner(rt, unit, attempt, cancel, &unloads).await;
    let done = std::mem::take(&mut *unloads.lock().unwrap_or_else(|e| e.into_inner()));
    if !matches!(&r, Err(e) if e.class == ErrorClass::Killed) {
        for (rs, u) in &done {
            if let Err(e) = rs.cleanup(u).await {
                tracing::warn!(error = %e, prefix = %u.member_prefix, "could not remove UNLOAD files; `atlatl gc` will");
            }
        }
    }
    r
}

async fn run_unit_inner(
    rt: &Runtime,
    unit: &[&ReplicationPlan],
    attempt: u32,
    cancel: CancellationToken,
    unloads: &Unloads,
) -> Result<LakeOutcome> {
    let first = unit[0];
    let tgt_lake = rt.lake(&first.target.name)?.clone();
    let mut session = rt.engine.session_with_cancel(cancel.clone())?;
    let ta = tgt_lake.clone();
    let target_before = session.run(move |c| ta.snapshot(c)).await?;
    let src_conn = rt.connection(&first.source.name)?;
    let src = match &src_conn.kind {
        ConnectionKind::Files(f) => Src::Files(Box::new(f.clone())),
        ConnectionKind::Rest(_) => Src::Rest,
        ConnectionKind::Clickhouse(ch) => Src::Clickhouse(Arc::new(
            atlatl_conn_clickhouse::ClickHouse::new(ch, clickhouse_options(rt, first, src_conn))?,
        )),
        ConnectionKind::Redshift(r) => Src::Redshift(Arc::new(Redshift::new(
            r,
            redshift_options(rt, first, src_conn),
        )?)),
        ConnectionKind::Postgres(_) => Src::Postgres(Arc::new(
            crate::pgsrc::PgSrc::open(
                &mut session,
                src_conn,
                unit.iter().any(|r| {
                    r.source_snapshot == Some(atlatl_core::config::model::SourceSnapshot::Shared)
                }),
            )
            .await?,
        )),
        ConnectionKind::Delta(d) => {
            let (d2, secret) = (d.clone(), format!("atlatl_delta_{}", first.source.name));
            session
                .run(move |c| {
                    atlatl_conn_delta::load(c, &d2)?;
                    if let Some(sql) = atlatl_conn_delta::secret_sql(&secret, &d2)? {
                        c.execute_batch(&sql).map_err(atlatl_ingest::sql::db)?;
                    }
                    Ok(())
                })
                .await?;
            Src::Delta(Box::new(d.clone()))
        }
        _ => {
            let sa = rt.lake(&first.source.name)?.clone();
            let alias = sa.alias.clone();
            Src::Lake {
                alias,
                snapshot: session.run(move |c| sa.current_snapshot(c)).await?,
            }
        }
    };
    let src_alias = match &src {
        Src::Lake { alias, .. } => alias.clone(),
        Src::Delta(_)
        | Src::Files(_)
        | Src::Redshift(_)
        | Src::Postgres(_)
        | Src::Rest
        | Src::Clickhouse(_) => String::new(),
    };
    let rs = match &src {
        Src::Redshift(r) => Some(r.as_ref()),
        _ => None,
    };
    let mut resolved = check_source_types(rt, &mut session, unit, &src, &cancel).await?;

    let mut works = Vec::new();
    for (ri, rep) in unit.iter().enumerate() {
        rt.report.start_replication(&rep.name);
        let _ = attempt;
        // REST: producers (queues, `depends_on`) extract first; feed-only ones only fill queues.
        let order: Vec<usize> = if matches!(src, Src::Rest) {
            crate::rest::extract_order(rt, rep)?
        } else {
            (0..rep.streams.len()).collect()
        };
        for si in order {
            let sp = &rep.streams[si];
            if sp.role == StreamRole::FeedOnly {
                // A reject retry replays stored requests; nothing reads the queues.
                if matches!(src, Src::Rest) && !rt.retry_rejects {
                    let s = rt.stream(&sp.id)?;
                    for (mi, m) in sp.members.iter().enumerate() {
                        let stg = staging::staging_name(&rt.run_id, ri * 1000 + si, mi);
                        crate::rest::stage(rt, &mut session, rep, sp, s, m, &stg).await?;
                    }
                }
                continue;
            }
            if cancel.is_cancelled() {
                return Err(AtlatlError::new(ErrorClass::Killed, "cancelled"));
            }
            let s = rt.stream(&sp.id)?;
            if matches!(s.config.mode.as_ref().map(|m| m.value), Some(Mode::Cdc)) {
                return Err(AtlatlError::config(
                    "cdc streams run through the serving runner",
                ));
            }
            let pg = match &src {
                Src::Postgres(p) => Some(p.as_ref()),
                _ => None,
            };
            let ms = match resolved.remove(&(ri, si)) {
                Some(ms) => ms,
                None => members(
                    rt,
                    &mut session,
                    &src_alias,
                    rs,
                    pg,
                    &cancel,
                    s,
                    sp,
                    rep.instance.as_ref(),
                )
                .await?
                .into_iter()
                .map(|m| (m, None))
                .collect(),
            };
            for (mi, (m, admitted)) in ms.into_iter().enumerate() {
                let m = &m;
                let t0 = std::time::Instant::now();
                let w = prepare(
                    rt,
                    &mut session,
                    rep,
                    sp,
                    s,
                    m,
                    (ri * 1000 + si, mi),
                    &src,
                    &tgt_lake.alias,
                    attempt,
                    &cancel,
                    unloads,
                    admitted,
                )
                .await?;
                rt.report.member(&rep.name, &sp.id, &m.name, |r| {
                    r.object = Some(w.target.clone());
                    r.rows.extracted = w.staged;
                    r.rows.staged = w.staged;
                    r.chunks = w.chunks.len() as u64;
                    r.state.before = w.state_before.clone();
                    r.duration_ms = t0.elapsed().as_millis() as u64;
                });
                works.push(w);
            }
        }
    }

    if let Src::Postgres(p) = &src {
        p.release_snapshot().await;
    }
    atlatl_core::fault::hit_async("lake.after_stage", &first.name).await?;
    if let Some(dry) = rt.dry_run {
        let (ws, limit) = (works.clone(), dry.samples);
        let diffs = session.run(move |c| dry_diff(c, &ws, limit)).await;
        cleanup(&mut session, &works).await;
        let mut changed = false;
        for (w, d) in works.iter().zip(diffs?) {
            changed |= d.changes()
                || state_to_write(w).is_some()
                || !w.rejects.is_empty()
                || !w.expired.is_empty()
                || w.resolved.as_ref().is_some_and(|(_, k)| !k.is_empty());
            rt.report
                .member(&w.replication, &w.stream_id, &w.member, |r| {
                    r.state.after = w
                        .state
                        .as_ref()
                        .map(|s| s.value.clone())
                        .or(w.state_before.clone());
                    if !w.pre_hooks.is_empty() || !w.post_hooks.is_empty() {
                        r.warn("dry_run_hooks", "hooks are not run in a dry run");
                    }
                    r.diff = Some(d);
                });
        }
        return Ok(LakeOutcome {
            lake_snapshot: None,
            changed,
        });
    }
    let mut keys: Vec<LockKey> = works
        .iter()
        .map(|w| {
            LockKey::table(
                first.target.name.clone(),
                w.target_schema.clone(),
                w.target_table.clone(),
            )
        })
        .collect();
    keys.sort_by_key(LockKey::name);
    keys.dedup();
    let lock_timeout = first
        .lock_timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(atlatl_core::exec::DEFAULT_LOCK_TIMEOUT);
    let guard = tokio::select! {
        g = rt.locks.acquire(&keys, lock_timeout) => g?,
        _ = cancel.cancelled() => return Err(AtlatlError::new(ErrorClass::Killed, "killed waiting for table locks")),
    };

    let policy = rt
        .project
        .replication(first.config_name())
        .map(|r| RetryPolicy::from_config(&r.retry).unwrap_or_default())
        .unwrap_or_default();
    let commit_policy = RetryPolicy {
        retry_on: vec![ErrorClass::Conflict],
        ..policy
    };
    let state = rt.state.clone();
    let works_c = works.clone();
    let session_ref = std::sync::Arc::new(tokio::sync::Mutex::new(session));
    let mut jitter: Box<dyn atlatl_core::retry::Jitter> = if cfg!(test) {
        Box::new(FixedJitter(1.0))
    } else {
        Box::new(RandomJitter::new())
    };
    let applied = retry(
        &commit_policy,
        RetryLevel::Commit,
        jitter.as_mut(),
        |a| {
            for w in &works_c {
                rt.report
                    .member(&w.replication, &w.stream_id, &w.member, |r| {
                        r.record_retry(a)
                    });
            }
        },
        |_| {
            let (s, works, state, cancel, lake) = (
                session_ref.clone(),
                works_c.clone(),
                state.clone(),
                cancel.clone(),
                tgt_lake.clone(),
            );
            async move {
                let mut s = s.lock().await;
                s.run(move |c| commit(c, &lake, &works, state.as_ref(), &cancel))
                    .await
            }
        },
    )
    .await
    .map_err(|e| e.error)?;
    let _ = guard.release().await;
    atlatl_core::fault::hit_async("lake.after_commit", &first.name).await?;

    let mut session = std::sync::Arc::try_unwrap(session_ref)
        .map(|m| m.into_inner())
        .map_err(|_| AtlatlError::internal("session still shared"))?;
    let ta = tgt_lake.clone();
    let after = session.run(move |c| ta.snapshot(c)).await?;
    let wrote = applied.iter().any(|a| a.wrote);
    // A plain DuckDB target has no snapshots: a commit that wrote is the change.
    let changed = wrote && (after.is_none() || after != target_before);
    let snapshot = after.filter(|_| changed);
    for (w, a) in works.iter().zip(&applied) {
        let n = &a.counts;
        rt.report
            .member(&w.replication, &w.stream_id, &w.member, |r| {
                r.rows.inserted = n.inserted;
                r.rows.updated = n.updated;
                r.rows.deleted = n.deleted;
                r.rows.unchanged = n.unchanged;
                r.state.after = w
                    .state
                    .as_ref()
                    .map(|s| s.value.clone())
                    .or(w.state_before.clone());
                r.lake_snapshot = snapshot.map(|s| s as u64);
            });
    }
    // A committed replication's streams that wrote nothing are `up_to_date` on their own.
    if changed {
        let mut quiet: indexmap::IndexMap<(&str, &str), bool> = Default::default();
        for (w, a) in works.iter().zip(&applied) {
            *quiet
                .entry((w.replication.as_str(), w.stream_id.as_str()))
                .or_insert(true) &= !a.wrote;
        }
        for ((rep, id), q) in quiet {
            if q {
                rt.report.replication(rep, |r| {
                    if let Some(s) = r.stream_mut(id) {
                        s.status = StreamStatus::UpToDate;
                    }
                });
            }
        }
    }
    cleanup(&mut session, &works).await;
    Ok(LakeOutcome {
        lake_snapshot: snapshot,
        changed,
    })
}

/// Drops staging, detaches Delta pins and runs each member's `after` SQL.
async fn cleanup(session: &mut Session, works: &[Work]) {
    let stgs: Vec<String> = works
        .iter()
        .filter(|w| w.chunks.is_empty() && !w.source.is_empty())
        .map(|w| w.source.clone())
        .collect();
    let pins: Vec<String> = works.iter().filter_map(|w| w.pinned.clone()).collect();
    for d in works.iter().filter_map(|w| w.scratch.as_ref()) {
        let _ = tokio::fs::remove_dir_all(d).await;
    }
    let afters: Vec<String> = works.iter().flat_map(|w| w.after.clone()).collect();
    let _ = session
        .run(move |c| {
            for s in &stgs {
                staging::drop(c, s)?;
            }
            for p in &pins {
                c.execute_batch(&format!("DETACH DATABASE IF EXISTS {}", ident(p)))
                    .map_err(atlatl_ingest::sql::db)?;
            }
            for a in &afters {
                c.execute_batch(a).map_err(atlatl_ingest::sql::db)?;
            }
            Ok(())
        })
        .await;
}

fn mode_name(k: Kind) -> &'static str {
    match k {
        Kind::FullRefresh => "full-refresh",
        Kind::Incremental => "incremental",
        Kind::Snapshot => "snapshot",
    }
}

/// The target as the commit would see it after its DDL (added columns NULL, widened ones cast),
/// its row count now, and that DDL.
fn diff_target(conn: &Connection, w: &Work) -> Result<(String, u64, Vec<String>)> {
    let a = &w.adoption;
    let cat = &w.replan.0;
    let Some(now) = staging::table_columns(conn, cat, &w.target_schema, &w.target_table)? else {
        let cols = a
            .columns
            .iter()
            .map(|c| format!("NULL::{} AS {}", c.kind, ident(&c.target)))
            .collect::<Vec<_>>()
            .join(", ");
        let rel = format!("(SELECT {cols} WHERE false)");
        return Ok((rel, 0, vec![drift::create_sql(&w.target, a)]));
    };
    let cols = a
        .columns
        .iter()
        .map(|c| {
            let q = ident(&c.target);
            if !now.iter().any(|n| n.name == c.target) {
                format!("NULL::{} AS {q}", c.kind)
            } else if let Some((_, _, to)) = a.widen.iter().find(|(n, _, _)| *n == c.target) {
                format!("CAST({q} AS {to}) AS {q}")
            } else {
                q
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let rows = drift::target_rows(conn, &w.target)?;
    Ok((
        format!("(SELECT {cols} FROM {})", w.target),
        rows,
        drift::ddl(&w.target, a),
    ))
}

/// `--dry-run`: each member's staged rows against its target, read-only.
fn dry_diff(conn: &Connection, works: &[Work], limit: usize) -> Result<Vec<DiffReport>> {
    works
        .iter()
        .map(|w| {
            let mode = mode_name(w.kind).to_owned();
            if w.skip {
                let rows = match staging::table_columns(
                    conn,
                    &w.replan.0,
                    &w.target_schema,
                    &w.target_table,
                )? {
                    Some(_) => drift::target_rows(conn, &w.target)?,
                    None => 0,
                };
                return Ok(DiffReport {
                    mode,
                    target_rows: rows,
                    counts_only: Some(
                        "nothing to apply: the source is empty and skipped (`on_empty: skip`, no files or no records)".into(),
                    ),
                    ..Default::default()
                });
            }
            let (rel, target_rows, ddl) = diff_target(conn, w)?;
            let cols = drift::projection(&w.adoption);
            let d = Apply {
                kind: w.kind,
                target: &rel,
                source: &w.source,
                columns: &cols,
                primary_key: &w.primary_key,
                null_safe: w.null_safe,
            }
            .diff(conn, &w.chunks, &w.unguarded, limit)?;
            let counts_only = (!d.keyed).then(|| {
                match w.kind {
                    Kind::FullRefresh => "no primary_key: a full refresh replaces every row, so rows aren't matched",
                    Kind::Incremental => "no primary_key: an append inserts every staged row",
                    Kind::Snapshot => "snapshot: appends every staged row",
                }
                .to_owned()
            });
            Ok(DiffReport {
                mode,
                target_rows,
                inserts: d.counts.inserted,
                updates: d.counts.updated,
                deletes: d.counts.deleted,
                unchanged: d.counts.unchanged,
                counts_only,
                ddl,
                samples: d.samples,
            })
        })
        .collect()
}

pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlatl_core::config::types::Dur;

    #[test]
    fn bind_rows_keep_exact_values_and_every_column() {
        let rows = bind_rows(&[
            r#"{"id": 170141183460469231731687303715884105727, "since": "2024-01-01", "x": null}"#
                .into(),
            r#"{"id": 12345678901234567890.123456789, "since": "o'k", "x": true}"#.into(),
        ])
        .expect("rows");
        assert_eq!(
            rows,
            [
                vec![
                    Lit::Number("170141183460469231731687303715884105727".into()),
                    Lit::String("2024-01-01".into()),
                    Lit::Null
                ],
                vec![
                    Lit::Number("12345678901234567890.123456789".into()),
                    Lit::String("o'k".into()),
                    Lit::Bool(true)
                ],
            ]
        );
        let e = bind_rows(&[r#"{"id": 1, "id": 2, "since": "x"}"#.into()]).expect_err("dup");
        assert_eq!(
            e,
            "the query returns two columns named `id`; alias one of them"
        );
        let e = bind_rows(&[r#"{"ids": [1, 2]}"#.into()]).expect_err("list");
        assert!(e.contains("must be a scalar"), "{e}");
    }

    #[test]
    fn bind_numbers_are_typed_by_their_source_column() {
        use atlatl_conn_clickhouse::bind::Num;
        let t = |ty: &str| bind_cols(&[("amt".into(), ty.into())]).map(|c| c[0].num);
        assert_eq!(t("DECIMAL(4,3)"), Ok(Num::Decimal(4, 3)));
        assert_eq!(t("numeric(5,2)"), Ok(Num::Decimal(5, 2)));
        assert_eq!(t("numeric(7)"), Ok(Num::Decimal(7, 0)));
        assert_eq!(t("DOUBLE"), Ok(Num::Float));
        assert_eq!(t("FLOAT"), Ok(Num::Float));
        assert_eq!(t("double precision"), Ok(Num::Float));
        assert_eq!(t("real"), Ok(Num::Float));
        for ty in ["HUGEINT", "UBIGINT", "VARCHAR", "bigint"] {
            assert_eq!(t(ty), Ok(Num::Other), "{ty}");
        }
        assert_eq!(
            t("numeric").expect_err("unconstrained"),
            "column `amt` is numeric without a precision and scale, so its values have no exact ClickHouse type; cast it in the bind query (`amt::numeric(18,4)`, or `amt::float8`)"
        );
        for ty in ["numeric(100,2)", "numeric(5,-2)", "numeric(2,4)"] {
            assert!(
                t(ty)
                    .expect_err(ty)
                    .contains("which no ClickHouse `Decimal` holds"),
                "{ty}"
            );
        }
        let cols = bind_cols(&[("id".into(), "HUGEINT".into()), ("x".into(), "JSON".into())])
            .expect("cols");
        let n = |v: &str| Lit::Number(v.into());
        bind_fractions(
            &cols,
            &[vec![n("170141183460469231731687303715884105727"), n("3")]],
        )
        .expect("integers");
        let e = bind_fractions(&cols, &[vec![n("1"), n("1.5")]]).expect_err("fraction");
        assert!(
            e.starts_with("column `x` is JSON, and its value 1.5 has no exact ClickHouse type"),
            "{e}"
        );
    }

    #[test]
    fn clickhouse_lookback_keeps_sub_second_precision() {
        let us = |s: &str| lookback_us(Some(Dur::parse(s).expect(s)));
        assert_eq!(us("250ms"), Some(250_000));
        assert_eq!(us("1500ms"), Some(1_500_000));
        assert_eq!(us("2h"), Some(7_200_000_000));
        assert_eq!(us("0"), None);
        assert_eq!(lookback_us(None), None);
    }

    #[test]
    fn clickhouse_watermarks_by_landed_type() {
        let c = Connection::open_in_memory().expect("duckdb");
        let render = |v: &str, ty: &str, lookback: Option<u64>| -> Result<String> {
            let (sql, lit) = clickhouse_watermark_sql(v, Some(ty), lookback)?;
            let t: String = c.query_row(&sql, [], |r| r.get(0)).expect(&sql);
            Ok(lit.render(t))
        };
        let cases = [
            ("-5", "TINYINT", "-5"),
            ("-5", "SMALLINT", "-5"),
            ("-5", "INTEGER", "-5"),
            ("-9223372036854775808", "BIGINT", "-9223372036854775808"),
            ("255", "UTINYINT", "255"),
            ("65535", "USMALLINT", "65535"),
            ("4294967295", "UINTEGER", "4294967295"),
            ("18446744073709551615", "UBIGINT", "18446744073709551615"),
            ("1.3", "FLOAT", "CAST('1.3' AS Float32)"),
            ("1.5", "DOUBLE", "1.5"),
            (
                "-170141183460469231731687303715884105728",
                "HUGEINT",
                "CAST('-170141183460469231731687303715884105728' AS Int128)",
            ),
            (
                "340282366920938463463374607431768211455",
                "UHUGEINT",
                "CAST('340282366920938463463374607431768211455' AS UInt128)",
            ),
            ("1.25", "DECIMAL(9,2)", "CAST('1.25' AS Decimal(9,2))"),
            ("1.2500", "DECIMAL(18,4)", "CAST('1.2500' AS Decimal(18,4))"),
            (
                "12345678901234567890.5",
                "DECIMAL(38,10)",
                "CAST('12345678901234567890.5000000000' AS Decimal(38,10))",
            ),
            ("true", "BOOLEAN", "true"),
            ("1900-10-01", "DATE", "'1900-10-01'"),
            (
                "2026-10-01 20:00:00.123456+00",
                "TIMESTAMP WITH TIME ZONE",
                "toDateTime64('2026-10-01 20:00:00.123456', 6, 'UTC')",
            ),
            (
                "2026-10-01 16:00:00-04",
                "TIMESTAMP WITH TIME ZONE",
                "toDateTime64('2026-10-01 20:00:00.000000', 6, 'UTC')",
            ),
            (
                "2026-10-01 20:00:00",
                "TIMESTAMP",
                "toDateTime64('2026-10-01 20:00:00.000000', 6, 'UTC')",
            ),
            ("abc", "VARCHAR", "'abc'"),
        ];
        for (v, ty, want) in cases {
            assert_eq!(render(v, ty, None).expect(ty), want, "{ty}");
        }
        assert_eq!(
            render("2026-10-08", "DATE", Some(7 * 86_400_000_000)).expect("date"),
            "'2026-10-01'"
        );
        assert_eq!(
            render(
                "2026-10-01 20:00:00+00",
                "TIMESTAMP WITH TIME ZONE",
                Some(3_600_000_000)
            )
            .expect("ts"),
            "toDateTime64('2026-10-01 19:00:00.000000', 6, 'UTC')"
        );
        assert_eq!(
            render(
                "2026-10-01 20:00:00+00",
                "TIMESTAMP WITH TIME ZONE",
                lookback_us(Some(Dur::parse("250ms").expect("dur")))
            )
            .expect("sub-second"),
            "toDateTime64('2026-10-01 19:59:59.750000', 6, 'UTC')"
        );
        let e = render("5", "BIGINT", Some(60_000_000)).expect_err("numeric lookback");
        assert!(
            e.message.contains("needs a date or timestamp"),
            "{}",
            e.message
        );
        let e = clickhouse_watermark_sql("\\x01", Some("BLOB"), None).expect_err("blob");
        assert!(e.message.contains("BLOB"), "{}", e.message);
    }

    #[test]
    fn objects_and_globs() {
        assert_eq!(split_object("br.t"), ("br".into(), "t".into()));
        assert_eq!(split_object("t"), ("main".into(), "t".into()));
        assert_eq!(split_object("catalog.br.t"), ("br".into(), "t".into()));
        assert_eq!(
            glob_to_like("est_v2.est_core_cons_*f"),
            "est\\_v2.est\\_core\\_cons\\_%f"
        );
        assert!(like_match("appdb.users_old", "appdb.*_old"));
        assert!(!like_match("appdb.users", "appdb.*_old"));
    }
}
