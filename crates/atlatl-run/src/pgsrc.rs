//! Postgres → lake ingest (06 §3.2, M8.2): the source is attached read-only into the engine for
//! the unit and read engine-natively; discovery, primary keys and column metadata come from
//! `pg_catalog`. Under `source_snapshot: shared` every member reads one exported snapshot.

use std::sync::Arc;

use atlatl_conn_postgres::ConnectOptions;
use atlatl_conn_postgres::source::{self, Cursor, PgSource};
use atlatl_core::config::connection::Connection as ConnConfig;
use atlatl_core::config::load::{Project, Stream};
use atlatl_core::config::model::ConnectionKind;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::plan::{MemberPlan, Plan, StreamPlan};
use atlatl_core::template::EnvSource;
use atlatl_engine::Session;
use duckdb::Connection;

/// A source attached for one unit; dropping it detaches.
pub struct PgSrc {
    pub alias: String,
    pub source: PgSource,
    duck: std::sync::Mutex<Connection>,
    /// The exported snapshot every member reads (`source_snapshot: shared`).
    snapshot: Option<String>,
}

/// A member's relation and whether the cursor predicate was evaluated in Postgres.
pub struct PgRelation {
    pub sql: String,
    pub pushed: bool,
}

fn options(conn: &ConnConfig) -> Result<ConnectOptions> {
    let mut opts = ConnectOptions::from_connection(conn)?;
    if let Some(t) = conn.common.timeouts.as_ref().and_then(|t| t.statement) {
        opts.statement_timeout = Some(t.0);
    }
    Ok(opts)
}

impl PgSrc {
    pub async fn open(session: &mut Session, conn: &ConnConfig, shared: bool) -> Result<Self> {
        let opts = options(conn)?;
        let alias = format!(
            "atlatl_pgsrc_{}",
            atlatl_core::ids::ulid().to_ascii_lowercase()
        );
        let sql = source::attach_sql(&alias, &opts);
        let duck = session
            .run(move |c| {
                c.execute_batch(&sql)
                    .map_err(|e| source::reclassify(atlatl_ingest::sql::db(e)))?;
                c.try_clone().map_err(atlatl_ingest::sql::db)
            })
            .await?;
        let source = PgSource::new(opts)?;
        let snapshot = if shared {
            Some(source.export_snapshot().await?)
        } else {
            None
        };
        Ok(Self {
            alias,
            source,
            duck: std::sync::Mutex::new(duck),
            snapshot,
        })
    }

    pub async fn discover(
        &self,
        pattern: &str,
        exclude: &[String],
        include_views: bool,
    ) -> Result<Vec<String>> {
        self.source.discover(pattern, exclude, include_views).await
    }

    /// The engine relation for one member: a table (columns narrowed by `columns.include` /
    /// `exclude`) or `from.query`. A `cursor` is evaluated in Postgres when the member is read
    /// through `postgres_query` and the predicate is safe there.
    pub async fn relation(
        &self,
        s: &Stream,
        sp: &StreamPlan,
        m: &MemberPlan,
        cursor: Option<&Cursor>,
    ) -> Result<PgRelation> {
        if let Some(q) = &m.query {
            return Ok(PgRelation {
                sql: source::query_relation(&self.alias, q),
                pushed: false,
            });
        }
        let Some(table) = &m.table else {
            return Err(AtlatlError::config(format!(
                "stream `{}`: postgres sources read `from.table`, `from.tables` or `from.query`",
                sp.id
            )));
        };
        let schema = m.schema.as_deref().unwrap_or("public");
        let (include, exclude) = narrowing(s);
        let cols = self
            .source
            .columns(schema, table, include.as_deref(), &exclude)
            .await?;
        let pred = cursor.and_then(|c| source::cursor_predicate(c, &cols));
        let (sql, pushed) =
            source::relation_where(&self.alias, schema, table, &cols, pred.as_deref())?;
        Ok(PgRelation { sql, pushed })
    }

    /// Runs one read (`CREATE TEMP TABLE … AS SELECT …` from the attach) in the shared snapshot,
    /// if there is one; otherwise as is (autocommit: its own snapshot).
    pub fn read_in_snapshot<T>(
        alias: &str,
        snapshot: Option<&str>,
        c: &Connection,
        read: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let Some(id) = snapshot else {
            return read(c);
        };
        c.execute_batch(&format!("BEGIN; {}", source::set_snapshot_sql(alias, id)))
            .map_err(|e| {
                let _ = c.execute_batch("ROLLBACK");
                source::reclassify(atlatl_ingest::sql::db(e))
            })?;
        match read(c) {
            Ok(v) => {
                c.execute_batch("COMMIT")
                    .map_err(|e| source::reclassify(atlatl_ingest::sql::db(e)))?;
                Ok(v)
            }
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    pub fn snapshot(&self) -> Option<&str> {
        self.snapshot.as_deref()
    }

    /// Ends the exported snapshot once every member is staged, releasing the xmin horizon.
    pub async fn release_snapshot(&self) {
        self.source.release_snapshot().await;
    }
}

impl Drop for PgSrc {
    fn drop(&mut self) {
        let c = self.duck.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = c.execute_batch(&source::detach_sql(&self.alias)) {
            tracing::debug!(error = %e, alias = %self.alias, "could not detach postgres source");
        }
    }
}

fn narrowing(s: &Stream) -> (Option<Vec<String>>, Vec<String>) {
    let cc = s.config.columns.as_ref();
    (
        cc.and_then(|c| c.include.clone()),
        cc.and_then(|c| c.exclude.clone()).unwrap_or_default(),
    )
}

/// A discovered member's primary key and where it came from: a `members` override, the
/// stream's `primary_key`, or (postgres sources) the table's own primary key constraint.
pub async fn member_key(
    pg: Option<&PgSource>,
    s: &Stream,
    name: &str,
) -> Result<(Option<Vec<String>>, Option<String>)> {
    if let Some(k) = s
        .config
        .member_config(name)
        .and_then(|m| m.primary_key.clone())
    {
        return Ok((Some(k), Some("members".into())));
    }
    if let Some(k) = &s.config.primary_key {
        return Ok((Some(k.value.clone()), Some("stream".into())));
    }
    let Some(pg) = pg else {
        return Ok((None, None));
    };
    let (schema, table) = crate::lake::split_object(name);
    let Some(key) = pg.primary_key(&schema, &table).await? else {
        return Ok((None, None));
    };
    let (include, exclude) = narrowing(s);
    let named = |list: &[String], n: &str| list.iter().any(|x| x.eq_ignore_ascii_case(n));
    if let Some(k) = key
        .iter()
        .find(|k| include.as_deref().is_some_and(|i| !named(i, k)) || named(&exclude, k))
    {
        return Err(AtlatlError::config(format!(
            "stream `{}`: `{name}`'s primary key ({}) needs column `{k}`, which `columns` leaves out; read it, or set `members.\"{name}\".primary_key`",
            s.id.value,
            key.join(", ")
        )));
    }
    Ok((Some(key), Some("source".into())))
}

/// `atlatl plan --discover`: expands the plan's postgres wildcard members against the source
/// catalog, with each member's rendered target and resolved primary key.
pub async fn discover_plan(
    project: &Project,
    plan: &mut Plan,
    vars: Arc<dyn EnvSource>,
) -> Result<()> {
    let env = plan.env.clone();
    let mut sources: indexmap::IndexMap<String, PgSource> = indexmap::IndexMap::new();
    let mut expanded: Vec<String> = Vec::new();
    for rep in &mut plan.replications {
        let rp = rep.clone();
        for sp in &mut rep.streams {
            if !sp.members.iter().any(|m| m.pattern) {
                continue;
            }
            let hop_source = rp
                .hops
                .get(sp.hop)
                .map_or(&rp.source.name, |h| &h.source.name)
                .clone();
            let s = project.stream(&sp.id).map(|(_, s)| s).ok_or_else(|| {
                AtlatlError::internal(format!("stream `{}` not in project", sp.id))
            })?;
            if !sources.contains_key(&hop_source) {
                let c = crate::runtime::resolve_connection(
                    project,
                    &hop_source,
                    env.as_deref(),
                    vars.clone(),
                )?;
                if !matches!(c.kind, ConnectionKind::Postgres(_)) {
                    continue;
                }
                sources.insert(hop_source.clone(), PgSource::new(options(&c)?)?);
            }
            let pg = &sources[&hop_source];
            let from = s.config.from.as_ref();
            let exclude = from.and_then(|f| f.exclude.clone()).unwrap_or_default();
            let views = from.and_then(|f| f.include_views).unwrap_or(false);
            let mut out = Vec::new();
            for m in std::mem::take(&mut sp.members) {
                if !m.pattern {
                    out.push(m);
                    continue;
                }
                for name in pg.discover(&m.name, &exclude, views).await? {
                    let (object, window) = crate::state_cmd::render_member(
                        project,
                        env.as_deref(),
                        &vars,
                        s,
                        sp,
                        rp.instance.as_ref(),
                        &name,
                    )?;
                    let (primary_key, primary_key_from) = member_key(Some(pg), s, &name).await?;
                    let (schema, table) = crate::lake::split_object(&name);
                    out.push(MemberPlan {
                        name,
                        schema: Some(schema),
                        table: Some(table),
                        pattern: false,
                        object: Some(object),
                        window,
                        query: None,
                        url: None,
                        primary_key,
                        primary_key_from,
                    });
                }
            }
            sp.members = out;
            expanded.push(sp.id.clone());
        }
    }
    plan.warnings.retain(|w| {
        w.code != "unexpanded"
            || !expanded
                .iter()
                .any(|id| w.message.starts_with(&format!("stream `{id}`: wildcard")))
    });
    for (_, pg) in sources {
        pg.close().await;
    }
    Ok(())
}
