//! DuckLake connector (spec 06 §2): attach, S3 secrets, data-path and version checks,
//! snapshot helpers, the lock backend. Also plain DuckDB files written as lakes (06 §10).

pub mod attach;
pub mod plain;
pub mod sql;

use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};

use atlatl_conn_postgres::{ConnectOptions, PgAdvisoryLocks};
use atlatl_core::config::model::{CatalogType, DuckLakeConn};
use atlatl_core::error::Result;
use atlatl_core::lock::{LocalLocks, LockManager};
use atlatl_core::redact::redact;
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_engine::error::{COMMIT_CONFLICT, classify_owned};
use atlatl_engine::{DbResultExt, Engine};
use duckdb::{Connection, OptionalExt};
use regex::Regex;

pub use attach::AttachPlan;
use sql::{ident, literal};

/// The DuckLake catalog format the bundled ducklake extension writes (DuckDB v1.5.6).
pub const FORMAT_VERSION: &str = "1.0";
pub const DATA_PATH_MISMATCH: &str = "data_path_mismatch";
pub const VERSION_MISMATCH: &str = "ducklake_version_mismatch";

static DUCKLAKE_MISMATCH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"DATA_PATH parameter "(.*)" does not match existing data path in the catalog "(.*)""#,
    )
    .expect("regex")
});

static DUCKLAKE_VERSION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"DuckLake catalog version mismatch: catalog version is (\S+?),").expect("regex")
});

static MISSING_FILE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"Cannot open file "(.*)": (No such file or directory|Permission denied)"#)
        .expect("regex")
});

/// `CREATE_IF_NOT_EXISTS false` on a catalog with no DuckLake metadata.
const NO_CATALOG: &str = "and creating a new DuckLake is explicitly disabled";

static CONFLICT_TABLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"table with index "(\d+)""#).expect("regex"));

/// An attached lake. Its helpers run on any engine connection: ATTACH is instance-wide.
#[derive(Clone, Debug)]
pub struct Lake {
    pub alias: String,
    pub data_path: String,
    /// `None` for a plain DuckDB database: no snapshots, change feed or catalog metadata.
    pub catalog: Option<CatalogType>,
}

/// Loads the extensions, creates the S3 secret, attaches, then checks the stored data path
/// (byte for byte) and the catalog format version. On a failed check the lake is detached.
pub async fn attach(engine: &Engine, name: &str, conn: &DuckLakeConn) -> Result<Lake> {
    let plan = AttachPlan::new(name, conn)?;
    let lake = Lake {
        alias: plan.alias.clone(),
        data_path: plan.data_path.clone(),
        catalog: Some(conn.catalog.kind),
    };
    let l = lake.clone();
    engine
        .with_conn(move |c| {
            for e in &plan.extensions {
                c.execute_batch(&format!("LOAD {e}")).db()?;
            }
            if let Some(s) = &plan.secret {
                c.execute_batch(s).db()?;
            }
            c.execute_batch(&plan.attach)
                .map_err(|e| attach_error(&l, e))?;
            let checked = l.verify(c);
            if checked.is_err() {
                let _ = c.execute_batch(&format!("DETACH {}", ident(&l.alias)));
            }
            checked
        })
        .await?;
    tracing::info!(alias = %lake.alias, data_path = %lake.data_path, "ducklake attached");
    Ok(lake)
}

/// [`attach`] for a dry run: read-only, and never creating the catalog. `None` when the catalog
/// doesn't exist yet (no DuckDB file, or no DuckLake metadata in the file or Postgres database);
/// an unreachable one is an error as for [`attach`].
pub async fn attach_existing(
    engine: &Engine,
    name: &str,
    conn: &DuckLakeConn,
) -> Result<Option<Lake>> {
    let plan = AttachPlan::new(name, conn)?;
    let lake = Lake {
        alias: plan.alias.clone(),
        data_path: plan.data_path.clone(),
        catalog: Some(conn.catalog.kind),
    };
    if conn.catalog.kind == CatalogType::Duckdb {
        let path = std::path::Path::new(conn.catalog.path.as_deref().unwrap_or_default());
        if !path.exists() {
            let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
            if dir.is_some_and(|d| !d.is_dir()) {
                return Err(missing_dir(&lake, &path.display().to_string()));
            }
            return Ok(None);
        }
    }
    let l = lake.clone();
    let found = engine
        .with_conn(move |c| {
            for e in &plan.extensions {
                c.execute_batch(&format!("LOAD {e}")).db()?;
            }
            if let Some(s) = &plan.secret {
                c.execute_batch(s).db()?;
            }
            if let Err(e) = c.execute_batch(&plan.attach_existing) {
                if e.to_string().contains(NO_CATALOG) {
                    return Ok(false);
                }
                return Err(attach_error(&l, e));
            }
            let checked = l.verify(c);
            if checked.is_err() {
                let _ = c.execute_batch(&format!("DETACH {}", ident(&l.alias)));
            }
            checked.map(|()| true)
        })
        .await?;
    if found {
        tracing::info!(alias = %lake.alias, data_path = %lake.data_path, "ducklake attached read-only");
    }
    Ok(found.then_some(lake))
}

/// An empty lake under `conn`'s alias, with an in-memory catalog and `data_path` (never written:
/// a dry run's stand-in for a catalog that doesn't exist yet).
pub async fn attach_empty(
    engine: &Engine,
    name: &str,
    conn: &DuckLakeConn,
    data_path: &str,
) -> Result<Lake> {
    let alias = conn.alias.clone().unwrap_or_else(|| name.to_owned());
    sql::check_alias(&alias)?;
    let sql = format!(
        "LOAD ducklake; ATTACH 'ducklake::memory:' AS {} (DATA_PATH {})",
        ident(&alias),
        literal(data_path)
    );
    engine
        .with_conn(move |c| c.execute_batch(&sql).db())
        .await?;
    Ok(Lake {
        alias,
        data_path: data_path.to_owned(),
        catalog: Some(CatalogType::Duckdb),
    })
}

fn missing_dir(l: &Lake, path: &str) -> AtlatlError {
    AtlatlError::config(format!(
        "attach `{}`: cannot open the catalog {path:?}: no such file or directory (DuckDB doesn't create missing directories)",
        l.alias
    ))
}

fn attach_error(l: &Lake, e: duckdb::Error) -> AtlatlError {
    let msg = e.to_string();
    if let Some(m) = DUCKLAKE_MISMATCH.captures(&msg) {
        return mismatch(&l.alias, &m[1], &m[2]);
    }
    if let Some(m) = DUCKLAKE_VERSION.captures(&msg) {
        return version_error(&l.alias, Some(&m[1]));
    }
    // DuckDB creates the catalog file but not its directory.
    if let Some(m) = MISSING_FILE.captures(&msg) {
        let hint = if m[2].starts_with("No such") {
            " (DuckDB doesn't create missing directories)"
        } else {
            ""
        };
        return AtlatlError::config(format!(
            "attach `{}`: cannot open the catalog {:?}: {}{hint}",
            l.alias,
            &m[1],
            m[2].to_lowercase()
        ));
    }
    let mut err = classify_owned(e);
    err.message = format!("attach `{}`: {}", l.alias, err.message);
    err
}

fn mismatch(alias: &str, configured: &str, stored: &str) -> AtlatlError {
    AtlatlError::config(format!(
        "lake `{alias}`: data_path {configured:?} does not match the catalog's data path \
         {stored:?} (compared byte for byte; DuckLake stores it with a trailing slash)"
    ))
    .code(DATA_PATH_MISMATCH)
}

pub fn check_format_version(alias: &str, found: Option<&str>) -> Result<()> {
    match found {
        Some(FORMAT_VERSION) => Ok(()),
        v => Err(version_error(alias, v)),
    }
}

fn version_error(alias: &str, v: Option<&str>) -> AtlatlError {
    AtlatlError::config(format!(
        "lake `{alias}`: catalog format version is {}, but atlatl's DuckLake writes \
             {FORMAT_VERSION}; the catalog was written by a different DuckDB/DuckLake version \
             (keep atlatl and dbt-duckdb on the same DuckDB release)",
        v.unwrap_or("missing")
    ))
    .code(VERSION_MISMATCH)
}

/// `LocalLocks` for a DuckDB-file catalog, advisory locks on the catalog database otherwise.
pub fn lock_manager(conn: &DuckLakeConn) -> Result<Arc<dyn LockManager>> {
    Ok(match conn.catalog.kind {
        CatalogType::Postgres => Arc::new(PgAdvisoryLocks::new(ConnectOptions::from_catalog(
            &conn.catalog,
        )?)),
        CatalogType::Duckdb => Arc::new(LocalLocks::new()),
    })
}

impl Lake {
    fn lit(&self) -> String {
        literal(&self.alias)
    }

    fn metadata(&self) -> String {
        ident(&format!("__ducklake_metadata_{}", self.alias))
    }

    fn option(&self, c: &Connection, name: &str) -> Result<Option<String>> {
        c.query_row(
            &format!(
                "SELECT value FROM ducklake_options({}) WHERE option_name = ? AND scope = 'GLOBAL'",
                self.lit()
            ),
            [name],
            |r| r.get(0),
        )
        .optional()
        .db()
    }

    pub fn stored_data_path(&self, c: &Connection) -> Result<Option<String>> {
        self.option(c, "data_path")
    }

    pub fn format_version(&self, c: &Connection) -> Result<Option<String>> {
        self.option(c, "version")
    }

    /// The post-ATTACH checks. DuckLake itself accepts a data path that differs only in trailing
    /// slashes; atlatl doesn't (spec 06 §2).
    pub fn verify(&self, c: &Connection) -> Result<()> {
        match self.stored_data_path(c)? {
            Some(s) if s == self.data_path => {}
            s => {
                return Err(mismatch(
                    &self.alias,
                    &self.data_path,
                    s.as_deref().unwrap_or(""),
                ));
            }
        }
        check_format_version(&self.alias, self.format_version(c)?.as_deref())
    }

    pub fn is_plain(&self) -> bool {
        self.catalog.is_none()
    }

    /// [`Self::current_snapshot`], or `None` for a plain DuckDB database.
    pub fn snapshot(&self, c: &Connection) -> Result<Option<i64>> {
        if self.is_plain() {
            return Ok(None);
        }
        self.current_snapshot(c).map(Some)
    }

    pub fn current_snapshot(&self, c: &Connection) -> Result<i64> {
        if self.is_plain() {
            return Err(AtlatlError::config(format!(
                "`{}` is a plain DuckDB database, not a DuckLake: it has no snapshots",
                self.alias
            )));
        }
        c.query_row(
            &format!(
                "SELECT id::BIGINT FROM ducklake_current_snapshot({})",
                self.lit()
            ),
            [],
            |r| r.get(0),
        )
        .db()
    }

    pub fn snapshot_exists(&self, c: &Connection, id: i64) -> Result<bool> {
        c.query_row(
            &format!(
                "SELECT count(*) > 0 FROM ducklake_snapshots({}) WHERE snapshot_id = ?",
                self.lit()
            ),
            [id],
            |r| r.get(0),
        )
        .db()
    }

    pub fn snapshot_time(&self, c: &Connection, id: i64) -> Result<Option<SystemTime>> {
        let us: Option<i64> = c
            .query_row(
                &format!(
                    "SELECT epoch_us(snapshot_time) FROM ducklake_snapshots({}) WHERE snapshot_id = ?",
                    self.lit()
                ),
                [id],
                |r| r.get(0),
            )
            .optional()
            .db()?;
        us.map(|us| {
            let d = Duration::from_micros(us.unsigned_abs());
            if us >= 0 {
                SystemTime::UNIX_EPOCH.checked_add(d)
            } else {
                SystemTime::UNIX_EPOCH.checked_sub(d)
            }
            .ok_or_else(|| AtlatlError::new(ErrorClass::Internal, "snapshot_time out of range"))
        })
        .transpose()
    }

    /// The oldest snapshot still retained (older ones were expired).
    pub fn oldest_snapshot(&self, c: &Connection) -> Result<Option<i64>> {
        c.query_row(
            &format!(
                "SELECT min(snapshot_id) FROM ducklake_snapshots({})",
                self.lit()
            ),
            [],
            |r| r.get(0),
        )
        .db()
    }

    /// `schema.table` for a `table_id`, preferring the live name over a dropped or renamed one.
    pub fn table_name(&self, c: &Connection, id: i64) -> Result<Option<String>> {
        c.query_row(
            &format!(
                "SELECT s.schema_name || '.' || t.table_name FROM {m}.ducklake_table t \
                 JOIN {m}.ducklake_schema s ON s.schema_id = t.schema_id WHERE t.table_id = ? \
                 ORDER BY t.end_snapshot IS NULL DESC, s.end_snapshot IS NULL DESC, \
                 t.begin_snapshot DESC, s.begin_snapshot DESC LIMIT 1",
                m = self.metadata()
            ),
            [id],
            |r| r.get(0),
        )
        .optional()
        .db()
    }

    /// Names the table a `commit_conflict` reports only by id (spec 06 §2). Best-effort: the
    /// error comes back unchanged if the lookup fails. Run it after the ROLLBACK.
    pub fn name_conflict(&self, c: &Connection, mut err: AtlatlError) -> AtlatlError {
        if self.is_plain() || err.code.as_deref() != Some(COMMIT_CONFLICT) {
            return err;
        }
        let Some((range, id)) = CONFLICT_TABLE.captures(&err.message).and_then(|m| {
            let id = m[1].parse::<i64>().ok()?;
            Some((m.get(0)?.range(), id))
        }) else {
            return err;
        };
        let Ok(Some(name)) = self.table_name(c, id) else {
            return err;
        };
        err.message
            .replace_range(range, &format!("table `{name}` (index \"{id}\")"));
        err.message = redact(&err.message).into_owned();
        err
    }

    /// The current `table_id` of `schema.table`; a recreated table gets a new one.
    pub fn table_id(&self, c: &Connection, schema: &str, table: &str) -> Result<Option<i64>> {
        c.query_row(
            &format!(
                "SELECT i.table_id FROM ducklake_table_info({}) i \
                 JOIN {}.ducklake_schema s ON s.schema_id = i.schema_id AND s.end_snapshot IS NULL \
                 WHERE s.schema_name = ? AND i.table_name = ?",
                self.lit(),
                self.metadata()
            ),
            [schema, table],
            |r| r.get(0),
        )
        .optional()
        .db()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_version_check() {
        check_format_version("l", Some(FORMAT_VERSION)).expect("same");
        for v in [Some("0.3"), Some("1.1"), None] {
            let e = check_format_version("l", v).expect_err("mismatch");
            assert_eq!(
                (e.class, e.code.as_deref()),
                (ErrorClass::Config, Some(VERSION_MISMATCH))
            );
        }
    }

    #[test]
    fn ducklake_mismatch_message_is_parsed() {
        let msg = "Invalid Configuration Error: DATA_PATH parameter \"/a/x/\" does not match existing \
                   data path in the catalog \"/a/\".\nYou can override the DATA_PATH by setting OVERRIDE_DATA_PATH to True.";
        let m = DUCKLAKE_MISMATCH.captures(msg).expect("match");
        let e = mismatch("lake", &m[1], &m[2]);
        assert_eq!(e.code.as_deref(), Some(DATA_PATH_MISMATCH));
        assert!(
            e.message.contains("\"/a/x/\"") && e.message.contains("\"/a/\""),
            "{}",
            e.message
        );

        let msg = "Invalid Input Error: DuckLake catalog version mismatch: catalog version is 0.3, \
                   but the extension requires version 1.0. To automatically migrate, set AUTOMATIC_MIGRATION to TRUE when attaching.";
        assert_eq!(&DUCKLAKE_VERSION.captures(msg).expect("match")[1], "0.3");
    }

    #[test]
    fn locks_by_catalog_type() {
        let file: DuckLakeConn = serde_json::from_value(serde_json::json!({
            "catalog": {"type": "duckdb", "path": "/m"}, "data_path": "/d/"
        }))
        .expect("conn");
        lock_manager(&file).expect("local");
        let pg: DuckLakeConn = serde_json::from_value(serde_json::json!({
            "catalog": {"type": "postgres", "host": "h", "database": "d", "user": "u"}, "data_path": "/d/"
        }))
        .expect("conn");
        lock_manager(&pg).expect("pg");
        let bad: DuckLakeConn = serde_json::from_value(serde_json::json!({
            "catalog": {"type": "postgres", "host": "h"}, "data_path": "/d/"
        }))
        .expect("conn");
        assert_eq!(
            lock_manager(&bad).err().map(|e| e.class),
            Some(ErrorClass::Config)
        );
    }
}
