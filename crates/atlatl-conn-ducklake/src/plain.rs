//! A plain DuckDB database file attached read-write as an ingest target (06 §10). It has no
//! snapshots: a commit is one DuckDB transaction, and nothing else may hold the file meanwhile.

use std::path::Path;

use atlatl_core::config::model::DuckdbConn;
use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_engine::Engine;
use atlatl_engine::error::classify_owned;

use crate::Lake;
use crate::sql::{check_alias, ident, literal};

/// Attaches `conn.path` under its alias (default `name`). The file must exist: a missing one is
/// a `config` error rather than a fresh empty database. `read_only` for a dry run.
pub async fn attach(
    engine: &Engine,
    name: &str,
    conn: &DuckdbConn,
    read_only: bool,
) -> Result<Lake> {
    let alias = conn.alias.clone().unwrap_or_else(|| name.to_owned());
    check_alias(&alias)?;
    if !Path::new(&conn.path).is_file() {
        return Err(AtlatlError::config(format!(
            "duckdb `{name}`: database file {:?} doesn't exist",
            conn.path
        )));
    }
    let sql = format!(
        "ATTACH {} AS {}{}",
        literal(&conn.path),
        ident(&alias),
        if read_only { " (READ_ONLY)" } else { "" }
    );
    let lake = Lake {
        alias,
        data_path: conn.path.clone(),
        catalog: None,
    };
    let l = lake.clone();
    engine
        .with_conn(move |c| c.execute_batch(&sql).map_err(|e| attach_error(&l, e)))
        .await?;
    tracing::info!(alias = %lake.alias, path = %lake.data_path, read_only, "duckdb attached");
    Ok(lake)
}

fn attach_error(l: &Lake, e: duckdb::Error) -> AtlatlError {
    let msg = e.to_string();
    // Another process (dbt, a DuckDB shell) holds the file's lock.
    if msg.contains("Could not set lock on file") || msg.contains("Conflicting lock is held") {
        return AtlatlError::new(
            ErrorClass::Transient,
            format!(
                "attach `{}`: {:?} is locked by another process (run atlatl and dbt one after the other): {msg}",
                l.alias, l.data_path
            ),
        )
        .code("duckdb_locked");
    }
    let mut err = classify_owned(e);
    err.message = format!("attach `{}`: {}", l.alias, err.message);
    err
}
