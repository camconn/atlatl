//! Metadata columns (04 §8) and hooks (02 §4). Values are constant per replication run.

use atlatl_core::config::model::{LoadedAtType, MetadataConfig};
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::template::{Context, Phase, Templates};
use minijinja::Value;

use crate::sql::{ident, literal};

/// `(column, SQL expression, DuckDB type)` appended to the staged projection.
pub fn columns(
    m: Option<&MetadataConfig>,
    started_epoch_s: i64,
    run_id: &str,
    source: &str,
) -> Vec<(String, String, String)> {
    let Some(m) = m else { return Vec::new() };
    let mut out = Vec::new();
    if let Some(l) = &m.loaded_at {
        let name = l.name.clone().unwrap_or_else(|| "_atlatl_loaded_at".into());
        match l.kind.unwrap_or(LoadedAtType::Timestamptz) {
            LoadedAtType::EpochSInt => out.push((
                name,
                format!("{started_epoch_s}::INTEGER"),
                "INTEGER".into(),
            )),
            LoadedAtType::Timestamptz => out.push((
                name,
                format!("to_timestamp({started_epoch_s})"),
                "TIMESTAMP WITH TIME ZONE".into(),
            )),
        }
    }
    if let Some(r) = &m.run_id {
        out.push((
            r.name.clone().unwrap_or_else(|| "_atlatl_run_id".into()),
            literal(run_id),
            "VARCHAR".into(),
        ));
    }
    if let Some(s) = &m.source {
        out.push((
            s.name.clone().unwrap_or_else(|| "_atlatl_source".into()),
            literal(source),
            "VARCHAR".into(),
        ));
    }
    out
}

/// Metadata columns whose value changes every run (`loaded_at`, `run_id`). The change guard
/// ignores them, so an unchanged row keeps its stamp and an unchanged rerun writes nothing.
pub fn run_varying(m: Option<&MetadataConfig>) -> Vec<String> {
    let Some(m) = m else { return Vec::new() };
    let loaded = m
        .loaded_at
        .as_ref()
        .map(|l| l.name.clone().unwrap_or_else(|| "_atlatl_loaded_at".into()));
    let run = m
        .run_id
        .as_ref()
        .map(|r| r.name.clone().unwrap_or_else(|| "_atlatl_run_id".into()));
    loaded.into_iter().chain(run).collect()
}

/// `SELECT *, <metadata> FROM <relation>`.
pub fn with_columns(relation: &str, cols: &[(String, String, String)]) -> String {
    if cols.is_empty() {
        return format!("SELECT * FROM {relation}");
    }
    let extra = cols
        .iter()
        .map(|(n, e, _)| format!("{e} AS {}", ident(n)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT *, {extra} FROM {relation}")
}

/// Renders hook SQL with `staging` and `target` (the hook phase). Hooks run inside the commit and
/// re-run on every commit retry.
pub fn render_hooks(
    t: &Templates,
    base: &Context,
    hooks: &[String],
    staging: &str,
    target: &str,
) -> Result<Vec<String>> {
    let ctx = base
        .clone()
        .with("staging", Value::from(staging))
        .with("target", Value::from(target));
    hooks
        .iter()
        .map(|h| {
            t.check(h, Phase::Hook, None)
                .map_err(|e| AtlatlError::config(e.message.clone()))?;
            t.render(h, &ctx)
                .map_err(|e| AtlatlError::config(e.message))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use atlatl_core::config::model::{LoadedAt, NamedColumn};
    use atlatl_core::template::MapEnv;
    use duckdb::Connection;

    use super::*;

    #[test]
    fn legacy_loaded_at_is_epoch_seconds() {
        let m = MetadataConfig {
            loaded_at: Some(LoadedAt {
                name: Some("_legacy_loaded_at".into()),
                kind: Some(LoadedAtType::EpochSInt),
            }),
            run_id: Some(NamedColumn { name: None }),
            source: None,
        };
        let cols = columns(Some(&m), 1_700_000_000, "01RUN", "s3://b/x.csv");
        let c = Connection::open_in_memory().expect("db");
        c.execute_batch("CREATE TABLE s AS SELECT 1 AS a")
            .expect("s");
        let sql = with_columns("s", &cols);
        let (a, l, r): (i32, i32, String) = c
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("row");
        assert_eq!((a, l, r.as_str()), (1, 1_700_000_000, "01RUN"));
        let ty: String = c
            .query_row(
                &format!("SELECT typeof(_legacy_loaded_at) FROM ({sql})"),
                [],
                |r| r.get(0),
            )
            .expect("type");
        assert_eq!(
            ty, "INTEGER",
            "adopted legacy tables store INTEGER epoch seconds"
        );
        assert!(columns(None, 0, "r", "s").is_empty());
        assert_eq!(
            run_varying(Some(&m)),
            ["_legacy_loaded_at", "_atlatl_run_id"],
            "`source` is the member's, the same every run"
        );
    }

    #[test]
    fn hooks_render_with_staging_and_target() {
        let t = Templates::new();
        let base = Context::load(Arc::new(MapEnv::default()), Path::new("/p"), None);
        let out = render_hooks(
            &t,
            &base,
            &["DELETE FROM {{ target }} WHERE id IN (SELECT id FROM {{ staging }})".into()],
            "stg_1_0_0",
            "lake.br.t",
        )
        .expect("render");
        assert_eq!(
            out,
            ["DELETE FROM lake.br.t WHERE id IN (SELECT id FROM stg_1_0_0)"]
        );
        assert!(render_hooks(&t, &base, &["{{ sync.x }}".into()], "s", "t").is_err());
    }
}
