//! From project config to a [`ReplicationSpec`] and the slot guard.

use atlatl_conn_postgres::ConnectOptions;
use atlatl_core::AtlatlError;
use atlatl_core::config::connection::Connection;
use atlatl_core::config::load::Replication;
use atlatl_core::config::model::{ConnectionKind, DeleteMode, Mode};
use atlatl_core::error::Result;
use atlatl_core::plan::state_config_hash;

use crate::slot::SlotPolicy;
use crate::source::{Limits, ReplicationSpec, StreamSpec};

pub struct Resolved {
    pub opts: ConnectOptions,
    pub policy: SlotPolicy,
    pub spec: ReplicationSpec,
}

fn split(s: &str, default_schema: &str) -> (String, String) {
    match s.split_once('.') {
        Some((a, b)) => (a.to_owned(), b.to_owned()),
        None => (default_schema.to_owned(), s.to_owned()),
    }
}

/// The enabled `mode: cdc` streams of `r`, reading from postgres `source` (resolved for
/// `active_env`). Fails with `config` when the guard refuses the slot.
pub fn resolve(
    r: &Replication,
    source: &Connection,
    active_env: Option<&str>,
    base_env: Option<&str>,
) -> Result<Resolved> {
    let ConnectionKind::Postgres(pg) = &source.kind else {
        return Err(AtlatlError::config(format!(
            "replication `{}`: source `{}` is {}, not postgres",
            r.name.value,
            source.name,
            source.type_name()
        )));
    };
    let policy = SlotPolicy::resolve(&source.name, pg.changes.as_ref(), active_env, base_env)?;
    let mut limits = Limits::default();
    let mut streams = Vec::new();
    for s in r.streams.iter().filter(|s| s.enabled()) {
        let c = &s.config;
        if c.mode.as_ref().map(|m| m.value) != Some(Mode::Cdc) {
            continue;
        }
        let from = c
            .from
            .as_ref()
            .and_then(|f| f.value.table.clone())
            .ok_or_else(|| {
                AtlatlError::config(format!("stream `{}` needs `from.table`", s.id.value))
            })?;
        let object = c.object.as_ref().map(|o| o.value.clone()).ok_or_else(|| {
            AtlatlError::config(format!("stream `{}` has no `object`", s.id.value))
        })?;
        if object.contains("{{") {
            return Err(AtlatlError::config(format!(
                "stream `{}`: `object` must be a literal table for postgres `changes` streams",
                s.id.value
            )));
        }
        let (source_schema, source_table) = split(&from, "public");
        let (target_schema, target_table) = split(&object, "main");
        if let Some(n) = c.max_changes {
            limits.max_changes = Some(limits.max_changes.map_or(n, |m| m.min(n)));
        }
        if let Some(d) = c.max_duration {
            limits.max_duration = Some(limits.max_duration.map_or(d.0, |m| m.min(d.0)));
        }
        streams.push(StreamSpec {
            id: s.id.value.clone(),
            source_schema,
            source_table,
            target_schema,
            target_table,
            primary_key: c.primary_key.as_ref().map(|p| p.value.clone()),
            deletes: c
                .deletes
                .as_ref()
                .and_then(|d| d.value.mode)
                .unwrap_or(DeleteMode::Hard),
            history: c.history.unwrap_or(false),
            columns: c.columns.clone(),
            config_hash: state_config_hash(s),
        });
    }
    Ok(Resolved {
        opts: ConnectOptions::from_connection(source)?,
        policy,
        spec: ReplicationSpec {
            name: r.name.value.clone(),
            streams,
            limits,
        },
    })
}
