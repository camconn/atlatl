//! Owned slots and publications (06 §9): one slot and one publication per replication, named
//! `atlatl_<replication>`; refusing production slots from other environments; lag stats;
//! decommission.

use atlatl_conn_postgres::{ConnectOptions, Session, connect, wrap};
use atlatl_core::config::model::{PgChanges, PgProtocol};
use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use serde::Serialize;

use crate::lsn::Lsn;

/// What this run may do with its slot. Only obtainable through [`SlotPolicy::resolve`], so a
/// slot is never touched without the production guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotPolicy {
    allow_create: bool,
    temporary: bool,
    protocol: PgProtocol,
}

impl SlotPolicy {
    /// The guard for connection `conn` in `active_env`, where `base_env` is
    /// `environments.base`. A connection marked `changes.production: true` may only own slots
    /// from the base environment (or in a project without environments).
    pub fn resolve(
        conn: &str,
        changes: Option<&PgChanges>,
        active_env: Option<&str>,
        base_env: Option<&str>,
    ) -> Result<Self> {
        let Some(ch) = changes else {
            return Err(AtlatlError::config(format!(
                "connection `{conn}` has no `changes:` block; logical replication must be enabled on the connection"
            ))
            .code("changes_disabled"));
        };
        if ch.production == Some(true)
            && let Some(env) = active_env
            && Some(env) != base_env
        {
            let base = base_env
                .map(|b| format!("`{b}`"))
                .unwrap_or_else(|| "unset".into());
            return Err(AtlatlError::config(format!(
                "refusing to create or use a replication slot: connection `{conn}` points at a production server (`changes.production: true`) and environment `{env}` is not the base environment ({base}). A slot left behind by a non-production run retains WAL without limit on PG 12. Give `{conn}` an `environments.{env}` override for a non-production server with `changes: {{production: false}}`, or run in the base environment"
            ))
            .code("slot_refused"));
        }
        let slots = ch.slots.clone().unwrap_or_default();
        Ok(Self {
            allow_create: slots.allow_create.unwrap_or(false),
            temporary: slots.temporary.unwrap_or(false),
            protocol: ch.protocol.unwrap_or_default(),
        })
    }

    pub fn allow_create(&self) -> bool {
        self.allow_create
    }

    pub fn temporary(&self) -> bool {
        self.temporary
    }

    pub fn protocol(&self) -> PgProtocol {
        self.protocol
    }
}

pub fn slot_name(replication: &str) -> String {
    PgChanges::slot_name(replication)
}

/// Slot lag for the report (M6.3). Byte counts are relative to `pg_current_wal_lsn()`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SlotStats {
    pub slot: String,
    pub active: bool,
    pub temporary: bool,
    pub current_wal_lsn: Lsn,
    pub confirmed_flush_lsn: Option<Lsn>,
    pub restart_lsn: Option<Lsn>,
    /// WAL written since the last confirmed flush: how far the lake is behind.
    pub lag_bytes: Option<u64>,
    /// WAL the slot keeps on the server (from `restart_lsn`).
    pub retained_bytes: Option<u64>,
    /// `age(catalog_xmin)`: how far the slot holds back catalog vacuum.
    pub catalog_xmin_age: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SlotRow {
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub slot_type: String,
    pub active: bool,
    pub active_pid: Option<i32>,
    pub confirmed_flush: Option<Lsn>,
}

fn lsn_opt(s: Option<String>) -> Result<Option<Lsn>> {
    s.map(|v| v.parse()).transpose()
}

pub(crate) async fn slot_row(sql: &Session, slot: &str) -> Result<Option<SlotRow>> {
    let rows = sql
        .client()
        .query(
            "SELECT plugin::text, database::text, slot_type::text, active, active_pid,
                    confirmed_flush_lsn::text
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(wrap)?;
    rows.first()
        .map(|r| {
            Ok(SlotRow {
                plugin: r.get(0),
                database: r.get(1),
                slot_type: r.get(2),
                active: r.get(3),
                active_pid: r.get(4),
                confirmed_flush: lsn_opt(r.get(5))?,
            })
        })
        .transpose()
}

pub(crate) async fn current_wal_lsn(sql: &Session) -> Result<Lsn> {
    let r = sql
        .client()
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(wrap)?;
    r.get::<_, String>(0).parse()
}

pub(crate) async fn stats(sql: &Session, slot: &str) -> Result<Option<SlotStats>> {
    let rows = sql
        .client()
        .query(
            "SELECT active, temporary, pg_current_wal_lsn()::text, confirmed_flush_lsn::text,
                    restart_lsn::text,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint,
                    age(catalog_xmin)::bigint
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(wrap)?;
    let Some(r) = rows.first() else {
        return Ok(None);
    };
    let bytes = |v: Option<i64>| v.map(|b| b.max(0) as u64);
    Ok(Some(SlotStats {
        slot: slot.to_owned(),
        active: r.get(0),
        temporary: r.get(1),
        current_wal_lsn: r.get::<_, String>(2).parse()?,
        confirmed_flush_lsn: lsn_opt(r.get(3))?,
        restart_lsn: lsn_opt(r.get(4))?,
        lag_bytes: bytes(r.get(5)),
        retained_bytes: bytes(r.get(6)),
        catalog_xmin_age: r.get(7),
    }))
}

/// Slot lag for replication `replication` on the server behind `opts`.
pub async fn slot_stats(opts: &ConnectOptions, replication: &str) -> Result<Option<SlotStats>> {
    let sql = connect(opts).await?;
    stats(&sql, &slot_name(replication)).await
}

pub(crate) fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub(crate) async fn publication_tables(
    sql: &Session,
    publication: &str,
) -> Result<Option<Vec<(String, String)>>> {
    let exists = !sql
        .client()
        .query(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(wrap)?
        .is_empty();
    if !exists {
        return Ok(None);
    }
    let rows = sql
        .client()
        .query(
            "SELECT schemaname::text, tablename::text FROM pg_publication_tables
             WHERE pubname = $1 ORDER BY 1, 2",
            &[&publication],
        )
        .await
        .map_err(wrap)?;
    Ok(Some(rows.iter().map(|r| (r.get(0), r.get(1))).collect()))
}

fn table_list(tables: &[(String, String)]) -> String {
    tables
        .iter()
        .map(|(s, t)| format!("{}.{}", quote_ident(s), quote_ident(t)))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) async fn create_publication(
    sql: &Session,
    publication: &str,
    tables: &[(String, String)],
) -> Result<()> {
    sql.client()
        .batch_execute(&format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            quote_ident(publication),
            table_list(tables)
        ))
        .await
        .map_err(wrap)
}

pub(crate) async fn set_publication_tables(
    sql: &Session,
    publication: &str,
    tables: &[(String, String)],
) -> Result<()> {
    sql.client()
        .batch_execute(&format!(
            "ALTER PUBLICATION {} SET TABLE {}",
            quote_ident(publication),
            table_list(tables)
        ))
        .await
        .map_err(wrap)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Decommissioned {
    pub slot: String,
    pub slot_dropped: bool,
    pub publication_dropped: bool,
}

/// Drops the replication's slot and publication (`atlatl decommission`). The lake keeps its
/// data and cursors; a later run fails as `data` (`slot_missing`) until `atlatl state reset`.
/// `force` terminates a walsender still holding the slot.
pub async fn decommission(
    opts: &ConnectOptions,
    _policy: &SlotPolicy,
    replication: &str,
    force: bool,
) -> Result<Decommissioned> {
    let slot = slot_name(replication);
    let sql = connect(opts).await?;
    let mut out = Decommissioned {
        slot: slot.clone(),
        ..Default::default()
    };
    if let Some(row) = slot_row(&sql, &slot).await? {
        if row.active {
            let Some(pid) = row.active_pid.filter(|_| force) else {
                return Err(AtlatlError::conflict(format!(
                    "slot `{slot}` is in use by pid {:?}; stop that run or pass force",
                    row.active_pid
                ))
                .code("slot_active"));
            };
            sql.client()
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .map_err(wrap)?;
        }
        let t = std::time::Instant::now();
        loop {
            match sql
                .client()
                .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    let e = wrap(e);
                    let busy =
                        e.class == ErrorClass::Conflict || e.code.as_deref() == Some("55006");
                    if !busy || t.elapsed() > std::time::Duration::from_secs(10) {
                        return Err(e);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
        out.slot_dropped = true;
    }
    if publication_tables(&sql, &slot).await?.is_some() {
        sql.client()
            .batch_execute(&format!("DROP PUBLICATION {}", quote_ident(&slot)))
            .await
            .map_err(wrap)?;
        out.publication_dropped = true;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlatl_core::config::model::SlotsConfig;

    fn changes(production: bool, allow_create: bool) -> PgChanges {
        PgChanges {
            production: Some(production),
            slots: Some(SlotsConfig {
                allow_create: Some(allow_create),
                temporary: None,
            }),
            protocol: None,
        }
    }

    #[test]
    fn production_slots_are_refused_outside_the_base_env() {
        let prod = changes(true, true);
        let e = SlotPolicy::resolve("admin_pg", Some(&prod), Some("dev"), Some("prod"))
            .expect_err("refused");
        assert_eq!(e.class, ErrorClass::Config);
        assert_eq!(e.code.as_deref(), Some("slot_refused"));
        assert!(e.message.contains("environment `dev`"), "{e}");
        let e =
            SlotPolicy::resolve("admin_pg", Some(&prod), Some("dev"), None).expect_err("no base");
        assert!(e.message.contains("(unset)"), "{e}");

        let p =
            SlotPolicy::resolve("admin_pg", Some(&prod), Some("prod"), Some("prod")).expect("prod");
        assert!(p.allow_create() && !p.temporary());
        assert!(SlotPolicy::resolve("admin_pg", Some(&prod), None, None).is_ok());
        let dev = changes(false, false);
        let p =
            SlotPolicy::resolve("admin_pg", Some(&dev), Some("dev"), Some("prod")).expect("dev");
        assert!(!p.allow_create());

        let e = SlotPolicy::resolve("admin_pg", None, None, None).expect_err("no changes");
        assert_eq!(e.code.as_deref(), Some("changes_disabled"));
    }
}
