//! The three SQLite-family backends behind one statement interface.

use std::path::PathBuf;
use std::time::Duration;

use atlatl_core::AtlatlError;
use atlatl_core::config::connection::Connection;
use atlatl_core::config::model::ConnectionKind;
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::Result;
use atlatl_core::retry::RetryPolicy;
use futures::{StreamExt, TryStreamExt};

use crate::d1::{self, D1, D1Options};
use crate::sqlite::Sqlite;
use crate::turso::Turso;
use crate::types::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct Stmt {
    pub sql: String,
    pub params: Vec<Value>,
    /// Returns rows (else only a change count).
    pub query: bool,
}

impl Stmt {
    pub fn exec(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
            query: false,
        }
    }

    pub fn query(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
            query: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Outcome {
    pub changes: u64,
    pub rows: Vec<Vec<Value>>,
}

/// Per-statement sizing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub params: usize,
    pub rows: usize,
    pub row_bytes: Option<usize>,
}

/// How an apply maps onto the target's transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Txns {
    /// One transaction for the whole apply, cursor included.
    Single,
    /// Interactive transactions of at most this many rows; one when the window fits.
    Split(u64),
    /// No transactions across requests (D1).
    None,
}

pub enum Conn {
    Sqlite(Sqlite),
    D1(D1),
    Turso(Box<Turso>),
}

pub(crate) const DEFAULT_ROWS: usize = 500;
const TURSO_PARAMS: usize = 32_766;
const DEFAULT_TRANSACTION_ROWS: u64 = 10_000;

fn secret(v: Option<&SecretValue>, what: &str) -> Result<Option<String>> {
    match v {
        None => Ok(None),
        Some(SecretValue::Literal(s)) => Ok(Some(s.clone())),
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(format!(
            "{what} secret was not resolved before connecting"
        ))),
    }
}

impl Conn {
    pub async fn open(c: &Connection) -> Result<(Self, Txns, bool)> {
        let statement = c
            .common
            .timeouts
            .as_ref()
            .and_then(|t| t.statement)
            .map(|d| d.0);
        let policy = match &c.common.retry {
            Some(r) => RetryPolicy::from_config(r).map_err(AtlatlError::config)?,
            None => RetryPolicy::default(),
        };
        Ok(match &c.kind {
            ConnectionKind::Sqlite(s) => (
                Conn::Sqlite(
                    Sqlite::open(
                        PathBuf::from(&s.path),
                        s.busy_timeout.map_or(Duration::from_secs(5), |d| d.0),
                    )
                    .await?,
                ),
                Txns::Single,
                s.create_tables.unwrap_or(true),
            ),
            ConnectionKind::D1(d) => (
                Conn::D1(
                    D1::open(D1Options {
                        endpoint: d
                            .endpoint
                            .clone()
                            .unwrap_or_else(|| d1::DEFAULT_ENDPOINT.to_owned()),
                        account_id: d.account_id.clone(),
                        database_id: d.database_id.clone(),
                        database: d.database.clone(),
                        token: secret(Some(&d.api_token), "api_token")?.unwrap_or_default(),
                        statements_per_request: d.statements_per_request.unwrap_or(1) as usize,
                        in_flight: c.common.max_concurrency.unwrap_or(4) as usize,
                        timeout: statement.unwrap_or(Duration::from_secs(60)),
                        policy,
                    })
                    .await?,
                ),
                Txns::None,
                d.create_tables.unwrap_or(true),
            ),
            ConnectionKind::Turso(t) => (
                Conn::Turso(Box::new(
                    Turso::open(
                        &t.url,
                        &secret(t.auth_token.as_ref(), "auth_token")?.unwrap_or_default(),
                        statement.unwrap_or(Duration::from_secs(60)),
                    )
                    .await?,
                )),
                Txns::Split(
                    t.transaction_rows
                        .unwrap_or(DEFAULT_TRANSACTION_ROWS)
                        .max(1),
                ),
                t.create_tables.unwrap_or(true),
            ),
            other => {
                return Err(AtlatlError::config(format!(
                    "`{}` is a {} connection, not sqlite, d1 or turso",
                    c.name,
                    other.type_name()
                )));
            }
        })
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Conn::Sqlite(_) => "sqlite",
            Conn::D1(_) => "d1",
            Conn::Turso(_) => "turso",
        }
    }

    pub fn limits(&self) -> Limits {
        match self {
            Conn::Sqlite(s) => Limits {
                params: s.max_params,
                rows: DEFAULT_ROWS,
                row_bytes: None,
            },
            Conn::D1(_) => Limits {
                params: d1::MAX_PARAMS,
                rows: DEFAULT_ROWS,
                row_bytes: Some(d1::MAX_ROW_BYTES),
            },
            Conn::Turso(_) => Limits {
                params: TURSO_PARAMS,
                rows: DEFAULT_ROWS,
                row_bytes: None,
            },
        }
    }

    pub async fn run(&mut self, stmts: Vec<Stmt>) -> Result<Vec<Outcome>> {
        match self {
            Conn::Sqlite(s) => s.run(stmts).await,
            Conn::D1(d) => d.run(stmts).await,
            Conn::Turso(t) => t.run(stmts).await,
        }
    }

    /// Independent statement groups; D1 sends up to `max_concurrency` at once, the others run
    /// them in order.
    pub async fn run_groups(&mut self, groups: Vec<Vec<Stmt>>) -> Result<Vec<Vec<Outcome>>> {
        match self {
            Conn::D1(d) => {
                let n = d.in_flight;
                let d = &*d;
                futures::stream::iter(groups)
                    .map(|g| d.run(g))
                    .buffered(n)
                    .try_collect()
                    .await
            }
            _ => {
                let mut out = Vec::with_capacity(groups.len());
                for g in groups {
                    out.push(self.run(g).await?);
                }
                Ok(out)
            }
        }
    }

    pub async fn begin(&mut self) -> Result<()> {
        match self {
            Conn::Sqlite(_) => self
                .run(vec![Stmt::exec("BEGIN IMMEDIATE", vec![])])
                .await
                .map(drop),
            Conn::D1(_) => Ok(()),
            Conn::Turso(t) => t.begin().await,
        }
    }

    pub async fn commit(&mut self) -> Result<()> {
        match self {
            Conn::Sqlite(_) => self.run(vec![Stmt::exec("COMMIT", vec![])]).await.map(drop),
            Conn::D1(_) => Ok(()),
            Conn::Turso(t) => t.commit().await,
        }
    }

    pub fn abandon(&mut self) {
        match self {
            Conn::Sqlite(s) => s.abandon(),
            Conn::D1(_) => {}
            Conn::Turso(t) => t.abandon(),
        }
    }

    pub async fn rollback(&mut self) -> Result<()> {
        match self {
            Conn::Sqlite(_) => self
                .run(vec![Stmt::exec("ROLLBACK", vec![])])
                .await
                .map(drop),
            Conn::D1(_) => Ok(()),
            Conn::Turso(t) => t.rollback().await,
        }
    }
}
