//! Turso / libSQL over Hrana (HTTP) with the `libsql` crate in remote mode (06 §8).

use std::time::Duration;

use atlatl_core::AtlatlError;
use atlatl_core::error::{ErrorClass, Result};
use libsql::params::Params;

use crate::conn::{Outcome, Stmt};
use crate::https::HttpsConnector;
use crate::types::Value;

pub struct Turso {
    conn: libsql::Connection,
    tx: Option<libsql::Transaction>,
    timeout: Duration,
    _db: libsql::Database,
}

impl Turso {
    pub async fn open(url: &str, token: &str, timeout: Duration) -> Result<Self> {
        let https = HttpsConnector::new()
            .map_err(|e| AtlatlError::internal(format!("turso: rustls: {e}")))?;
        let db = libsql::Builder::new_remote(url.to_owned(), token.to_owned())
            .connector(https)
            .build()
            .await
            .map_err(classify)?;
        let conn = db.connect().map_err(classify)?;
        Ok(Self {
            conn,
            tx: None,
            timeout,
            _db: db,
        })
    }

    pub async fn begin(&mut self) -> Result<()> {
        if self.tx.is_some() {
            return Err(AtlatlError::internal(
                "turso: a transaction is already open",
            ));
        }
        let tx = self
            .timed(
                self.conn
                    .transaction_with_behavior(libsql::TransactionBehavior::Immediate),
            )
            .await?;
        self.tx = Some(tx);
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        match self.tx.take() {
            Some(tx) => self.timed(tx.commit()).await,
            None => Ok(()),
        }
    }

    pub async fn rollback(&mut self) -> Result<()> {
        match self.tx.take() {
            Some(tx) => self.timed(tx.rollback()).await,
            None => Ok(()),
        }
    }

    /// Drops an open transaction; the server rolls it back when its stream closes.
    pub fn abandon(&mut self) {
        self.tx = None;
    }

    pub async fn run(&mut self, stmts: Vec<Stmt>) -> Result<Vec<Outcome>> {
        let mut out = Vec::with_capacity(stmts.len());
        for s in stmts {
            let c: &libsql::Connection = match &self.tx {
                Some(tx) => tx,
                None => &self.conn,
            };
            let params = Params::Positional(s.params.iter().map(to_libsql).collect());
            if !s.query {
                let changes = tokio::time::timeout(self.timeout, c.execute(&s.sql, params))
                    .await
                    .map_err(|_| timeout())?
                    .map_err(classify)?;
                out.push(Outcome {
                    changes,
                    rows: Vec::new(),
                });
                continue;
            }
            let fetch = async {
                let mut rows = c.query(&s.sql, params).await?;
                let n = rows.column_count();
                let mut v = Vec::new();
                while let Some(r) = rows.next().await? {
                    v.push(
                        (0..n)
                            .map(|i| r.get_value(i).map(from_libsql))
                            .collect::<libsql::Result<Vec<_>>>()?,
                    );
                }
                Ok::<_, libsql::Error>(v)
            };
            let rows = tokio::time::timeout(self.timeout, fetch)
                .await
                .map_err(|_| timeout())?
                .map_err(classify)?;
            out.push(Outcome { changes: 0, rows });
        }
        Ok(out)
    }

    async fn timed<T>(&self, f: impl Future<Output = libsql::Result<T>>) -> Result<T> {
        tokio::time::timeout(self.timeout, f)
            .await
            .map_err(|_| timeout())?
            .map_err(classify)
    }
}

fn timeout() -> AtlatlError {
    AtlatlError::new(ErrorClass::Timeout, "turso: request timed out")
}

fn to_libsql(v: &Value) -> libsql::Value {
    match v {
        Value::Null => libsql::Value::Null,
        Value::Integer(i) => libsql::Value::Integer(*i),
        Value::Real(f) => libsql::Value::Real(*f),
        Value::Text(s) => libsql::Value::Text(s.clone()),
        Value::Blob(b) => libsql::Value::Blob(b.clone()),
    }
}

fn from_libsql(v: libsql::Value) -> Value {
    match v {
        libsql::Value::Null => Value::Null,
        libsql::Value::Integer(i) => Value::Integer(i),
        libsql::Value::Real(f) => Value::Real(f),
        libsql::Value::Text(s) => Value::Text(s),
        libsql::Value::Blob(b) => Value::Blob(b),
    }
}

/// libsql reports HTTP failures as `status=<n>, body=…` and SQLite failures with their
/// `SQLITE_*` code in the text.
pub fn classify(e: libsql::Error) -> AtlatlError {
    let msg = e.to_string();
    let status = msg
        .split("status=")
        .nth(1)
        .and_then(|s| s.get(..3))
        .and_then(|s| s.parse::<u16>().ok());
    let code = msg.split_once("SQLITE_").map(|(_, rest)| {
        let c: String = rest
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || *c == '_')
            .collect();
        format!("SQLITE_{c}")
    });
    let class = match (&e, status, code.as_deref()) {
        (_, Some(s), _) => {
            atlatl_core::error::classify_http_status(s).unwrap_or(ErrorClass::Internal)
        }
        (_, _, Some(c)) if c.starts_with("SQLITE_BUSY") || c.starts_with("SQLITE_LOCKED") => {
            ErrorClass::Conflict
        }
        (_, _, Some(c))
            if c.starts_with("SQLITE_CONSTRAINT")
                || c.starts_with("SQLITE_MISMATCH")
                || c.starts_with("SQLITE_TOOBIG") =>
        {
            ErrorClass::Data
        }
        (_, _, Some(c)) if c.starts_with("SQLITE_AUTH") || c.starts_with("SQLITE_PERM") => {
            ErrorClass::Auth
        }
        (_, _, Some(_)) => ErrorClass::Config,
        (libsql::Error::ConnectionFailed(_), _, _) => ErrorClass::Transient,
        (libsql::Error::Sqlite3SyntaxError(..) | libsql::Error::Sqlite3ParserError(_), _, _) => {
            ErrorClass::Config
        }
        (libsql::Error::Hrana(_), _, _)
            if msg.contains("error trying to connect")
                || msg.contains("connection")
                || msg.contains("stream closed") =>
        {
            ErrorClass::Transient
        }
        (libsql::Error::Hrana(_), _, _) if msg.contains("SQL_PARSE_ERROR") => ErrorClass::Config,
        _ => ErrorClass::Internal,
    };
    let mut err = AtlatlError::new(class, format!("turso: {msg}")).source(e);
    err.code = code.map(|c| c.to_ascii_lowercase());
    err
}
