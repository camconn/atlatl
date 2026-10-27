//! Local SQLite files through rusqlite, linked against the system libsqlite3 (06 §6).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use atlatl_core::AtlatlError;
use atlatl_core::error::{ErrorClass, Result};
use rusqlite::limits::Limit;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, ErrorCode, params_from_iter};

use crate::conn::{Outcome, Stmt};
use crate::types::Value;

/// Oldest library with `ON CONFLICT … DO UPDATE` (3.24).
pub const MIN_VERSION: i32 = 3_024_000;

#[derive(Clone)]
pub struct Sqlite {
    c: Arc<Mutex<Connection>>,
    pub max_params: usize,
}

impl Sqlite {
    pub async fn open(path: PathBuf, busy_timeout: Duration) -> Result<Self> {
        blocking(move || {
            if rusqlite::version_number() < MIN_VERSION {
                return Err(AtlatlError::config(format!(
                    "the system SQLite is {}; atlatl needs 3.24 or later",
                    rusqlite::version()
                )));
            }
            let c = Connection::open(&path)
                .map_err(|e| classify(e).with_context(&format!("opening {}", path.display())))?;
            c.busy_timeout(busy_timeout).map_err(classify)?;
            c.execute_batch(
                "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
            )
            .map_err(classify)?;
            let max_params = c
                .limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER)
                .map_err(classify)?;
            Ok(Self {
                c: Arc::new(Mutex::new(c)),
                max_params: usize::try_from(max_params).unwrap_or(999).max(1),
            })
        })
        .await
    }

    /// Rolls back an open transaction without awaiting (an abandoned apply).
    pub fn abandon(&self) {
        let c = self.c.lock().unwrap_or_else(|e| e.into_inner());
        if !c.is_autocommit() {
            let _ = c.execute_batch("ROLLBACK");
        }
    }

    pub async fn run(&self, stmts: Vec<Stmt>) -> Result<Vec<Outcome>> {
        let c = self.c.clone();
        blocking(move || {
            let c = c.lock().unwrap_or_else(|e| e.into_inner());
            stmts.iter().map(|s| run_one(&c, s)).collect()
        })
        .await
    }
}

fn run_one(c: &Connection, s: &Stmt) -> Result<Outcome> {
    let mut st = c.prepare(&s.sql).map_err(classify)?;
    let params = params_from_iter(s.params.iter().map(to_sql));
    if st.column_count() == 0 {
        let changes = st.execute(params).map_err(classify)?;
        return Ok(Outcome {
            changes: changes as u64,
            rows: Vec::new(),
        });
    }
    let n = st.column_count();
    let mut rows = st.query(params).map_err(classify)?;
    let mut out = Vec::new();
    while let Some(r) = rows.next().map_err(classify)? {
        out.push(
            (0..n)
                .map(|i| r.get_ref(i).map(from_sql).map_err(classify))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    Ok(Outcome {
        changes: 0,
        rows: out,
    })
}

fn to_sql(v: &Value) -> SqlValue {
    match v {
        Value::Null => SqlValue::Null,
        Value::Integer(i) => SqlValue::Integer(*i),
        Value::Real(f) => SqlValue::Real(*f),
        Value::Text(s) => SqlValue::Text(s.clone()),
        Value::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

fn from_sql(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(f) => Value::Real(f),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| AtlatlError::internal(format!("sqlite worker: {e}")))?
}

trait Context {
    fn with_context(self, what: &str) -> Self;
}

impl Context for AtlatlError {
    fn with_context(mut self, what: &str) -> Self {
        self.message = format!("{what}: {}", self.message);
        self
    }
}

/// Maps rusqlite errors into the taxonomy (03 §5).
pub fn classify(e: rusqlite::Error) -> AtlatlError {
    let (class, code) = match &e {
        rusqlite::Error::SqliteFailure(f, _) => (
            match f.code {
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => ErrorClass::Conflict,
                ErrorCode::ConstraintViolation | ErrorCode::TypeMismatch | ErrorCode::TooBig => {
                    ErrorClass::Data
                }
                ErrorCode::CannotOpen
                | ErrorCode::ReadOnly
                | ErrorCode::NotADatabase
                | ErrorCode::Unknown => ErrorClass::Config,
                ErrorCode::PermissionDenied | ErrorCode::AuthorizationForStatementDenied => {
                    ErrorClass::Auth
                }
                ErrorCode::SystemIoFailure | ErrorCode::DiskFull => ErrorClass::Transient,
                _ => ErrorClass::Internal,
            },
            Some(format!("sqlite_{}", f.extended_code)),
        ),
        _ => (ErrorClass::Internal, None),
    };
    let mut err = AtlatlError::new(class, e.to_string()).source(e);
    err.code = code;
    err
}
