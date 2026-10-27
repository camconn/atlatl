//! SQLite-family serving targets: SQLite, D1 and Turso (spec 06 §6–8, 08 §6.2–6.3).

pub mod apply;
pub mod conn;
pub mod cursors;
pub mod d1;
pub mod https;
pub mod sqlite;
pub mod turso;
pub mod types;

pub use apply::{ApplyCounts, ApplyTxn, Client, DELETE_TS, DeleteKey, Deletes, TableInfo, Target};
pub use cursors::{Cursor, CursorMode, CursorPlan, CursorWrite, Pending, fingerprint};
pub use types::{Store, Value};
