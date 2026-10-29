//! Postgres connector (spec 06 §3).

pub mod apply;
pub mod connect;
pub mod copy;
pub mod cursors;
pub mod error;
pub mod locks;
pub mod pool;
pub mod source;
pub mod tls;
pub mod types;

pub use apply::{
    ApplyCounts, ApplyOptions, ApplyTxn, DELETE_TS, Deletes, PgApplier, StreamTarget, check_target,
};
pub use connect::{ConnectOptions, Keepalive, Session, connect, connect_with};
pub use copy::{CopyEncoder, CopyWriter, column_oids, copyable};
pub use cursors::{
    Cursor, CursorMode, CursorWrite, advance_cursor, ensure_state_schema, manages_state_schema,
    read_cursor, read_cursors,
};
pub use error::{classify, classify_sqlstate, wrap};
pub use locks::PgAdvisoryLocks;
pub use pool::{Pool, PoolOptions, PoolStatus, PooledSession};
pub use source::PgSource;
