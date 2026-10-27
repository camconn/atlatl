//! atlatl engine: the embedded DuckDB and the apply pipeline.

pub mod alloc;
pub mod batch;
pub mod budget;
pub mod engine;
pub mod error;
mod ffi;
pub mod json_canon;

pub use batch::{Batch, BatchStream, ByteBudget};
pub use budget::{Budget, BudgetParams, Detected};
pub use engine::{
    DUCKDB_VERSION, Engine, EngineOptions, Session, StmtOptions, StreamOptions, append_temp,
    check_version, resolve_extension_dir,
};
pub use error::DbResultExt;
