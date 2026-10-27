//! Configuration: YAML → spanned nodes → deep merge → typed model (spec 02).

pub mod apis;
pub mod connection;
pub mod dbt;
pub mod de;
pub mod diag;
pub mod env;
pub mod graph;
pub mod load;
pub mod model;
pub mod node;
pub mod types;

pub use diag::{ConfigError, Diagnostic, Diagnostics, Severity, SourceMap};
pub use load::{Project, Replication, Stream, load};
