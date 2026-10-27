//! Test fixtures: Postgres 12/16, S3 (RustFS), SFTP, ClickHouse and toxiproxy containers, DuckLake
//! catalogs, temp projects.
//!
//! Containers are started with the `docker` CLI, shared per test binary, and removed by a reaper
//! process when the test process exits. Point tests at existing services instead with
//! `ATLATL_TEST_PG12_DSN`, `ATLATL_TEST_PG16_DSN`, `ATLATL_TEST_S3_ENDPOINT` (+ `_KEY`, `_SECRET`)
//! and `ATLATL_TEST_CLICKHOUSE_URL` (+ `_USER`, `_PASSWORD`).

pub mod clickhouse;
pub mod container;
pub mod d1_fake;
pub mod lake;
pub mod libsql;
pub mod project;
pub mod redshift_fake;
pub mod s3;
pub mod sftp;
pub mod toxiproxy;

pub use clickhouse::{ClickHouse, clickhouse};
pub use container::{Postgres, PostgresBuilder, pg12, pg16};
pub use lake::{LocalLake, extension_dir};
pub use libsql::{Libsql, libsql};
pub use project::TempProject;
pub use s3::{S3, rustfs};
pub use sftp::{Sftp, sftp};
pub use toxiproxy::{Proxy, Toxiproxy, toxiproxy};

/// A short random suffix for database, bucket and table names.
pub fn unique(prefix: &str) -> String {
    use rand::RngExt;
    let n: u64 = rand::rng().random();
    format!("{prefix}_{:012x}", n & 0xffff_ffff_ffff)
}
