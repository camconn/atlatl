//! Integration tests against Postgres 12 and 16 containers.

use atlatl_conn_postgres::ConnectOptions;
use atlatl_testkit::Postgres;

/// For each `async fn name(pg: &'static Postgres)`, a test per server: `name::pg12`, `name::pg16`.
macro_rules! on_both {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg12() {
                super::$name(atlatl_testkit::pg12()).await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pg16() {
                super::$name(atlatl_testkit::pg16()).await
            }
        }
    )*};
}

mod apply;
mod copy;
mod cursors;
mod errors;
mod locks;
mod pool;
mod source;
mod tls;

pub fn opts(pg: &Postgres, db: &str) -> ConnectOptions {
    ConnectOptions::new(&pg.host, pg.port, db, &pg.user, Some(pg.password.clone()))
}

/// Options for a new, empty database (advisory locks are per database).
pub fn fresh(pg: &Postgres) -> ConnectOptions {
    let db = pg.fresh_db().expect("fresh db");
    opts(pg, &db)
}
