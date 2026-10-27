//! DuckLake catalogs for tests: a DuckDB-file catalog, or a Postgres catalog; local data path.

use std::path::{Path, PathBuf};

use duckdb::{Config, Connection};

use crate::container::Postgres;

pub const DUCKDB_VERSION: &str = "v1.5.6";

/// `ATLATL_EXTENSION_DIR`, else the repo's `extensions/` (see `scripts/fetch-duckdb-extensions.sh`).
pub fn extension_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("ATLATL_EXTENSION_DIR") {
        return PathBuf::from(d);
    }
    let mut dir = Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    loop {
        let cand = dir.join("extensions");
        if cand.join(DUCKDB_VERSION).is_dir() {
            return cand;
        }
        if !dir.pop() {
            panic!(
                "no extensions/{DUCKDB_VERSION} found; run scripts/fetch-duckdb-extensions.sh or set ATLATL_EXTENSION_DIR"
            );
        }
    }
}

/// An engine configured like atlatl's: offline extensions, UTC.
pub fn open_engine() -> duckdb::Result<Connection> {
    let dir = extension_dir();
    let c = Connection::open_in_memory_with_flags(
        Config::default()
            .with("extension_directory", dir.to_string_lossy())?
            .with("autoinstall_known_extensions", "false")?
            .with("autoload_known_extensions", "false")?
            .with("allow_unsigned_extensions", "false")?,
    )?;
    c.execute_batch("LOAD ducklake; LOAD postgres; SET TimeZone = 'UTC';")?;
    Ok(c)
}

enum Catalog {
    File(PathBuf),
    Postgres {
        dsn: String,
        host: String,
        port: u16,
        database: String,
        user: String,
        password: String,
    },
}

/// A DuckLake catalog with a local data path under a temp dir.
pub struct LocalLake {
    pub dir: tempfile::TempDir,
    catalog: Catalog,
}

impl LocalLake {
    /// A DuckDB-file catalog.
    pub fn file() -> std::io::Result<Self> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("data"))?;
        let meta = dir.path().join("meta.ducklake");
        Ok(Self {
            dir,
            catalog: Catalog::File(meta),
        })
    }

    /// A Postgres catalog in a fresh database on `pg`.
    pub fn on_postgres(pg: &Postgres) -> Result<Self, String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        std::fs::create_dir_all(dir.path().join("data")).map_err(|e| e.to_string())?;
        let database = pg.fresh_db()?;
        Ok(Self {
            dir,
            catalog: Catalog::Postgres {
                dsn: pg.dsn(&database),
                host: pg.host.clone(),
                port: pg.port,
                database,
                user: pg.user.clone(),
                password: pg.password.clone(),
            },
        })
    }

    /// The data path, with a trailing slash (DuckLake compares it byte for byte).
    pub fn data_path(&self) -> String {
        format!("{}/", self.dir.path().join("data").display())
    }

    pub fn attach_sql(&self, alias: &str) -> String {
        let target = match &self.catalog {
            Catalog::File(p) => p.display().to_string(),
            Catalog::Postgres { dsn, .. } => format!("postgres:{dsn}"),
        };
        format!(
            "ATTACH 'ducklake:{target}' AS {alias} (DATA_PATH '{}')",
            self.data_path()
        )
    }

    /// An engine connection with the lake attached as `alias`.
    pub fn open(&self, alias: &str) -> duckdb::Result<Connection> {
        let c = open_engine()?;
        c.execute_batch(&self.attach_sql(alias))?;
        Ok(c)
    }

    /// [`open`](Self::open) with extra ATTACH options, e.g. `DATA_INLINING_ROW_LIMIT 0`.
    pub fn open_with(&self, alias: &str, options: &str) -> duckdb::Result<Connection> {
        let c = open_engine()?;
        let sql = self.attach_sql(alias);
        let sql = sql.strip_suffix(')').unwrap_or(&sql);
        c.execute_batch(&format!("{sql}, {options})"))?;
        Ok(c)
    }

    /// A `connections:` entry for `atlatl.yaml` pointing at this lake.
    pub fn connection_yaml(&self, name: &str, alias: &str) -> String {
        self.yaml(name, alias, None)
    }

    /// [`connection_yaml`](Self::connection_yaml) with a Postgres catalog reached at `host:port`
    /// instead (e.g. through toxiproxy).
    pub fn connection_yaml_via(&self, name: &str, alias: &str, host: &str, port: u16) -> String {
        self.yaml(name, alias, Some((host, port)))
    }

    fn yaml(&self, name: &str, alias: &str, via: Option<(&str, u16)>) -> String {
        let catalog = match &self.catalog {
            Catalog::File(p) => format!("{{type: duckdb, path: \"{}\"}}", p.display()),
            Catalog::Postgres {
                host,
                port,
                database,
                user,
                password,
                ..
            } => {
                let (host, port) = via.unwrap_or((host.as_str(), *port));
                format!(
                    "{{type: postgres, host: {host}, port: {port}, database: {database}, user: {user}, password: \"{password}\", sslmode: disable}}"
                )
            }
        };
        format!(
            "  {name}:\n    type: ducklake\n    alias: {alias}\n    catalog: {catalog}\n    data_path: \"{}\"\n    storage: {{type: local}}\n",
            self.data_path()
        )
    }
}
