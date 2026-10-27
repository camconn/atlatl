//! A ClickHouse server on its HTTP port, or `ATLATL_TEST_CLICKHOUSE_URL` (+ `_USER`,
//! `_PASSWORD`). Each test should take its own database with [`ClickHouse::fresh_db`].

use std::sync::OnceLock;
use std::time::Duration;

use crate::container::Container;

pub const IMAGE: &str = "clickhouse/clickhouse-server:26.4";

#[derive(Debug)]
pub struct ClickHouse {
    /// `http://host:port`.
    pub url: String,
    pub user: String,
    pub password: String,
    container: Option<Container>,
}

impl ClickHouse {
    fn start() -> Result<Self, String> {
        if let Ok(url) = std::env::var("ATLATL_TEST_CLICKHOUSE_URL") {
            return Ok(Self {
                url,
                user: std::env::var("ATLATL_TEST_CLICKHOUSE_USER").unwrap_or("default".into()),
                password: std::env::var("ATLATL_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default(),
                container: None,
            });
        }
        let c = Container::start(
            IMAGE,
            8123,
            &[
                ("CLICKHOUSE_USER", "atlatl"),
                ("CLICKHOUSE_PASSWORD", "atlatl"),
                ("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1"),
            ],
            &["/var/lib/clickhouse"],
            &[],
        )?;
        let ch = Self {
            url: format!("http://{}:{}", c.host, c.port),
            user: "atlatl".into(),
            password: "atlatl".into(),
            container: None,
        };
        c.wait_for(IMAGE, Duration::from_secs(60), || {
            ch.exec("SELECT 1").is_ok()
        })?;
        Ok(Self {
            container: Some(c),
            ..ch
        })
    }

    /// Runs one statement through `clickhouse-client` in the container (or over HTTP with
    /// `curl` for an external server); returns its TSV output.
    pub fn exec(&self, sql: &str) -> Result<String, String> {
        match &self.container {
            Some(c) => c.exec(&[
                "clickhouse-client",
                "--user",
                &self.user,
                "--password",
                &self.password,
                "--query",
                sql,
            ]),
            None => {
                let out = std::process::Command::new("curl")
                    .args([
                        "-sS",
                        "--fail-with-body",
                        "-H",
                        &format!("X-ClickHouse-User: {}", self.user),
                        "-H",
                        &format!("X-ClickHouse-Key: {}", self.password),
                        "--data-binary",
                        sql,
                        &format!("{}/", self.url),
                    ])
                    .output()
                    .map_err(|e| format!("curl: {e}"))?;
                if !out.status.success() {
                    return Err(String::from_utf8_lossy(&out.stdout).into_owned());
                }
                Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
            }
        }
    }

    /// Creates a new empty database and returns its name.
    pub fn fresh_db(&self) -> Result<String, String> {
        let name = crate::unique("t");
        self.exec(&format!("CREATE DATABASE {name}"))?;
        Ok(name)
    }
}

static CLICKHOUSE: OnceLock<Result<ClickHouse, String>> = OnceLock::new();

/// Panics with the reason if it can't start.
pub fn clickhouse() -> &'static ClickHouse {
    CLICKHOUSE
        .get_or_init(ClickHouse::start)
        .as_ref()
        .unwrap_or_else(|e| panic!("clickhouse: {e}"))
}
