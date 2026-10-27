use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// A running container, removed when the test process exits.
#[derive(Debug)]
pub struct Container {
    pub id: String,
    pub host: String,
    pub port: u16,
}

fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("docker {}: {e}", args.join(" ")))?;
    if !out.status.success() {
        return Err(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

impl Container {
    /// `docker run -d` publishing `port` on a random loopback port. `tmpfs` paths keep container
    /// data in memory (faster, and no host disk).
    pub fn start(
        image: &str,
        port: u16,
        env: &[(&str, &str)],
        tmpfs: &[&str],
        cmd: &[&str],
    ) -> Result<Self, String> {
        let publish = format!("127.0.0.1::{port}");
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--rm".into(),
            "--label".into(),
            "atlatl-testkit=1".into(),
            "-p".into(),
            publish,
        ];
        for t in tmpfs {
            args.push("--tmpfs".into());
            args.push((*t).into());
        }
        for (k, v) in env {
            args.push("-e".into());
            args.push(format!("{k}={v}"));
        }
        args.push(image.into());
        args.extend(cmd.iter().map(|s| s.to_string()));
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let id = docker(&argv)?;
        reap_on_exit(&id);
        let mapped = docker(&["port", &id, &format!("{port}/tcp")])?;
        let hostport = mapped.lines().next().ok_or("no mapped port")?;
        let (host, p) = hostport
            .rsplit_once(':')
            .ok_or_else(|| format!("bad port mapping {hostport}"))?;
        Ok(Self {
            id,
            host: host.to_owned(),
            port: p.parse().map_err(|e| format!("{e}"))?,
        })
    }

    /// `docker run -d --network host`: the container shares the host's loopback, so it can reach
    /// other containers' published ports. `port` is whatever the command makes it listen on.
    pub fn start_host_network(image: &str, port: u16, cmd: &[&str]) -> Result<Self, String> {
        let mut args = vec![
            "run",
            "-d",
            "--rm",
            "--label",
            "atlatl-testkit=1",
            "--network",
            "host",
            image,
        ];
        args.extend_from_slice(cmd);
        let id = docker(&args)?;
        reap_on_exit(&id);
        Ok(Self {
            id,
            host: "127.0.0.1".into(),
            port,
        })
    }

    pub fn exec(&self, cmd: &[&str]) -> Result<String, String> {
        let mut args = vec!["exec", self.id.as_str()];
        args.extend_from_slice(cmd);
        docker(&args)
    }

    pub fn wait_for(
        &self,
        what: &str,
        timeout: Duration,
        mut ready: impl FnMut() -> bool,
    ) -> Result<(), String> {
        let t = Instant::now();
        while t.elapsed() < timeout {
            if ready() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(format!("{what} not ready after {timeout:?}"))
    }
}

/// Removes the container once this process is gone, even if it is killed.
fn reap_on_exit(id: &str) {
    let script = format!(
        "while kill -0 {} 2>/dev/null; do sleep 1; done; docker rm -f {id} >/dev/null 2>&1",
        std::process::id()
    );
    let _ = Command::new("sh")
        .args(["-c", &script])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

/// A Postgres server. Each test should take its own database with [`Postgres::fresh_db`].
#[derive(Debug)]
pub struct Postgres {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub version: u32,
    _container: Option<Container>,
}

impl Postgres {
    fn start(version: u32) -> Result<Self, String> {
        if let Ok(dsn) = std::env::var(format!("ATLATL_TEST_PG{version}_DSN")) {
            let cfg: tokio_postgres::Config = dsn.parse().map_err(|e| format!("{e}"))?;
            let host = match cfg.get_hosts().first() {
                Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
                _ => "127.0.0.1".into(),
            };
            return Ok(Self {
                host,
                port: cfg.get_ports().first().copied().unwrap_or(5432),
                user: cfg.get_user().unwrap_or("postgres").to_owned(),
                password: cfg
                    .get_password()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .unwrap_or_default(),
                version,
                _container: None,
            });
        }
        let image = format!("postgres:{version}-alpine");
        let c = Container::start(
            &image,
            5432,
            &[("POSTGRES_PASSWORD", "atlatl")],
            &["/var/lib/postgresql/data"],
            &[],
        )?;
        let pg = Self {
            host: c.host.clone(),
            port: c.port,
            user: "postgres".into(),
            password: "atlatl".into(),
            version,
            _container: None,
        };
        // pg_isready inside the container, then a real connection from outside.
        c.wait_for(&image, Duration::from_secs(60), || {
            c.exec(&["pg_isready", "-U", "postgres", "-h", "127.0.0.1"])
                .is_ok()
                && pg.psql_ok()
        })?;
        Ok(Self {
            _container: Some(c),
            ..pg
        })
    }

    fn psql_ok(&self) -> bool {
        let dsn = self.dsn("postgres");
        std::thread::spawn(move || {
            let rt = tokio_rt();
            rt.block_on(async {
                tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .is_ok()
            })
        })
        .join()
        .unwrap_or(false)
    }

    /// A libpq keyword DSN for `database`.
    pub fn dsn(&self, database: &str) -> String {
        format!(
            "host={} port={} user={} password={} dbname={database}",
            self.host, self.port, self.user, self.password
        )
    }

    /// The same server and credentials reached at another address (e.g. a toxiproxy listener).
    pub fn via(&self, host: &str, port: u16) -> Postgres {
        Postgres {
            host: host.to_owned(),
            port,
            user: self.user.clone(),
            password: self.password.clone(),
            version: self.version,
            _container: None,
        }
    }

    /// Creates a new empty database and returns its name.
    pub fn fresh_db(&self) -> Result<String, String> {
        let name = crate::unique("t");
        let dsn = self.dsn("postgres");
        let sql = format!("CREATE DATABASE {name}");
        std::thread::spawn(move || {
            tokio_rt().block_on(async {
                let (c, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| e.to_string())?;
                let h = tokio::spawn(conn);
                c.batch_execute(&sql).await.map_err(|e| e.to_string())?;
                drop(c);
                let _ = h.await;
                Ok::<_, String>(())
            })
        })
        .join()
        .map_err(|_| "panicked".to_owned())??;
        Ok(name)
    }
}

fn tokio_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

static PG12: OnceLock<Result<Postgres, String>> = OnceLock::new();
static PG16: OnceLock<Result<Postgres, String>> = OnceLock::new();

/// Postgres 12 (prod is Aurora PG 12). Panics with the reason if it can't start.
pub fn pg12() -> &'static Postgres {
    PG12.get_or_init(|| Postgres::start(12))
        .as_ref()
        .unwrap_or_else(|e| panic!("postgres 12: {e}"))
}

pub fn pg16() -> &'static Postgres {
    PG16.get_or_init(|| Postgres::start(16))
        .as_ref()
        .unwrap_or_else(|e| panic!("postgres 16: {e}"))
}

/// A dedicated Postgres server with extra files and settings (e.g. TLS), not shared like
/// [`pg12`] / [`pg16`] and never replaced by `ATLATL_TEST_PG*_DSN`.
#[derive(Clone, Debug)]
pub struct PostgresBuilder {
    version: u32,
    files: Vec<(String, String)>,
    settings: Vec<(String, String)>,
}

impl Postgres {
    pub fn builder(version: u32) -> PostgresBuilder {
        PostgresBuilder {
            version,
            files: Vec::new(),
            settings: Vec::new(),
        }
    }
}

impl PostgresBuilder {
    /// A text file written before the server starts, owned by `postgres` with mode 0600 (as a
    /// server key must be).
    pub fn file(mut self, path: &str, contents: impl Into<String>) -> Self {
        self.files.push((path.to_owned(), contents.into()));
        self
    }

    /// A server setting, passed as `-c name=value`.
    pub fn setting(mut self, name: &str, value: &str) -> Self {
        self.settings.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn start(self) -> Result<Postgres, String> {
        let q = |s: &str| format!("'{}'", s.replace('\'', r"'\''"));
        let mut script = String::from("set -e; ");
        let mut env: Vec<(String, String)> = vec![("POSTGRES_PASSWORD".into(), "atlatl".into())];
        for (i, (path, contents)) in self.files.iter().enumerate() {
            let var = format!("ATLATL_TESTKIT_FILE_{i}");
            let p = q(path);
            script.push_str(&format!(
                "mkdir -p \"$(dirname {p})\"; printf '%s' \"${var}\" > {p}; \
                 chown postgres {p}; chmod 600 {p}; "
            ));
            env.push((var, contents.clone()));
        }
        script.push_str("exec docker-entrypoint.sh postgres");
        for (k, v) in &self.settings {
            script.push_str(&format!(" -c {}", q(&format!("{k}={v}"))));
        }
        let image = format!("postgres:{}-alpine", self.version);
        let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let c = Container::start(
            &image,
            5432,
            &env,
            &["/var/lib/postgresql/data"],
            &["sh", "-c", &script],
        )?;
        let pg = Postgres {
            host: c.host.clone(),
            port: c.port,
            user: "postgres".into(),
            password: "atlatl".into(),
            version: self.version,
            _container: None,
        };
        c.wait_for(&image, Duration::from_secs(60), || {
            c.exec(&["pg_isready", "-U", "postgres", "-h", "127.0.0.1"])
                .is_ok()
                && pg.psql_ok()
        })?;
        Ok(Postgres {
            _container: Some(c),
            ..pg
        })
    }
}
