//! Connection options and sessions (spec 06 §3.1).

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use atlatl_core::config::connection::Connection;
use atlatl_core::config::model::{
    CatalogConfig, CatalogType, ConnectionKind, Keepalives, PostgresConn, SslMode,
};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::Result;
use atlatl_core::{AtlatlError, ErrorClass};
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

use crate::error::{classify, wrap};
use crate::tls::{MakeRustls, client_config};

pub const DEFAULT_PORT: u16 = 5432;
pub const DEFAULT_APPLICATION_NAME: &str = "atlatl";
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keepalive {
    pub idle: Duration,
    pub interval: Duration,
    pub count: u32,
}

impl Default for Keepalive {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(30),
            interval: Duration::from_secs(10),
            count: 3,
        }
    }
}

impl Keepalive {
    pub fn from_config(k: Option<&Keepalives>) -> Self {
        let d = Self::default();
        let Some(k) = k else { return d };
        Self {
            idle: k.idle.map_or(d.idle, |v| v.0),
            interval: k.interval.map_or(d.interval, |v| v.0),
            count: k.count.unwrap_or(d.count),
        }
    }
}

/// Resolved connection parameters. `Debug` hides the password.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectOptions {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    pub sslmode: SslMode,
    /// PEM CA bundle for `verify-full` (the RDS bundle); the OS trust store when unset.
    pub sslrootcert: Option<PathBuf>,
    pub application_name: String,
    pub statement_timeout: Option<Duration>,
    /// Bounds TCP connect, TLS and startup together.
    pub connect_timeout: Option<Duration>,
    /// `None` turns TCP keepalives off.
    pub keepalive: Option<Keepalive>,
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "****"))
            .field("sslmode", &self.sslmode)
            .field("sslrootcert", &self.sslrootcert)
            .field("application_name", &self.application_name)
            .field("statement_timeout", &self.statement_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("keepalive", &self.keepalive)
            .finish()
    }
}

impl ConnectOptions {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        user: impl Into<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            database: database.into(),
            user: user.into(),
            password,
            sslmode: SslMode::Disable,
            sslrootcert: None,
            application_name: DEFAULT_APPLICATION_NAME.into(),
            statement_timeout: None,
            connect_timeout: Some(DEFAULT_CONNECT_TIMEOUT),
            keepalive: Some(Keepalive::default()),
        }
    }

    pub fn from_postgres(p: &PostgresConn) -> Result<Self> {
        let mut o = Self::new(
            &p.host,
            p.port.unwrap_or(DEFAULT_PORT),
            &p.database,
            &p.user,
            resolved(p.password.as_ref())?,
        );
        o.sslmode = p.sslmode.unwrap_or(SslMode::Require);
        o.sslrootcert = p.sslrootcert.as_ref().map(PathBuf::from);
        if let Some(a) = &p.application_name {
            o.application_name.clone_from(a);
        }
        o.statement_timeout = p.statement_timeout.map(|d| d.0);
        o.keepalive = Some(Keepalive::from_config(p.keepalives.as_ref()));
        Ok(o)
    }

    /// A DuckLake catalog database; the lock backend for lake targets.
    pub fn from_catalog(c: &CatalogConfig) -> Result<Self> {
        if c.kind != CatalogType::Postgres {
            return Err(AtlatlError::config(
                "the catalog is not a postgres database",
            ));
        }
        if c.iam_auth == Some(true) {
            return Err(AtlatlError::config("catalog.iam_auth is not supported yet"));
        }
        let need = |v: &Option<String>, k: &str| {
            v.clone()
                .ok_or_else(|| AtlatlError::config(format!("catalog.{k} is required")))
        };
        let mut o = Self::new(
            need(&c.host, "host")?,
            c.port.unwrap_or(DEFAULT_PORT),
            need(&c.database, "database")?,
            need(&c.user, "user")?,
            resolved(c.password.as_ref())?,
        );
        o.sslmode = c.sslmode.unwrap_or(SslMode::Require);
        Ok(o)
    }

    /// A `postgres` connection, or a `ducklake` connection's postgres catalog.
    pub fn from_connection(c: &Connection) -> Result<Self> {
        let mut o = match &c.kind {
            ConnectionKind::Postgres(p) => Self::from_postgres(p)?,
            ConnectionKind::Ducklake(d) => Self::from_catalog(&d.catalog)?,
            _ => {
                return Err(AtlatlError::config(format!(
                    "connection `{}` ({}) has no postgres database",
                    c.name,
                    c.type_name()
                )));
            }
        };
        if let Some(t) = c.common.timeouts.as_ref().and_then(|t| t.connect) {
            o.connect_timeout = Some(t.0);
        }
        Ok(o)
    }

    pub fn to_config(&self) -> Result<tokio_postgres::Config> {
        let mut c = tokio_postgres::Config::new();
        c.host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .application_name(&self.application_name)
            .ssl_mode(match self.sslmode {
                SslMode::Disable => tokio_postgres::config::SslMode::Disable,
                SslMode::Require | SslMode::VerifyFull => tokio_postgres::config::SslMode::Require,
            });
        if let Some(p) = &self.password {
            c.password(p);
        }
        if let Some(t) = self.statement_timeout {
            c.options(format!("-c statement_timeout={}", t.as_millis()));
        }
        if let Some(t) = self.connect_timeout {
            c.connect_timeout(t);
        }
        match self.keepalive {
            Some(k) => {
                c.keepalives(true)
                    .keepalives_idle(k.idle)
                    .keepalives_interval(k.interval)
                    .keepalives_retries(k.count);
            }
            None => {
                c.keepalives(false);
            }
        }
        Ok(c)
    }

    /// The rustls config for `sslmode`; `None` for `disable`. Build once and reuse per server.
    pub fn tls(&self) -> Result<Option<Arc<ClientConfig>>> {
        client_config(self.sslmode, self.sslrootcert.as_deref())
    }
}

fn resolved(p: Option<&SecretValue>) -> Result<Option<String>> {
    match p {
        None => Ok(None),
        Some(SecretValue::Literal(s)) => Ok(Some(s.clone())),
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(
            "password secret was not resolved before connecting",
        )),
    }
}

/// A client whose connection task runs in the background. Dropping it closes the session.
#[derive(Debug)]
pub struct Session {
    client: Client,
    conn: JoinHandle<()>,
}

impl Session {
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    pub fn is_closed(&self) -> bool {
        self.client.is_closed() || self.conn.is_finished()
    }

    /// Closes the socket without waiting for the server.
    pub fn abort(self) {
        self.conn.abort();
    }
}

pub async fn connect(o: &ConnectOptions) -> Result<Session> {
    connect_with(o, o.tls()?.as_ref()).await
}

/// [`connect`] with a prebuilt [`ConnectOptions::tls`] config.
pub async fn connect_with(o: &ConnectOptions, tls: Option<&Arc<ClientConfig>>) -> Result<Session> {
    let cfg = o.to_config()?;
    match (o.sslmode, tls) {
        (SslMode::Disable, _) => start(o, cfg.connect(NoTls)).await,
        (_, Some(t)) => start(o, cfg.connect(MakeRustls(t.clone()))).await,
        (_, None) => Err(AtlatlError::internal("sslmode needs a TLS config")),
    }
}

async fn start<S, T>(
    o: &ConnectOptions,
    fut: impl Future<
        Output = std::result::Result<
            (Client, tokio_postgres::Connection<S, T>),
            tokio_postgres::Error,
        >,
    >,
) -> Result<Session>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let res = match o.connect_timeout {
        Some(t) => tokio::time::timeout(t, fut).await.map_err(|_| {
            AtlatlError::new(
                ErrorClass::Timeout,
                format!("connecting to {}:{} timed out after {t:?}", o.host, o.port),
            )
        })?,
        None => fut.await,
    };
    let (client, conn) = res.map_err(wrap)?;
    let conn = tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(error = %classify(&e), "postgres connection ended");
        }
    });
    Ok(Session { client, conn })
}

#[cfg(test)]
mod tests {
    use super::*;
    use atlatl_core::config::types::Dur;

    fn pg() -> PostgresConn {
        PostgresConn {
            host: "db.internal".into(),
            port: None,
            database: "app".into(),
            user: "svc".into(),
            password: Some(SecretValue::Literal("pw".into())),
            sslmode: None,
            sslrootcert: None,
            application_name: None,
            statement_timeout: None,
            keepalives: None,
            manage_state_schema: None,
            changes: None,
        }
    }

    #[test]
    fn defaults() {
        let mut o = ConnectOptions::from_postgres(&pg()).expect("options");
        assert_eq!(o.port, 5432);
        assert_eq!(
            o.sslmode,
            SslMode::Require,
            "unset sslmode must not silently mean plaintext"
        );
        o.sslmode = SslMode::Disable;
        assert_eq!(o.application_name, "atlatl");
        assert_eq!(o.connect_timeout, Some(DEFAULT_CONNECT_TIMEOUT));
        assert_eq!(o.keepalive, Some(Keepalive::default()));
        let c = o.to_config().expect("config");
        assert_eq!(c.get_dbname(), Some("app"));
        assert_eq!(c.get_user(), Some("svc"));
        assert_eq!(c.get_password(), Some(&b"pw"[..]));
        assert_eq!(c.get_application_name(), Some("atlatl"));
        assert!(c.get_keepalives());
        assert_eq!(c.get_keepalives_idle(), Duration::from_secs(30));
        assert_eq!(c.get_keepalives_interval(), Some(Duration::from_secs(10)));
        assert_eq!(c.get_keepalives_retries(), Some(3));
        assert_eq!(c.get_options(), None);
    }

    #[test]
    fn explicit_settings() {
        let mut p = pg();
        p.port = Some(6543);
        p.sslmode = Some(SslMode::Disable);
        p.application_name = Some("atlatl-run-7".into());
        p.statement_timeout = Some(Dur(Duration::from_secs(90)));
        p.keepalives = Some(Keepalives {
            idle: Some(Dur(Duration::from_secs(5))),
            interval: None,
            count: Some(9),
        });
        let c = ConnectOptions::from_postgres(&p)
            .expect("options")
            .to_config()
            .expect("config");
        assert_eq!(c.get_ports(), &[6543]);
        assert_eq!(c.get_application_name(), Some("atlatl-run-7"));
        assert_eq!(c.get_options(), Some("-c statement_timeout=90000"));
        assert_eq!(c.get_keepalives_idle(), Duration::from_secs(5));
        assert_eq!(c.get_keepalives_interval(), Some(Duration::from_secs(10)));
        assert_eq!(c.get_keepalives_retries(), Some(9));

        let mut o = ConnectOptions::from_postgres(&pg()).expect("options");
        o.sslmode = SslMode::Disable;
        o.keepalive = None;
        assert!(!o.to_config().expect("config").get_keepalives());
    }

    #[test]
    fn ssl_modes() {
        use tokio_postgres::config::SslMode as W;
        let mut p = pg();
        for (mode, wire, tls) in [
            (SslMode::Disable, W::Disable, false),
            (SslMode::Require, W::Require, true),
            (SslMode::VerifyFull, W::Require, true),
        ] {
            p.sslmode = Some(mode);
            let o = ConnectOptions::from_postgres(&p).expect("options");
            assert_eq!(o.to_config().expect("config").get_ssl_mode(), wire);
            assert_eq!(o.tls().expect("tls").is_some(), tls);
        }
    }

    #[test]
    fn bad_sslrootcert_is_config() {
        let dir = tempfile::tempdir().expect("tmp");
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a cert").expect("write");
        let mut p = pg();
        p.sslmode = Some(SslMode::VerifyFull);
        for path in [empty, dir.path().join("missing.pem")] {
            p.sslrootcert = Some(path.display().to_string());
            let o = ConnectOptions::from_postgres(&p).expect("options");
            assert_eq!(o.sslrootcert.as_deref(), Some(path.as_path()));
            let e = o.tls().expect_err("bad root");
            assert_eq!(e.class, ErrorClass::Config, "{e}");
            assert!(e.message.contains("sslrootcert"), "{e}");
        }
    }

    #[test]
    fn unresolved_password_is_an_error() {
        let mut p = pg();
        p.password = Some(SecretValue::Ref(
            atlatl_core::config::types::SecretRef::Env("PW".into()),
        ));
        let e = ConnectOptions::from_postgres(&p).expect_err("unresolved");
        assert_eq!(e.class, ErrorClass::Internal);
    }

    #[test]
    fn catalog() {
        let mut c = CatalogConfig {
            kind: CatalogType::Postgres,
            host: Some("cat".into()),
            port: Some(20947),
            database: Some("ducklake_meta".into()),
            user: Some("postgres".into()),
            password: None,
            sslmode: Some(SslMode::Disable),
            iam_auth: None,
            path: None,
        };
        let o = ConnectOptions::from_catalog(&c).expect("options");
        assert_eq!((o.host.as_str(), o.port), ("cat", 20947));
        assert_eq!(o.database, "ducklake_meta");
        c.database = None;
        assert_eq!(
            ConnectOptions::from_catalog(&c).expect_err("db").class,
            ErrorClass::Config
        );
        c.kind = CatalogType::Duckdb;
        assert!(ConnectOptions::from_catalog(&c).is_err());
    }

    #[test]
    fn debug_hides_password() {
        let o = ConnectOptions::from_postgres(&pg()).expect("options");
        let s = format!("{o:?}");
        assert!(!s.contains("\"pw\""), "{s}");
        assert!(s.contains("****"), "{s}");
    }
}
