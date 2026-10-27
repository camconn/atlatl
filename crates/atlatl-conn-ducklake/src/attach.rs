//! The SQL that attaches a lake (spec 06 §2): extensions, the S3 secret, ATTACH.

use atlatl_conn_postgres::ConnectOptions;
use atlatl_core::AtlatlError;
use atlatl_core::config::model::{
    CatalogConfig, CatalogType, DuckLakeConn, S3Provider, SslMode, StorageConfig, StorageType,
    UrlStyle,
};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::Result;
use atlatl_core::redact;

use crate::sql::{check_alias, ident, libpq_value, literal};

/// The statements to run, in order, and the values to compare afterwards.
#[derive(Clone)]
pub struct AttachPlan {
    pub alias: String,
    pub data_path: String,
    pub extensions: Vec<&'static str>,
    /// `CREATE OR REPLACE SECRET …`; holds credentials, never log it.
    pub secret: Option<String>,
    /// Holds the catalog password, never log it.
    pub attach: String,
    /// [`attach`](Self::attach) read-only and never creating the catalog (a dry run).
    pub attach_existing: String,
}

impl std::fmt::Debug for AttachPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachPlan")
            .field("alias", &self.alias)
            .field("data_path", &self.data_path)
            .field("extensions", &self.extensions)
            .field("secret", &self.secret.as_ref().map(|_| "****"))
            .finish_non_exhaustive()
    }
}

impl AttachPlan {
    /// Registers every credential it embeds (raw and as quoted inside the SQL) for redaction.
    pub fn new(name: &str, c: &DuckLakeConn) -> Result<Self> {
        let alias = c.alias.clone().unwrap_or_else(|| name.to_owned());
        check_alias(&alias)?;
        let s3 = is_s3(c);
        let mut extensions = vec!["ducklake"];
        if c.catalog.kind == CatalogType::Postgres {
            extensions.push("postgres");
        }
        if s3 {
            extensions.extend(["httpfs", "aws"]);
        }
        Ok(Self {
            secret: secret_sql(
                &format!("atlatl_lake_{alias}"),
                c.storage.as_ref(),
                &c.data_path,
            )?,
            attach: attach_sql(&alias, c)?,
            attach_existing: attach_sql_with(
                &alias,
                c,
                &["READ_ONLY", "CREATE_IF_NOT_EXISTS false"],
            )?,
            alias,
            data_path: c.data_path.clone(),
            extensions,
        })
    }
}

fn is_s3(c: &DuckLakeConn) -> bool {
    c.data_path.starts_with("s3://")
        || c.storage
            .as_ref()
            .is_some_and(|s| s.kind == StorageType::S3)
}

/// `ducklake:postgres:<dsn>` or `ducklake:<path>`.
pub fn catalog_target(c: &CatalogConfig) -> Result<String> {
    match c.kind {
        CatalogType::Postgres => Ok(format!(
            "postgres:{}",
            libpq_dsn(&ConnectOptions::from_catalog(c)?)
        )),
        CatalogType::Duckdb => {
            let p = c.path.as_deref().filter(|p| !p.is_empty()).ok_or_else(|| {
                AtlatlError::config("catalog.path is required for a duckdb catalog")
            })?;
            Ok(p.to_owned())
        }
    }
}

/// A libpq keyword DSN with every value quoted. Registers the password for redaction.
pub fn libpq_dsn(o: &ConnectOptions) -> String {
    let mut parts = vec![
        ("host", o.host.clone()),
        ("port", o.port.to_string()),
        ("dbname", o.database.clone()),
        ("user", o.user.clone()),
    ];
    if let Some(p) = &o.password {
        register_secret(p);
        register_secret(&libpq_value(p));
        parts.push(("password", p.clone()));
    }
    parts.push((
        "sslmode",
        match o.sslmode {
            SslMode::Disable => "disable",
            SslMode::Require => "require",
            SslMode::VerifyFull => "verify-full",
        }
        .into(),
    ));
    if let Some(ca) = &o.sslrootcert {
        parts.push(("sslrootcert", ca.to_string_lossy().into_owned()));
    }
    if let Some(t) = o.connect_timeout {
        parts.push(("connect_timeout", t.as_secs().max(1).to_string()));
    }
    parts.push(("application_name", o.application_name.clone()));
    parts
        .iter()
        .map(|(k, v)| format!("{k}={}", libpq_value(v)))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn attach_sql(alias: &str, c: &DuckLakeConn) -> Result<String> {
    attach_sql_with(alias, c, &[])
}

fn attach_sql_with(alias: &str, c: &DuckLakeConn, extra: &[&str]) -> Result<String> {
    if c.data_path.is_empty() {
        return Err(AtlatlError::config("data_path is required"));
    }
    let target = format!("ducklake:{}", catalog_target(&c.catalog)?);
    let lit = literal(&target);
    if let Some(p) = &c.catalog.password
        && let SecretValue::Literal(p) = p
    {
        // As it appears inside the SQL literal: libpq-escaped, then '' doubled.
        let q = libpq_value(p);
        register_secret(&q[1..q.len() - 1].replace('\'', "''"));
    }
    let mut opts = vec![format!("DATA_PATH {}", literal(&c.data_path))];
    if let Some(n) = c.data_inlining_row_limit {
        opts.push(format!("DATA_INLINING_ROW_LIMIT {n}"));
    }
    opts.extend(extra.iter().map(|o| (*o).to_owned()));
    Ok(format!(
        "ATTACH {lit} AS {} ({})",
        ident(alias),
        opts.join(", ")
    ))
}

/// The bucket of an `s3://bucket/…` path.
pub fn s3_bucket(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("s3://")?;
    let b = rest.split('/').next()?;
    (!b.is_empty()).then_some(b)
}

/// `CREATE OR REPLACE SECRET` for the data path's bucket; `None` for local storage. A missing
/// `storage` with an `s3://` data path means `provider: credential_chain`.
pub fn secret_sql(
    name: &str,
    st: Option<&StorageConfig>,
    data_path: &str,
) -> Result<Option<String>> {
    let default = StorageConfig {
        kind: StorageType::S3,
        provider: None,
        region: None,
        endpoint: None,
        url_style: None,
        use_ssl: None,
        key_id: None,
        secret: None,
        session_token: None,
        role_arn: None,
    };
    let st = match st {
        Some(s) if s.kind == StorageType::Local => return Ok(None),
        Some(s) => s,
        None if data_path.starts_with("s3://") => &default,
        None => return Ok(None),
    };
    let bucket = s3_bucket(data_path).ok_or_else(|| {
        AtlatlError::config(format!(
            "storage.type is s3 but data_path `{data_path}` is not an s3://bucket/… path"
        ))
    })?;

    let mut opts = vec!["TYPE s3".to_owned()];
    match st.provider.unwrap_or(S3Provider::CredentialChain) {
        S3Provider::CredentialChain => opts.push("PROVIDER credential_chain".into()),
        S3Provider::Config => {
            opts.push("PROVIDER config".into());
            let key = resolved(st.key_id.as_ref(), "storage.key_id")?.ok_or_else(|| {
                AtlatlError::config("storage.key_id is required for provider: config")
            })?;
            let secret = resolved(st.secret.as_ref(), "storage.secret")?.ok_or_else(|| {
                AtlatlError::config("storage.secret is required for provider: config")
            })?;
            register_secret(&secret);
            opts.push(format!("KEY_ID {}", literal(&key)));
            opts.push(format!("SECRET {}", literal(&secret)));
            if let Some(t) = resolved(st.session_token.as_ref(), "storage.session_token")? {
                register_secret(&t);
                opts.push(format!("SESSION_TOKEN {}", literal(&t)));
            }
        }
        S3Provider::AssumeRole => {
            return Err(AtlatlError::config(
                "storage.provider: assume_role is not supported yet; use credential_chain or config",
            ));
        }
    }
    if let Some(r) = st.region.as_deref().filter(|r| !r.is_empty()) {
        opts.push(format!("REGION {}", literal(r)));
    }
    let mut use_ssl = st.use_ssl;
    if let Some(ep) = st.endpoint.as_deref().filter(|e| !e.is_empty()) {
        let (host, scheme_ssl) = parse_endpoint(ep)?;
        if let (Some(a), Some(b)) = (use_ssl, scheme_ssl)
            && a != b
        {
            return Err(AtlatlError::config(format!(
                "storage.endpoint `{ep}` implies use_ssl: {b}, but use_ssl is {a}"
            )));
        }
        use_ssl = use_ssl.or(scheme_ssl);
        opts.push(format!("ENDPOINT {}", literal(host)));
    }
    if let Some(u) = st.url_style {
        opts.push(format!(
            "URL_STYLE {}",
            literal(match u {
                UrlStyle::Path => "path",
                UrlStyle::Vhost => "vhost",
            })
        ));
    }
    if let Some(s) = use_ssl {
        opts.push(format!("USE_SSL {s}"));
    }
    opts.push(format!("SCOPE {}", literal(&format!("s3://{bucket}/"))));
    Ok(Some(format!(
        "CREATE OR REPLACE SECRET {} ({})",
        ident(name),
        opts.join(", ")
    )))
}

/// `host:port`, with `http://` / `https://` stripped into `use_ssl`.
pub fn parse_endpoint(ep: &str) -> Result<(&str, Option<bool>)> {
    let (rest, ssl) = if let Some(r) = ep.strip_prefix("https://") {
        (r, Some(true))
    } else if let Some(r) = ep.strip_prefix("http://") {
        (r, Some(false))
    } else if ep.contains("://") {
        return Err(AtlatlError::config(format!(
            "storage.endpoint `{ep}`: only http:// and https:// are supported"
        )));
    } else {
        (ep, None)
    };
    let host = rest.trim_end_matches('/');
    if host.is_empty() || host.contains('/') {
        return Err(AtlatlError::config(format!(
            "storage.endpoint `{ep}` must be host[:port] (no path)"
        )));
    }
    Ok((host, ssl))
}

fn resolved(v: Option<&SecretValue>, what: &str) -> Result<Option<String>> {
    match v {
        None => Ok(None),
        Some(SecretValue::Literal(s)) => Ok(Some(s.clone())),
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(format!(
            "{what} was not resolved before attaching"
        ))),
    }
}

fn register_secret(s: &str) {
    redact::register(s);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(v: serde_json::Value) -> StorageConfig {
        serde_json::from_value(v).expect("storage")
    }

    fn lake(v: serde_json::Value) -> DuckLakeConn {
        serde_json::from_value(v).expect("ducklake conn")
    }

    #[test]
    fn dsn_quotes_every_value() {
        let mut o = ConnectOptions::new("db host", 5433, "my db", "u'x", Some(r"p w'x\y".into()));
        o.sslmode = SslMode::Disable;
        let dsn = libpq_dsn(&o);
        assert_eq!(
            dsn,
            r"host='db host' port='5433' dbname='my db' user='u\'x' password='p w\'x\\y' sslmode='disable' connect_timeout='30' application_name='atlatl'"
        );
        let r = redact::redact(&dsn);
        assert!(!r.contains("p w"), "{r}");
    }

    #[test]
    fn attach_postgres() {
        let c = lake(serde_json::json!({
            "alias": "catalog",
            "catalog": {"type": "postgres", "host": "h", "port": 5432, "database": "d", "user": "u",
                        "password": "pw'q7", "sslmode": "require"},
            "data_path": "s3://b/lake/",
            "data_inlining_row_limit": 10,
        }));
        let p = AttachPlan::new("lake", &c).expect("plan");
        assert_eq!(p.alias, "catalog");
        assert_eq!(p.extensions, ["ducklake", "postgres", "httpfs", "aws"]);
        assert_eq!(
            p.attach,
            r#"ATTACH 'ducklake:postgres:host=''h'' port=''5432'' dbname=''d'' user=''u'' password=''pw\''q7'' sslmode=''require'' connect_timeout=''30'' application_name=''atlatl''' AS "catalog" (DATA_PATH 's3://b/lake/', DATA_INLINING_ROW_LIMIT 10)"#
        );
        assert_eq!(
            p.secret.as_deref(),
            Some(
                r#"CREATE OR REPLACE SECRET "atlatl_lake_catalog" (TYPE s3, PROVIDER credential_chain, SCOPE 's3://b/')"#
            )
        );
        let shown = format!("{p:?}");
        assert!(
            !shown.contains("pw'q7") && !shown.contains("ATTACH"),
            "{shown}"
        );
        let r = redact::redact(&p.attach);
        assert!(!r.contains("pw\\''q7"), "{r}");
    }

    #[test]
    fn attach_file_catalog() {
        let c = lake(serde_json::json!({
            "catalog": {"type": "duckdb", "path": "/tmp/o'neil/meta.ducklake"},
            "data_path": "/tmp/o'neil/data/",
            "storage": {"type": "local"},
        }));
        let p = AttachPlan::new("lake", &c).expect("plan");
        assert_eq!(p.extensions, ["ducklake"]);
        assert!(p.secret.is_none());
        assert_eq!(
            p.attach,
            r#"ATTACH 'ducklake:/tmp/o''neil/meta.ducklake' AS "lake" (DATA_PATH '/tmp/o''neil/data/')"#
        );
        assert_eq!(
            p.attach_existing,
            r#"ATTACH 'ducklake:/tmp/o''neil/meta.ducklake' AS "lake" (DATA_PATH '/tmp/o''neil/data/', READ_ONLY, CREATE_IF_NOT_EXISTS false)"#
        );

        let c = lake(serde_json::json!({"catalog": {"type": "duckdb"}, "data_path": "/d/"}));
        assert_eq!(
            AttachPlan::new("lake", &c).expect_err("no path").class,
            atlatl_core::ErrorClass::Config
        );
        let c = lake(
            serde_json::json!({"catalog": {"type": "duckdb", "path": "/m"}, "data_path": "/d/"}),
        );
        assert!(AttachPlan::new("bad-alias", &c).is_err());
        let c = lake(serde_json::json!({
            "catalog": {"type": "postgres", "host": "h", "database": "d", "user": "u", "iam_auth": true},
            "data_path": "/d/"
        }));
        assert!(AttachPlan::new("lake", &c).is_err());
    }

    #[test]
    fn secret_config_provider_with_endpoint() {
        let st = storage(serde_json::json!({
            "type": "s3", "provider": "config", "region": "us-east-1",
            "endpoint": "http://localhost:9000/", "url_style": "path",
            "key_id": "AK", "secret": "s3cr'et", "session_token": "tok-abcdef",
        }));
        let sql = secret_sql("s", Some(&st), "s3://bkt/lake/ducklake/")
            .expect("sql")
            .expect("some");
        assert_eq!(
            sql,
            r#"CREATE OR REPLACE SECRET "s" (TYPE s3, PROVIDER config, KEY_ID 'AK', SECRET 's3cr''et', SESSION_TOKEN 'tok-abcdef', REGION 'us-east-1', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, SCOPE 's3://bkt/')"#
        );
        assert!(!redact::redact("x s3cr'et tok-abcdef").contains("s3cr"));
    }

    #[test]
    fn secret_variants() {
        let sql = |v: serde_json::Value, path: &str| secret_sql("s", Some(&storage(v)), path);
        let s = sql(
            serde_json::json!({"type": "s3", "endpoint": "https://minio.example:443"}),
            "s3://b/",
        )
        .expect("ok")
        .expect("some");
        assert!(
            s.contains("ENDPOINT 'minio.example:443'") && s.contains("USE_SSL true"),
            "{s}"
        );
        let s = sql(serde_json::json!({"type": "s3", "endpoint": "rustfs:9000", "use_ssl": false, "url_style": "vhost"}), "s3://b/x")
            .expect("ok")
            .expect("some");
        assert!(
            s.contains("ENDPOINT 'rustfs:9000'")
                && s.contains("USE_SSL false")
                && s.contains("URL_STYLE 'vhost'"),
            "{s}"
        );
        assert!(s.contains("PROVIDER credential_chain"));
        assert!(!s.contains("REGION"));

        assert!(
            sql(serde_json::json!({"type": "local"}), "s3://b/")
                .expect("ok")
                .is_none()
        );
        assert!(secret_sql("s", None, "/local/path/").expect("ok").is_none());
        let s = secret_sql("s", None, "s3://b/p/")
            .expect("ok")
            .expect("some");
        assert!(
            s.contains("PROVIDER credential_chain") && s.contains("SCOPE 's3://b/'"),
            "{s}"
        );

        let config =
            |v: serde_json::Value, path: &str| sql(v, path).expect_err("config error").class;
        use atlatl_core::ErrorClass::Config;
        assert_eq!(
            config(
                serde_json::json!({"type": "s3", "provider": "assume_role", "role_arn": "arn:x"}),
                "s3://b/"
            ),
            Config
        );
        assert_eq!(
            config(
                serde_json::json!({"type": "s3", "provider": "config", "secret": "x"}),
                "s3://b/"
            ),
            Config
        );
        assert_eq!(
            config(
                serde_json::json!({"type": "s3", "provider": "config", "key_id": "x"}),
                "s3://b/"
            ),
            Config
        );
        assert_eq!(config(serde_json::json!({"type": "s3"}), "/local/"), Config);
        assert_eq!(
            config(serde_json::json!({"type": "s3"}), "s3:///nobucket"),
            Config
        );
        assert_eq!(
            config(
                serde_json::json!({"type": "s3", "endpoint": "http://h:1", "use_ssl": true}),
                "s3://b/"
            ),
            Config
        );
        assert_eq!(
            config(
                serde_json::json!({"type": "s3", "endpoint": "h:1/path"}),
                "s3://b/"
            ),
            Config
        );
        assert_eq!(
            config(
                serde_json::json!({"type": "s3", "endpoint": "ftp://h"}),
                "s3://b/"
            ),
            Config
        );

        let st = StorageConfig {
            key_id: Some(SecretValue::Literal("k".into())),
            secret: Some(SecretValue::Ref(
                atlatl_core::config::types::SecretRef::Env("X".into()),
            )),
            ..storage(serde_json::json!({"type": "s3", "provider": "config"}))
        };
        assert_eq!(
            secret_sql("s", Some(&st), "s3://b/")
                .expect_err("unresolved")
                .class,
            atlatl_core::ErrorClass::Internal
        );
    }

    #[test]
    fn buckets_and_endpoints() {
        assert_eq!(s3_bucket("s3://b/x/y"), Some("b"));
        assert_eq!(s3_bucket("s3://b"), Some("b"));
        assert_eq!(s3_bucket("s3://"), None);
        assert_eq!(s3_bucket("/b/x"), None);
        assert_eq!(parse_endpoint("h:1").expect("ok"), ("h:1", None));
        assert_eq!(
            parse_endpoint("http://h:1").expect("ok"),
            ("h:1", Some(false))
        );
        assert_eq!(parse_endpoint("https://h/").expect("ok"), ("h", Some(true)));
        assert!(parse_endpoint("http://").is_err());
    }
}
