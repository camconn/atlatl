use atlatl_core::config::model::{S3Provider, StorageConfig, StorageType, UrlStyle};
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_ingest::sql::{ident, literal};

pub fn bucket(url: &str) -> Option<&str> {
    let b = url.strip_prefix("s3://")?.split('/').next()?;
    (!b.is_empty()).then_some(b)
}

/// DuckDB extensions an S3 read with this storage needs.
pub fn extensions(st: Option<&StorageConfig>) -> &'static [&'static str] {
    match st {
        Some(s) if s.kind == StorageType::Local => &[],
        Some(s) if s.provider == Some(S3Provider::Config) => &["httpfs"],
        _ => &["httpfs", "aws"],
    }
}

/// `CREATE OR REPLACE SECRET` scoped to `s3://<bucket>/`; `None` for `type: local`. No
/// `storage` means `provider: credential_chain`. Secret values must already be resolved.
pub fn secret_sql(name: &str, st: Option<&StorageConfig>, bucket: &str) -> Result<Option<String>> {
    if st.is_some_and(|s| s.kind == StorageType::Local) {
        return Ok(None);
    }
    let mut opts = vec!["TYPE s3".to_owned()];
    match st
        .and_then(|s| s.provider)
        .unwrap_or(S3Provider::CredentialChain)
    {
        S3Provider::CredentialChain => opts.push("PROVIDER credential_chain".into()),
        S3Provider::Config => {
            let st = st.ok_or_else(|| AtlatlError::internal("provider config without storage"))?;
            opts.push("PROVIDER config".into());
            let key = resolved(st.key_id.as_ref(), "storage.key_id")?;
            let secret = resolved(st.secret.as_ref(), "storage.secret")?;
            atlatl_core::redact::register(&secret);
            opts.push(format!("KEY_ID {}", literal(&key)));
            opts.push(format!("SECRET {}", literal(&secret)));
            if st.session_token.is_some() {
                let t = resolved(st.session_token.as_ref(), "storage.session_token")?;
                atlatl_core::redact::register(&t);
                opts.push(format!("SESSION_TOKEN {}", literal(&t)));
            }
        }
        S3Provider::AssumeRole => {
            return Err(AtlatlError::config(
                "storage.provider: assume_role is not supported for files yet; use credential_chain or config",
            ));
        }
    }
    if let Some(st) = st {
        if let Some(r) = st.region.as_deref().filter(|r| !r.is_empty()) {
            opts.push(format!("REGION {}", literal(r)));
        }
        let mut use_ssl = st.use_ssl;
        if let Some(ep) = st.endpoint.as_deref().filter(|e| !e.is_empty()) {
            let (host, ssl) = if let Some(h) = ep.strip_prefix("https://") {
                (h, Some(true))
            } else if let Some(h) = ep.strip_prefix("http://") {
                (h, Some(false))
            } else {
                (ep, None)
            };
            use_ssl = use_ssl.or(ssl);
            opts.push(format!("ENDPOINT {}", literal(host.trim_end_matches('/'))));
        }
        if let Some(u) = st.url_style {
            let u = match u {
                UrlStyle::Path => "path",
                UrlStyle::Vhost => "vhost",
            };
            opts.push(format!("URL_STYLE {}", literal(u)));
        }
        if let Some(s) = use_ssl {
            opts.push(format!("USE_SSL {s}"));
        }
    }
    opts.push(format!("SCOPE {}", literal(&format!("s3://{bucket}/"))));
    Ok(Some(format!(
        "CREATE OR REPLACE SECRET {} ({})",
        ident(name),
        opts.join(", ")
    )))
}

fn resolved(v: Option<&SecretValue>, what: &str) -> Result<String> {
    match v {
        Some(SecretValue::Literal(s)) => Ok(s.clone()),
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(format!(
            "{what} was not resolved before reading"
        ))),
        None => Err(AtlatlError::config(format!(
            "{what} is required for provider: config"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(v: serde_json::Value) -> StorageConfig {
        serde_json::from_value(v).expect("storage")
    }

    #[test]
    fn secrets() {
        assert_eq!(bucket("s3://b/k/x.csv"), Some("b"));
        assert_eq!(bucket("s3:///k"), None);
        assert_eq!(
            secret_sql("s", None, "b").expect("sql").as_deref(),
            Some(
                r#"CREATE OR REPLACE SECRET "s" (TYPE s3, PROVIDER credential_chain, SCOPE 's3://b/')"#
            )
        );
        let st = storage(serde_json::json!({
            "type": "s3", "provider": "config", "key_id": "AK", "secret": "s'x",
            "region": "us-east-1", "endpoint": "http://localhost:9000/", "url_style": "path"
        }));
        assert_eq!(
            secret_sql("s", Some(&st), "b").expect("sql").as_deref(),
            Some(
                r#"CREATE OR REPLACE SECRET "s" (TYPE s3, PROVIDER config, KEY_ID 'AK', SECRET 's''x', REGION 'us-east-1', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, SCOPE 's3://b/')"#
            )
        );
        assert_eq!(extensions(Some(&st)), ["httpfs"]);
        let local = storage(serde_json::json!({"type": "local"}));
        assert!(secret_sql("s", Some(&local), "b").expect("sql").is_none());
        let missing = storage(serde_json::json!({"type": "s3", "provider": "config"}));
        assert!(secret_sql("s", Some(&missing), "b").is_err());
    }
}
