//! AWS credentials: static keys, the environment, IRSA web identity and STS `AssumeRole`.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use atlatl_core::error::{AtlatlError, ErrorClass, Result, classify_http_status};
use atlatl_core::retry::{RandomJitter, RetryLevel, RetryPolicy, retry};
use tokio::sync::Mutex;

use crate::aws::{self, Endpoint, Signed};
use crate::sigv4::encode;
use crate::xml;

/// Refresh temporary credentials this long before they expire.
pub const REFRESH_BEFORE: Duration = Duration::from_secs(10 * 60);
const SESSION_SECONDS: u32 = 3600;

#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub key_id: String,
    pub secret: String,
    pub session_token: Option<String>,
    pub expires: Option<SystemTime>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("key_id", &self.key_id)
            .field("secret", &"****")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "****"),
            )
            .field("expires", &self.expires)
            .finish()
    }
}

impl Credentials {
    pub fn new(
        key_id: impl Into<String>,
        secret: impl Into<String>,
        token: Option<String>,
    ) -> Self {
        let c = Self {
            key_id: key_id.into(),
            secret: secret.into(),
            session_token: token.filter(|t| !t.is_empty()),
            expires: None,
        };
        c.register();
        c
    }

    fn register(&self) {
        atlatl_core::redact::register(&self.secret);
        if let Some(t) = &self.session_token {
            atlatl_core::redact::register(t);
        }
    }

    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let key = get("AWS_ACCESS_KEY_ID").filter(|s| !s.is_empty())?;
        let secret = get("AWS_SECRET_ACCESS_KEY").filter(|s| !s.is_empty())?;
        Some(Self::new(key, secret, get("AWS_SESSION_TOKEN")))
    }

    pub fn fresh_at(&self, now: SystemTime) -> bool {
        self.expires.is_none_or(|e| e > now + REFRESH_BEFORE)
    }
}

#[derive(Clone, Debug)]
pub enum Source {
    Static(Credentials),
    /// Environment keys, else IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`).
    Default,
    WebIdentity {
        token_file: PathBuf,
        role_arn: String,
    },
    AssumeRole {
        base: Arc<Provider>,
        role_arn: String,
    },
}

/// Caches and refreshes credentials from a [`Source`]. Cheap to share behind an `Arc`.
pub struct Provider {
    source: Source,
    sts: Endpoint,
    region: String,
    http: reqwest::Client,
    retry: RetryPolicy,
    cache: Mutex<Option<Credentials>>,
}

impl fmt::Debug for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Provider")
            .field("source", &self.source)
            .field("sts", &self.sts)
            .finish()
    }
}

pub fn default_sts(region: &str) -> String {
    format!("https://sts.{region}.amazonaws.com")
}

impl Provider {
    pub fn new(
        source: Source,
        region: &str,
        sts: Option<&str>,
        http: reqwest::Client,
        retry: RetryPolicy,
    ) -> Result<Self> {
        let sts = Endpoint::parse(sts.unwrap_or(&default_sts(region)))?;
        Ok(Self {
            source,
            sts,
            region: region.to_owned(),
            http,
            retry,
            cache: Mutex::new(None),
        })
    }

    pub fn is_static(&self) -> bool {
        matches!(self.source, Source::Static(_))
    }

    /// An `AssumeRole` provider on top of this one.
    pub fn assume(self: &Arc<Self>, role_arn: &str) -> Self {
        Self {
            source: Source::AssumeRole {
                base: self.clone(),
                role_arn: role_arn.to_owned(),
            },
            sts: self.sts.clone(),
            region: self.region.clone(),
            http: self.http.clone(),
            retry: self.retry.clone(),
            cache: Mutex::new(None),
        }
    }

    pub async fn get(&self) -> Result<Credentials> {
        let mut cache = self.cache.lock().await;
        if let Some(c) = cache.as_ref()
            && c.fresh_at(SystemTime::now())
        {
            return Ok(c.clone());
        }
        let c = Box::pin(self.resolve()).await?;
        *cache = Some(c.clone());
        Ok(c)
    }

    async fn resolve(&self) -> Result<Credentials> {
        match &self.source {
            Source::Static(c) => Ok(c.clone()),
            Source::Default => {
                if let Some(c) = Credentials::from_env(|k| std::env::var(k).ok()) {
                    return Ok(c);
                }
                match (
                    std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE"),
                    std::env::var("AWS_ROLE_ARN"),
                ) {
                    (Some(f), Ok(role)) => self.web_identity(&PathBuf::from(f), &role).await,
                    _ => Err(AtlatlError::new(
                        ErrorClass::Auth,
                        "no AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, \
                         AWS_WEB_IDENTITY_TOKEN_FILE/AWS_ROLE_ARN, or static keys in the connection",
                    )),
                }
            }
            Source::WebIdentity {
                token_file,
                role_arn,
            } => self.web_identity(token_file, role_arn).await,
            Source::AssumeRole { base, role_arn } => {
                let base = base.get().await?;
                self.sts_call(
                    Some(&base),
                    &[
                        ("Action", "AssumeRole"),
                        ("RoleArn", role_arn),
                        ("RoleSessionName", "atlatl"),
                        ("DurationSeconds", &SESSION_SECONDS.to_string()),
                    ],
                )
                .await
            }
        }
    }

    async fn web_identity(&self, file: &std::path::Path, role: &str) -> Result<Credentials> {
        let token = tokio::fs::read_to_string(file).await.map_err(|e| {
            AtlatlError::new(
                ErrorClass::Auth,
                format!("web identity token {}: {e}", file.display()),
            )
        })?;
        atlatl_core::redact::register(token.trim());
        self.sts_call(
            None,
            &[
                ("Action", "AssumeRoleWithWebIdentity"),
                ("RoleArn", role),
                ("RoleSessionName", "atlatl"),
                ("WebIdentityToken", token.trim()),
                ("DurationSeconds", &SESSION_SECONDS.to_string()),
            ],
        )
        .await
    }

    /// STS query API, signed with `base` (web identity calls are unsigned).
    async fn sts_call(
        &self,
        base: Option<&Credentials>,
        params: &[(&str, &str)],
    ) -> Result<Credentials> {
        let mut body = params
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect::<Vec<_>>();
        body.push("Version=2011-06-15".into());
        let body = body.join("&");
        let action = params.first().map(|p| p.1).unwrap_or_default();
        let headers = vec![(
            "content-type".to_owned(),
            "application/x-www-form-urlencoded; charset=utf-8".to_owned(),
        )];
        let resp = retry(
            &self.retry,
            RetryLevel::Request,
            &mut RandomJitter::new(),
            |a| tracing::warn!(attempt = a.attempt, error = %a.message, "retrying STS {action}"),
            |_| {
                let (body, headers) = (body.clone(), headers.clone());
                async move {
                    let r = match base {
                        Some(c) => {
                            aws::send(
                                &self.http,
                                c,
                                Signed {
                                    service: "sts",
                                    region: &self.region,
                                    method: "POST",
                                    endpoint: &self.sts,
                                    path: "/",
                                    query: &[],
                                    headers: &headers,
                                    body: body.into_bytes(),
                                    content_sha: false,
                                },
                            )
                            .await?
                        }
                        None => {
                            let r = self
                                .http
                                .post(format!("{}/", self.sts.base))
                                .header("content-type", &headers[0].1)
                                .body(body)
                                .send()
                                .await
                                .map_err(aws::transport)?;
                            let status = r.status().as_u16();
                            aws::Response {
                                status,
                                headers: Vec::new(),
                                body: r.bytes().await.map_err(aws::transport)?.to_vec(),
                            }
                        }
                    };
                    if r.status == 200 {
                        Ok(r)
                    } else {
                        Err(sts_error(action, r.status, &r.text()))
                    }
                }
            },
        )
        .await
        .map_err(|e| e.error)?;
        parse_sts(&resp.text())
    }
}

fn sts_error(action: &str, status: u16, body: &str) -> AtlatlError {
    let code = xml::text(body, "Code").unwrap_or_default();
    let msg = xml::text(body, "Message").unwrap_or_else(|| body.chars().take(300).collect());
    let class = match code.as_str() {
        "Throttling" | "ThrottlingException" | "RequestLimitExceeded" => ErrorClass::Throttled,
        "AccessDenied"
        | "InvalidClientTokenId"
        | "ExpiredToken"
        | "ExpiredTokenException"
        | "SignatureDoesNotMatch"
        | "InvalidIdentityToken" => ErrorClass::Auth,
        "IDPCommunicationError" => ErrorClass::Transient,
        _ => classify_http_status(status).unwrap_or(ErrorClass::Internal),
    };
    let e = AtlatlError::new(class, format!("STS {action}: HTTP {status} {code}: {msg}"));
    if code.is_empty() { e } else { e.code(code) }
}

pub fn parse_sts(body: &str) -> Result<Credentials> {
    let c = xml::first(body, "Credentials")
        .ok_or_else(|| AtlatlError::new(ErrorClass::Decode, "STS response has no <Credentials>"))?;
    let need = |n: &str| {
        xml::text(c, n).ok_or_else(|| {
            AtlatlError::new(ErrorClass::Decode, format!("STS response has no <{n}>"))
        })
    };
    let mut creds = Credentials::new(
        need("AccessKeyId")?,
        need("SecretAccessKey")?,
        xml::text(c, "SessionToken"),
    );
    creds.expires = xml::text(c, "Expiration")
        .and_then(|e| chrono::DateTime::parse_from_rfc3339(&e).ok())
        .map(SystemTime::from);
    Ok(creds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_and_freshness() {
        let env = |k: &str| match k {
            "AWS_ACCESS_KEY_ID" => Some("AK".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("SK".to_owned()),
            "AWS_SESSION_TOKEN" => Some(String::new()),
            _ => None,
        };
        let c = Credentials::from_env(env).expect("env");
        assert_eq!(
            (c.key_id.as_str(), c.session_token.as_deref()),
            ("AK", None)
        );
        assert!(Credentials::from_env(|_| None).is_none());
        let now = SystemTime::now();
        let mut t = c.clone();
        t.expires = Some(now + Duration::from_secs(60));
        assert!(!t.fresh_at(now));
        t.expires = Some(now + Duration::from_secs(3600));
        assert!(t.fresh_at(now));
        assert!(!format!("{c:?}").contains("SK"));
    }

    #[test]
    fn sts_parsing() {
        let body = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult>
            <Credentials><AccessKeyId>ASIA1</AccessKeyId><SecretAccessKey>s/k+1</SecretAccessKey>
            <SessionToken>tok&amp;en</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials>
            </AssumeRoleResult></AssumeRoleResponse>"#;
        let c = parse_sts(body).expect("creds");
        assert_eq!(c.session_token.as_deref(), Some("tok&en"));
        assert!(c.expires.is_some());
        let e = sts_error(
            "AssumeRole",
            403,
            "<ErrorResponse><Error><Code>AccessDenied</Code><Message>no</Message></Error></ErrorResponse>",
        );
        assert_eq!(e.class, ErrorClass::Auth);
        assert_eq!(
            sts_error("x", 400, "<Code>Throttling</Code>").class,
            ErrorClass::Throttled
        );
        assert_eq!(sts_error("x", 503, "").class, ErrorClass::Transient);
    }
}
