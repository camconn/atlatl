//! `aws-sm:<secret-id>[#json-key]` (spec 02 §2.2): Secrets Manager `GetSecretValue` (JSON 1.1)
//! signed with a hand-rolled SigV4; no AWS SDK.
//!
//! - **Credentials**, first found: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`[/`AWS_SESSION_TOKEN`];
//!   web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, via STS
//!   `AssumeRoleWithWebIdentity`); the ECS/EKS container endpoint
//!   (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` / `_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN[_FILE]`);
//!   static keys of `AWS_PROFILE` (default `default`) in `AWS_SHARED_CREDENTIALS_FILE` or
//!   `~/.aws/credentials`. EC2 instance metadata is not supported.
//! - **Region**: the ARN's, else `AWS_REGION` / `AWS_DEFAULT_REGION`.
//! - **Endpoint**: `AWS_ENDPOINT_URL_SECRETS_MANAGER`, `AWS_ENDPOINT_URL`, else the regional one
//!   (`AWS_ENDPOINT_URL_STS` likewise for STS).
//!
//! Resolution is synchronous (config resolution is): the request runs on a helper thread with its
//! own current-thread runtime, under `block_in_place` when called from a multi-threaded runtime.
//! Values are cached per process by (endpoint, secret id), so several `#key`s of one secret cost
//! one call.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime};

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use super::SecretError;
use crate::template::EnvSource;

pub const SERVICE: &str = "secretsmanager";
const TARGET: &str = "secretsmanager.GetSecretValue";
const CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const ATTEMPTS: u32 = 3;

#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

static CACHE: LazyLock<Mutex<HashMap<(String, String), String>>> = LazyLock::new(Default::default);

fn err(id: &str, m: impl std::fmt::Display) -> SecretError {
    SecretError(format!("secret `aws-sm:{id}`: {m}"))
}

/// The value of `aws-sm:<id>[#json_key]`.
pub fn resolve(
    id: &str,
    json_key: Option<&str>,
    env: &dyn EnvSource,
) -> Result<String, SecretError> {
    let region = region(id, env).ok_or_else(|| {
        err(
            id,
            "no region: use a full ARN, or set AWS_REGION / AWS_DEFAULT_REGION",
        )
    })?;
    let endpoint = endpoint(env, "SECRETS_MANAGER")
        .unwrap_or_else(|| format!("https://secretsmanager.{region}.amazonaws.com"));
    let key = (endpoint.clone(), id.to_owned());
    let cached = CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .cloned();
    let whole = match cached {
        Some(v) => v,
        None => {
            let creds = credentials(env, &region).map_err(|e| err(id, e))?;
            let (idc, ep, rg) = (id.to_owned(), endpoint.clone(), region.clone());
            let v = blocking(move || get_secret_value(&ep, &rg, &creds, &idc, SystemTime::now()))
                .map_err(|e| err(id, e))?;
            CACHE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, v.clone());
            v
        }
    };
    match json_key {
        None => Ok(whole),
        Some(k) => {
            let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&whole)
                .map_err(|_| err(id, format!("`#{k}` needs a JSON-object secret")))?;
            match obj.get(k) {
                Some(serde_json::Value::String(s)) => Ok(s.clone()),
                Some(v) => Ok(v.to_string()),
                None => Err(err(
                    id,
                    format!(
                        "no key `{k}` (keys: {})",
                        obj.keys().cloned().collect::<Vec<_>>().join(", ")
                    ),
                )),
            }
        }
    }
}

fn region(id: &str, env: &dyn EnvSource) -> Option<String> {
    let from_arn = id
        .strip_prefix("arn:")
        .and_then(|r| r.split(':').nth(2))
        .filter(|r| !r.is_empty());
    from_arn
        .map(str::to_owned)
        .or_else(|| env.var("AWS_REGION"))
        .or_else(|| env.var("AWS_DEFAULT_REGION"))
        .filter(|r| !r.is_empty())
}

fn endpoint(env: &dyn EnvSource, service: &str) -> Option<String> {
    env.var(&format!("AWS_ENDPOINT_URL_{service}"))
        .or_else(|| env.var("AWS_ENDPOINT_URL"))
        .filter(|u| !u.is_empty())
        .map(|u| u.trim_end_matches('/').to_owned())
}

/// Runs `f` on a helper thread with its own runtime; safe from sync code and from any runtime.
fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<T, String>> + Send>>
    + Send
    + 'static,
) -> Result<T, String> {
    let join = move || {
        std::thread::Builder::new()
            .name("atlatl-aws".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                rt.block_on(f())
            })
            .map_err(|e| e.to_string())?
            .join()
            .map_err(|_| "AWS request thread panicked".to_owned())?
    };
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(join)
        }
        _ => join(),
    }
}

fn http() -> Result<reqwest::Client, String> {
    crate::tls::http_client()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .user_agent(concat!("atlatl/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| e.to_string())
}

/// `GetSecretValue`; `SecretString`, or `SecretBinary` decoded as UTF-8.
pub fn get_secret_value(
    endpoint: &str,
    region: &str,
    creds: &Credentials,
    id: &str,
    now: SystemTime,
) -> std::pin::Pin<Box<dyn Future<Output = Result<String, String>> + Send>> {
    let url = format!("{endpoint}/");
    let body = serde_json::json!({ "SecretId": id }).to_string();
    let (endpoint, region, creds) = (endpoint.to_owned(), region.to_owned(), creds.clone());
    Box::pin(async move {
        let host = host_of(&endpoint)?;
        let client = http()?;
        let mut last = String::new();
        for attempt in 1..=ATTEMPTS {
            let headers = sign(
                &Request {
                    method: "POST",
                    host: &host,
                    path: "/",
                    query: "",
                    headers: &[("content-type", CONTENT_TYPE), ("x-amz-target", TARGET)],
                    body: body.as_bytes(),
                },
                &creds,
                &region,
                SERVICE,
                now,
            );
            let mut rb = client.post(&url).body(body.clone());
            for (k, v) in &headers {
                if k != "host" {
                    rb = rb.header(k.as_str(), v.as_str());
                }
            }
            let retry = match rb.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let text = resp.text().await.map_err(|e| e.to_string())?;
                    if status == 200 {
                        return secret_from_body(&text);
                    }
                    let (kind, msg) = aws_error(&text);
                    last = format!("HTTP {status} {kind}: {msg}");
                    status >= 500 || kind.contains("Throttling")
                }
                Err(e) => {
                    last = e.without_url().to_string();
                    true
                }
            };
            if !retry || attempt == ATTEMPTS {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
        }
        Err(last)
    })
}

fn host_of(endpoint: &str) -> Result<String, String> {
    let u = reqwest::Url::parse(endpoint).map_err(|e| format!("endpoint `{endpoint}`: {e}"))?;
    let h = u
        .host_str()
        .ok_or_else(|| format!("endpoint `{endpoint}` has no host"))?;
    Ok(match u.port() {
        Some(p) => format!("{h}:{p}"),
        None => h.to_owned(),
    })
}

fn secret_from_body(text: &str) -> Result<String, String> {
    let v: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("bad GetSecretValue response: {e}"))?;
    if let Some(s) = v.get("SecretString").and_then(|s| s.as_str()) {
        return Ok(s.to_owned());
    }
    if let Some(b) = v.get("SecretBinary").and_then(|s| s.as_str()) {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b)
            .map_err(|e| format!("SecretBinary: {e}"))?;
        return String::from_utf8(bytes).map_err(|_| "SecretBinary is not UTF-8".to_owned());
    }
    Err("response has neither SecretString nor SecretBinary".into())
}

fn aws_error(text: &str) -> (String, String) {
    let v: serde_json::Value = serde_json::from_str(text).unwrap_or_default();
    let kind = v
        .get("__type")
        .and_then(|t| t.as_str())
        .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
        .unwrap_or_else(|| "error".into());
    let msg = v
        .get("message")
        .or_else(|| v.get("Message"))
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_owned();
    (kind, crate::redact::redact(&msg).into_owned())
}

// ----------------------------------------------------------------------------- SigV4

pub struct Request<'a> {
    pub method: &'a str,
    /// `host[:port]`, signed as the `host` header.
    pub host: &'a str,
    /// Already URI-encoded.
    pub path: &'a str,
    /// Already canonical (sorted, encoded), or empty.
    pub query: &'a str,
    /// Extra headers to sign; names lowercase.
    pub headers: &'a [(&'a str, &'a str)],
    pub body: &'a [u8],
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hmac(key: &[u8], msg: &str) -> Vec<u8> {
    #[allow(clippy::expect_used)]
    let mut m = <Hmac<Sha256>>::new_from_slice(key).expect("hmac takes any key length");
    m.update(msg.as_bytes());
    m.finalize().into_bytes().to_vec()
}

/// `20260928T120000Z`.
pub fn amz_date(t: SystemTime) -> String {
    let s = crate::ids::rfc3339(t);
    format!(
        "{}{}{}T{}{}{}Z",
        &s[0..4],
        &s[5..7],
        &s[8..10],
        &s[11..13],
        &s[14..16],
        &s[17..19]
    )
}

/// The headers to send (lowercase names, `host` included): `x-amz-date`, the security token if
/// any, the request's own headers, and `authorization`.
pub fn sign(
    req: &Request<'_>,
    creds: &Credentials,
    region: &str,
    service: &str,
    now: SystemTime,
) -> Vec<(String, String)> {
    let date = amz_date(now);
    let day = &date[..8];
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), req.host.to_owned()),
        ("x-amz-date".into(), date.clone()),
    ];
    if let Some(t) = &creds.session_token {
        headers.push(("x-amz-security-token".into(), t.clone()));
    }
    headers.extend(
        req.headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned())),
    );
    headers.sort();
    let signed = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let canonical = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed}\n{}",
        req.method,
        req.path,
        req.query,
        hex(&Sha256::digest(req.body))
    );
    let scope = format!("{day}/{region}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let k = hmac(format!("AWS4{}", creds.secret_access_key).as_bytes(), day);
    let k = hmac(&k, region);
    let k = hmac(&k, service);
    let k = hmac(&k, "aws4_request");
    let sig = hex(&hmac(&k, &to_sign));
    headers.push((
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={sig}",
            creds.access_key_id
        ),
    ));
    headers
}

// ----------------------------------------------------------------------------- credentials

pub fn credentials(env: &dyn EnvSource, region: &str) -> Result<Credentials, String> {
    let nonempty = |k: &str| env.var(k).filter(|v| !v.is_empty());
    if let (Some(a), Some(s)) = (
        nonempty("AWS_ACCESS_KEY_ID"),
        nonempty("AWS_SECRET_ACCESS_KEY"),
    ) {
        return Ok(register(Credentials {
            access_key_id: a,
            secret_access_key: s,
            session_token: nonempty("AWS_SESSION_TOKEN"),
        }));
    }
    if let (Some(file), Some(role)) = (
        nonempty("AWS_WEB_IDENTITY_TOKEN_FILE"),
        nonempty("AWS_ROLE_ARN"),
    ) {
        let token = std::fs::read_to_string(&file)
            .map_err(|e| format!("web identity token `{file}`: {e}"))?;
        crate::redact::register(token.trim());
        let sts =
            endpoint(env, "STS").unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));
        let session = nonempty("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| "atlatl".into());
        let token = token.trim().to_owned();
        return blocking(move || {
            Box::pin(assume_role_with_web_identity(sts, role, session, token))
        })
        .map(register);
    }
    let full = nonempty("AWS_CONTAINER_CREDENTIALS_FULL_URI").or_else(|| {
        nonempty("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            .map(|p| format!("http://169.254.170.2{p}"))
    });
    if let Some(uri) = full {
        let auth = match nonempty("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
            Some(f) => Some(
                std::fs::read_to_string(&f)
                    .map_err(|e| format!("container authorization token `{f}`: {e}"))?
                    .trim()
                    .to_owned(),
            ),
            None => nonempty("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
        };
        return blocking(move || Box::pin(container_credentials(uri, auth))).map(register);
    }
    let file = nonempty("AWS_SHARED_CREDENTIALS_FILE")
        .or_else(|| nonempty("HOME").map(|h| format!("{h}/.aws/credentials")));
    let profile = nonempty("AWS_PROFILE").unwrap_or_else(|| "default".into());
    if let Some(file) = file
        && let Ok(text) = std::fs::read_to_string(&file)
        && let Some(c) = profile_credentials(&text, &profile)
    {
        return Ok(register(c));
    }
    Err(format!(
        "no AWS credentials (env, web identity, container endpoint, or profile `{profile}` static keys)"
    ))
}

fn register(c: Credentials) -> Credentials {
    crate::redact::register(&c.secret_access_key);
    if let Some(t) = &c.session_token {
        crate::redact::register(t);
    }
    c
}

/// Static keys of `[profile]` in an INI credentials file.
pub fn profile_credentials(text: &str, profile: &str) -> Option<Credentials> {
    let mut section = String::new();
    let mut kv: HashMap<String, String> = HashMap::new();
    for line in text.lines() {
        let l = line.trim();
        if l.starts_with('#') || l.starts_with(';') || l.is_empty() {
            continue;
        }
        if let Some(s) = l.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            section = s.trim().trim_start_matches("profile ").trim().to_owned();
            continue;
        }
        if section == profile
            && let Some((k, v)) = l.split_once('=')
        {
            kv.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    Some(Credentials {
        access_key_id: kv.remove("aws_access_key_id")?,
        secret_access_key: kv.remove("aws_secret_access_key")?,
        session_token: kv.remove("aws_session_token"),
    })
}

async fn container_credentials(uri: String, auth: Option<String>) -> Result<Credentials, String> {
    let mut rb = http()?.get(&uri);
    if let Some(a) = &auth {
        rb = rb.header("authorization", a);
    }
    let resp = rb
        .send()
        .await
        .map_err(|e| format!("container credentials: {}", e.without_url()))?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("container credentials: HTTP {status}"));
    }
    let v: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("container credentials: {e}"))?;
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_owned);
    Ok(Credentials {
        access_key_id: s("AccessKeyId").ok_or("container credentials: no AccessKeyId")?,
        secret_access_key: s("SecretAccessKey")
            .ok_or("container credentials: no SecretAccessKey")?,
        session_token: s("Token"),
    })
}

async fn assume_role_with_web_identity(
    sts: String,
    role: String,
    session: String,
    token: String,
) -> Result<Credentials, String> {
    let form = [
        ("Action", "AssumeRoleWithWebIdentity"),
        ("Version", "2011-06-15"),
        ("RoleArn", role.as_str()),
        ("RoleSessionName", session.as_str()),
        ("WebIdentityToken", token.as_str()),
    ];
    let body = form
        .iter()
        .map(|(k, v)| {
            format!(
                "{k}={}",
                percent_encoding::utf8_percent_encode(v, percent_encoding::NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    let resp = http()?
        .post(format!("{sts}/"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("STS AssumeRoleWithWebIdentity: {}", e.without_url()))?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        let code = xml_text(&text, "Code").unwrap_or_default();
        let msg = xml_text(&text, "Message").unwrap_or_default();
        return Err(format!(
            "STS AssumeRoleWithWebIdentity: HTTP {status} {code}: {}",
            crate::redact::redact(&msg)
        ));
    }
    let get = |t: &str| {
        xml_text(&text, t).ok_or_else(|| format!("STS AssumeRoleWithWebIdentity: no <{t}>"))
    };
    Ok(Credentials {
        access_key_id: get("AccessKeyId")?,
        secret_access_key: get("SecretAccessKey")?,
        session_token: Some(get("SessionToken")?),
    })
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{tag}>"))? + start;
    Some(
        xml[start..end]
            .trim()
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'"),
    )
}

#[cfg(test)]
mod tests;
