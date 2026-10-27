//! A minimal S3 client (GET, PUT, DELETE, ListObjectsV2) for manifests, cleanup and `gc`.
//! Parquet data is read by DuckDB, not here.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use atlatl_core::config::model::{StorageConfig, UrlStyle};
use atlatl_core::error::{AtlatlError, ErrorClass, Result, classify_http_status};
use atlatl_core::retry::{RandomJitter, RetryLevel, RetryPolicy, retry};

use crate::aws::{self, Endpoint, Response, Signed};
use crate::creds::{Credentials, Provider};
use crate::sigv4::encode_path;
use crate::xml;

/// Where and how to reach S3. Built from `unload.storage` (endpoint overrides) and the region.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Location {
    pub region: String,
    /// `host[:port]`; `None` means AWS (`s3.<region>.amazonaws.com`).
    pub endpoint: Option<String>,
    pub use_ssl: bool,
    pub path_style: bool,
}

impl S3Location {
    pub fn aws(region: &str) -> Self {
        Self {
            region: region.to_owned(),
            endpoint: None,
            use_ssl: true,
            path_style: false,
        }
    }

    pub fn from_storage(st: Option<&StorageConfig>, region: &str) -> Self {
        let mut l = Self::aws(st.and_then(|s| s.region.as_deref()).unwrap_or(region));
        let Some(st) = st else { return l };
        if let Some(ep) = st.endpoint.as_deref().filter(|e| !e.is_empty()) {
            let (host, ssl) = if let Some(h) = ep.strip_prefix("https://") {
                (h, Some(true))
            } else if let Some(h) = ep.strip_prefix("http://") {
                (h, Some(false))
            } else {
                (ep, None)
            };
            l.endpoint = Some(host.trim_end_matches('/').to_owned());
            l.use_ssl = st.use_ssl.or(ssl).unwrap_or(true);
            l.path_style = true;
        } else if let Some(s) = st.use_ssl {
            l.use_ssl = s;
        }
        match st.url_style {
            Some(UrlStyle::Path) => l.path_style = true,
            Some(UrlStyle::Vhost) => l.path_style = false,
            None => {}
        }
        l
    }

    fn host(&self) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| format!("s3.{}.amazonaws.com", self.region))
    }

    fn scheme(&self) -> &'static str {
        if self.use_ssl { "https" } else { "http" }
    }

    /// Endpoint and encoded path for an object (or the bucket when `key` is empty).
    fn target(&self, bucket: &str, key: &str) -> Result<(Endpoint, String)> {
        let host = self.host();
        if self.path_style {
            let ep = Endpoint::parse(&format!("{}://{host}", self.scheme()))?;
            let p = if key.is_empty() {
                format!("/{bucket}")
            } else {
                format!("/{bucket}/{key}")
            };
            Ok((ep, encode_path(&p)))
        } else {
            let ep = Endpoint::parse(&format!("{}://{bucket}.{host}", self.scheme()))?;
            Ok((ep, encode_path(&format!("/{key}"))))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<SystemTime>,
}

pub struct S3Client {
    http: reqwest::Client,
    loc: S3Location,
    creds: Arc<Provider>,
    retry: RetryPolicy,
}

impl std::fmt::Debug for S3Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Client").field("loc", &self.loc).finish()
    }
}

/// `s3://bucket/key` → (bucket, key).
pub fn split_url(url: &str) -> Result<(&str, &str)> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| AtlatlError::config(format!("`{url}` is not an s3:// URL")))?;
    let (b, k) = rest.split_once('/').unwrap_or((rest, ""));
    if b.is_empty() {
        return Err(AtlatlError::config(format!("`{url}` has no bucket")));
    }
    Ok((b, k))
}

fn s3_error(op: &str, target: &str, r: &Response) -> AtlatlError {
    let body = r.text();
    let code = xml::text(&body, "Code").unwrap_or_default();
    let msg = xml::text(&body, "Message").unwrap_or_default();
    let class = match code.as_str() {
        "SlowDown" | "Throttling" | "RequestLimitExceeded" => ErrorClass::Throttled,
        "ExpiredToken"
        | "InvalidAccessKeyId"
        | "SignatureDoesNotMatch"
        | "AccessDenied"
        | "InvalidToken" => ErrorClass::Auth,
        "NoSuchBucket" | "NoSuchKey" => ErrorClass::Config,
        "InternalError" | "ServiceUnavailable" | "RequestTimeout" => ErrorClass::Transient,
        _ => classify_http_status(r.status).unwrap_or(ErrorClass::Internal),
    };
    let e = AtlatlError::new(
        class,
        format!("S3 {op} {target}: HTTP {} {code} {msg}", r.status)
            .trim_end()
            .to_owned(),
    );
    if code.is_empty() { e } else { e.code(code) }
}

impl S3Client {
    pub fn new(
        loc: S3Location,
        creds: Arc<Provider>,
        http: reqwest::Client,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            http,
            loc,
            creds,
            retry,
        }
    }

    pub fn location(&self) -> &S3Location {
        &self.loc
    }

    pub async fn credentials(&self) -> Result<Credentials> {
        self.creds.get().await
    }

    async fn request(
        &self,
        method: &'static str,
        bucket: &str,
        key: &str,
        query: &[(String, String)],
        body: Vec<u8>,
        ok: &[u16],
    ) -> Result<Response> {
        let (ep, path) = self.loc.target(bucket, key)?;
        let shown = format!("s3://{bucket}/{key}");
        retry(
            &self.retry,
            RetryLevel::Request,
            &mut RandomJitter::new(),
            |a| tracing::warn!(attempt = a.attempt, error = %a.message, "retrying S3 {method} {shown}"),
            |_| {
                let body = body.clone();
                let (ep, path, shown) = (&ep, &path, &shown);
                async move {
                    let creds = self.creds.get().await?;
                    let r = aws::send(
                        &self.http,
                        &creds,
                        Signed {
                            service: "s3",
                            region: &self.loc.region,
                            method,
                            endpoint: ep,
                            path,
                            query,
                            headers: &[],
                            body,
                            content_sha: true,
                        },
                    )
                    .await?;
                    if ok.contains(&r.status) {
                        Ok(r)
                    } else {
                        Err(s3_error(method, shown, &r))
                    }
                }
            },
        )
        .await
        .map_err(|e| e.error)
    }

    /// The object, or `None` if it doesn't exist.
    pub async fn get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let r = self
            .request("GET", bucket, key, &[], Vec::new(), &[200, 404])
            .await?;
        Ok((r.status == 200).then_some(r.body))
    }

    pub async fn put(&self, bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
        self.request("PUT", bucket, key, &[], body, &[200])
            .await
            .map(|_| ())
    }

    /// Deleting a missing key succeeds.
    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        self.request("DELETE", bucket, key, &[], Vec::new(), &[200, 204, 404])
            .await
            .map(|_| ())
    }

    /// Every object under `prefix`, in key order.
    pub async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<Object>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type".to_owned(), "2".to_owned()),
                ("prefix".to_owned(), prefix.to_owned()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".to_owned(), t.clone()));
            }
            let r = self
                .request("GET", bucket, "", &q, Vec::new(), &[200])
                .await?;
            let body = r.text();
            for c in xml::all(&body, "Contents") {
                let key = xml::text(c, "Key").ok_or_else(|| {
                    AtlatlError::new(
                        ErrorClass::Decode,
                        "S3 ListObjectsV2: <Contents> without <Key>",
                    )
                })?;
                out.push(Object {
                    key,
                    size: xml::text(c, "Size")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0),
                    last_modified: xml::text(c, "LastModified")
                        .and_then(|t| chrono::DateTime::parse_from_rfc3339(&t).ok())
                        .map(SystemTime::from),
                });
            }
            let more = xml::text(&body, "IsTruncated").is_some_and(|t| t == "true");
            token = xml::text(&body, "NextContinuationToken");
            if !more || token.is_none() {
                break;
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    /// Deletes every object under `prefix` (8 at a time); returns how many were deleted.
    pub async fn delete_prefix(&self, bucket: &str, prefix: &str) -> Result<u64> {
        if prefix.is_empty() || !prefix.ends_with('/') {
            return Err(AtlatlError::internal(format!(
                "refusing to delete prefix `{prefix}`: it must be non-empty and end with `/`"
            )));
        }
        let objs = self.list(bucket, prefix).await?;
        use futures::{StreamExt, TryStreamExt};
        let n = objs.len() as u64;
        futures::stream::iter(objs.into_iter().map(|o| o.key))
            .map(|k| async move { self.delete(bucket, &k).await })
            .buffer_unordered(8)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(n)
    }
}

/// Request retry for S3 calls.
pub fn default_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 5,
        base: Duration::from_millis(200),
        max: Duration::from_secs(10),
        ..RetryPolicy::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets() {
        let l = S3Location::aws("us-east-1");
        let (ep, p) = l.target("b", "atlatl_unload/r 1/x").expect("t");
        assert_eq!(ep.base, "https://b.s3.us-east-1.amazonaws.com");
        assert_eq!(p, "/atlatl_unload/r%201/x");
        let st: StorageConfig = serde_json::from_value(serde_json::json!({
            "type": "s3", "endpoint": "http://127.0.0.1:9000", "region": "eu-west-1"
        }))
        .expect("st");
        let l = S3Location::from_storage(Some(&st), "us-east-1");
        assert_eq!(
            (l.region.as_str(), l.use_ssl, l.path_style),
            ("eu-west-1", false, true)
        );
        let (ep, p) = l.target("b", "").expect("t");
        assert_eq!(
            (ep.base.as_str(), p.as_str()),
            ("http://127.0.0.1:9000", "/b")
        );
        assert_eq!(split_url("s3://b/k/x").expect("u"), ("b", "k/x"));
        assert!(split_url("s3:///k").is_err());
    }
}
