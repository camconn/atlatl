//! HTTPS downloads with retry and Range resume (06 §5): signed URLs can expire mid-read, so the
//! file lands on local disk before DuckDB sees it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use atlatl_core::error::{AtlatlError, ErrorClass, Result, classify_http_status, wait_hint};
use atlatl_core::retry::{AttemptRecord, RandomJitter, RetryLevel, RetryPolicy, retry};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub struct Downloader {
    client: reqwest::Client,
    policy: RetryPolicy,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Download {
    pub path: PathBuf,
    pub bytes: u64,
    pub attempts: u32,
    /// Attempts that continued a partial body with `Range`.
    pub resumed: u32,
    pub retries: Vec<String>,
}

/// URL without its query string, for messages: signed URLs carry credentials there.
pub fn display_url(url: &str) -> &str {
    url.split(['?', '#']).next().unwrap_or(url)
}

impl Downloader {
    pub fn new(policy: RetryPolicy) -> Result<Self> {
        let client = atlatl_core::tls::http_client()
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| AtlatlError::internal(format!("http client: {e}")))?;
        Ok(Self::with_client(client, policy))
    }

    /// [`new`](Self::new), sending `headers` with every request.
    pub fn with_headers(policy: RetryPolicy, headers: &[(String, String)]) -> Result<Self> {
        let mut map = reqwest::header::HeaderMap::new();
        for (k, v) in headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| AtlatlError::config(format!("files header `{k}`: {e}")))?;
            let value = reqwest::header::HeaderValue::from_str(v)
                .map_err(|e| AtlatlError::config(format!("files header `{k}`: {e}")))?;
            map.insert(name, value);
        }
        let client = atlatl_core::tls::http_client()
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(120))
            .default_headers(map)
            .build()
            .map_err(|e| AtlatlError::internal(format!("http client: {e}")))?;
        Ok(Self::with_client(client, policy))
    }

    pub fn with_client(client: reqwest::Client, policy: RetryPolicy) -> Self {
        Self { client, policy }
    }

    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }

    /// Downloads `url` to `dest`, resuming a partial body on retry. The partial file is
    /// `<dest>.part` until the body is complete.
    pub async fn download(&self, url: &str, dest: &Path) -> Result<Download> {
        if let Some(p) = dest.parent() {
            tokio::fs::create_dir_all(p)
                .await
                .map_err(|e| crate::error::io_at(p, e))?;
        }
        let part = dest.with_extension(match dest.extension() {
            Some(e) => format!("{}.part", e.to_string_lossy()),
            None => "part".into(),
        });
        let _ = tokio::fs::remove_file(&part).await;
        let state = Arc::new(Mutex::new(State::default()));
        let retries = Mutex::new(Vec::new());
        let mut jitter = RandomJitter::new();
        let res = retry(
            &self.policy,
            RetryLevel::Request,
            &mut jitter,
            |a: &AttemptRecord| {
                if let Ok(mut r) = retries.lock() {
                    r.push(format!(
                        "attempt {}: {} ({})",
                        a.attempt, a.message, a.class
                    ));
                }
            },
            |_| self.attempt(url, &part, state.clone()),
        )
        .await;
        let st = state.lock().map(|s| s.clone()).unwrap_or_default();
        let retries = retries.into_inner().unwrap_or_default();
        match res {
            Ok(bytes) => {
                tokio::fs::rename(&part, dest)
                    .await
                    .map_err(|e| crate::error::io_at(dest, e))?;
                Ok(Download {
                    path: dest.to_owned(),
                    bytes,
                    attempts: st.attempts,
                    resumed: st.resumed,
                    retries,
                })
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&part).await;
                let mut err = e.error.clone();
                err.message = format!("{e}");
                Err(err)
            }
        }
    }

    async fn attempt(&self, url: &str, part: &Path, state: Arc<Mutex<State>>) -> Result<u64> {
        let shown = display_url(url);
        let validator = {
            let mut s = state
                .lock()
                .map_err(|_| AtlatlError::internal("download state poisoned"))?;
            s.attempts += 1;
            s.validator.clone()
        };
        let have = tokio::fs::metadata(part)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let mut req = self.client.get(url);
        if have > 0 {
            req = req.header(RANGE, format!("bytes={have}-"));
            if let Some(v) = &validator {
                req = req.header(IF_RANGE, v);
            }
        }
        let mut resp = req.send().await.map_err(|e| reqwest_err(shown, e))?;
        let status = resp.status();
        let append = match status {
            StatusCode::PARTIAL_CONTENT if have > 0 => {
                let start = resp
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(range_start);
                if start != Some(have) {
                    return Err(AtlatlError::transient(format!(
                        "GET {shown}: resumed at an unexpected offset ({start:?}, have {have})"
                    )));
                }
                true
            }
            StatusCode::RANGE_NOT_SATISFIABLE if have > 0 => {
                let total = resp
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(range_total);
                if total == Some(have) {
                    return Ok(have);
                }
                let _ = tokio::fs::remove_file(part).await;
                return Err(AtlatlError::transient(format!(
                    "GET {shown}: range not satisfiable; restarting"
                )));
            }
            s if s.is_success() => false,
            s => {
                let class = classify_http_status(s.as_u16()).unwrap_or(ErrorClass::Transient);
                let headers: Vec<(String, String)> = resp
                    .headers()
                    .iter()
                    .filter_map(|(k, v)| Some((k.as_str().to_owned(), v.to_str().ok()?.to_owned())))
                    .collect();
                let hint = wait_hint(
                    headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                    SystemTime::now(),
                );
                return Err(AtlatlError::new(class, format!("GET {shown}: HTTP {s}"))
                    .code(s.as_u16().to_string())
                    .retry_after(hint));
            }
        };
        if !append {
            let v = resp
                .headers()
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .filter(|e| !e.starts_with("W/"))
                .or_else(|| {
                    resp.headers()
                        .get(LAST_MODIFIED)
                        .and_then(|v| v.to_str().ok())
                })
                .map(str::to_owned);
            let mut s = state
                .lock()
                .map_err(|_| AtlatlError::internal("download state poisoned"))?;
            s.validator = v;
        } else if let Ok(mut s) = state.lock() {
            s.resumed += 1;
        }
        let total = if append {
            resp.headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(range_total)
        } else {
            resp.headers()
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
        };
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(part)
            .await
            .map_err(|e| crate::error::io_at(part, e))?;
        let mut n = if append { have } else { 0 };
        let body = async {
            while let Some(chunk) = resp.chunk().await.map_err(|e| reqwest_err(shown, e))? {
                f.write_all(&chunk)
                    .await
                    .map_err(|e| crate::error::io_at(part, e))?;
                n += chunk.len() as u64;
            }
            Ok::<_, AtlatlError>(())
        }
        .await;
        f.flush().await.map_err(|e| crate::error::io_at(part, e))?;
        f.sync_all()
            .await
            .map_err(|e| crate::error::io_at(part, e))?;
        body?;
        if let Some(t) = total
            && t != n
        {
            return Err(AtlatlError::transient(format!(
                "GET {shown}: body ended at {n} of {t} bytes"
            )));
        }
        Ok(n)
    }
}

#[derive(Clone, Debug, Default)]
struct State {
    attempts: u32,
    resumed: u32,
    validator: Option<String>,
}

/// `bytes 100-199/200` → 100.
fn range_start(v: &str) -> Option<u64> {
    v.strip_prefix("bytes ")?
        .split('-')
        .next()?
        .trim()
        .parse()
        .ok()
}

/// `bytes 100-199/200` or `bytes */200` → 200.
fn range_total(v: &str) -> Option<u64> {
    v.rsplit('/').next()?.trim().parse().ok()
}

fn reqwest_err(url: &str, e: reqwest::Error) -> AtlatlError {
    let class = if e.is_timeout() {
        ErrorClass::Timeout
    } else if e.is_builder() {
        ErrorClass::Config
    } else if e.is_connect() || e.is_request() || e.is_body() || e.is_decode() {
        ErrorClass::Transient
    } else {
        ErrorClass::Internal
    };
    let mut causes = String::new();
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        causes.push_str(&format!(": {s}"));
        src = s.source();
    }
    let msg = e.without_url().to_string();
    AtlatlError::new(class, format!("GET {url}: {msg}{causes}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(range_start("bytes 100-199/200"), Some(100));
        assert_eq!(range_total("bytes 100-199/200"), Some(200));
        assert_eq!(range_total("bytes */200"), Some(200));
        assert_eq!(range_total("bytes 0-1/*"), None);
        assert_eq!(
            display_url("https://h/p/f.zip?X-Amz-Signature=abc"),
            "https://h/p/f.zip"
        );
    }
}
