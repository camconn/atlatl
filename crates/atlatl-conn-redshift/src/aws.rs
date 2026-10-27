//! Signed AWS requests over reqwest.

use std::time::Duration;

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use chrono::Utc;

use crate::creds::Credentials;
use crate::sigv4::{self, Canonical};

/// A service endpoint: `scheme://authority`, without a path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub base: String,
    /// `host[:port]` as sent in `Host` (default ports dropped).
    pub host: String,
}

impl Endpoint {
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.trim().trim_end_matches('/');
        let (scheme, rest) = url.split_once("://").ok_or_else(|| {
            AtlatlError::config(format!("endpoint `{url}` needs http:// or https://"))
        })?;
        if !matches!(scheme, "http" | "https") || rest.is_empty() || rest.contains('/') {
            return Err(AtlatlError::config(format!(
                "endpoint `{url}` must be http(s)://host[:port]"
            )));
        }
        let host = match (scheme, rest.rsplit_once(':')) {
            ("https", Some((h, "443"))) | ("http", Some((h, "80"))) => h.to_owned(),
            _ => rest.to_owned(),
        };
        Ok(Self {
            base: format!("{scheme}://{rest}"),
            host,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One signed request (no retry). `path` is already encoded.
pub struct Signed<'a> {
    pub service: &'a str,
    pub region: &'a str,
    pub method: &'a str,
    pub endpoint: &'a Endpoint,
    pub path: &'a str,
    pub query: &'a [(String, String)],
    pub headers: &'a [(String, String)],
    pub body: Vec<u8>,
    /// Send `x-amz-content-sha256` (S3).
    pub content_sha: bool,
}

pub async fn send(http: &reqwest::Client, creds: &Credentials, r: Signed<'_>) -> Result<Response> {
    let payload = sigv4::sha256_hex(&r.body);
    let mut headers: Vec<(String, String)> = vec![("host".into(), r.endpoint.host.clone())];
    headers.extend(
        r.headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone())),
    );
    if r.content_sha {
        headers.push(("x-amz-content-sha256".into(), payload.clone()));
    }
    let c = Canonical {
        method: r.method,
        path: r.path,
        query: r.query,
        headers: &headers,
        payload_hash: &payload,
    };
    let add = sigv4::sign(creds, r.region, r.service, Utc::now(), &c);
    let qs = sigv4::canonical_query(r.query);
    let url = if qs.is_empty() {
        format!("{}{}", r.endpoint.base, r.path)
    } else {
        format!("{}{}?{qs}", r.endpoint.base, r.path)
    };
    let method = reqwest::Method::from_bytes(r.method.as_bytes())
        .map_err(|e| AtlatlError::internal(format!("bad method {}: {e}", r.method)))?;
    let mut req = http.request(method, &url);
    for (k, v) in headers.iter().chain(&add) {
        if k != "host" {
            req = req.header(k.as_str(), v.as_str());
        }
    }
    let resp = req.body(r.body).send().await.map_err(transport)?;
    let status = resp.status().as_u16();
    let headers = resp
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                v.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    let body = resp.bytes().await.map_err(transport)?.to_vec();
    Ok(Response {
        status,
        headers,
        body,
    })
}

pub fn transport(e: reqwest::Error) -> AtlatlError {
    let class = if e.is_timeout() {
        ErrorClass::Timeout
    } else if e.is_builder() {
        ErrorClass::Config
    } else {
        ErrorClass::Transient
    };
    let mut m = e.to_string();
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        m.push_str(&format!(": {s}"));
        src = s.source();
    }
    AtlatlError::new(class, atlatl_core::redact::redact(&m).into_owned())
}

pub fn client(timeout: Duration) -> Result<reqwest::Client> {
    atlatl_core::tls::http_client()
        .connect_timeout(Duration::from_secs(10))
        .timeout(timeout)
        .build()
        .map_err(|e| AtlatlError::internal(format!("http client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        let e = Endpoint::parse("https://redshift-data.us-east-1.amazonaws.com:443/").expect("ep");
        assert_eq!(e.host, "redshift-data.us-east-1.amazonaws.com");
        assert_eq!(e.base, "https://redshift-data.us-east-1.amazonaws.com:443");
        let e = Endpoint::parse("http://127.0.0.1:9000").expect("ep");
        assert_eq!(e.host, "127.0.0.1:9000");
        assert!(Endpoint::parse("localhost:9000").is_err());
        assert!(Endpoint::parse("http://h/p").is_err());
    }
}
