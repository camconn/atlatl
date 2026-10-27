//! reqwest (rustls) transport. Responses are captured whole, with a body cap, before any rule or
//! decoder runs; a failure mid-body is `transient`.

use std::time::Duration;

use atlatl_core::{AtlatlError, ErrorClass};
use tokio::time::Instant;

use crate::request::Prepared;
use crate::response::Captured;

#[derive(Clone, Debug)]
pub struct Transport {
    http: reqwest::Client,
    cassette: Option<crate::cassette::Cassette>,
}

impl Transport {
    pub fn new(user_agent: &str, connect_timeout: Option<Duration>) -> Result<Self, AtlatlError> {
        let mut b = atlatl_core::tls::http_client().user_agent(user_agent);
        if let Some(t) = connect_timeout {
            b = b.connect_timeout(t);
        }
        Ok(Self {
            http: b
                .build()
                .map_err(|e| AtlatlError::internal(format!("http client: {e}")))?,
            cassette: None,
        })
    }

    /// Records through, or replays from, a cassette.
    pub fn with_cassette(mut self, c: Option<crate::cassette::Cassette>) -> Self {
        self.cassette = c;
        self
    }

    pub async fn send(&self, req: &Prepared, max_body: u64) -> Result<Captured, AtlatlError> {
        match &self.cassette {
            Some(c) => c.send(self, req, max_body).await,
            None => self.send_live(req, max_body).await,
        }
    }

    pub(crate) async fn send_live(
        &self,
        req: &Prepared,
        max_body: u64,
    ) -> Result<Captured, AtlatlError> {
        let url = atlatl_core::redact::redact(req.url.as_str()).into_owned();
        let method = reqwest::Method::from_bytes(req.method.as_str().as_bytes())
            .map_err(|e| AtlatlError::internal(e.to_string()))?;
        let mut rb = self.http.request(method, req.url.clone());
        let mut has_ct = false;
        for (k, v) in &req.headers {
            has_ct |= k.eq_ignore_ascii_case("content-type");
            rb = rb.header(k.as_str(), v.as_str());
        }
        if let Some(b) = &req.body {
            if !has_ct {
                rb = rb.header("content-type", b.content_type);
            }
            rb = rb.body(b.bytes.clone());
        }
        if let Some(t) = req.timeout {
            rb = rb.timeout(t);
        }
        let t0 = Instant::now();
        let mut resp = rb.send().await.map_err(|e| transport_error(e, &url))?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(v.as_bytes()).into_owned(),
                )
            })
            .collect();
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(|e| transport_error(e, &url))? {
            if (body.len() + chunk.len()) as u64 > max_body {
                return Err(AtlatlError::data(format!(
                    "response body from {url} exceeds max_body ({max_body} bytes)"
                ))
                .code("body_too_large"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Captured {
            status,
            headers,
            body,
            elapsed: t0.elapsed(),
            url,
        })
    }
}

fn transport_error(e: reqwest::Error, url: &str) -> AtlatlError {
    let e = e.without_url();
    let class = if e.is_timeout() {
        ErrorClass::Timeout
    } else if e.is_builder() {
        ErrorClass::Config
    } else {
        ErrorClass::Transient
    };
    let mut msg = e.to_string();
    let mut src: Option<&dyn std::error::Error> = std::error::Error::source(&e);
    while let Some(s) = src {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        src = s.source();
    }
    AtlatlError::new(class, format!("request to {url} failed: {msg}"))
}
