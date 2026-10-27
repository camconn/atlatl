//! In-process HTTP/1.1 mock server. Each connection serves one scripted reply and closes, so
//! replies can be malformed on purpose (truncated bodies, no Content-Type, hang-ups).

#![allow(dead_code)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use atlatl_http::{ApiClient, ApiSpec, ClientOptions};
use indexmap::IndexMap;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct Req {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub at: Instant,
}

impl Req {
    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or_default()
    }

    pub fn query(&self) -> &str {
        self.target.split_once('?').map(|x| x.1).unwrap_or_default()
    }

    pub fn param(&self, k: &str) -> Option<String> {
        reqwest::Url::parse(&format!("http://x{}", self.target))
            .ok()?
            .query_pairs()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.into_owned())
    }

    pub fn header(&self, k: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Clone, Debug)]
pub enum Reply {
    Full {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// Claims `claimed` bytes but sends `body` and closes.
    Truncated {
        status: u16,
        body: Vec<u8>,
        claimed: usize,
    },
}

impl Reply {
    pub fn json(status: u16, body: &str) -> Self {
        Self::raw(status, Some("application/json"), body)
    }

    pub fn raw(status: u16, content_type: Option<&str>, body: &str) -> Self {
        Reply::Full {
            status,
            headers: content_type
                .map(|c| vec![("Content-Type".to_owned(), c.to_owned())])
                .unwrap_or_default(),
            body: body.as_bytes().to_vec(),
        }
    }

    pub fn header(mut self, k: &str, v: &str) -> Self {
        if let Reply::Full { headers, .. } = &mut self {
            headers.push((k.to_owned(), v.to_owned()));
        }
        self
    }
}

type Handler = dyn Fn(&Req, usize) -> Reply + Send + Sync;

pub struct Mock {
    pub url: String,
    log: Arc<Mutex<Vec<Req>>>,
}

impl Mock {
    pub async fn start(f: impl Fn(&Req, usize) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let log: Arc<Mutex<Vec<Req>>> = Arc::default();
        let handler: Arc<Handler> = Arc::new(f);
        let l = log.clone();
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                let (l, h) = (l.clone(), handler.clone());
                tokio::spawn(async move {
                    let _ = serve(sock, l, h).await;
                });
            }
        });
        Self { url, log }
    }

    pub fn requests(&self) -> Vec<Req> {
        self.log.lock().expect("log").clone()
    }

    pub fn paths(&self) -> Vec<String> {
        self.requests().into_iter().map(|r| r.target).collect()
    }
}

async fn serve(
    sock: tokio::net::TcpStream,
    log: Arc<Mutex<Vec<Req>>>,
    handler: Arc<Handler>,
) -> std::io::Result<()> {
    let mut r = BufReader::new(sock);
    let mut line = String::new();
    r.read_line(&mut line).await?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        r.read_line(&mut h).await?;
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_owned(), v.trim().to_owned()));
        }
    }
    let len = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0usize);
    let mut body = vec![0; len];
    r.read_exact(&mut body).await?;
    let req = Req {
        method,
        target,
        headers,
        body,
        at: Instant::now(),
    };
    let n = {
        let mut l = log.lock().expect("log");
        l.push(req.clone());
        l.len() - 1
    };
    let reply = handler(&req, n);
    let mut out = Vec::new();
    match reply {
        Reply::Full {
            status,
            headers,
            body,
        } => {
            out.extend(format!("HTTP/1.1 {status} X\r\n").as_bytes());
            for (k, v) in headers {
                out.extend(format!("{k}: {v}\r\n").as_bytes());
            }
            out.extend(
                format!(
                    "Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            );
            out.extend(body);
        }
        Reply::Truncated {
            status,
            body,
            claimed,
        } => {
            out.extend(format!("HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {claimed}\r\nConnection: close\r\n\r\n").as_bytes());
            out.extend(body);
        }
    }
    let mut sock = r.into_inner();
    sock.write_all(&out).await?;
    sock.flush().await?;
    sock.shutdown().await
}

pub fn spec(yaml: &str) -> Arc<ApiSpec> {
    let node = atlatl_core::config::node::parse_yaml(yaml, Arc::from(Path::new("apis/test.yaml")))
        .expect("yaml");
    Arc::new(ApiSpec::parse(&node).unwrap_or_else(|e| panic!("{e}")))
}

/// No timeouts (tests run on a paused clock) and deterministic jitter.
pub fn opts() -> ClientOptions {
    ClientOptions {
        timeout: None,
        connect_timeout: None,
        jitter: Some(1.0),
        clock: atlatl_http::funcs::fixed_clock("2026-09-28T12:00:00Z"),
        ..Default::default()
    }
}

pub fn client(yaml: &str, secrets: &[(&str, &str)]) -> ApiClient {
    let secrets: IndexMap<String, String> = secrets
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    ApiClient::new(spec(yaml), secrets, opts()).expect("client")
}

pub fn values(recs: &[atlatl_http::Record]) -> Vec<String> {
    recs.iter().map(|r| r.record.to_string()).collect()
}
