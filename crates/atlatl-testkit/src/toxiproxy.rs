//! Toxiproxy (12 §3): TCP proxies between atlatl and a service, with network faults added and
//! removed through its HTTP API. The container runs on the host network so it can reach other
//! containers' published loopback ports.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;
use std::time::Duration;

use crate::container::{Container, Postgres};

pub const TOXIPROXY_IMAGE: &str = "ghcr.io/shopify/toxiproxy:2.12.0";

#[derive(Debug)]
pub struct Toxiproxy {
    /// `host:port` of the API.
    pub api: String,
    _container: Container,
}

/// A proxy listening on `host:port`, forwarding to its upstream. Deleted on drop.
#[derive(Debug)]
pub struct Proxy<'a> {
    tp: &'a Toxiproxy,
    pub name: String,
    pub host: String,
    pub port: u16,
}

fn free_port() -> Result<u16, String> {
    let l = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    Ok(l.local_addr().map_err(|e| e.to_string())?.port())
}

impl Toxiproxy {
    fn start() -> Result<Self, String> {
        let port = free_port()?;
        let p = port.to_string();
        let c = Container::start_host_network(
            TOXIPROXY_IMAGE,
            port,
            &["-host", "127.0.0.1", "-port", &p],
        )?;
        let tp = Self {
            api: format!("127.0.0.1:{port}"),
            _container: c,
        };
        tp._container
            .wait_for("toxiproxy", Duration::from_secs(30), || {
                tp.request("GET", "/version", "")
                    .is_ok_and(|(st, _)| st == 200)
            })?;
        Ok(tp)
    }

    /// One HTTP/1.1 request to the API; returns (status, body).
    pub fn request(&self, method: &str, path: &str, body: &str) -> Result<(u16, String), String> {
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.api,
            body.len()
        );
        let mut s = TcpStream::connect(&self.api).map_err(|e| e.to_string())?;
        s.set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
        let mut resp = String::new();
        s.read_to_string(&mut resp).map_err(|e| e.to_string())?;
        let status = resp
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("bad response: {resp}"))?;
        let body = resp
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_owned())
            .unwrap_or_default();
        Ok((status, body))
    }

    fn ok(&self, method: &str, path: &str, body: &str) -> Result<String, String> {
        match self.request(method, path, body)? {
            (200..=299, b) => Ok(b),
            (st, b) => Err(format!("toxiproxy {method} {path}: HTTP {st}: {b}")),
        }
    }

    /// A new proxy on a free loopback port forwarding to `upstream` (`host:port`).
    pub fn proxy(&self, upstream: &str) -> Result<Proxy<'_>, String> {
        let name = crate::unique("p");
        let body = format!(
            r#"{{"name":"{name}","listen":"127.0.0.1:0","upstream":"{upstream}","enabled":true}}"#
        );
        let resp = self.ok("POST", "/proxies", &body)?;
        // {"name":…,"listen":"127.0.0.1:34567",…}
        let listen = resp
            .split(r#""listen":""#)
            .nth(1)
            .and_then(|r| r.split('"').next())
            .ok_or_else(|| format!("no listen address in {resp}"))?;
        let (host, port) = listen
            .rsplit_once(':')
            .ok_or_else(|| format!("bad listen address {listen}"))?;
        Ok(Proxy {
            tp: self,
            name,
            host: host.to_owned(),
            port: port.parse().map_err(|e| format!("{e}"))?,
        })
    }

    /// A proxy in front of `pg`.
    pub fn postgres(&self, pg: &Postgres) -> Result<Proxy<'_>, String> {
        self.proxy(&format!("{}:{}", pg.host, pg.port))
    }
}

impl Proxy<'_> {
    /// Adds toxic `name` of `kind` (`latency`, `timeout`, `reset_peer`, `bandwidth`,
    /// `slow_close`, `limit_data`, …) on `stream` (`upstream` / `downstream`); `attributes` is
    /// a JSON object.
    pub fn toxic(
        &self,
        name: &str,
        kind: &str,
        stream: &str,
        attributes: &str,
    ) -> Result<(), String> {
        let body = format!(
            r#"{{"name":"{name}","type":"{kind}","stream":"{stream}","toxicity":1.0,"attributes":{attributes}}}"#
        );
        self.tp
            .ok("POST", &format!("/proxies/{}/toxics", self.name), &body)
            .map(drop)
    }

    pub fn remove_toxic(&self, name: &str) -> Result<(), String> {
        self.tp
            .ok(
                "DELETE",
                &format!("/proxies/{}/toxics/{name}", self.name),
                "",
            )
            .map(drop)
    }

    /// Adds `ms` of latency to responses.
    pub fn latency(&self, ms: u64) -> Result<(), String> {
        self.toxic(
            "latency",
            "latency",
            "downstream",
            &format!(r#"{{"latency":{ms},"jitter":0}}"#),
        )
    }

    /// Stops all data and closes the connection after `ms` (0: never closes, data is dropped).
    pub fn timeout(&self, ms: u64) -> Result<(), String> {
        self.toxic(
            "timeout",
            "timeout",
            "upstream",
            &format!(r#"{{"timeout":{ms}}}"#),
        )
    }

    /// Resets connections (TCP RST) `after_ms` after data arrives.
    pub fn reset_peer(&self, after_ms: u64) -> Result<(), String> {
        self.toxic(
            "reset_peer",
            "reset_peer",
            "upstream",
            &format!(r#"{{"timeout":{after_ms}}}"#),
        )
    }

    /// Disabling closes every open connection and refuses new ones until re-enabled.
    pub fn set_enabled(&self, on: bool) -> Result<(), String> {
        self.tp
            .ok(
                "POST",
                &format!("/proxies/{}", self.name),
                &format!(r#"{{"enabled":{on}}}"#),
            )
            .map(drop)
    }
}

impl Drop for Proxy<'_> {
    fn drop(&mut self) {
        let _ = self
            .tp
            .request("DELETE", &format!("/proxies/{}", self.name), "");
    }
}

static TOXIPROXY: OnceLock<Result<Toxiproxy, String>> = OnceLock::new();

/// A shared toxiproxy server. Panics with the reason if it can't start.
pub fn toxiproxy() -> &'static Toxiproxy {
    TOXIPROXY
        .get_or_init(Toxiproxy::start)
        .as_ref()
        .unwrap_or_else(|e| panic!("toxiproxy: {e}"))
}
