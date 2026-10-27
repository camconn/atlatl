//! A libSQL server (sqld) for the Turso target. Each call starts its own server, so every test
//! gets an empty database.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::container::Container;

pub const LIBSQL_IMAGE: &str = "ghcr.io/tursodatabase/libsql-server:v0.24.33";

#[derive(Debug)]
pub struct Libsql {
    /// `http://127.0.0.1:<port>` (Hrana over HTTP, no auth).
    pub url: String,
    container: Container,
}

impl Libsql {
    pub fn start() -> Result<Self, String> {
        let c = Container::start(LIBSQL_IMAGE, 8080, &[], &["/var/lib/sqld"], &[])?;
        let addr = format!("{}:{}", c.host, c.port);
        c.wait_for("libsql-server", Duration::from_secs(60), || health(&addr))?;
        Ok(Self {
            url: format!("http://{addr}"),
            container: c,
        })
    }

    pub fn id(&self) -> &str {
        &self.container.id
    }
}

impl Drop for Libsql {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.container.id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
}

fn health(addr: &str) -> bool {
    let Ok(mut s) = TcpStream::connect(addr) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    if s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 16];
    matches!(s.read(&mut buf), Ok(n) if n >= 12 && &buf[9..12] == b"200")
}

/// A fresh libSQL server. Panics with the reason if it can't start.
pub fn libsql() -> Libsql {
    Libsql::start().unwrap_or_else(|e| panic!("libsql-server: {e}"))
}
