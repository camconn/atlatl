//! An S3-compatible store (RustFS) and a minimal SigV4 client for bucket setup.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::container::Container;

pub const RUSTFS_IMAGE: &str = "rustfs/rustfs:1.0.0";

#[derive(Debug)]
pub struct S3 {
    /// `host:port`, no scheme (DuckDB secret `ENDPOINT`).
    pub endpoint: String,
    pub key_id: String,
    pub secret: String,
    pub region: String,
    _container: Option<Container>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hmac(key: &[u8], msg: &str) -> Vec<u8> {
    let mut m = <Hmac<Sha256>>::new_from_slice(key).expect("hmac key");
    m.update(msg.as_bytes());
    m.finalize().into_bytes().to_vec()
}

fn amz_date(t: SystemTime) -> (String, String) {
    // httpdate gives "Sun, 06 Nov 1994 08:49:37 GMT"; rebuild as 19941106T084937Z.
    let s = httpdate::fmt_http_date(t);
    let p: Vec<&str> = s.split_whitespace().collect();
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|m| *m == p[2])
    .map(|i| i + 1)
    .unwrap_or(1);
    let date = format!("{}{month:02}{}", p[3], p[1]);
    (format!("{date}T{}Z", p[4].replace(':', "")), date)
}

impl S3 {
    fn start() -> Result<Self, String> {
        if let Ok(ep) = std::env::var("ATLATL_TEST_S3_ENDPOINT") {
            return Ok(Self {
                endpoint: ep,
                key_id: std::env::var("ATLATL_TEST_S3_KEY").unwrap_or_default(),
                secret: std::env::var("ATLATL_TEST_S3_SECRET").unwrap_or_default(),
                region: "us-east-1".into(),
                _container: None,
            });
        }
        let c = Container::start(
            RUSTFS_IMAGE,
            9000,
            &[
                ("RUSTFS_ACCESS_KEY", "atlatl"),
                ("RUSTFS_SECRET_KEY", "atlatl-secret"),
            ],
            &["/data:rw,mode=1777"],
            &[],
        )?;
        let s3 = Self {
            endpoint: format!("{}:{}", c.host, c.port),
            key_id: "atlatl".into(),
            secret: "atlatl-secret".into(),
            region: "us-east-1".into(),
            _container: None,
        };
        c.wait_for("rustfs", Duration::from_secs(60), || {
            s3.request("GET", "/", "").is_ok_and(|(st, _)| st == 200)
        })?;
        Ok(Self {
            _container: Some(c),
            ..s3
        })
    }

    /// One signed HTTP/1.1 request; returns (status, body).
    pub fn request(&self, method: &str, path: &str, body: &str) -> Result<(u16, String), String> {
        self.request_bytes(method, path, body.as_bytes())
    }

    /// [`request`](Self::request) with a binary body (archives, Parquet).
    pub fn request_bytes(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<(u16, String), String> {
        let (stamp, date) = amz_date(SystemTime::now());
        let payload = hex(&Sha256::digest(body));
        let canonical = format!(
            "{method}\n{path}\n\nhost:{}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}",
            self.endpoint
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        let k = hmac(
            &hmac(
                &hmac(
                    &hmac(format!("AWS4{}", self.secret).as_bytes(), &date),
                    &self.region,
                ),
                "s3",
            ),
            "aws4_request",
        );
        let sig = hex(&hmac(&k, &to_sign));
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}",
            self.key_id
        );
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nx-amz-date: {stamp}\r\nx-amz-content-sha256: {payload}\r\nAuthorization: {auth}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.endpoint,
            body.len()
        );
        let mut s = TcpStream::connect(&self.endpoint).map_err(|e| e.to_string())?;
        s.set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
        s.write_all(body).map_err(|e| e.to_string())?;
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

    /// Creates a new bucket and returns its name.
    pub fn fresh_bucket(&self) -> Result<String, String> {
        let name = crate::unique("b").replace('_', "-");
        match self.request("PUT", &format!("/{name}"), "")? {
            (200, _) => Ok(name),
            (st, body) => Err(format!("create bucket {name}: HTTP {st}: {body}")),
        }
    }

    /// `CREATE SECRET` SQL for DuckDB's httpfs scoped to `bucket`.
    pub fn duckdb_secret(&self, name: &str, bucket: &str) -> String {
        format!(
            "CREATE OR REPLACE SECRET {name} (TYPE s3, KEY_ID '{}', SECRET '{}', REGION '{}', ENDPOINT '{}', URL_STYLE 'path', USE_SSL false, SCOPE 's3://{bucket}')",
            self.key_id, self.secret, self.region, self.endpoint
        )
    }
}

static RUSTFS: OnceLock<Result<S3, String>> = OnceLock::new();

pub fn rustfs() -> &'static S3 {
    RUSTFS
        .get_or_init(S3::start)
        .as_ref()
        .unwrap_or_else(|e| panic!("rustfs: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amz_dates() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(amz_date(t), ("20231114T221320Z".into(), "20231114".into()));
    }
}
