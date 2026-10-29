//! A `replication=database` connection: startup (optionally over TLS), auth, simple query and
//! COPY BOTH. Frontend encoding and SCRAM come from `postgres-protocol`; framing is ours because
//! it has no CopyBothResponse. Reads are buffered so `recv` is cancel-safe.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use atlatl_conn_postgres::ConnectOptions;
use atlatl_conn_postgres::classify_sqlstate;
use atlatl_core::config::model::SslMode;
use atlatl_core::error::{Result, classify_io};
use atlatl_core::redact::redact;
use atlatl_core::{AtlatlError, ErrorClass};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::authentication::{md5_hash, sasl};
use postgres_protocol::message::frontend;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::lsn::Lsn;

const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
const MAX_MESSAGE: usize = 1 << 30;

trait Io: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> Io for T {}

pub(crate) fn io_err(what: &str, e: std::io::Error) -> AtlatlError {
    let class = match classify_io(&e) {
        ErrorClass::Internal => ErrorClass::Transient,
        c => c,
    };
    AtlatlError::new(class, format!("replication connection: {what}: {e}"))
}

pub(crate) fn server_error(mut b: Bytes) -> AtlatlError {
    let (mut code, mut message, mut detail) = (String::new(), String::new(), None);
    while b.has_remaining() {
        let field = b.get_u8();
        if field == 0 {
            break;
        }
        let v = cstr_lossy(&mut b);
        match field {
            b'C' => code = v,
            b'M' => message = v,
            b'D' => detail = Some(v),
            _ => {}
        }
    }
    let class = match code.as_str() {
        // object_in_use: the slot is still held by another walsender.
        "55006" => ErrorClass::Conflict,
        c => classify_sqlstate(c),
    };
    let mut m = format!("{code}: {message}");
    if let Some(d) = detail {
        m.push_str(&format!("; detail: {d}"));
    }
    AtlatlError::new(class, redact(&m).into_owned()).code(code)
}

fn cstr_lossy(b: &mut Bytes) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    let s = String::from_utf8_lossy(&b[..end]).into_owned();
    b.advance((end + 1).min(b.len()));
    s
}

fn proto(m: impl Into<String>) -> AtlatlError {
    AtlatlError::internal(format!("replication protocol: {}", m.into()))
}

pub(crate) enum Repl {
    XLogData { data: Bytes },
    Keepalive { wal_end: Lsn, reply: bool },
}

pub(crate) struct ReplConn {
    io: Box<dyn Io>,
    rbuf: BytesMut,
    out: BytesMut,
    pub server_version: u32,
}

/// `server_version` as `major * 10000 + minor` (`12.22` → 120022, `16.4` → 160004).
fn parse_version(v: &str) -> u32 {
    let mut it = v
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u32>().unwrap_or(0));
    let major = it.next().unwrap_or(0);
    let minor = it.next().unwrap_or(0);
    major * 10000 + minor
}

impl ReplConn {
    pub async fn connect(o: &ConnectOptions, tls: Option<&Arc<ClientConfig>>) -> Result<Self> {
        let fut = Self::connect_inner(o, tls);
        match o.connect_timeout {
            Some(t) => tokio::time::timeout(t, fut).await.map_err(|_| {
                AtlatlError::new(
                    ErrorClass::Timeout,
                    format!(
                        "replication connection to {}:{} timed out after {t:?}",
                        o.host, o.port
                    ),
                )
            })?,
            None => fut.await,
        }
    }

    async fn connect_inner(o: &ConnectOptions, tls: Option<&Arc<ClientConfig>>) -> Result<Self> {
        let mut tcp = TcpStream::connect((o.host.as_str(), o.port))
            .await
            .map_err(|e| io_err("connect", e))?;
        tcp.set_nodelay(true).map_err(|e| io_err("nodelay", e))?;
        let io: Box<dyn Io> = match (o.sslmode, tls) {
            (SslMode::Disable, _) => Box::new(tcp),
            (_, None) => return Err(AtlatlError::internal("sslmode needs a TLS config")),
            (_, Some(cfg)) => {
                let mut req = BytesMut::new();
                frontend::ssl_request(&mut req);
                tcp.write_all(&req)
                    .await
                    .map_err(|e| io_err("SSLRequest", e))?;
                let answer = tcp.read_u8().await.map_err(|e| io_err("SSLRequest", e))?;
                if answer != b'S' {
                    return Err(AtlatlError::config(format!(
                        "{}:{} does not accept TLS (sslmode {:?})",
                        o.host, o.port, o.sslmode
                    )));
                }
                let name = ServerName::try_from(o.host.clone())
                    .map_err(|e| AtlatlError::config(format!("TLS server name: {e}")))?;
                let s = TlsConnector::from(cfg.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|e| io_err("TLS handshake", e))?;
                Box::new(s)
            }
        };
        let mut c = Self {
            io,
            rbuf: BytesMut::with_capacity(128 * 1024),
            out: BytesMut::new(),
            server_version: 0,
        };
        // The walsender's type output functions render the text values; pin their GUCs.
        frontend::startup_message(
            [
                ("user", o.user.as_str()),
                ("database", o.database.as_str()),
                ("replication", "database"),
                ("application_name", o.application_name.as_str()),
                ("client_encoding", "UTF8"),
                ("TimeZone", "UTC"),
                ("DateStyle", "ISO, MDY"),
                ("IntervalStyle", "postgres"),
                ("extra_float_digits", "3"),
                ("bytea_output", "hex"),
                ("lc_monetary", "C"),
            ],
            &mut c.out,
        )
        .map_err(|e| io_err("startup", e))?;
        c.flush().await?;
        c.authenticate(o).await?;
        Ok(c)
    }

    async fn authenticate(&mut self, o: &ConnectOptions) -> Result<()> {
        let password = o.password.clone().unwrap_or_default();
        let auth = |m: String| AtlatlError::new(ErrorClass::Auth, m);
        let mut scram: Option<sasl::ScramSha256> = None;
        loop {
            let (tag, mut b) = self.read().await?;
            match tag {
                b'R' => {
                    let kind = get_i32(&mut b)?;
                    match kind {
                        0 => {}
                        3 => {
                            frontend::password_message(password.as_bytes(), &mut self.out)
                                .map_err(|e| io_err("auth", e))?;
                            self.flush().await?;
                        }
                        5 => {
                            if b.remaining() < 4 {
                                return Err(proto("short MD5 salt"));
                            }
                            let salt = [b.get_u8(), b.get_u8(), b.get_u8(), b.get_u8()];
                            let h = md5_hash(o.user.as_bytes(), password.as_bytes(), salt);
                            frontend::password_message(h.as_bytes(), &mut self.out)
                                .map_err(|e| io_err("auth", e))?;
                            self.flush().await?;
                        }
                        10 => {
                            let mut mechs = vec![];
                            while b.has_remaining() && b[0] != 0 {
                                mechs.push(cstr_lossy(&mut b));
                            }
                            if !mechs.iter().any(|m| m == sasl::SCRAM_SHA_256) {
                                return Err(auth(format!("no SCRAM-SHA-256 in {mechs:?}")));
                            }
                            let s = sasl::ScramSha256::new(
                                password.as_bytes(),
                                sasl::ChannelBinding::unsupported(),
                            );
                            frontend::sasl_initial_response(
                                sasl::SCRAM_SHA_256,
                                s.message(),
                                &mut self.out,
                            )
                            .map_err(|e| io_err("auth", e))?;
                            self.flush().await?;
                            scram = Some(s);
                        }
                        11 => {
                            let s = scram.as_mut().ok_or_else(|| proto("SASLContinue"))?;
                            s.update(&b).map_err(|e| auth(format!("SCRAM: {e}")))?;
                            frontend::sasl_response(s.message(), &mut self.out)
                                .map_err(|e| io_err("auth", e))?;
                            self.flush().await?;
                        }
                        12 => scram
                            .as_mut()
                            .ok_or_else(|| proto("SASLFinal"))?
                            .finish(&b)
                            .map_err(|e| auth(format!("SCRAM: {e}")))?,
                        n => return Err(auth(format!("unsupported auth request {n}"))),
                    }
                }
                b'S' => {
                    let k = cstr_lossy(&mut b);
                    let v = cstr_lossy(&mut b);
                    if k == "server_version" {
                        self.server_version = parse_version(&v);
                    }
                }
                b'E' => return Err(server_error(b)),
                b'Z' => return Ok(()),
                _ => {}
            }
        }
    }

    async fn flush(&mut self) -> Result<()> {
        let buf = self.out.split();
        self.io
            .write_all(&buf)
            .await
            .map_err(|e| io_err("write", e))?;
        self.io.flush().await.map_err(|e| io_err("write", e))
    }

    /// One backend message. Cancel-safe: partial reads stay in `rbuf`.
    async fn read(&mut self) -> Result<(u8, Bytes)> {
        loop {
            if self.rbuf.len() >= 5 {
                let len =
                    i32::from_be_bytes([self.rbuf[1], self.rbuf[2], self.rbuf[3], self.rbuf[4]]);
                if !(4..=MAX_MESSAGE as i32).contains(&len) {
                    return Err(proto(format!("bad message length {len}")));
                }
                let total = 1 + len as usize;
                if self.rbuf.len() >= total {
                    let mut m = self.rbuf.split_to(total).freeze();
                    let tag = m.get_u8();
                    m.advance(4);
                    return Ok((tag, m));
                }
                self.rbuf.reserve(total - self.rbuf.len());
            }
            let n = self
                .io
                .read_buf(&mut self.rbuf)
                .await
                .map_err(|e| io_err("read", e))?;
            if n == 0 {
                return Err(AtlatlError::transient(
                    "replication connection closed by the server",
                ));
            }
        }
    }

    /// Simple query; text rows. Replication commands and plain SQL go through here.
    pub async fn query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        frontend::query(sql, &mut self.out).map_err(|e| io_err("query", e))?;
        self.flush().await?;
        let (mut rows, mut err) = (vec![], None);
        loop {
            let (tag, mut b) = self.read().await?;
            match tag {
                b'D' => {
                    let n = get_i16(&mut b)?;
                    let mut row = Vec::with_capacity(n.max(0) as usize);
                    for _ in 0..n {
                        let len = get_i32(&mut b)?;
                        if len < 0 {
                            row.push(None);
                            continue;
                        }
                        let v = take(&mut b, len as usize)?;
                        row.push(Some(String::from_utf8_lossy(&v).into_owned()));
                    }
                    rows.push(row);
                }
                b'E' => err = Some(server_error(b)),
                b'Z' => return err.map_or(Ok(rows), Err),
                _ => {}
            }
        }
    }

    /// `START_REPLICATION SLOT … LOGICAL <lsn> (<options>)`; the connection is then in COPY BOTH.
    pub async fn start(&mut self, slot: &str, lsn: Lsn, options: &str) -> Result<()> {
        let sql = format!("START_REPLICATION SLOT {slot} LOGICAL {lsn} ({options})");
        frontend::query(&sql, &mut self.out).map_err(|e| io_err("start", e))?;
        self.flush().await?;
        loop {
            let (tag, b) = self.read().await?;
            match tag {
                b'W' => return Ok(()),
                b'E' => {
                    let e = server_error(b);
                    while self.read().await?.0 != b'Z' {}
                    return Err(e);
                }
                _ => {}
            }
        }
    }

    /// The next replication message. Cancel-safe.
    pub async fn recv(&mut self) -> Result<Repl> {
        loop {
            let (tag, mut b) = self.read().await?;
            match tag {
                b'd' => {
                    let kind = get_u8(&mut b)?;
                    match kind {
                        b'w' => {
                            let _start = get_u64(&mut b)?;
                            let _end = get_u64(&mut b)?;
                            let _time = get_u64(&mut b)?;
                            return Ok(Repl::XLogData { data: b });
                        }
                        b'k' => {
                            let wal_end = Lsn(get_u64(&mut b)?);
                            let _time = get_u64(&mut b)?;
                            let reply = get_u8(&mut b)? == 1;
                            return Ok(Repl::Keepalive { wal_end, reply });
                        }
                        t => return Err(proto(format!("unknown CopyData kind {t}"))),
                    }
                }
                b'c' => return Err(AtlatlError::transient("the server ended replication")),
                b'E' => return Err(server_error(b)),
                _ => {}
            }
        }
    }

    /// Standby status update; `flush` is what the slot's `confirmed_flush_lsn` advances to.
    pub async fn status(&mut self, flush: Lsn, reply: bool) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros() as i64;
        self.out.put_u8(b'd');
        self.out.put_i32(4 + 1 + 8 * 4 + 1);
        self.out.put_u8(b'r');
        self.out.put_u64(flush.0);
        self.out.put_u64(flush.0);
        self.out.put_u64(flush.0);
        self.out.put_i64(now - PG_EPOCH_MICROS);
        self.out.put_u8(reply as u8);
        self.flush().await
    }

    pub async fn terminate(mut self) -> Result<()> {
        frontend::terminate(&mut self.out);
        self.flush().await?;
        let _ = self.io.shutdown().await;
        Ok(())
    }
}

pub(crate) fn get_u8(b: &mut Bytes) -> Result<u8> {
    (b.remaining() >= 1)
        .then(|| b.get_u8())
        .ok_or_else(|| proto("short message"))
}

pub(crate) fn get_i16(b: &mut Bytes) -> Result<i16> {
    (b.remaining() >= 2)
        .then(|| b.get_i16())
        .ok_or_else(|| proto("short message"))
}

pub(crate) fn get_i32(b: &mut Bytes) -> Result<i32> {
    (b.remaining() >= 4)
        .then(|| b.get_i32())
        .ok_or_else(|| proto("short message"))
}

pub(crate) fn get_u32(b: &mut Bytes) -> Result<u32> {
    (b.remaining() >= 4)
        .then(|| b.get_u32())
        .ok_or_else(|| proto("short message"))
}

pub(crate) fn get_u64(b: &mut Bytes) -> Result<u64> {
    (b.remaining() >= 8)
        .then(|| b.get_u64())
        .ok_or_else(|| proto("short message"))
}

pub(crate) fn take(b: &mut Bytes, n: usize) -> Result<Bytes> {
    (b.remaining() >= n)
        .then(|| b.split_to(n))
        .ok_or_else(|| proto("short message"))
}

pub(crate) fn cstr(b: &mut Bytes) -> Result<String> {
    let end = b
        .iter()
        .position(|&c| c == 0)
        .ok_or_else(|| proto("unterminated string"))?;
    let s = String::from_utf8(b[..end].to_vec()).map_err(|_| proto("non-UTF-8 string"))?;
    b.advance(end + 1);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert_eq!(parse_version("12.22"), 120022);
        assert_eq!(parse_version("16.4 (Debian 16.4-1)"), 160004);
        assert_eq!(parse_version("17beta1"), 170001);
    }

    #[test]
    fn errors_are_classified() {
        let mut b = BytesMut::new();
        b.put_u8(b'C');
        b.put_slice(b"55006\0");
        b.put_u8(b'M');
        b.put_slice(b"replication slot \"x\" is active\0");
        b.put_u8(0);
        let e = server_error(b.freeze());
        assert_eq!(e.class, ErrorClass::Conflict);
        assert_eq!(e.code.as_deref(), Some("55006"));
        let mut b = BytesMut::new();
        b.put_u8(b'C');
        b.put_slice(b"42704\0");
        b.put_u8(0);
        assert_eq!(server_error(b.freeze()).class, ErrorClass::Config);
    }

    #[test]
    fn short_reads_fail_cleanly() {
        let mut b = Bytes::from_static(&[0, 1]);
        assert!(get_u32(&mut b).is_err());
        assert!(cstr(&mut Bytes::from_static(b"abc")).is_err());
        assert_eq!(cstr(&mut Bytes::from_static(b"ab\0c")).expect("cstr"), "ab");
    }
}
