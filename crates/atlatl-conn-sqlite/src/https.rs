//! The libsql connector: libsql's own (`tls` feature) pins rustls 0.22, so we bring
//! tokio-rustls 0.26 with the OS trust store. HTTP/1.1 only.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::client::connect::{Connected, Connection};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

/// TCP connect plus TLS handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct HttpsConnector(TlsConnector);

impl HttpsConnector {
    pub fn new() -> Result<Self, rustls::Error> {
        let mut cfg = atlatl_core::tls::client_config()?;
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self(TlsConnector::from(Arc::new(cfg))))
    }
}

#[derive(Debug, PartialEq)]
struct Target {
    tls: bool,
    host: String,
    port: u16,
}

fn target(uri: &http::Uri) -> io::Result<Target> {
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, format!("{uri}: {m}"));
    let tls = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        _ => return Err(bad("scheme must be http or https")),
    };
    let host = uri
        .host()
        .ok_or_else(|| bad("no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
    Ok(Target { tls, host, port })
}

impl tower_service::Service<http::Uri> for HttpsConnector {
    type Response = Stream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Stream>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let tls = self.0.clone();
        Box::pin(async move {
            let t = target(&uri)?;
            let connect = async {
                let tcp = TcpStream::connect((t.host.as_str(), t.port)).await?;
                tcp.set_nodelay(true)?;
                if !t.tls {
                    return Ok(Stream::Plain(tcp));
                }
                let name = ServerName::try_from(t.host.clone())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                Ok(Stream::Tls(Box::new(tls.connect(name, tcp).await?)))
            };
            tokio::time::timeout(CONNECT_TIMEOUT, connect)
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("{}:{}: connect timed out", t.host, t.port),
                    )
                })?
        })
    }
}

pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Connection for Stream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower_service::Service;

    fn uri(s: &str) -> http::Uri {
        s.parse().expect("uri")
    }

    #[test]
    fn targets() {
        let t = |s: &str| target(&uri(s)).expect("target");
        assert_eq!(
            t("https://db.turso.io/v2"),
            Target {
                tls: true,
                host: "db.turso.io".into(),
                port: 443
            }
        );
        assert_eq!(
            t("http://127.0.0.1:8080"),
            Target {
                tls: false,
                host: "127.0.0.1".into(),
                port: 8080
            }
        );
        assert_eq!(
            t("https://[::1]:9443"),
            Target {
                tls: true,
                host: "::1".into(),
                port: 9443
            }
        );
        assert!(target(&uri("ftp://x")).is_err());
        assert!(target(&uri("/no/authority")).is_err());
    }

    #[test]
    fn offers_http1_only() {
        let c = HttpsConnector::new().expect("connector");
        assert_eq!(c.0.config().alpn_protocols, vec![b"http/1.1".to_vec()]);
    }

    #[tokio::test]
    async fn plain_http_round_trip() {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = l.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.expect("accept");
            let mut b = [0u8; 4];
            s.read_exact(&mut b).await.expect("read");
            s.write_all(&b).await.expect("write");
        });
        let mut c = HttpsConnector::new().expect("connector");
        let mut s = c
            .call(uri(&format!("http://127.0.0.1:{port}")))
            .await
            .expect("connect");
        assert!(matches!(s, Stream::Plain(_)));
        s.write_all(b"ping").await.expect("write");
        let mut b = [0u8; 4];
        s.read_exact(&mut b).await.expect("read");
        assert_eq!(&b, b"ping");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn rejects_cert_not_in_trust_store() {
        let key = KeyPair::generate().expect("key");
        let cert = CertificateParams::new(vec!["localhost".to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert.der().to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .expect("server cfg");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = l.local_addr().expect("addr").port();
        tokio::spawn(async move {
            let (s, _) = l.accept().await.expect("accept");
            let _ = acceptor.accept(s).await;
        });
        let mut c = HttpsConnector::new().expect("connector");
        let err = match c.call(uri(&format!("https://localhost:{port}"))).await {
            Ok(_) => panic!("self-signed cert accepted"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    }
}
