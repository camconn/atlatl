//! TLS against a dedicated Postgres 16 with `ssl=on` and a test CA.

use std::path::PathBuf;
use std::sync::OnceLock;

use atlatl_conn_postgres::{ConnectOptions, Pool, PoolOptions, Session, connect};
use atlatl_core::ErrorClass;
use atlatl_core::config::model::SslMode;
use atlatl_testkit::Postgres;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair};

struct TlsServer {
    pg: Postgres,
    ca: PathBuf,
    wrong_ca: PathBuf,
    _dir: tempfile::TempDir,
}

fn ca(name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut p = CertificateParams::new(Vec::<String>::new()).expect("params");
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name.push(DnType::CommonName, name);
    CertifiedIssuer::self_signed(p, KeyPair::generate().expect("key")).expect("ca")
}

fn server() -> &'static TlsServer {
    static S: OnceLock<TlsServer> = OnceLock::new();
    S.get_or_init(|| {
        let wrong = ca("someone else's CA");
        let ca = ca("atlatl test CA");
        let key = KeyPair::generate().expect("key");
        let mut p = CertificateParams::new(vec!["localhost".to_owned()]).expect("params");
        p.distinguished_name.push(DnType::CommonName, "localhost");
        let cert = p.signed_by(&key, &ca).expect("server cert");
        let pg = Postgres::builder(16)
            .file("/certs/server.crt", cert.pem())
            .file("/certs/server.key", key.serialize_pem())
            .setting("ssl", "on")
            .setting("ssl_cert_file", "/certs/server.crt")
            .setting("ssl_key_file", "/certs/server.key")
            .start()
            .expect("tls postgres");
        let dir = tempfile::tempdir().expect("tmp");
        let write = |name: &str, pem: String| {
            let path = dir.path().join(name);
            std::fs::write(&path, pem).expect("write pem");
            path
        };
        TlsServer {
            ca: write("ca.pem", ca.pem()),
            wrong_ca: write("wrong.pem", wrong.pem()),
            pg,
            _dir: dir,
        }
    })
}

fn opts(host: &str, mode: SslMode, root: Option<&PathBuf>) -> ConnectOptions {
    let s = server();
    let mut o = ConnectOptions::new(
        host,
        s.pg.port,
        "postgres",
        &s.pg.user,
        Some(s.pg.password.clone()),
    );
    o.sslmode = mode;
    o.sslrootcert = root.cloned();
    o
}

async fn ssl_in_use(s: &Session) -> bool {
    s.client()
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .expect("pg_stat_ssl")
        .get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn require_encrypts_without_verifying() {
    let s = connect(&opts("127.0.0.1", SslMode::Require, None))
        .await
        .expect("require");
    assert!(ssl_in_use(&s).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disable_is_plaintext() {
    let s = connect(&opts("127.0.0.1", SslMode::Disable, None))
        .await
        .expect("disable");
    assert!(!ssl_in_use(&s).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_full_with_the_right_ca() {
    let s = connect(&opts("localhost", SslMode::VerifyFull, Some(&server().ca)))
        .await
        .expect("verify-full");
    assert!(ssl_in_use(&s).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_full_rejects_a_wrong_ca() {
    let e = connect(&opts(
        "localhost",
        SslMode::VerifyFull,
        Some(&server().wrong_ca),
    ))
    .await
    .expect_err("wrong ca");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(e.message.contains("UnknownIssuer"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_full_rejects_a_hostname_mismatch() {
    let e = connect(&opts("127.0.0.1", SslMode::VerifyFull, Some(&server().ca)))
        .await
        .expect_err("name");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(e.message.contains("certificate not valid for name"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_full_without_a_root_uses_public_roots() {
    let e = connect(&opts("localhost", SslMode::VerifyFull, None))
        .await
        .expect_err("self-signed is not publicly trusted");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn require_against_a_plaintext_server_is_config() {
    let pg = atlatl_testkit::pg16();
    let mut o = crate::opts(pg, "postgres");
    o.sslmode = SslMode::Require;
    let e = connect(&o).await.expect_err("no tls");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(e.message.contains("server does not support TLS"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_over_tls() {
    let p = Pool::new(
        opts("localhost", SslMode::VerifyFull, Some(&server().ca)),
        PoolOptions::default(),
    )
    .expect("pool");
    let a = p.get().await.expect("a");
    assert!(ssl_in_use(&a).await);
    drop(a);
    let b = p.get().await.expect("b");
    assert!(ssl_in_use(&b).await);
}
