use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use atlatl_conn_files::{Downloader, FileSet, FilesRead, FilesSource};
use atlatl_core::config::model::{BackoffKind, OnNoMatch, SftpConfig};
use atlatl_core::error::{AtlatlError, ErrorClass};
use atlatl_core::retry::RetryPolicy;
use atlatl_testkit::Sftp;
use base64::Engine;
use duckdb::Connection;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn srv() -> &'static Sftp {
    atlatl_testkit::sftp()
}

fn engine() -> Connection {
    atlatl_testkit::lake::open_engine().expect("engine")
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 4,
        backoff: BackoffKind::Constant,
        base: Duration::from_millis(10),
        max: Duration::from_millis(10),
        ..Default::default()
    }
}

/// Password auth, server key pinned by fingerprint.
fn conn(extra: Value) -> SftpConfig {
    let s = srv();
    let mut v = json!({
        "host": s.host, "port": s.port, "user": s.user, "password": s.password,
        "host_key": s.host_key_fingerprints,
    });
    if let (Some(m), Value::Object(e)) = (v.as_object_mut(), extra) {
        for (k, x) in e {
            if x.is_null() {
                m.remove(&k);
            } else {
                m.insert(k, x);
            }
        }
    }
    serde_json::from_value(v).expect("sftp config")
}

fn read(sel: Value, cfg: SftpConfig) -> FilesRead {
    let mut r = FilesRead::new(serde_json::from_value(sel).expect("selector"));
    r.sftp = Some(cfg);
    r
}

async fn fetch_discover(
    c: &Connection,
    r: FilesRead,
    work: &Path,
) -> Result<(FilesSource, FileSet), AtlatlError> {
    let dl = Downloader::new(fast_retry())?;
    let mut src = FilesSource::new(r)?;
    assert!(src.needs_fetch());
    src.fetch(&dl, work).await?;
    assert!(!src.needs_fetch());
    let fs = src.discover(c, work)?;
    Ok((src, fs))
}

fn count(c: &Connection, t: &str) -> i64 {
    c.query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
        .expect("count")
}

#[tokio::test]
async fn password_auth_and_sorted_globs() {
    let s = srv();
    let dir = s.fresh_dir().expect("dir");
    s.put(&format!("{dir}/b.csv"), b"id,v\n3,c\n").expect("put");
    s.put(&format!("{dir}/a.csv"), b"id,v\n1,a\n2,b\n")
        .expect("put");
    s.put(&format!("{dir}/sub/c.csv"), b"id,v\n4,d\n")
        .expect("put");
    s.put(&format!("{dir}/notes.txt"), b"x").expect("put");
    let url = format!("sftp://{}:{}{dir}", s.host, s.port);
    let shown = |f: &str| format!("sftp://{}:{}{dir}/{f}", s.host, s.port);

    let w = tempfile::tempdir().expect("tmp");
    let c = engine();
    let (src, fs) = fetch_discover(
        &c,
        read(json!({"url": url, "glob": "*.csv"}), conn(json!({}))),
        w.path(),
    )
    .await
    .expect("glob");
    assert_eq!(fs.names(), [shown("a.csv"), shown("b.csv")]);
    assert_eq!(src.downloads().len(), 2);
    assert_eq!(fs.stage(&c, "stg").expect("stage"), 3);

    let w = tempfile::tempdir().expect("tmp");
    let (_, fs) = fetch_discover(
        &c,
        read(
            json!({"url": format!("sftp:///{}/**/*.csv", dir.trim_start_matches('/'))}),
            conn(json!({})),
        ),
        w.path(),
    )
    .await
    .expect("recursive");
    assert_eq!(
        fs.names(),
        [shown("a.csv"), shown("b.csv"), shown("sub/c.csv")]
    );
    // The remote layout is mirrored locally, so Hive-style segments survive.
    assert!(fs.files[2].path.ends_with(&format!("{dir}/sub/c.csv")));
}

#[tokio::test]
async fn key_auth_with_known_hosts() {
    let s = srv();
    let dir = s.fresh_dir().expect("dir");
    s.put(&format!("{dir}/k.csv"), b"id\n1\n2\n").expect("put");
    let t = tempfile::tempdir().expect("tmp");
    let kh = t.path().join("known_hosts");
    std::fs::write(&kh, &s.known_hosts).expect("known_hosts");
    let url = format!("sftp://{}:{}{dir}/k.csv", s.host, s.port);
    let c = engine();

    // ed25519 (OpenSSH format), and RSA PEM passed base64-encoded as some vendors store it.
    let rsa_b64 = base64::engine::general_purpose::STANDARD.encode(&s.rsa_key);
    for key in [s.ed25519_key.clone(), rsa_b64] {
        let cfg = conn(json!({
            "password": null, "host_key": null, "private_key": key,
            "known_hosts": kh.to_string_lossy(),
        }));
        let w = tempfile::tempdir().expect("tmp");
        let (_, fs) = fetch_discover(&c, read(json!({"url": url}), cfg), w.path())
            .await
            .expect("key auth");
        assert_eq!(fs.stage(&c, "stg").expect("stage"), 2);
    }
}

#[tokio::test]
async fn wrong_host_key_is_rejected() {
    let s = srv();
    let url = format!("sftp://{}:{}/data/x.csv", s.host, s.port);
    let c = engine();
    let t = tempfile::tempdir().expect("tmp");

    let e = fetch_discover(
        &c,
        read(
            json!({"url": url}),
            conn(json!({"host_key": ["SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]})),
        ),
        t.path(),
    )
    .await
    .expect_err("fingerprint");
    assert_eq!(e.class, ErrorClass::Auth);
    assert_eq!(e.code.as_deref(), Some("host_key"));
    assert!(e.message.contains("is not in sftp.host_key"), "{e}");

    // Same host, another key: a changed key.
    let other = "AAAAC3NzaC1lZDI1NTE5AAAAIAABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4f";
    let kh = t.path().join("changed");
    std::fs::write(
        &kh,
        format!("[{}]:{} ssh-ed25519 {other}\n", s.host, s.port),
    )
    .expect("kh");
    let e = fetch_discover(
        &c,
        read(
            json!({"url": url}),
            conn(json!({"host_key": null, "known_hosts": kh.to_string_lossy()})),
        ),
        t.path(),
    )
    .await
    .expect_err("changed");
    assert_eq!(e.class, ErrorClass::Auth);
    assert!(e.message.contains("does not match"), "{e}");

    let e = fetch_discover(
        &c,
        read(
            json!({"url": url}),
            conn(json!({"host_key": null, "known_hosts": t.path().join("none").to_string_lossy()})),
        ),
        t.path(),
    )
    .await
    .expect_err("unknown");
    assert_eq!(e.class, ErrorClass::Auth);
    assert!(e.message.contains("is not in"), "{e}");

    // The explicit opt-out.
    let e = fetch_discover(
        &c,
        read(
            json!({"url": url}),
            conn(json!({"host_key": null, "insecure_skip_host_key_check": true})),
        ),
        t.path(),
    )
    .await
    .expect_err("missing file");
    assert_eq!(e.class, ErrorClass::Config);
}

#[tokio::test]
async fn zip_member_over_sftp() {
    let s = srv();
    let dir = s.fresh_dir().expect("dir");
    let mut z = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, body) in [
        ("README.txt", &b"hi"[..]),
        ("US_FIN_INCOME.csv", b"ticker,val\nA,1\nB,2\n"),
    ] {
        z.start_file(name, zip::write::SimpleFileOptions::default())
            .expect("member");
        z.write_all(body).expect("write");
    }
    let bytes = z.finish().expect("zip").into_inner();
    s.put(&format!("{dir}/bulk.zip"), &bytes).expect("put");
    let c = engine();
    let w = tempfile::tempdir().expect("tmp");
    let (_, fs) = fetch_discover(
        &c,
        read(
            json!({"url": format!("sftp://{}:{}{dir}/bulk.zip", s.host, s.port),
                   "archive": {"member": "US_FIN_*.csv"}}),
            conn(json!({})),
        ),
        w.path(),
    )
    .await
    .expect("zip");
    assert_eq!(
        fs.names(),
        [format!(
            "sftp://{}:{}{dir}/bulk.zip!US_FIN_INCOME.csv",
            s.host, s.port
        )]
    );
    assert_eq!(fs.stage(&c, "stg").expect("stage"), 2);
    assert_eq!(count(&c, "stg"), 2);
}

#[tokio::test]
async fn missing_files_follow_on_no_match() {
    let s = srv();
    let dir = s.fresh_dir().expect("dir");
    let c = engine();
    for url in [
        format!(
            "sftp://{}:{}{dir}/20240101_constituents.csv",
            s.host, s.port
        ),
        format!("sftp://{}:{}{dir}/nope/*.csv", s.host, s.port),
        format!("sftp://{}:{}{dir}/*.csv", s.host, s.port),
    ] {
        let w = tempfile::tempdir().expect("tmp");
        let e = fetch_discover(&c, read(json!({"url": url}), conn(json!({}))), w.path())
            .await
            .expect_err("missing");
        assert_eq!(e.class, ErrorClass::Config, "{e}");
        assert!(e.message.contains("no files match"), "{e}");

        let mut r = read(json!({"url": url}), conn(json!({})));
        r.on_no_match = OnNoMatch::Warn;
        let (_, fs) = fetch_discover(&c, r, w.path()).await.expect("warn");
        assert!(fs.is_empty());
        assert_eq!(fs.warnings.len(), 1, "{:?}", fs.warnings);
    }
}

#[tokio::test]
async fn auth_failures_are_auth() {
    let s = srv();
    let url = format!("sftp://{}:{}/data/x.csv", s.host, s.port);
    let c = engine();
    let w = tempfile::tempdir().expect("tmp");
    let e = fetch_discover(
        &c,
        read(
            json!({"url": url}),
            conn(json!({"password": "wrong-pw-9f3a"})),
        ),
        w.path(),
    )
    .await
    .expect_err("bad password");
    assert_eq!(e.class, ErrorClass::Auth);
    assert!(e.message.contains("authentication failed"), "{e}");
    assert!(!e.message.contains("attempts"), "not retried: {e}");
    assert!(!e.to_string().contains("wrong-pw-9f3a"), "{e}");

    let e = fetch_discover(
        &c,
        read(
            json!({"url": format!("sftp://nobody@{}:{}/data/x.csv", s.host, s.port)}),
            conn(json!({})),
        ),
        w.path(),
    )
    .await
    .expect_err("bad user");
    assert_eq!(e.class, ErrorClass::Auth);
}

/// Forwards to `upstream`; the first connection is cut after `cut` server-to-client bytes.
async fn flaky_proxy(upstream: String, cut: usize) -> (u16, Arc<AtomicUsize>) {
    let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = l.local_addr().expect("addr").port();
    let conns = Arc::new(AtomicUsize::new(0));
    let n = conns.clone();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = l.accept().await {
            let first = n.fetch_add(1, Ordering::SeqCst) == 0;
            let up = upstream.clone();
            tokio::spawn(async move {
                let Ok(mut server) = TcpStream::connect(&up).await else {
                    return;
                };
                if !first {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    return;
                }
                let (mut cr, mut cw) = client.split();
                let (mut sr, mut sw) = server.split();
                let up = tokio::io::copy(&mut cr, &mut sw);
                let down = async {
                    let mut sent = 0;
                    let mut buf = vec![0u8; 16 * 1024];
                    while sent < cut {
                        let k = match sr.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(k) => k.min(cut - sent),
                        };
                        if cw.write_all(&buf[..k]).await.is_err() {
                            break;
                        }
                        sent += k;
                    }
                };
                tokio::select! {
                    _ = up => {}
                    _ = down => {}
                }
            });
        }
    });
    (port, conns)
}

#[tokio::test]
async fn dropped_connection_resumes() {
    let s = srv();
    let dir = s.fresh_dir().expect("dir");
    let mut body = String::from("id,v\n");
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..60_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        body.push_str(&format!("{i},{x:016x}\n"));
    }
    s.put(&format!("{dir}/big.csv"), body.as_bytes())
        .expect("put");
    let (port, conns) = flaky_proxy(format!("{}:{}", s.host, s.port), 400 * 1024).await;
    let c = engine();
    let w = tempfile::tempdir().expect("tmp");
    let (src, fs) = fetch_discover(
        &c,
        read(
            json!({"url": format!("sftp://127.0.0.1:{port}{dir}/big.csv")}),
            conn(json!({})),
        ),
        w.path(),
    )
    .await
    .expect("resumed");
    let d = &src.downloads()[0];
    assert_eq!((d.attempts, d.resumed), (2, 1), "{d:?}");
    assert!(d.retries[0].contains("transient"), "{:?}", d.retries);
    assert_eq!(conns.load(Ordering::SeqCst), 2);
    assert_eq!(
        std::fs::read(&fs.files[0].path).expect("read"),
        body.as_bytes()
    );
    assert_eq!(fs.stage(&c, "stg").expect("stage"), 60_000);
}
