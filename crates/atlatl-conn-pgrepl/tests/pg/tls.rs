//! The replication connection over TLS (`verify-full` against a test CA), on PG 16.

use atlatl_conn_pgrepl::{PgChangeSource, slot_name};
use atlatl_conn_postgres::{ConnectOptions, connect};
use atlatl_core::config::model::SslMode;
use atlatl_testkit::{LocalLake, Postgres, unique};
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair};

use crate::{policy, stream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_over_tls() {
    let mut p = CertificateParams::new(Vec::<String>::new()).expect("params");
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name
        .push(DnType::CommonName, "atlatl test CA");
    let ca = CertifiedIssuer::self_signed(p, KeyPair::generate().expect("key")).expect("ca");
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
        .setting("wal_level", "logical")
        .start()
        .expect("tls postgres");
    let dir = tempfile::tempdir().expect("tmp");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, ca.pem()).expect("ca");

    let mut opts = ConnectOptions::new(
        "localhost",
        pg.port,
        "postgres",
        &pg.user,
        Some(pg.password.clone()),
    );
    opts.sslmode = SslMode::VerifyFull;
    opts.sslrootcert = Some(ca_path);
    let sql = connect(&opts).await.expect("connect");
    sql.client()
        .batch_execute(
            "CREATE TABLE s (id int PRIMARY KEY, v text); INSERT INTO s VALUES (1, 'a');",
        )
        .await
        .expect("setup");

    let lake = LocalLake::file().expect("lake");
    let duck = lake.open("lake").expect("attach");
    let name = unique("tls");
    let spec = atlatl_conn_pgrepl::ReplicationSpec {
        name: name.clone(),
        streams: vec![stream("s")],
        limits: Default::default(),
    };
    let mut src = PgChangeSource::open(opts, policy(true, false), spec)
        .await
        .expect("open");
    let w = src
        .changes(duck.try_clone().expect("clone"), "lake", "run-tls")
        .await
        .expect("window");
    let ssl: bool = sql
        .client()
        .query_one(
            "SELECT s.ssl FROM pg_replication_slots r JOIN pg_stat_ssl s ON s.pid = r.active_pid
             WHERE r.slot_name = $1",
            &[&slot_name(&name)],
        )
        .await
        .expect("walsender")
        .get(0);
    assert!(ssl, "the walsender connection is not encrypted");
    let applied = w.apply().expect("apply");
    src.confirm(applied.confirm.expect("confirm"))
        .await
        .expect("confirm");
    src.close().await.expect("close");
    let n: i64 = duck
        .query_row("SELECT count(*) FROM lake.br.s", [], |r| r.get(0))
        .expect("count");
    assert_eq!(n, 1);
}
