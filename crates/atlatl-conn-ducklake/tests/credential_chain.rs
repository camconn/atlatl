//! DuckLake storage with the default `provider: credential_chain` on RustFS: credentials come from
//! the AWS environment and shared files, never from atlatl's config. Its own test binary with one
//! test, because the chain reads process-wide environment variables.

use std::path::Path;

use atlatl_conn_ducklake::attach;
use atlatl_core::ErrorClass;
use atlatl_core::config::model::{DuckLakeConn, EngineConfig};
use atlatl_core::config::types::{Auto, ByteSize};
use atlatl_engine::{Budget, BudgetParams, DbResultExt, Detected, Engine, EngineOptions};
use atlatl_testkit::{LocalLake, S3, rustfs};
use serde_json::json;

fn engine(spill: &Path) -> Engine {
    let cfg = EngineConfig {
        memory: Some(Auto::Value(ByteSize(1 << 30))),
        threads: Some(Auto::Value(2)),
        temp_directory: Some(spill.to_string_lossy().into_owned()),
        ..Default::default()
    };
    let b =
        Budget::compute(&cfg, None, &Detected::detect(), &BudgetParams::default()).expect("budget");
    let mut opts = EngineOptions::new(b);
    opts.extension_dir = Some(atlatl_testkit::extension_dir());
    Engine::open(opts).expect("engine")
}

/// No `provider`, no keys: the default chain.
fn lake_conn(local: &LocalLake, s3: &S3, bucket: &str) -> DuckLakeConn {
    serde_json::from_value(json!({
        "catalog": {"type": "duckdb", "path": local.dir.path().join("meta.ducklake")},
        "data_path": format!("s3://{bucket}/lake/"),
        "storage": {"type": "s3", "region": s3.region, "endpoint": format!("http://{}", s3.endpoint),
                    "url_style": "path"},
        "data_inlining_row_limit": 0,
    }))
    .expect("ducklake connection")
}

/// Attaches a fresh lake in a fresh bucket and writes a parquet file through the chain.
async fn write_through_chain(s3: &S3) -> atlatl_core::error::Result<i64> {
    let bucket = s3.fresh_bucket().expect("bucket");
    let local = LocalLake::file().expect("catalog dir");
    let spill = tempfile::tempdir().expect("spill");
    let e = engine(spill.path());
    attach(&e, "lake", &lake_conn(&local, s3, &bucket)).await?;
    let glob = format!("SELECT count(*) FROM glob('s3://{bucket}/lake/**')");
    e.with_conn(move |c| {
        c.execute_batch("CREATE TABLE lake.main.t AS SELECT range AS i FROM range(10)")
            .db()?;
        c.query_row(&glob, [], |r| r.get(0)).db()
    })
    .await
}

/// SAFETY (for every `set_var`/`remove_var` here): the only test in this binary, no engine is
/// alive while the environment changes, and nothing else reads it concurrently.
fn reset_aws_env(files: &Path) {
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("AWS_") {
            unsafe { std::env::remove_var(&k) };
        }
    }
    let config = files.join("config");
    let creds = files.join("credentials");
    std::fs::write(&config, "").expect("config");
    std::fs::write(&creds, "").expect("credentials");
    unsafe {
        std::env::set_var("AWS_CONFIG_FILE", &config);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &creds);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn credential_chain_reads_env_and_shared_files() {
    let s3 = rustfs();
    let files = tempfile::tempdir().expect("aws files");

    reset_aws_env(files.path());
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", &s3.key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &s3.secret);
    }
    let n = write_through_chain(s3).await.expect("env credentials");
    assert!(n >= 1, "parquet files in the bucket: {n}");

    reset_aws_env(files.path());
    std::fs::write(
        files.path().join("credentials"),
        format!(
            "[atlatl-test]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            s3.key_id, s3.secret
        ),
    )
    .expect("credentials");
    unsafe { std::env::set_var("AWS_PROFILE", "atlatl-test") };
    let n = write_through_chain(s3)
        .await
        .expect("shared credentials file");
    assert!(n >= 1, "parquet files in the bucket: {n}");
    unsafe { std::env::set_var("AWS_PROFILE", "no-such-profile") };
    let e = write_through_chain(s3).await.expect_err("no credentials");
    assert_eq!(e.class, ErrorClass::Config, "{e}");
    assert!(e.message.contains("Secret Validation Failure"), "{e}");

    reset_aws_env(files.path());
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", &s3.key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "not-the-secret");
    }
    let e = write_through_chain(s3).await.expect_err("wrong secret");
    assert_eq!(e.class, ErrorClass::Auth, "{e}");
    assert!(!e.message.contains(&s3.secret), "{e}");
}
