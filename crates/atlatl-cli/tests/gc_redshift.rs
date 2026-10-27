//! `atlatl gc` over Redshift UNLOAD prefixes in RustFS (M5.4).

use std::path::Path;
use std::process::{Command, Output};

fn atlatl(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_atlatl"))
        .arg("--project")
        .arg(root)
        .args(args)
        .env_remove("ATLATL_PROJECT")
        .output()
        .expect("run atlatl")
}

fn put(bucket: &str, key: &str) {
    let (st, body) = atlatl_testkit::rustfs()
        .request("PUT", &format!("/{bucket}/{key}"), "x")
        .expect("put");
    assert_eq!(st, 200, "{body}");
}

fn keys(bucket: &str) -> i64 {
    let s3 = atlatl_testkit::rustfs();
    let c = atlatl_testkit::lake::open_engine().expect("engine");
    c.execute_batch("LOAD httpfs").expect("httpfs");
    c.execute_batch(&s3.duckdb_secret("list", bucket))
        .expect("secret");
    c.query_row(
        &format!("SELECT count(*) FROM glob('s3://{bucket}/atlatl_unload/**')"),
        [],
        |r| r.get(0),
    )
    .expect("glob")
}

#[test]
fn gc_lists_and_removes_stale_unload_prefixes() {
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    put(
        &bucket,
        "atlatl_unload/01JOLDRUN/s/m/a1/part_0000_part_00.parquet",
    );
    put(&bucket, "atlatl_unload/01JOLDRUN/s/m/a1/part_manifest");
    put(&bucket, "other/keep.txt");
    let d = tempfile::tempdir().expect("tmp");
    let yaml = format!(
        "version: 1\nengine: {{temp_directory: \"{}\"}}\nconnections:\n  warehouse:\n    type: redshift\n    data_api: {{workgroup: wg, database: dev, region: us-east-1, endpoint: \"http://127.0.0.1:9\", key_id: AKIDGC, secret: gc-secret-value}}\n    unload:\n      bucket: {bucket}\n      iam_role: arn:aws:iam::000000000000:role/unload\n      read_role: none\n      storage: {{type: s3, region: us-east-1, endpoint: \"http://{}\", url_style: path, key_id: {}, secret: {}}}\n",
        d.path().join("spill").display(),
        s3.endpoint,
        s3.key_id,
        s3.secret
    );
    std::fs::write(d.path().join("atlatl.yaml"), yaml).expect("write");

    let o = atlatl(d.path(), &["gc", "--older-than", "1h", "--dry-run"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(
        !String::from_utf8_lossy(&o.stdout).contains("atlatl_unload"),
        "too new to collect"
    );

    let o = atlatl(d.path(), &["gc", "--older-than", "0s", "--dry-run"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(
        out.contains(&format!(
            "would remove s3://{bucket}/atlatl_unload/01JOLDRUN/ (warehouse: 2 objects"
        )),
        "{out}"
    );
    assert_eq!(keys(&bucket), 2, "dry run removes nothing");

    let o = atlatl(d.path(), &["gc", "--older-than", "0s"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(
        String::from_utf8_lossy(&o.stdout).contains("removing s3://"),
        "{}",
        String::from_utf8_lossy(&o.stdout)
    );
    assert_eq!(keys(&bucket), 0);
    let (st, _) = s3
        .request("GET", &format!("/{bucket}/other/keep.txt"), "")
        .expect("get");
    assert_eq!(st, 200, "objects outside the UNLOAD prefix are kept");
}
