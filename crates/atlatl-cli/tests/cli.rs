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

fn project(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tmp");
    for (p, t) in files {
        let p = dir.path().join(p);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, t).expect("write");
    }
    dir
}

const P: &str = "version: 1\nconnections:\n  lake: {type: ducklake, catalog: {type: duckdb, path: cat.db}, data_path: \"{{ env.ATLATL_TEST_DATA_PATH }}\"}\n";
const R: &str = "replications:\n  copy:\n    source: lake\n    target: lake\n    streams:\n      - {id: a/b, mode: full-refresh, from: {table: s.t}, object: \"out.{{ member.table }}\"}\n";

#[test]
fn version() {
    let o = Command::new(env!("CARGO_BIN_EXE_atlatl"))
        .arg("--version")
        .output()
        .expect("run");
    assert!(o.status.success());
    assert_eq!(
        String::from_utf8_lossy(&o.stdout).trim(),
        format!("atlatl {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn static_plan_json() {
    let d = project(&[("atlatl.yaml", P), ("replications/copy.yaml", R)]);
    let o = atlatl(d.path(), &["plan", "--static", "--json"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v["static"], true);
    assert_eq!(v["replications"][0]["op"], "copy");
    assert_eq!(v["replications"][0]["streams"][0]["asset"]["key"], "a/b");
}

#[test]
fn live_plan_renders_with_env() {
    let d = project(&[("atlatl.yaml", P), ("replications/copy.yaml", R)]);
    let o = Command::new(env!("CARGO_BIN_EXE_atlatl"))
        .args(["--project"])
        .arg(d.path())
        .args(["plan", "--json"])
        .env("ATLATL_TEST_DATA_PATH", "/tmp/x/")
        .output()
        .expect("run");
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(
        v["replications"][0]["streams"][0]["members"][0]["object"],
        "out.t"
    );
    assert_eq!(v["connections"][0]["data_path"], "/tmp/x/");
    let o = atlatl(d.path(), &["plan"]);
    assert_eq!(
        o.status.code(),
        Some(2),
        "missing env var is a config error"
    );
    assert!(String::from_utf8_lossy(&o.stderr).contains("ATLATL_TEST_DATA_PATH"));
}

#[test]
fn validate_reports_located_errors() {
    let d = project(&[
        ("atlatl.yaml", P),
        ("replications/copy.yaml", &R.replace("object:", "objekt:")),
    ]);
    let o = atlatl(d.path(), &["validate"]);
    assert_eq!(o.status.code(), Some(2));
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("unknown field `objekt`") && err.contains("copy.yaml:6:"),
        "{err}"
    );
    let o = atlatl(d.path(), &["validate", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v[0]["line"], 6);

    let ok = project(&[("atlatl.yaml", P), ("replications/copy.yaml", R)]);
    let o = atlatl(ok.path(), &["validate"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
}

#[test]
fn validate_parses_api_specs() {
    let p = format!("{P}  api: {{type: rest, spec: apis/api.yaml}}\n");
    let r = "replications:\n  pull:\n    source: api\n    target: lake\n    streams:\n      - {id: api/items, mode: full-refresh, from: {endpoint: items}, object: br.items}\n";
    let spec = |path: &str| {
        format!(
            "base_url: https://x\nendpoints:\n  items:\n    request: {{path: /items}}\n    records: {{path: \"{path}\"}}\n"
        )
    };
    let d = project(&[
        ("atlatl.yaml", &p),
        ("replications/pull.yaml", r),
        ("apis/api.yaml", &spec(".")),
    ]);
    let o = atlatl(d.path(), &["validate"]);
    let err = String::from_utf8_lossy(&o.stderr);
    assert_eq!(o.status.code(), Some(2), "{err}");
    assert!(
        err.contains("error[api-spec]: endpoint `items`: invalid JMESPath")
            && err.contains("api.yaml:5:")
            && err.contains(r#"records: {path: "."}"#),
        "{err}"
    );
    let o = atlatl(d.path(), &["validate", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    assert_eq!(v[0]["line"], 5, "{v}");

    for args in [&["plan"][..], &["run", "--replication", "pull"]] {
        let o = Command::new(env!("CARGO_BIN_EXE_atlatl"))
            .arg("--project")
            .arg(d.path())
            .args(args)
            .env("ATLATL_TEST_DATA_PATH", d.path().join("data/"))
            .env_remove("ATLATL_PROJECT")
            .output()
            .expect("run");
        let err = String::from_utf8_lossy(&o.stderr);
        assert_eq!(o.status.code(), Some(2), "{args:?}: {err}");
        assert!(err.contains("invalid JMESPath"), "{args:?}: {err}");
    }
    assert!(
        !d.path().join("cat.db").exists(),
        "the spec check runs before the lake is attached"
    );

    let ok = project(&[
        ("atlatl.yaml", &p),
        ("replications/pull.yaml", r),
        ("apis/api.yaml", &spec("@")),
    ]);
    let o = atlatl(ok.path(), &["validate"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
}

#[test]
fn json_schemas() {
    let d = project(&[]);
    for kind in [
        "config",
        "project",
        "replications",
        "source-schema",
        "plan",
        "report",
        "events",
    ] {
        let o = atlatl(d.path(), &["json-schema", kind]);
        assert!(o.status.success(), "{kind}");
        let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
        assert!(v.get("$schema").is_some(), "{kind}");
    }
}

#[test]
fn report_and_event_schemas_describe_the_contract() {
    let d = project(&[]);
    let o = atlatl(d.path(), &["json-schema", "report"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    let status = &v["properties"]["status"];
    let status = v
        .pointer(&format!(
            "/$defs/{}",
            status["$ref"]
                .as_str()
                .and_then(|r| r.strip_prefix("#/$defs/"))
                .expect("status ref")
        ))
        .expect("status def");
    let text = status.to_string();
    for s in [
        "succeeded",
        "partial_failure",
        "failed",
        "killed",
        "config_error",
        "internal_error",
    ] {
        assert!(text.contains(&format!("\"{s}\"")), "{s} in {text}");
    }
    let o = atlatl(d.path(), &["json-schema", "events"]);
    let text = String::from_utf8_lossy(&o.stdout);
    for t in [
        "run.started",
        "replication.started",
        "stream.progress",
        "retrying",
        "hop.committed",
        "replication.completed",
        "throttle",
        "schema.drift",
        "warning",
        "run.completed",
    ] {
        assert!(text.contains(&format!("\"{t}\"")), "{t}");
    }
}

#[test]
fn global_observability_flags_parse() {
    let d = project(&[("atlatl.yaml", P), ("replications/r.yaml", R)]);
    let o = atlatl(
        d.path(),
        &[
            "--log-format",
            "json",
            "plan",
            "--static",
            "--events",
            "-",
            "--report",
            "r.json",
        ],
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let o = atlatl(d.path(), &["--log-format", "yaml", "plan", "--static"]);
    assert_eq!(o.status.code(), Some(2));
    let o = atlatl(d.path(), &["--events", "fd:nope", "plan", "--static"]);
    assert_eq!(o.status.code(), Some(2));
}
