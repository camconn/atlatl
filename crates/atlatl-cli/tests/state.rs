use std::process::{Command, Output};

use atlatl_testkit::{LocalLake, TempProject};

const REPS: &str = r#"replications:
  inc:
    source: lake
    target: lake
    streams:
      - {id: s/events, mode: incremental, from: {table: src.events}, object: br.events, primary_key: [id], update_key: {column: updated_at}}
"#;

struct Env {
    lake: LocalLake,
    project: TempProject,
}

fn setup() -> Env {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch(
        "CREATE SCHEMA lake.src; CREATE TABLE lake.src.events AS SELECT range AS id, TIMESTAMP '2025-01-01' + INTERVAL (range) HOUR AS updated_at FROM range(1, 6);",
    )
    .expect("seed");
    drop(c);
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml("lake", "lake"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", REPS))
        .expect("project");
    Env { lake, project }
}

impl Env {
    fn atlatl(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_atlatl"))
            .arg("--project")
            .arg(self.project.path())
            .args(args)
            .env("ATLATL_EXTENSION_DIR", atlatl_testkit::extension_dir())
            .env_remove("ATLATL_PROJECT")
            .output()
            .expect("atlatl")
    }
    fn ok(&self, args: &[&str]) -> serde_json::Value {
        let o = self.atlatl(args);
        assert!(
            o.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        if o.stdout.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&o.stdout).unwrap_or(serde_json::Value::Null)
        }
    }
    fn cursor(&self) -> Option<String> {
        let v = self.ok(&["state", "show", "--stream", "s/events"]);
        v.as_array()
            .and_then(|a| a.first())
            .and_then(|r| r["value"]["value"].as_str().map(str::to_owned))
    }
    fn count(&self) -> i64 {
        self.lake
            .open("lake")
            .expect("o")
            .query_row("SELECT count(*) FROM lake.br.events", [], |r| r.get(0))
            .expect("q")
    }
}

#[test]
fn show_set_reset_and_manual_actions_are_logged() {
    let e = setup();
    assert_eq!(e.ok(&["state", "show"]), serde_json::json!([]));
    e.ok(&["run", "--replication", "inc"]);
    assert_eq!(e.cursor().as_deref(), Some("2025-01-01 05:00:00"));

    let v = e.ok(&[
        "state",
        "set",
        "s/events",
        "--cursor",
        "2025-01-01 03:00:00",
    ]);
    assert_eq!(v["after"]["value"], "2025-01-01 03:00:00");
    assert_eq!(v["after"]["type"], "TIMESTAMP", "keeps the stored type");
    let o = e.atlatl(&[
        "--report",
        &e.project.path().join("r.json").display().to_string(),
        "run",
        "--replication",
        "inc",
    ]);
    assert!(o.status.success());
    let r: serde_json::Value =
        serde_json::from_slice(&std::fs::read(e.project.path().join("r.json")).expect("r"))
            .expect("json");
    assert_eq!(
        r["replications"][0]["streams"][0]["members"][0]["rows"]["staged"], 3,
        "rewound cursor re-reads 03:00..05:00"
    );

    assert_eq!(e.ok(&["state", "reset", "s/events"])["removed"], 1);
    assert_eq!(e.cursor(), None);
    let kinds: Vec<String> = {
        let c = e.lake.open("lake").expect("o");
        let mut s = c
            .prepare("SELECT kind FROM lake._atlatl.runs ORDER BY recorded_at")
            .expect("p");
        s.query_map([], |r| r.get(0))
            .expect("q")
            .collect::<Result<_, _>>()
            .expect("rows")
    };
    assert!(
        kinds.contains(&"state set".to_owned()) && kinds.contains(&"state reset".to_owned()),
        "{kinds:?}"
    );
}

#[test]
fn init_from_target_accept_and_move() {
    let e = setup();
    e.ok(&["run", "--replication", "inc"]);
    e.ok(&["state", "reset", "s/events"]);
    let v = e.ok(&["state", "init", "s/events", "--from-target"]);
    assert_eq!(v["after"]["value"], "2025-01-01 05:00:00");
    assert_eq!(v["after"]["type"], "TIMESTAMP");

    // A config change that alters state meaning fails the run until accepted.
    std::fs::write(
        e.project.path().join("replications/all.yaml"),
        REPS.replace(
            "from: {table: src.events}",
            "from: {table: src.events}, window: \"id > 0\"",
        ),
    )
    .expect("rewrite");
    let o = e.atlatl(&["run", "--replication", "inc"]);
    assert_eq!(o.status.code(), Some(1));
    e.ok(&["state", "accept", "s/events"]);
    e.ok(&["run", "--replication", "inc"]);
    assert_eq!(e.count(), 5);

    std::fs::write(
        e.project.path().join("replications/all.yaml"),
        REPS.replace("id: s/events", "id: s/renamed"),
    )
    .expect("rename");
    assert_eq!(e.ok(&["state", "move", "s/events", "s/renamed"])["rows"], 1);
    let v = e.ok(&["state", "show", "--stream", "s/renamed"]);
    assert_eq!(v.as_array().map(Vec::len), Some(1));
    let o = e.atlatl(&["state", "move", "s/renamed", "s/nope"]);
    assert_eq!(o.status.code(), Some(2), "target id must exist in config");
}

#[test]
fn init_from_target_seeds_named_wildcard_members() {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch(
        "CREATE SCHEMA lake.src; CREATE SCHEMA lake.br;
         CREATE TABLE lake.src.ev_a AS SELECT range AS id, DATE '2025-01-01' + range::INTEGER AS d FROM range(1, 6);
         CREATE TABLE lake.src.ev_b AS SELECT range AS id, DATE '2025-01-01' + range::INTEGER AS d FROM range(1, 6);
         CREATE TABLE lake.br.ev_a AS SELECT * FROM lake.src.ev_a WHERE id <= 2;
         CREATE TABLE lake.br.ev_b AS SELECT * FROM lake.src.ev_b WHERE id <= 4;",
    )
    .expect("seed");
    drop(c);
    let reps = r#"replications:
  inc:
    source: lake
    target: lake
    streams:
      - {id: s/all, mode: incremental, from: {tables: "src.ev_*"}, object: "br.{{ member.table }}", primary_key: [id], update_key: d}
"#;
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml("lake", "lake"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", reps))
        .expect("project");
    let e = Env { lake, project };

    let o = e.atlatl(&["state", "init", "s/all", "--from-target"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("--member"),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    let o = e.atlatl(&[
        "state",
        "init",
        "s/all",
        "--from-target",
        "--member",
        "src.other",
    ]);
    assert_eq!(o.status.code(), Some(2), "not a member of the pattern");

    let v = e.ok(&[
        "state",
        "init",
        "s/all",
        "--from-target",
        "--member",
        "src.ev_a",
        "--member",
        "src.ev_b",
    ]);
    assert_eq!(v["after"]["src.ev_a"]["value"], "2025-01-03");
    assert_eq!(v["after"]["src.ev_b"]["type"], "DATE");
    let report = e.project.path().join("r.json");
    let o = e.atlatl(&[
        "--report",
        &report.display().to_string(),
        "run",
        "--replication",
        "inc",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let r: serde_json::Value =
        serde_json::from_slice(&std::fs::read(report).expect("r")).expect("json");
    let staged: Vec<u64> = r["replications"][0]["streams"][0]["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|m| m["rows"]["staged"].as_u64().expect("n"))
        .collect();
    assert_eq!(
        staged,
        [4, 2],
        "each member reads from its own seeded watermark"
    );
}

const TENANTS: &str = r#"replications:
  inc:
    source: lake
    target: lake
    for_each: {over: [acme, zed], as: tenant}
    streams:
      - {id: s/events, mode: incremental, from: {table: src.events}, window: "tenant = '{{ tenant }}'", object: br.events, primary_key: [tenant, id], update_key: {column: updated_at}}
"#;

fn tenants() -> Env {
    let lake = LocalLake::file().expect("lake");
    let c = lake.open("lake").expect("open");
    c.execute_batch(
        "CREATE SCHEMA lake.src; CREATE TABLE lake.src.events AS
           SELECT range AS id, CASE WHEN range % 2 = 0 THEN 'acme' ELSE 'zed' END AS tenant,
             TIMESTAMP '2025-01-01' + INTERVAL (range) HOUR AS updated_at FROM range(1, 7);",
    )
    .expect("seed");
    drop(c);
    let project = TempProject::new()
        .and_then(|p| {
            p.project(
                &lake.connection_yaml("lake", "lake"),
                "state: {lake: lake}\n",
            )
        })
        .and_then(|p| p.replication("all", TENANTS))
        .expect("project");
    Env { lake, project }
}

impl Env {
    fn ids(&self, args: &[&str]) -> Vec<(String, String)> {
        self.ok(args)
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| {
                (
                    r["stream_id"].as_str().expect("id").to_owned(),
                    r["value"]["value"].as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect()
    }
    fn fails(&self, args: &[&str], want: &str) {
        let o = self.atlatl(args);
        let err = String::from_utf8_lossy(&o.stderr);
        assert!(
            o.status.code() == Some(2) && err.contains(want),
            "{args:?}: {:?} {err}",
            o.status.code()
        );
    }
}

#[test]
fn instance_state_is_addressed_by_key() {
    let e = tenants();
    e.ok(&["run", "--replication", "inc"]);
    let both = vec![
        ("s/events@acme".to_owned(), "2025-01-01 06:00:00".to_owned()),
        ("s/events@zed".to_owned(), "2025-01-01 05:00:00".to_owned()),
    ];
    assert_eq!(e.ids(&["state", "show"]), both);
    assert_eq!(
        e.ids(&["state", "show", "--stream", "s/events"]),
        both,
        "a stream id covers its instances"
    );
    assert_eq!(
        e.ids(&["state", "show", "--stream", "s/events", "--instance", "zed"]),
        both[1..]
    );
    assert_eq!(
        e.ids(&["state", "show", "--stream", "s/events@acme"]),
        both[..1]
    );
    e.fails(
        &[
            "state",
            "show",
            "--stream",
            "s/events@acme",
            "--instance",
            "zed",
        ],
        "names instance `acme` but --instance is `zed`",
    );

    // set: one instance's cursor, which only that instance re-reads.
    e.fails(
        &[
            "state",
            "set",
            "s/events",
            "--cursor",
            "2025-01-01 00:00:00",
        ],
        "name one with --instance <key> or `s/events@<key>`",
    );
    e.fails(
        &[
            "state",
            "set",
            "s/events@nope",
            "--cursor",
            "2025-01-01 00:00:00",
        ],
        "has no instance `nope` (its instances: acme, zed)",
    );
    let v = e.ok(&[
        "state",
        "set",
        "s/events",
        "--instance",
        "zed",
        "--cursor",
        "2025-01-01 03:00:00",
    ]);
    assert_eq!(
        (v["after"]["value"].as_str(), v["after"]["type"].as_str()),
        (Some("2025-01-01 03:00:00"), Some("TIMESTAMP"))
    );
    let report = e.project.path().join("r.json");
    let o = e.atlatl(&[
        "--report",
        &report.display().to_string(),
        "run",
        "--instance",
        "inc[zed]",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let r: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&report).expect("r")).expect("json");
    let reps = r["replications"].as_array().expect("reps");
    assert_eq!(reps.len(), 1, "only the named instance runs");
    assert_eq!(reps[0]["replication"], "inc[zed]");
    assert_eq!(
        reps[0]["streams"][0]["members"][0]["rows"]["staged"], 2,
        "zed re-reads 03:00 and 05:00"
    );

    // accept and reset: one instance, or the stream with all of them.
    std::fs::write(
        e.project.path().join("replications/all.yaml"),
        TENANTS.replace("window: \"tenant", "window: \"id > 0 AND tenant"),
    )
    .expect("rewrite");
    let o = e.atlatl(&["run", "--instance", "inc[acme]"]);
    assert_eq!(o.status.code(), Some(1), "config drift");
    let v = e.ok(&["state", "accept", "s/events@acme"]);
    assert_eq!(v["state_ids"], serde_json::json!(["s/events@acme"]));
    e.ok(&["run", "--instance", "inc[acme]"]);
    let v = e.ok(&["state", "accept", "s/events"]);
    assert_eq!(
        v["state_ids"],
        serde_json::json!(["s/events@acme", "s/events@zed"])
    );
    assert_eq!(
        e.ok(&["state", "reset", "s/events", "--instance", "acme"])["removed"],
        1
    );
    assert_eq!(e.ids(&["state", "show"]).len(), 1);

    // move: the stream and every instance, keeping keys.
    e.ok(&["run", "--replication", "inc"]);
    std::fs::write(
        e.project.path().join("replications/all.yaml"),
        TENANTS.replace("id: s/events", "id: s/renamed"),
    )
    .expect("rename");
    e.fails(
        &["state", "move", "s/events@acme", "s/renamed"],
        "move an instance to an instance",
    );
    let v = e.ok(&[
        "state",
        "move",
        "s/events",
        "s/renamed",
        "--instance",
        "zed",
    ]);
    assert_eq!(v["rows"], 1);
    let ids = |e: &Env| {
        e.ids(&["state", "show"])
            .into_iter()
            .map(|(i, _)| i)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&e), ["s/events@acme", "s/renamed@zed"]);
    let v = e.ok(&["state", "move", "s/events", "s/renamed"]);
    assert_eq!(
        v["moved"],
        serde_json::json!({"s/events@acme": "s/renamed@acme"})
    );
    assert_eq!(ids(&e), ["s/renamed@acme", "s/renamed@zed"]);
    assert_eq!(e.ok(&["state", "reset", "s/renamed"])["removed"], 2);
    assert_eq!(e.ok(&["state", "show"]), serde_json::json!([]));

    e.fails(
        &["state", "init", "s/renamed", "--from-target"],
        "runs once per `for_each` instance",
    );
    let v = e.ok(&[
        "state",
        "init",
        "s/renamed",
        "--from-target",
        "--instance",
        "zed",
    ]);
    assert_eq!(v["after"]["type"], "TIMESTAMP");
    assert_eq!(
        e.ids(&["state", "show"])
            .into_iter()
            .map(|(i, _)| i)
            .collect::<Vec<_>>(),
        ["s/renamed@zed"]
    );
}

#[test]
fn instance_selection_in_plan() {
    let e = tenants();
    let o = e.atlatl(&["plan", "--json", "--instance", "inc[zed]"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let p: serde_json::Value = serde_json::from_slice(&o.stdout).expect("json");
    let names: Vec<&str> = p["replications"]
        .as_array()
        .expect("reps")
        .iter()
        .map(|r| {
            r["replication"]
                .as_str()
                .or(r["name"].as_str())
                .expect("name")
        })
        .collect();
    assert_eq!(names, ["inc[zed]"]);
    assert_eq!(p["selection"]["instances"], serde_json::json!(["inc[zed]"]));
    let o = e.atlatl(&["plan", "--instance", "inc[nope]"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&o.stderr)
            .contains("has no instance `nope` (its instances: acme, zed)"),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    let o = e.atlatl(&["run", "--instance", "zed"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("expected `<replication>[<key>]`"),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}
