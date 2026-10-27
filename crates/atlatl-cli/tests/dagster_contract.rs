//! M0.21: a synthetic project in the shapes of a legacy deployment, expressed as atlatl
//! config, must reproduce its Dagster contract byte for byte: asset keys, groups, ops and deps,
//! the legacy integration's defects included.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::Value;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dagster_contract")
}

fn atlatl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_atlatl"))
        .arg("--project")
        .arg(fixture())
        .args(args)
        .env_remove("ATLATL_PROJECT")
        .env_remove("DBT_PROFILE")
        .output()
        .expect("run atlatl")
}

fn static_plan() -> Value {
    let o = atlatl(&["plan", "--static", "--json"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    serde_json::from_slice(&o.stdout).expect("plan json")
}

/// (asset key, op, group, deps). The deps reproduce the legacy integration's defaults, defects included:
/// phantom deps on the sanitized stream name, and `au/metric_feed`.
const EXPECTED: &[(&str, &str, &str, &[&str])] = &[
    ("br/appdb", "appdb", "appdb", &["appdb/_"]),
    (
        "br/warehouse/est_v2/est_core_consf",
        "warehouse_est_consensus",
        "warehouse",
        &["est_v2/est_core_cons__f"],
    ),
    (
        "br/warehouse/est_v2/est_core_cons_rec",
        "warehouse_est_consensus",
        "warehouse",
        &["est_v2/est_core_cons_rec"],
    ),
    (
        "br/warehouse/est_v2/est_plus_cons",
        "warehouse_est_consensus",
        "warehouse",
        &["est_v2/est_plus_cons__"],
    ),
    (
        "br/warehouse/fin_v1/fin_core",
        "warehouse_fin_statements",
        "warehouse",
        &["fin_v1/fin_core__"],
    ),
    (
        "br/warehouse/fin_v1/fin_plus",
        "warehouse_fin_statements",
        "warehouse",
        &["fin_v1/fin_plus__"],
    ),
    (
        "br/warehouse/est_v2/est_core_act",
        "warehouse_est_actuals",
        "warehouse",
        &["est_v2/est_core_act__"],
    ),
    (
        "br/warehouse/est_v2/est_plus_act",
        "warehouse_est_actuals",
        "warehouse",
        &["est_v2/est_plus_act__"],
    ),
    (
        "br/warehouse/fin_v1/fin_sym_coverage",
        "warehouse_reference",
        "warehouse",
        &["fin_v1/fin_sym_coverage"],
    ),
    (
        "br/warehouse/fin_v1/fin_sym_map",
        "warehouse_reference",
        "warehouse",
        &["fin_v1/fin_sym_map"],
    ),
    (
        "br/warehouse/fin_v1/fin_sym_entity",
        "warehouse_reference",
        "warehouse",
        &["fin_v1/fin_sym_entity"],
    ),
    (
        "br/warehouse/fin_v1/fin_sym_entity_hist",
        "warehouse_reference",
        "warehouse",
        &["fin_v1/fin_sym_entity_hist"],
    ),
    (
        "br/warehouse/est_v2/est_sym_coverage",
        "warehouse_reference",
        "warehouse",
        &["est_v2/est_sym_coverage"],
    ),
    (
        "br/warehouse/est_v2/est_sym_map",
        "warehouse_reference",
        "warehouse",
        &["est_v2/est_sym_map"],
    ),
    (
        "br/warehouse/est_v2/est_sym_entity",
        "warehouse_reference",
        "warehouse",
        &["est_v2/est_sym_entity"],
    ),
    (
        "br/warehouse/est_v2/est_sym_entity_hist",
        "warehouse_reference",
        "warehouse",
        &["est_v2/est_sym_entity_hist"],
    ),
    (
        "br/warehouse_hourly/est_v2/est_core_consf",
        "warehouse_hourly_est_consensus",
        "warehouse_hourly",
        &["est_v2/est_core_cons__f"],
    ),
    (
        "br/warehouse_hourly/est_v2/est_core_cons_rec",
        "warehouse_hourly_est_consensus",
        "warehouse_hourly",
        &["est_v2/est_core_cons_rec"],
    ),
    (
        "br/warehouse_hourly/est_v2/est_plus_cons",
        "warehouse_hourly_est_consensus",
        "warehouse_hourly",
        &["est_v2/est_plus_cons__"],
    ),
    (
        "br/warehouse_hourly/est_v2/est_core_act",
        "warehouse_hourly_est_actuals",
        "warehouse_hourly",
        &["est_v2/est_core_act__"],
    ),
    (
        "br/warehouse_hourly/est_v2/est_plus_act",
        "warehouse_hourly_est_actuals",
        "warehouse_hourly",
        &["est_v2/est_plus_act__"],
    ),
    (
        "br/warehouse_hourly/fin_v1/fin_core",
        "warehouse_hourly_fin_statements",
        "warehouse_hourly",
        &["fin_v1/fin_core__"],
    ),
    (
        "br/warehouse_hourly/fin_v1/fin_plus",
        "warehouse_hourly_fin_statements",
        "warehouse_hourly",
        &["fin_v1/fin_plus__"],
    ),
    (
        "br/queue_chain/entities",
        "queue_chain_history",
        "queue_chain_history",
        &["entities"],
    ),
    (
        "br/queue_chain/reports",
        "queue_chain_history",
        "queue_chain_history",
        &["reports"],
    ),
    (
        "br/queue_chain/line_items",
        "queue_chain_history",
        "queue_chain_history",
        &["line_items"],
    ),
    (
        "br/queue_chain/listings",
        "queue_chain",
        "queue_chain",
        &["listings"],
    ),
    (
        "br/queue_chain/std_income",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_std_income/csv"],
    ),
    (
        "br/queue_chain/std_balance",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_std_balance/csv"],
    ),
    (
        "br/queue_chain/std_cashflow",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_std_cashflow/csv"],
    ),
    (
        "br/queue_chain/std_metrics",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_std_metrics/csv"],
    ),
    (
        "br/queue_chain/ind_income",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_ind_income/csv"],
    ),
    (
        "br/queue_chain/ind_balance",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_ind_balance/csv"],
    ),
    (
        "br/queue_chain/ind_cashflow",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_ind_cashflow/csv"],
    ),
    (
        "br/queue_chain/ind_metrics",
        "bulk_load",
        "queue_chain",
        &["file_tmp_bulk_files_ind_metrics/csv"],
    ),
    (
        "br/record_queue/instruments",
        "record_queue",
        "record_queue",
        &["instruments"],
    ),
    (
        "br/record_queue/ratings",
        "record_queue",
        "record_queue",
        &["ratings"],
    ),
    ("br/windowed/events", "windowed", "windowed", &["events"]),
    (
        "br/throttled/releases",
        "throttled_releases",
        "throttled",
        &["br/throttled/fanout"],
    ),
    (
        "br/throttled/symbols",
        "throttled_symbols",
        "throttled",
        &["br/throttled/fanout"],
    ),
    ("br/monthly/calendar", "monthly", "monthly", &["calendar"]),
    (
        "target/br/orgs",
        "paged_queue",
        "paged_queue_orgs",
        &["orgs"],
    ),
    (
        "target/br/rounds",
        "paged_queue",
        "paged_queue_orgs",
        &["rounds"],
    ),
    ("target/app/metric", "metric", "metric", &["au/metric_feed"]),
    (
        "org_event/load/app/org_event",
        "org_event",
        "org_event",
        &["org_event"],
    ),
    (
        "org_event/load/app/org_event_listing",
        "org_event",
        "org_event",
        &["org_event_listing"],
    ),
    (
        "org_event/load/app/org_event_report",
        "org_event",
        "org_event",
        &["org_event_report"],
    ),
    (
        "org_event/load/app/org_event_rating",
        "org_event",
        "org_event",
        &["org_event_rating"],
    ),
    (
        "org_event/load/app/org_event_funding",
        "org_event",
        "org_event",
        &["org_event_funding"],
    ),
    (
        "target/app/funding_participant",
        "funding_participant",
        "funding_participant",
        &["funding_participant"],
    ),
];

type Asset = (String, String, Vec<String>);

fn assets(plan: &Value) -> BTreeMap<String, Asset> {
    let mut out = BTreeMap::new();
    for r in plan["replications"].as_array().expect("replications") {
        let op = r["op"].as_str().expect("op").to_owned();
        for s in r["streams"].as_array().expect("streams") {
            assert_eq!(s["role"], "selected", "{}", s["id"]);
            let a = &s["asset"];
            let key = a["key"].as_str().expect("key").to_owned();
            let group = a["group"].as_str().unwrap_or_default().to_owned();
            let deps = a["deps"]
                .as_array()
                .expect("deps")
                .iter()
                .map(|d| d.as_str().expect("dep").to_owned())
                .collect();
            let prev = out.insert(key.clone(), (op.clone(), group, deps));
            assert!(prev.is_none(), "duplicate asset key {key}");
        }
    }
    out
}

#[test]
fn validate_is_clean() {
    let o = atlatl(&["validate", "--json"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
    let d: Value = serde_json::from_slice(&o.stdout).expect("json");
    let errors: Vec<&Value> = d
        .as_array()
        .expect("diagnostics")
        .iter()
        .filter(|i| i["severity"] == "error")
        .collect();
    assert!(errors.is_empty(), "{errors:#?}");
}

#[test]
fn reproduces_the_dagster_contract() {
    let plan = static_plan();
    let got = assets(&plan);
    assert_eq!(EXPECTED.len(), 50);
    let want: BTreeMap<String, Asset> = EXPECTED
        .iter()
        .map(|(k, op, g, deps)| {
            (
                (*k).to_owned(),
                (
                    (*op).to_owned(),
                    (*g).to_owned(),
                    deps.iter().map(|d| (*d).to_owned()).collect(),
                ),
            )
        })
        .collect();
    let got_keys: BTreeSet<&String> = got.keys().collect();
    let want_keys: BTreeSet<&String> = want.keys().collect();
    assert_eq!(
        got_keys
            .symmetric_difference(&want_keys)
            .collect::<Vec<_>>(),
        Vec::<&&String>::new(),
        "asset key sets differ"
    );
    for (k, w) in &want {
        assert_eq!(&got[k], w, "{k}: (op, group, deps)");
    }
}

#[test]
fn referenced_by_name() {
    let plan = static_plan();
    let got = assets(&plan);
    // One downstream job selects this key exactly; another selects by substring.
    assert!(got.contains_key("target/app/metric"));
    let ev: Vec<&String> = got
        .keys()
        .filter(|k| k.contains("org_event/load"))
        .collect();
    assert_eq!(ev.len(), 5, "{ev:?}");
    assert!(ev.iter().all(|k| got[*k].1 == "org_event"));
    // dbt source links that no legacy asset produces stay dangling (a known defect, 09 §6).
    for k in [
        "br/warehouse/est_v2/est_core",
        "br/warehouse/est_v2/est_plus",
        "br/warehouse/est_v2/est_sym",
        "br/warehouse/fin_v1/fin_sym",
        "br/paged_queue/orgs",
        "br/paged_queue/rounds",
        "br/oauth_cursor/sessions",
        "br/oauth_cursor/live_sessions",
        "metric_feed",
    ] {
        assert!(!got.contains_key(k), "{k} must not be produced");
    }
    let ops: BTreeMap<&str, Vec<&str>> = plan["ops"]
        .as_array()
        .expect("ops")
        .iter()
        .map(|o| {
            (
                o["name"].as_str().expect("name"),
                o["replications"]
                    .as_array()
                    .expect("reps")
                    .iter()
                    .map(|r| r.as_str().expect("rep"))
                    .collect(),
            )
        })
        .collect();
    let daily: BTreeSet<&str> = ops
        .keys()
        .copied()
        .filter(|o| o.starts_with("warehouse_") && !o.starts_with("warehouse_hourly_"))
        .collect();
    assert_eq!(
        daily,
        BTreeSet::from([
            "warehouse_est_consensus",
            "warehouse_fin_statements",
            "warehouse_est_actuals",
            "warehouse_reference"
        ])
    );
    assert_eq!(ops["bulk_load"], ["queue_chain_bulk"]);
}

/// Whether two `*`-globs can match a common string.
fn globs_intersect(a: &[u8], b: &[u8]) -> bool {
    match (a.split_first(), b.split_first()) {
        (None, None) => true,
        (Some((b'*', ra)), _) => {
            globs_intersect(ra, b) || (!b.is_empty() && globs_intersect(a, &b[1..]))
        }
        (_, Some((b'*', rb))) => {
            globs_intersect(a, rb) || (!a.is_empty() && globs_intersect(&a[1..], b))
        }
        (Some((x, ra)), Some((y, rb))) => x == y && globs_intersect(ra, rb),
        _ => false,
    }
}

#[test]
fn glob_intersection() {
    let t = |a: &str, b: &str| globs_intersect(a.as_bytes(), b.as_bytes());
    assert!(t("est_core_cons_*f", "est_core_cons_af"));
    assert!(t("est_core_cons_*f", "est_core_*"));
    assert!(t("a*", "*b"));
    assert!(!t("est_core_cons_*f", "est_core_cons_rec"));
    assert!(!t("fin_core_*", "fin_plus_*"));
    assert!(!t("x", "xy"));
    assert!(t("x*", "x"));
}

/// The table pattern a stream writes: its `object` with `member.*` replaced by the source
/// selector's parts (globs for `from.tables`).
fn written(s: &Value) -> String {
    let from = &s["from"];
    let src = from["table"]
        .as_str()
        .or_else(|| from["tables"].as_str())
        .unwrap_or_default();
    let (schema, table) = src.rsplit_once('.').unwrap_or(("", src));
    let obj = s["object"].as_str().expect("object");
    let out = obj
        .replace("{{ member.table }}", table)
        .replace("{{ member.schema }}", schema);
    assert!(!out.contains("{{"), "unsupported object template {obj}");
    out
}

/// No two replications write the same table unless they share a `lock_group`.
#[test]
fn shared_tables_share_a_lock_group() {
    let plan = static_plan();
    let mut writers: Vec<(String, Option<String>, String, String)> = Vec::new();
    for r in plan["replications"].as_array().expect("replications") {
        let name = r["name"].as_str().expect("name").to_owned();
        let lock = r["lock_group"].as_str().map(str::to_owned);
        let target = r["target"]["name"].as_str().expect("target").to_owned();
        for s in r["streams"].as_array().expect("streams") {
            writers.push((name.clone(), lock.clone(), target.clone(), written(s)));
        }
    }
    let mut shared = BTreeSet::new();
    for (i, a) in writers.iter().enumerate() {
        for b in &writers[i + 1..] {
            if a.2 != b.2 || !globs_intersect(a.3.as_bytes(), b.3.as_bytes()) {
                continue;
            }
            assert_ne!(a.0, b.0, "two streams of `{}` write {}", a.0, a.3);
            assert!(
                a.1.is_some() && a.1 == b.1,
                "`{}` and `{}` both write {}.{} without a shared lock_group",
                a.0,
                b.0,
                a.2,
                a.3
            );
            let (x, y) = if a.0 < b.0 { (a, b) } else { (b, a) };
            shared.insert((x.0.clone(), y.0.clone()));
        }
    }
    // Daily and hourly warehouse replications overlap on every hourly table family.
    let pairs: BTreeSet<(&str, &str)> = shared
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    assert_eq!(
        pairs,
        BTreeSet::from([
            ("warehouse_est_actuals", "warehouse_hourly_est_actuals"),
            ("warehouse_est_consensus", "warehouse_hourly_est_consensus"),
            (
                "warehouse_fin_statements",
                "warehouse_hourly_fin_statements"
            ),
        ])
    );
}
