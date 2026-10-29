use std::path::Path;
use std::sync::Arc;

use atlatl_core::config::de::from_node;
use atlatl_core::config::model::SchemaFile;
use atlatl_core::config::node::parse_yaml;
use atlatl_core::error::ErrorClass;

use super::*;

fn schema(yaml: &str) -> SourceSchema {
    let node = parse_yaml(yaml, Arc::from(Path::new("schemas/t.yaml"))).expect("yaml");
    from_node::<SchemaFile>(&node)
        .unwrap_or_else(|e| panic!("{e}"))
        .schema
        .value
}

fn staged(records: &[&str]) -> Connection {
    let c = Connection::open_in_memory().expect("db");
    stage_json(
        &c,
        "stg",
        records.iter().enumerate().map(|(i, r)| (i as i64, *r)),
    )
    .expect("stage");
    c
}

fn eval(c: &Connection, yaml: &str) -> Evaluation {
    evaluate(c, &Staged::records("stg"), &schema(yaml)).unwrap_or_else(|e| panic!("{e}"))
}

fn violation(r: &SchemaReport, field: &str, kind: ViolationKind) -> Option<u64> {
    r.violations
        .iter()
        .find(|v| v.field == field && v.kind == kind)
        .map(|v| v.count)
}

const CALENDAR: &str = r#"
schema:
  fields:
    Symbol:       {type: string, nullable: false}
    Date:         {type: timestamp, format: "%Y-%m-%dT%H:%M:%S"}
    ActualValue:  {type: decimal}
    Importance:   {type: integer, values: [1, 2, 3]}
    Revenue:      {type: string}
"#;

#[test]
fn clean_records_pass() {
    let c = staged(&[
        r#"{"Symbol": "AAPL", "Date": "2026-01-02T10:00:00", "ActualValue": 1.10, "Importance": 1, "Revenue": "253.15B"}"#,
        r#"{"Symbol": "MSFT", "Date": null, "ActualValue": "-0.5", "Importance": "3", "Revenue": 12}"#,
    ]);
    let e = eval(&c, CALENDAR);
    assert!(e.failure.is_none(), "{:?}", e.report);
    assert_eq!(e.report.status, SchemaStatus::Ok);
    assert_eq!(e.report.severity, None);
    assert_eq!(e.report.records, 2);
    assert!(e.report.violations.is_empty());
    assert!(!e.drifted());
}

#[test]
fn enforce_fails_with_counts_and_samples() {
    let c = staged(&[
        r#"{"Symbol": "AAPL", "Date": "2026-01-02", "ActualValue": "1.2M", "Importance": 4, "Revenue": "x", "Extra": 1}"#,
        r#"{"Date": "2026-01-02T10:00:00", "ActualValue": 1, "Importance": 2, "Revenue": "y"}"#,
        r#"{"Symbol": null, "Date": "2026-01-02T10:00:00", "ActualValue": 2, "Importance": 9, "Revenue": {"a": 1}}"#,
    ]);
    let e = eval(&c, CALENDAR);
    let r = &e.report;
    assert_eq!(r.status, SchemaStatus::Violated);
    assert_eq!(r.severity, Some(Severity::Error));
    assert_eq!(r.mode, SchemaMode::Enforce);
    assert_eq!(r.new_fields, vec!["Extra".to_owned()]);
    assert!(r.missing_fields.is_empty());
    assert_eq!(violation(r, "Symbol", ViolationKind::Null), Some(2));
    assert_eq!(violation(r, "Date", ViolationKind::Type), Some(1));
    assert_eq!(violation(r, "ActualValue", ViolationKind::Type), Some(1));
    assert_eq!(violation(r, "Importance", ViolationKind::Domain), Some(2));
    assert_eq!(violation(r, "Revenue", ViolationKind::Type), Some(1));
    let imp = r
        .violations
        .iter()
        .find(|v| v.field == "Importance")
        .expect("imp");
    assert_eq!(
        imp.samples,
        vec![
            json!({"ordinal": 0, "value": "4"}),
            json!({"ordinal": 2, "value": "9"})
        ]
    );
    let nulls = r
        .violations
        .iter()
        .find(|v| v.field == "Symbol")
        .expect("sym");
    assert_eq!(nulls.samples[0], json!({"ordinal": 1, "value": null}));
    let err = e.failure.expect("failure");
    assert_eq!(err.class, ErrorClass::Data);
    assert_eq!(err.code.as_deref(), Some("schema_violation"));
    assert!(err.message.contains("new fields Extra"), "{}", err.message);
    assert!(
        err.message
            .contains("domain violations on Importance (2 of 3)")
    );
    assert!(!err.samples.is_empty());
}

#[test]
fn observe_downgrades_to_warn() {
    let c = staged(&[r#"{"Symbol": "A", "Extra": true}"#]);
    let e = eval(
        &c,
        "schema:\n  mode: observe\n  fields:\n    Symbol: {type: integer}\n",
    );
    assert!(e.failure.is_none());
    assert_eq!(e.report.mode, SchemaMode::Observe);
    assert_eq!(e.report.severity, Some(Severity::Warn));
    assert_eq!(e.report.status, SchemaStatus::Violated);
    assert!(e.drifted());
    assert_eq!(violation(&e.report, "Symbol", ViolationKind::Type), Some(1));
}

#[test]
fn warn_and_ignore_policies() {
    let c = staged(&[r#"{"a": "x", "b": 1}"#]);
    let e = eval(
        &c,
        "schema:\n  fields:\n    a: {type: integer}\n    c: {type: string}\n  on_new_field: ignore\n  on_missing_field: warn\n  on_type_violation: warn\n",
    );
    assert!(e.failure.is_none());
    assert_eq!(e.report.severity, Some(Severity::Warn));
    assert!(e.report.new_fields.is_empty());
    assert_eq!(e.report.missing_fields, vec!["c".to_owned()]);
    let e = eval(
        &c,
        "schema:\n  fields:\n    a: {type: integer}\n    b: {type: integer}\n  on_type_violation: ignore\n",
    );
    assert_eq!(e.report.status, SchemaStatus::Ok);
    assert!(e.report.violations.is_empty());
}

#[test]
fn tolerance_counts_without_acting() {
    let recs: Vec<String> = (0..100)
        .map(|i| {
            if i < 3 {
                r#"{"n": "bad"}"#.to_owned()
            } else {
                format!(r#"{{"n": {i}}}"#)
            }
        })
        .collect();
    let refs: Vec<&str> = recs.iter().map(String::as_str).collect();
    let c = staged(&refs);
    let y = |t: &str| format!("schema:\n  fields:\n    n: {{type: integer}}\n  tolerance: {t}\n");
    let e = eval(&c, &y("{ratio: 0.05}"));
    assert_eq!(e.report.status, SchemaStatus::Ok);
    assert_eq!(violation(&e.report, "n", ViolationKind::Type), Some(3));
    assert!(e.failure.is_none());
    let e = eval(&c, &y("{ratio: 0.03}"));
    assert!(e.failure.is_some());
    let e = eval(&c, &y("{min_count: 4}"));
    assert!(e.failure.is_none());
}

#[test]
fn missing_fields_and_empty_staging() {
    let c = staged(&[r#"{"a": 1}"#]);
    let y = "schema:\n  fields:\n    a: {type: integer}\n    b: {type: string, nullable: false}\n";
    let e = eval(&c, y);
    assert_eq!(e.report.missing_fields, vec!["b".to_owned()]);
    // A missing field isn't also a null violation.
    assert!(e.report.violations.is_empty());
    assert!(e.failure.is_some());
    let c = staged(&[]);
    let e = eval(&c, y);
    assert_eq!(e.report.status, SchemaStatus::Ok);
    assert_eq!(e.report.records, 0);
}

#[test]
fn logical_types() {
    let c = staged(&[
        r#"{"i": "+42", "big": 123456789012345678901234567890, "k": "1,234,567", "d": 1e5, "f": "NaN", "b": "T", "dt": "2026-01-02", "ts": "2026-01-02 03:04:05", "tz": "2026-01-02T03:04:05Z", "j": [1], "cd": "02/01/2026"}"#,
        r#"{"i": -7, "big": "123456789012345678901234567890", "k": 12, "d": ".5", "f": 1.5, "b": false, "dt": "2026-12-31", "ts": "2026-01-02T03:04:05", "tz": "2026-01-02 03:04:05+05:30", "j": {"a": 1}, "cd": "31/12/2026"}"#,
    ]);
    let ok = r#"
schema:
  fields:
    i: {type: integer}
    big: {type: integer}
    k: {type: integer, format: thousands_sep}
    d: {type: decimal}
    f: {type: float}
    b: {type: boolean}
    dt: {type: date}
    ts: {type: timestamp}
    tz: {type: timestamptz}
    j: {type: json}
    cd: {type: date, format: "%d/%m/%Y"}
"#;
    let e = eval(&c, ok);
    assert!(e.report.violations.is_empty(), "{:?}", e.report.violations);

    let bad = r#"
schema:
  fields:
    i: {type: boolean}
    k: {type: integer}
    d: {type: integer}
    f: {type: decimal}
    b: {type: integer}
    dt: {type: timestamptz}
    ts: {type: date}
    tz: {type: json}
    j: {type: string}
    cd: {type: date}
"#;
    let e = eval(&c, bad);
    let r = &e.report;
    assert_eq!(violation(r, "i", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "k", ViolationKind::Type), Some(1));
    assert_eq!(violation(r, "d", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "f", ViolationKind::Type), Some(1));
    assert_eq!(violation(r, "b", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "dt", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "ts", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "tz", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "j", ViolationKind::Type), Some(2));
    assert_eq!(violation(r, "cd", ViolationKind::Type), Some(2));
}

#[test]
fn awkward_keys() {
    let c = staged(&[r#"{"a/b": "x", "t~n": 1, "q\"k": 2}"#]);
    let e = eval(
        &c,
        "schema:\n  fields:\n    a/b: {type: integer}\n    t~n: {type: integer}\n    'q\"k': {type: integer}\n",
    );
    assert_eq!(violation(&e.report, "a/b", ViolationKind::Type), Some(1));
    assert_eq!(e.report.violations.len(), 1, "{:?}", e.report);
    assert!(e.report.new_fields.is_empty());
}

#[test]
fn bad_schema_config() {
    let c = staged(&[r#"{"a": 1}"#]);
    let s = schema("schema:\n  fields:\n    a: {type: integer, format: \"%Y\"}\n");
    let e = evaluate(&c, &Staged::records("stg"), &s).expect_err("format");
    assert_eq!(e.class, ErrorClass::Config);
    let s = schema("schema:\n  fields:\n    a: {type: integer, values: [x]}\n");
    assert!(evaluate(&c, &Staged::records("stg"), &s).is_err());
}

#[test]
fn columnar_text_and_typed_staging() {
    let c = Connection::open_in_memory().expect("db");
    c.execute_batch(
        "CREATE TABLE t (id VARCHAR, px VARCHAR, day VARCHAR, _atlatl_loaded_at TIMESTAMP);
         INSERT INTO t VALUES ('1', '1.5', '2026-01-01', now()), ('x', NULL, 'soon', now());
         CREATE TABLE typed (id BIGINT, px DECIMAL(10,2), day DATE, tags VARCHAR[], extra INT);
         INSERT INTO typed VALUES (1, 1.50, DATE '2026-01-01', ['a'], 1);",
    )
    .expect("setup");
    let s = schema(
        "schema:\n  fields:\n    id: {type: integer}\n    px: {type: decimal, nullable: false}\n    day: {type: date}\n    tags: {type: json}\n  on_missing_field: warn\n",
    );
    let st = Staged::Columns {
        relation: "t",
        exclude: &["_atlatl_loaded_at"],
        ordinal: None,
    };
    let e = evaluate(&c, &st, &s).expect("eval");
    let r = &e.report;
    assert!(r.new_fields.is_empty());
    assert_eq!(r.missing_fields, vec!["tags".to_owned()]);
    assert_eq!(violation(r, "id", ViolationKind::Type), Some(1));
    assert_eq!(violation(r, "px", ViolationKind::Null), Some(1));
    assert_eq!(violation(r, "day", ViolationKind::Type), Some(1));
    assert_eq!(
        r.violations[0].samples,
        vec![json!({"value": "x"})],
        "{:?}",
        r.violations
    );

    let st = Staged::Columns {
        relation: "typed",
        exclude: &[],
        ordinal: None,
    };
    let e = evaluate(&c, &st, &s).expect("eval");
    assert_eq!(e.report.new_fields, vec!["extra".to_owned()]);
    assert!(e.report.violations.is_empty(), "{:?}", e.report.violations);
}

#[test]
fn infer_roundtrips() {
    let c = staged(&[
        r#"{"sym": "AAPL", "id": 7, "zip": "02134", "px": 1.10, "ok": true, "at": "2026-01-02", "ts": "2026-01-02T03:04:05Z", "meta": {"a": 1}, "n": null, "odd key": "1"}"#,
        r#"{"sym": "MSFT", "id": "8", "zip": "10001", "px": "2", "ok": false, "at": "2026-01-03", "ts": "2026-01-02T03:04:05+01:00", "meta": [], "n": null}"#,
    ]);
    let inf = infer(&c, &Staged::records("stg")).expect("infer");
    assert_eq!(inf.records, 2);
    let kinds: Vec<(&str, &str, Option<bool>)> = inf
        .schema
        .fields
        .iter()
        .map(|(k, f)| (k.as_str(), type_name(f.kind), f.nullable))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("sym", "string", Some(false)),
            ("id", "integer", Some(false)),
            ("zip", "string", Some(false)),
            ("px", "decimal", Some(false)),
            ("ok", "boolean", Some(false)),
            ("at", "date", Some(false)),
            ("ts", "timestamptz", Some(false)),
            ("meta", "json", Some(false)),
            ("n", "string", None),
            ("odd key", "integer", None),
        ]
    );
    let y = to_yaml(&inf.schema, "drafted\nreview me");
    assert!(
        y.starts_with("# drafted\n# review me\nschema:\n  mode: observe\n"),
        "{y}"
    );
    assert!(y.contains("    \"odd key\": {type: integer}\n"), "{y}");
    let back = schema(&y);
    assert_eq!(back, inf.schema);
    let e = evaluate(&c, &Staged::records("stg"), &back).expect("eval");
    assert_eq!(e.report.status, SchemaStatus::Ok, "{:?}", e.report);
}

/// Per input (JSON text for field `a`), whether it is a domain violation.
fn domain_violations(field: &str, inputs: &[&str]) -> Vec<bool> {
    let y = format!("schema:\n  fields:\n    a: {field}\n");
    inputs
        .iter()
        .map(|i| {
            let c = staged(&[&format!(r#"{{"a": {i}}}"#)]);
            let r = eval(&c, &y).report;
            assert_eq!(violation(&r, "a", ViolationKind::Type), None, "{i}: {r:?}");
            violation(&r, "a", ViolationKind::Domain) == Some(1)
        })
        .collect()
}

#[test]
fn integer_values_compare_exactly() {
    let v = domain_violations(
        "{type: integer, values: [9007199254740992]}",
        &[
            "9007199254740993",
            r#""9007199254740993""#,
            "9007199254740992",
            r#""+9007199254740992""#,
        ],
    );
    assert_eq!(v, [true, true, false, false]);

    let v = domain_violations(
        r#"{type: integer, values: ["170141183460469231731687303715884105727", "-170141183460469231731687303715884105728"]}"#,
        &[
            r#""170141183460469231731687303715884105727""#,
            r#""170141183460469231731687303715884105726""#,
            r#""170141183460469231731687303715884105728""#,
            r#""-170141183460469231731687303715884105728""#,
        ],
    );
    assert_eq!(v, [false, true, true, false]);

    let v = domain_violations(
        r#"{type: integer, values: [1, "2", 3.0, 1.5, "4.5"]}"#,
        &["1", r#""2""#, "3", "4", r#""5""#],
    );
    assert_eq!(v, [false, false, false, true, true]);

    let v = domain_violations("{type: integer, values: [1.5]}", &["1", "2"]);
    assert_eq!(v, [true, true]);
}

#[test]
fn decimal_values_compare_exactly() {
    let v = domain_violations(
        r#"{type: decimal, values: ["1.12345678901234567890"]}"#,
        &[
            r#""1.12345678901234567891""#,
            r#""1.123456789012345678900""#,
            r#""1.1234567890123456789""#,
        ],
    );
    assert_eq!(v, [true, false, false]);

    let v = domain_violations(
        "{type: decimal, values: [1.5]}",
        &[
            r#""1.55""#,
            "1.55",
            r#""1.5000000001""#,
            r#""1.45""#,
            r#""1.50""#,
            "1.5",
            r#""0.15e1""#,
            r#""+1.5""#,
        ],
    );
    assert_eq!(v, [true, true, true, true, false, false, false, false]);

    let v = domain_violations(
        r#"{type: decimal, values: ["12345678901234567890123456", "1e-3", -2, "0"]}"#,
        &[
            r#""12345678901234567890123456.000""#,
            r#""12345678901234567890123457""#,
            r#""0.001""#,
            r#""0.0011""#,
            "-2",
            r#""-2.0""#,
            r#""-0.000""#,
            r#""123456789012345678901234567890123456789""#,
        ],
    );
    assert_eq!(v, [false, true, false, true, false, false, false, true]);
}

#[test]
fn float_values_keep_double_semantics() {
    let v = domain_violations(
        "{type: float, values: [9007199254740992, 1.5]}",
        &[r#""9007199254740993""#, "9007199254740992", "1.5", "1.55"],
    );
    assert_eq!(v, [false, false, false, true]);
}

#[test]
fn decimal_text_parts() {
    let p = |s: &str| decimal_parts(s).map(|(n, d, e)| decimal_text(n, &d, e));
    assert_eq!(p("1.50").as_deref(), Some("1.5"));
    assert_eq!(p("1e-3").as_deref(), Some("0.001"));
    assert_eq!(p("-12.5E2").as_deref(), Some("-1250"));
    assert_eq!(p(".05").as_deref(), Some("0.05"));
    assert_eq!(p("-0.00").as_deref(), Some("0"));
    assert_eq!(p("00120").as_deref(), Some("120"));
    for bad in ["", ".", "1.2.3", "abc", "1e", "NaN", "inf", "1_0"] {
        assert_eq!(decimal_parts(bad), None, "{bad}");
    }
}
