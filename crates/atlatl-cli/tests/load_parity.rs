//! M1.10: the three load replications of the contract fixture (hub + FK shards with enums and
//! `text[]`, a no-include feed with tombstones, a composite-key link table) against a synthetic
//! lake and a legacy-populated Postgres. `cdc bootstrap --dry-run` is the parity report.

use std::path::PathBuf;
use std::process::{Command, Output};

use atlatl_testkit::{LocalLake, TempProject};
use serde_json::Value;

const LOADS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/dagster_contract/replications/load"
);

// `metric_feed` and `funding_participant` have no `columns.include`,
// so every lake column is served: PK, payload, `updated_at` and the au tombstone `deleted_at`.
// `org_event.source` is not in the hub's include and must be ignored.
const LAKE: &str = r#"CREATE SCHEMA lake.au;
CREATE TABLE lake.au.org_event (object_id BIGINT, org_id BIGINT, "type" VARCHAR, "date" DATE, source VARCHAR, updated_at TIMESTAMP);
INSERT INTO lake.au.org_event VALUES
  (1, 100, 'listing', '2025-01-10', 'feed_a', '2025-01-10 09:00'),
  (2, 100, 'report', '2025-01-30', 'feed_b', '2025-01-30 09:00'),
  (3, 101, 'rating', '2025-02-03', 'feed_c', '2025-02-03 09:00'),
  (4, 102, 'funding', '2025-02-05', 'feed_d', '2025-02-05 09:00'),
  (5, 103, 'listing', '2025-02-10', 'feed_a', '2025-02-10 09:00'),
  (6, 101, 'report', '2025-04-24', 'feed_b', '2025-04-24 09:00'),
  (7, 100, 'rating', '2025-02-07', 'feed_c', '2025-02-07 09:00'),
  (8, 104, 'funding', '2025-03-01', 'feed_d', '2025-03-01 09:00');
CREATE TABLE lake.au.org_event_listing (event_id BIGINT, status VARCHAR, proposed_symbol VARCHAR, instrument_id BIGINT,
  venue_id BIGINT, offer_value DECIMAL(20,2), offer_value_currency VARCHAR, unit_price DECIMAL(18,4), unit_price_low DECIMAL(18,4),
  unit_price_high DECIMAL(18,4), unit_count BIGINT, unit_count_low BIGINT, unit_count_high BIGINT, notice_url VARCHAR,
  doc_url VARCHAR, updated_at TIMESTAMP);
INSERT INTO lake.au.org_event_listing VALUES
  (1, 'priced', 'ACME', 5001, 1, 250000000.00, 'USD', 20.0000, 18.0000, 21.0000, 12500000, 12000000, 13000000,
   'https://example.com/a/1', 'https://example.com/f/1', '2025-01-10 09:00'),
  (5, 'expected', 'BETA', NULL, 2, NULL, 'USD', NULL, 14.0000, 16.0000, NULL, 8000000, 9000000,
   'https://example.com/a/5', NULL, '2025-02-10 09:00');
CREATE TABLE lake.au.org_event_report (event_id BIGINT, fiscal_year INTEGER, fiscal_period VARCHAR,
  periodicity VARCHAR, session VARCHAR, call_at TIMESTAMPTZ, updated_at TIMESTAMP_NS);
INSERT INTO lake.au.org_event_report VALUES
  (2, 2024, 'Q4', 'quarterly', 'after_market', '2025-01-30 21:30:00+00', '2025-01-30 09:00:00.000000400'),
  (6, 2025, 'Q1', 'quarterly', 'before_market', '2025-04-24 12:00:00+00', '2025-04-24 09:00:00');
CREATE TABLE lake.au.org_event_rating (event_id BIGINT, instrument_id BIGINT, source_name VARCHAR,
  author VARCHAR, "time" TIMESTAMPTZ, rating_change VARCHAR, rating_current VARCHAR, rating_prior VARCHAR, target_action VARCHAR,
  target_current DECIMAL(18,4), target_prior DECIMAL(18,4), target_currency VARCHAR, updated_at TIMESTAMP);
INSERT INTO lake.au.org_event_rating VALUES
  (3, 5003, 'Firm One', 'J. Doe', '2025-02-03 13:00:00+00', 'upgrade', 'Strong Buy', 'Hold', 'raises', 210.0000, 180.0000, 'USD', '2025-02-03 09:00'),
  (7, 5001, 'Firm Two', NULL, '2025-02-07 13:00:00+00', 'maintain', 'Overweight', 'Overweight', 'maintains', 25.5000, 25.5000, 'USD', '2025-02-07 09:00');
CREATE TABLE lake.au.org_event_funding (event_id BIGINT, name VARCHAR, category VARCHAR, status VARCHAR,
  announced_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, amount_raised DECIMAL(24,6), amount_raised_currency VARCHAR, pre_value DECIMAL(24,6),
  pre_value_currency VARCHAR, post_value DECIMAL(24,6), post_value_currency VARCHAR, unit_price DECIMAL(24,10),
  unit_price_currency VARCHAR, unlinked_names VARCHAR[], updated_at TIMESTAMP, deleted_at TIMESTAMPTZ);
INSERT INTO lake.au.org_event_funding VALUES
  (4, 'Series B', 'venture', 'completed', '2025-02-05 00:00:00+00', '2025-02-20 00:00:00+00', 40000000, 'USD', 160000000, 'USD',
   200000000, 'USD', 12.3456789012, 'USD', ['Acme Ventures', 'Beta Capital'], '2025-02-20 09:00', NULL),
  (8, 'Seed', 'venture', 'announced', '2025-03-01 00:00:00+00', NULL, 2500000.5, 'USD', NULL, NULL,
   NULL, NULL, NULL, NULL, []::VARCHAR[], '2025-03-02 09:00', '2025-03-02 00:00:00+00');
CREATE TABLE lake.au.funding_participant (participant_org_id BIGINT, funding_event_id BIGINT, role VARCHAR,
  is_lead BOOLEAN, amount DECIMAL(24,6), amount_currency VARCHAR, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ);
INSERT INTO lake.au.funding_participant VALUES
  (200, 4, 'lead', true, 20000000, 'USD', '2025-02-20 09:00', NULL),
  (201, 4, 'participant', false, NULL, NULL, '2025-02-20 09:00', NULL),
  (202, 8, 'participant', false, 1000000, 'USD', '2025-03-02 09:00', NULL);
CREATE TABLE lake.au.metric_feed (org_id BIGINT, instrument_id BIGINT, series_id BIGINT, fiscal_year INTEGER,
  fiscal_period VARCHAR, periodicity VARCHAR, value DECIMAL(38,10), currency VARCHAR, period_end_date DATE, updated_at TIMESTAMP, deleted_at TIMESTAMPTZ);
INSERT INTO lake.au.metric_feed VALUES
  (100, 5001, 1, 2024, 'FY', 'annual', 1234.5, 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (100, 5001, 1, 2024, 'Q4', 'quarterly', 310.25, 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (100, 5001, 2, 2024, 'FY', 'annual', -12.75, 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (101, 5003, 1, 2024, 'FY', 'annual', 99.9, 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (100, 5001, 1, 2023, 'FY', 'annual', 1100, 'USD', '2023-12-31', '2024-02-01 09:00', '2025-01-15 00:00:00+00');"#;

const PG_DDL: &str = r#"CREATE SCHEMA app;
CREATE TYPE app.event_type AS ENUM ('listing', 'report', 'rating', 'funding');
CREATE TYPE app.listing_status AS ENUM ('expected', 'priced', 'withdrawn', 'postponed');
CREATE TYPE app.periodicity AS ENUM ('annual', 'semiannual', 'quarterly', 'ttm');
CREATE TYPE app.report_session AS ENUM ('before_market', 'during_market', 'after_market');
CREATE TYPE app.rating_change AS ENUM ('initiate', 'upgrade', 'downgrade', 'maintain', 'reiterate');
CREATE TYPE app.rating AS ENUM ('STRONG_BUY', 'BUY', 'OUTPERFORM', 'OVERWEIGHT', 'HOLD', 'NEUTRAL', 'UNDERWEIGHT', 'SELL', 'STRONG_SELL');
CREATE TABLE app.org_event (object_id bigint PRIMARY KEY, org_id bigint NOT NULL, type app.event_type NOT NULL,
  "date" date, updated_at timestamp, deleted_at timestamptz, inserted_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE app.org_event_listing (event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  status app.listing_status, proposed_symbol text, instrument_id bigint, venue_id bigint, offer_value numeric, offer_value_currency text,
  unit_price numeric(18,4), unit_price_low numeric(18,4), unit_price_high numeric(18,4), unit_count bigint, unit_count_low bigint,
  unit_count_high bigint, notice_url text, doc_url text, updated_at timestamp, deleted_at timestamptz);
CREATE TABLE app.org_event_report (event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  fiscal_year int, fiscal_period text, periodicity app.periodicity, session app.report_session, call_at timestamptz,
  updated_at timestamp, deleted_at timestamptz);
CREATE TABLE app.org_event_rating (event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  instrument_id bigint, source_name text, author text, "time" timestamptz, rating_change app.rating_change,
  rating_current app.rating, rating_prior app.rating, target_action text, target_current numeric(18,4),
  target_prior numeric(18,4), target_currency text, updated_at timestamp, deleted_at timestamptz);
CREATE TABLE app.org_event_funding (event_id bigint PRIMARY KEY REFERENCES app.org_event (object_id),
  name text, category text, status text, announced_at timestamptz, completed_at timestamptz, amount_raised numeric(24,6), amount_raised_currency text,
  pre_value numeric(24,6), pre_value_currency text, post_value numeric(24,6), post_value_currency text,
  unit_price numeric(24,10), unit_price_currency text, unlinked_names text[], updated_at timestamp, deleted_at timestamptz);
CREATE TABLE app.funding_participant (participant_org_id bigint,
  funding_event_id bigint REFERENCES app.org_event_funding (event_id), role text, is_lead boolean,
  amount numeric(24,6), amount_currency text, updated_at timestamp, deleted_at timestamptz,
  PRIMARY KEY (participant_org_id, funding_event_id));
CREATE TABLE app.metric (org_id bigint, instrument_id bigint, series_id bigint, fiscal_year int, fiscal_period text,
  periodicity app.periodicity, value numeric, currency text, period_end_date date, updated_at timestamp, deleted_at timestamptz,
  PRIMARY KEY (org_id, instrument_id, series_id, fiscal_year, fiscal_period, periodicity));"#;

// What the legacy loader left behind, with deliberate discrepancies:
//   stale:        hub 2 `date`, report 6 `session`, rating 7 `rating_current`, funding 4 `unlinked_names`,
//                 participant (201, 4) `amount`, metric series 2 `value`
//   missing:      hub 5 + its listing shard, participant (202, 8), metric org 101
//   target-only:  hub 99 + its listing shard
//   equal by type only (must not be reported): listing 1 `offer_value` '250000000' vs 250000000.00, metric FY
//                 `value` '1234.5000' vs 1234.5, report 2 `updated_at` without the lake's 400 ns, rating 3 raw
//                 'Strong Buy' vs STRONG_BUY (transform), funding 8 and metric 2023 tombstones.
const PG_ROWS: &str = r#"INSERT INTO app.org_event (object_id, org_id, type, "date", updated_at) VALUES
  (1, 100, 'listing', '2025-01-10', '2025-01-10 09:00'),
  (2, 100, 'report', '2025-01-29', '2025-01-30 09:00'),
  (3, 101, 'rating', '2025-02-03', '2025-02-03 09:00'),
  (4, 102, 'funding', '2025-02-05', '2025-02-05 09:00'),
  (6, 101, 'report', '2025-04-24', '2025-04-24 09:00'),
  (7, 100, 'rating', '2025-02-07', '2025-02-07 09:00'),
  (8, 104, 'funding', '2025-03-01', '2025-03-01 09:00'),
  (99, 105, 'listing', '2024-11-01', '2024-11-01 09:00');
INSERT INTO app.org_event_listing VALUES
  (1, 'priced', 'ACME', 5001, 1, '250000000', 'USD', 20, 18, 21, 12500000, 12000000, 13000000,
   'https://example.com/a/1', 'https://example.com/f/1', '2025-01-10 09:00', NULL),
  (99, 'withdrawn', 'GONE', NULL, 1, NULL, 'USD', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-11-01 09:00', NULL);
INSERT INTO app.org_event_report VALUES
  (2, 2024, 'Q4', 'quarterly', 'after_market', '2025-01-30 21:30:00+00', '2025-01-30 09:00:00', NULL),
  (6, 2025, 'Q1', 'quarterly', 'after_market', '2025-04-24 12:00:00+00', '2025-04-24 09:00:00', NULL);
INSERT INTO app.org_event_rating VALUES
  (3, 5003, 'Firm One', 'J. Doe', '2025-02-03 13:00:00+00', 'upgrade', 'STRONG_BUY', 'HOLD', 'raises', 210, 180, 'USD', '2025-02-03 09:00', NULL),
  (7, 5001, 'Firm Two', NULL, '2025-02-07 13:00:00+00', 'maintain', 'NEUTRAL', 'OVERWEIGHT', 'maintains', 25.5, 25.5, 'USD', '2025-02-07 09:00', NULL);
INSERT INTO app.org_event_funding VALUES
  (4, 'Series B', 'venture', 'completed', '2025-02-05 00:00:00+00', '2025-02-20 00:00:00+00', 40000000, 'USD', 160000000, 'USD',
   200000000, 'USD', 12.3456789012, 'USD', '{"Acme Ventures"}', '2025-02-20 09:00', NULL),
  (8, 'Seed', 'venture', 'announced', '2025-03-01 00:00:00+00', NULL, 2500000.5, 'USD', NULL, NULL,
   NULL, NULL, NULL, NULL, '{}', '2025-03-02 09:00', '2025-03-02 00:00:00+00');
INSERT INTO app.funding_participant VALUES
  (200, 4, 'lead', true, 20000000, 'USD', '2025-02-20 09:00', NULL),
  (201, 4, 'participant', false, 5000000, 'USD', '2025-02-20 09:00', NULL);
INSERT INTO app.metric VALUES
  (100, 5001, 1, 2024, 'FY', 'annual', '1234.5000', 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (100, 5001, 1, 2024, 'Q4', 'quarterly', 310.25, 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (100, 5001, 2, 2024, 'FY', 'annual', -12.5, 'USD', '2024-12-31', '2025-02-01 09:00', NULL),
  (100, 5001, 1, 2023, 'FY', 'annual', 1100, 'USD', '2023-12-31', '2024-02-01 09:00', '2025-01-15 00:00:00+00');"#;

const TABLES: [&str; 7] = [
    "org_event",
    "org_event_listing",
    "org_event_report",
    "org_event_rating",
    "org_event_funding",
    "funding_participant",
    "metric",
];

struct Env {
    _lake: LocalLake,
    project: TempProject,
    dsn: String,
}

impl Env {
    fn new(lake_sql: &str, pg_sql: &str, reps: &[(&str, String)]) -> Self {
        let lake = LocalLake::file().expect("lake");
        lake.open("lake")
            .expect("open")
            .execute_batch(lake_sql)
            .expect("lake seed");
        let pg = atlatl_testkit::pg16();
        let db = pg.fresh_db().expect("db");
        let conns = format!(
            "{}  app_pg: {{type: postgres, host: {}, port: {}, database: {db}, user: {}, password: \"{}\", sslmode: disable}}\n",
            lake.connection_yaml("lake", "lake"),
            pg.host,
            pg.port,
            pg.user,
            pg.password
        );
        let mut project = TempProject::new()
            .and_then(|p| p.project(&conns, ""))
            .expect("project");
        for (name, yaml) in reps {
            project = project
                .replication(&format!("load/{name}"), yaml)
                .expect("replication");
        }
        let e = Env {
            _lake: lake,
            project,
            dsn: pg.dsn(&db),
        };
        e.pg(pg_sql);
        e
    }

    fn client<T>(&self, f: impl AsyncFnOnce(&tokio_postgres::Client) -> T) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt")
            .block_on(async {
                let (c, conn) = tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                    .await
                    .expect("connect");
                tokio::spawn(conn);
                f(&c).await
            })
    }

    fn pg(&self, sql: &str) {
        self.client(async |c| c.batch_execute(sql).await.expect(sql));
    }

    fn rows(&self, sql: &str) -> Vec<String> {
        self.client(async |c| {
            c.query(sql, &[])
                .await
                .expect(sql)
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect()
        })
    }

    fn checksums(&self) -> Vec<String> {
        TABLES
            .iter()
            .flat_map(|t| {
                self.rows(&format!("SELECT '{t}:' || count(*) || ':' || md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) FROM app.{t} t"))
            })
            .collect()
    }

    fn cursors_table(&self) -> bool {
        self.rows("SELECT (to_regclass('atlatl.cursors') IS NOT NULL)::text")[0] == "true"
    }

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

    fn bootstrap(&self, rep: &str, dry: bool) -> Value {
        let mut args = vec!["cdc", "bootstrap", rep, "--mode", "adopt"];
        if dry {
            args.push("--dry-run");
        }
        let o = self.atlatl(&args);
        assert!(
            o.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        serde_json::from_slice(&o.stdout).expect("bootstrap json")
    }

    fn run_status(&self, rep: &str) -> String {
        let report: PathBuf = self.project.path().join(format!("{rep}.report.json"));
        let o = self.atlatl(&[
            "--report",
            &report.display().to_string(),
            "run",
            "--replication",
            rep,
        ]);
        assert!(
            o.status.success(),
            "run {rep}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        let r: Value =
            serde_json::from_slice(&std::fs::read(report).expect("report")).expect("json");
        r["replications"][0]["status"]
            .as_str()
            .expect("status")
            .to_owned()
    }
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("{LOADS}/{name}.yaml")).expect("fixture")
}

fn stream<'a>(b: &'a Value, id: &str) -> &'a Value {
    b["streams"]
        .as_array()
        .expect("streams")
        .iter()
        .find(|s| s["stream"] == id)
        .unwrap_or_else(|| panic!("no {id} in {b}"))
}

/// `(op, pk)` per sample, sorted; the pk is joined with `/`.
fn sampled(s: &Value, pk: &[&str]) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = s["samples"]
        .as_array()
        .expect("samples")
        .iter()
        .map(|x| {
            let op = x["op"].as_str().expect("op").to_owned();
            let row = if op == "delete" {
                &x["target"]
            } else {
                &x["lake"]
            };
            let key = pk
                .iter()
                .map(|k| row[*k].to_string().trim_matches('"').to_owned())
                .collect::<Vec<_>>()
                .join("/");
            (op, key)
        })
        .collect();
    v.sort();
    v
}

type Expect = (
    &'static str,
    &'static str,
    &'static [&'static str],
    u64,
    u64,
    &'static [(&'static str, &'static str)],
);

// (replication, stream, pk, upserts, deletes, samples)
const EXPECT: &[Expect] = &[
    (
        "funding_participant",
        "load/funding_participant",
        &["participant_org_id", "funding_event_id"],
        2,
        0,
        &[("insert", "202/8"), ("update", "201/4")],
    ),
    (
        "org_event",
        "load/org_event",
        &["object_id"],
        2,
        1,
        &[("delete", "99"), ("insert", "5"), ("update", "2")],
    ),
    (
        "org_event",
        "load/org_event_listing",
        &["event_id"],
        1,
        1,
        &[("delete", "99"), ("insert", "5")],
    ),
    (
        "org_event",
        "load/org_event_report",
        &["event_id"],
        1,
        0,
        &[("update", "6")],
    ),
    (
        "org_event",
        "load/org_event_rating",
        &["event_id"],
        1,
        0,
        &[("update", "7")],
    ),
    (
        "org_event",
        "load/org_event_funding",
        &["event_id"],
        1,
        0,
        &[("update", "4")],
    ),
    (
        "metric",
        "load/metric",
        &[
            "org_id",
            "instrument_id",
            "series_id",
            "fiscal_year",
            "fiscal_period",
            "periodicity",
        ],
        2,
        0,
        &[
            ("insert", "101/5003/1/2024/FY/annual"),
            ("update", "100/5001/2/2024/FY/annual"),
        ],
    ),
];

const REPS: [&str; 3] = ["funding_participant", "org_event", "metric"];

#[test]
fn dry_run_parity_report_then_adopt() {
    let reps: Vec<(&str, String)> = REPS.iter().map(|r| (*r, fixture(r))).collect();
    let e = Env::new(LAKE, &format!("{PG_DDL}\n{PG_ROWS}"), &reps);
    let before = e.checksums();

    for rep in REPS {
        let b = e.bootstrap(rep, true);
        assert_eq!(
            (b["dry_run"].as_bool(), b["mode"].as_str()),
            (Some(true), Some("adopt")),
            "{b}"
        );
        let want: Vec<&Expect> = EXPECT.iter().filter(|x| x.0 == rep).collect();
        assert_eq!(
            b["streams"].as_array().map(Vec::len),
            Some(want.len()),
            "{b}"
        );
        for (_, id, pk, up, del, samples) in want {
            let s = stream(&b, id);
            assert_eq!(
                (s["upserts"].as_u64(), s["deletes"].as_u64()),
                (Some(*up), Some(*del)),
                "{id}: {s}"
            );
            let want: Vec<(String, String)> = samples
                .iter()
                .map(|(o, k)| ((*o).to_owned(), (*k).to_owned()))
                .collect();
            assert_eq!(sampled(s, pk), want, "{id}: {s}");
            assert!(s["cursor_before"].is_null(), "{s}");
        }
    }
    let listing = stream(&e.bootstrap("org_event", true), "load/org_event_listing").clone();
    let del = listing["samples"]
        .as_array()
        .expect("samples")
        .iter()
        .find(|x| x["op"] == "delete")
        .expect("delete sample")
        .clone();
    assert!(
        del["target"]["_ts"].is_string(),
        "deletes carry the stamp they'd get: {del}"
    );
    let fm = stream(&e.bootstrap("metric", true), "load/metric").clone();
    let upd = fm["samples"]
        .as_array()
        .expect("samples")
        .iter()
        .find(|x| x["op"] == "update")
        .expect("update sample")
        .clone();
    assert_eq!(
        (
            upd["lake"]["value"].as_f64(),
            // Unconstrained numeric is read as exact text.
            upd["target"]["value"]
                .as_str()
                .and_then(|v| v.parse::<f64>().ok())
        ),
        (Some(-12.75), Some(-12.5)),
        "{upd}"
    );

    assert_eq!(e.checksums(), before, "--dry-run wrote to the target");
    assert!(!e.cursors_table(), "--dry-run created atlatl.cursors");

    for rep in REPS {
        let b = e.bootstrap(rep, false);
        for (_, id, _, up, del, samples) in EXPECT.iter().filter(|x| x.0 == rep) {
            let s = stream(&b, id);
            let ins = samples.iter().filter(|x| x.0 == "insert").count() as u64;
            assert_eq!(
                (
                    s["rows_inserted"].as_u64(),
                    s["rows_updated"].as_u64(),
                    s["rows_deleted"].as_u64()
                ),
                (Some(ins), Some(up - ins), Some(*del)),
                "{id}: {s}"
            );
        }
    }
    assert_eq!(
        e.rows("SELECT string_agg(stream_id, ',' ORDER BY stream_id) FROM atlatl.cursors"),
        [
            "load/funding_participant,load/metric,load/org_event,load/org_event_funding,load/org_event_listing,load/org_event_rating,load/org_event_report"
        ]
    );
    assert_eq!(
        e.rows(
            "SELECT string_agg(x, ',' ORDER BY x) FROM (
               SELECT 'hub' || object_id AS x FROM app.org_event WHERE deleted_at IS NOT NULL
               UNION ALL SELECT 'listing' || event_id FROM app.org_event_listing WHERE deleted_at IS NOT NULL) d"
        ),
        ["hub99,listing99"],
        "target-only rows are soft-deleted"
    );
    assert_eq!(
        e.rows("SELECT offer_value::text FROM app.org_event_listing WHERE event_id = 1"),
        ["250000000"],
        "scale-only row untouched"
    );
    assert_eq!(
        e.rows("SELECT rating_current::text FROM app.org_event_rating WHERE event_id = 7"),
        ["OVERWEIGHT"]
    );
    assert_eq!(
        e.rows("SELECT array_to_string(unlinked_names, ',') FROM app.org_event_funding WHERE event_id = 4"),
        ["Acme Ventures,Beta Capital"]
    );

    for rep in REPS {
        let b = e.bootstrap(rep, true);
        for s in b["streams"].as_array().expect("streams") {
            assert_eq!(
                (s["upserts"].as_u64(), s["deletes"].as_u64()),
                (Some(0), Some(0)),
                "{rep} after adopt: {s}"
            );
            assert_eq!(s["samples"], serde_json::json!([]), "{s}");
            assert!(s["cursor_before"].is_i64(), "{s}");
        }
        assert_eq!(e.run_status(rep), "up_to_date", "{rep}");
    }
}

fn one(stream: &str, table: &str, include: &str) -> String {
    format!(
        "replications:\n  r:\n    source: lake\n    target: app_pg\n    streams:\n      - id: {stream}\n        mode: cdc\n        from: {{table: au.{table}}}\n        object: app.{table}\n        primary_key: [id]\n        deletes: {{mode: soft, column: deleted_at}}\n        columns: {{include: [{include}]}}\n"
    )
}

fn counts(b: &Value) -> (Option<u64>, Option<u64>) {
    (
        b["streams"][0]["upserts"].as_u64(),
        b["streams"][0]["deletes"].as_u64(),
    )
}

#[test]
fn served_soft_delete_is_not_redelivered() {
    let e = Env::new(
        "CREATE SCHEMA lake.au; CREATE TABLE lake.au.t AS SELECT 1::BIGINT AS id, 'a' AS v, NULL::TIMESTAMPTZ AS deleted_at;",
        "CREATE SCHEMA app; CREATE TABLE app.t (id bigint PRIMARY KEY, v text, deleted_at timestamptz);
         INSERT INTO app.t VALUES (1, 'a', NULL), (2, 'b', NULL);",
        &[("r", one("s/t", "t", "id, v, deleted_at"))],
    );
    assert_eq!(e.bootstrap("r", false)["streams"][0]["rows_deleted"], 1);
    assert_eq!(counts(&e.bootstrap("r", true)), (Some(0), Some(0)));
}

#[test]
fn jsonb_compares_by_value() {
    let e = Env::new(
        r#"CREATE SCHEMA lake.au; CREATE TABLE lake.au.j AS SELECT 1::BIGINT AS id, '{"src":"x","n":1}'::JSON AS meta;"#,
        r#"CREATE SCHEMA app; CREATE TABLE app.j (id bigint PRIMARY KEY, meta jsonb, deleted_at timestamptz);
           INSERT INTO app.j VALUES (1, '{"src":"x","n":1}');"#,
        &[("r", one("s/j", "j", "id, meta"))],
    );
    assert_eq!(counts(&e.bootstrap("r", true)), (Some(0), Some(0)));
}

#[test]
fn unconstrained_numeric_keeps_precision() {
    let e = Env::new(
        "CREATE SCHEMA lake.au; CREATE TABLE lake.au.n AS SELECT 1::BIGINT AS id, 1234567890123.4567890123::DECIMAL(38,10) AS v;",
        "CREATE SCHEMA app; CREATE TABLE app.n (id bigint PRIMARY KEY, v numeric, deleted_at timestamptz);",
        &[("r", one("s/n", "n", "id, v"))],
    );
    e.bootstrap("r", false);
    assert_eq!(
        e.rows("SELECT v::text FROM app.n"),
        ["1234567890123.4567890123"]
    );
}

#[test]
fn nested_lake_values_compare_and_land_as_json() {
    let e = Env::new(
        "CREATE SCHEMA lake.au; CREATE TABLE lake.au.s AS SELECT 1::BIGINT AS id, {'a': 1, 'b': [1, 2]} AS meta UNION ALL SELECT 2, {'a': 3, 'b': []};",
        r#"CREATE SCHEMA app; CREATE TABLE app.s (id bigint PRIMARY KEY, meta jsonb, deleted_at timestamptz);
           INSERT INTO app.s VALUES (1, '{"b": [1, 2], "a": 1}');"#,
        &[("r", one("s/s", "s", "id, meta"))],
    );
    assert_eq!(
        counts(&e.bootstrap("r", true)),
        (Some(1), Some(0)),
        "row 1 equal as JSON; row 2 missing"
    );
    e.bootstrap("r", false);
    assert_eq!(
        e.rows("SELECT meta::text FROM app.s ORDER BY id"),
        [r#"{"a": 1, "b": [1, 2]}"#, r#"{"a": 3, "b": []}"#]
    );
    assert_eq!(counts(&e.bootstrap("r", true)), (Some(0), Some(0)));
}

#[test]
fn reduced_precision_time_compares_rounded() {
    let e = Env::new(
        "CREATE SCHEMA lake.au; CREATE TABLE lake.au.t AS SELECT * FROM (VALUES
           (1::BIGINT, TIME '10:00:00.4', TIMESTAMP '2025-01-01 10:00:00.6'),
           (2, TIME '23:59:59.6', TIMESTAMP '2025-01-01 23:59:59.6')) t(id, tm, ts);",
        "CREATE SCHEMA app; CREATE TABLE app.t (id bigint PRIMARY KEY, tm time(0), ts timestamp(0), deleted_at timestamptz);
         INSERT INTO app.t VALUES (1, '10:00:00', '2025-01-01 10:00:01'), (2, '23:59:59.6', '2025-01-01 23:59:59.6');",
        &[("r", one("s/t", "t", "id, tm, ts"))],
    );
    assert_eq!(
        e.rows("SELECT tm::text FROM app.t WHERE id = 2"),
        ["24:00:00"],
        "Postgres doesn't wrap"
    );
    assert_eq!(
        counts(&e.bootstrap("r", true)),
        (Some(0), Some(0)),
        "Postgres stored the rounded values"
    );
}

#[test]
fn documents_too_deep_to_canonicalise_still_compare() {
    let deep = |v: &str| format!("{}{v}{}", "[".repeat(600), "]".repeat(600));
    let e = Env::new(
        &format!(
            "CREATE SCHEMA lake.au; CREATE TABLE lake.au.d AS SELECT * FROM (VALUES (1::BIGINT, '{a}'::JSON), (2, '{a}'::JSON), (3, '{a}'::JSON)) t(id, meta);",
            a = deep("1")
        ),
        &format!(
            "CREATE SCHEMA app; CREATE TABLE app.d (id bigint PRIMARY KEY, meta jsonb, deleted_at timestamptz);
             INSERT INTO app.d VALUES (1, '{b}'), (2, NULL), (3, '{a}');",
            a = deep("1"),
            b = deep("2")
        ),
        &[("r", one("s/d", "d", "id, meta"))],
    );
    assert_eq!(
        counts(&e.bootstrap("r", true)),
        (Some(2), Some(0)),
        "a different deep document and a NULL are changes"
    );
}
