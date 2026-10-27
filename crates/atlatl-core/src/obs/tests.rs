use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};

use super::events::*;
use super::report::*;
use super::schema_check::validate;
use super::*;
use crate::error::{AtlatlError, ErrorClass};
use crate::retry::{AttemptRecord, RetryLevel};

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("buf").extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Buf {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("buf").clone()).expect("utf8")
    }
    fn lines(&self) -> Vec<Value> {
        self.text()
            .lines()
            .map(|l| serde_json::from_str(l).expect("json line"))
            .collect()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
    type Writer = Buf;
    fn make_writer(&'a self) -> Buf {
        self.clone()
    }
}

fn secret(tag: &str) -> String {
    let s = format!("s3cr3t-{tag}-{}", crate::ids::ulid());
    crate::redact::register(&s);
    s
}

fn at(body: EventBody) -> String {
    serde_json::to_string(&Event {
        v: EVENTS_VERSION,
        ts: "2026-09-28T12:00:00.000Z".into(),
        run_id: "01RUN".into(),
        body,
    })
    .expect("ser")
}

const HEAD: &str = r#"{"v":1,"ts":"2026-09-28T12:00:00.000Z","run_id":"01RUN","#;

fn sample_member() -> MemberReport {
    MemberReport {
        object: Some("catalog.br.est_core_cons_af".into()),
        duration_ms: 1200,
        retries: vec![RetryReport {
            level: RetryLevel::Request,
            attempt: 1,
            class: ErrorClass::Transient,
            message: "connection reset".into(),
            wait_ms: 2000,
        }],
        rows: RowCounts {
            extracted: 10,
            staged: 10,
            inserted: 7,
            updated: 2,
            deleted: 0,
            unchanged: 1,
            rejected: 0,
        },
        bytes: ByteCounts { read: 4096 },
        state: StateChange {
            before: None,
            after: Some(json!({"watermark": "2026-09-27"})),
        },
        lake_snapshot: Some(12345),
        chunks: 8,
        cdc: Some(CdcReport {
            window: CdcWindow {
                from_snapshot: Some(10),
                to_snapshot: Some(12),
                from_time: Some("2026-09-28T11:00:00.000Z".into()),
                to_time: Some("2026-09-28T12:00:00.000Z".into()),
            },
            touched_keys: 5,
            upserts_sent: 5,
            rows_inserted: 2,
            rows_updated: 2,
            rows_unchanged: 1,
            rows_deleted: 1,
            deletes_ignored: 0,
            snapshots_behind: 0,
            seconds_behind: 0.5,
            resynced: false,
            slot: None,
        }),
        http: Some(HttpStats {
            requests: 3,
            retries: 1,
            throttle_wait_ms: 250,
            status_counts: [("200".to_owned(), 2), ("429".to_owned(), 1)].into(),
            items_deferred: 7,
            items_held: 2,
            skipped_items: vec![SkippedItem {
                key: "symbol=XYZ".into(),
                reason: "404".into(),
            }],
        }),
        schema: Some(SchemaReport {
            status: SchemaStatus::Violated,
            mode: SchemaMode::Enforce,
            severity: Some(Severity::Warn),
            records: 10,
            new_fields: vec!["Extra".into()],
            missing_fields: vec![],
            violations: vec![SchemaViolation {
                field: "Importance".into(),
                kind: ViolationKind::Domain,
                count: 2,
                samples: vec![json!("4")],
            }],
        }),
        rejects: RejectReport {
            new: 1,
            resolved: 0,
            expired: 0,
            open: 1,
            samples: vec![json!({"pk": [1], "error": "bad cast"})],
        },
        warnings: vec![ReportWarning {
            code: "member_missing".into(),
            message: "x".into(),
        }],
        ..MemberReport::new("est_v2.est_core_cons_af")
    }
}

fn sample_replication(name: &str, status: ReplicationStatus) -> ReplicationReport {
    let mut stream = StreamReport::new(
        "warehouse/est_v2/est_core_consf",
        "br/warehouse/est_v2/est_core_consf",
        false,
    );
    stream.members.push(sample_member());
    ReplicationReport {
        status,
        attempts: 1,
        hops: vec![HopReport {
            target: "lake".into(),
            status: HopStatus::Committed,
            lake_snapshot: Some(12345),
            error: None,
        }],
        streams: vec![stream],
        partial: false,
        ..ReplicationReport::new(name, "op_a")
    }
}

fn sample_report() -> RunReport {
    let mut r = RunReport::new(RunMeta {
        run_id: "01RUN".into(),
        external_run_id: Some("dagster-uuid".into()),
        env: Some("dev".into()),
        plan_hash: Some("abc".into()),
    });
    r.engine = EngineStats {
        duckdb_version: Some("1.5.0".into()),
        memory_limit: Some("4GB".into()),
        threads: Some(4),
        memory_mode: Some(crate::config::model::MemoryMode::Low),
        peak_rss_bytes: 1,
        peak_duckdb_bytes: 2,
    };
    r.replications
        .push(sample_replication("a", ReplicationStatus::Succeeded));
    let mut failed = sample_replication("b", ReplicationStatus::Failed);
    failed.error = Some(
        (&AtlatlError::data("duplicate PKs")
            .code("duplicate_pk")
            .samples(vec![json!({"pk": 1})]))
            .into(),
    );
    r.replications.push(failed);
    r.finalize();
    r
}

fn every_event() -> Vec<EventBody> {
    vec![
        EventBody::RunStarted(RunStarted {
            env: Some("dev".into()),
            selection: RunSelection {
                replications: vec!["a".into()],
                ..Default::default()
            },
            plan_hash: "abc".into(),
            atlatl_version: "0.1.0".into(),
            duckdb_version: None,
        }),
        EventBody::ReplicationStarted(ReplicationStarted {
            replication: "a".into(),
            op: "op_a".into(),
            streams: vec!["s1".into()],
        }),
        EventBody::StreamProgress(StreamProgress {
            replication: "a".into(),
            stream_id: "s1".into(),
            member: Some("m".into()),
            phase: Phase::Extract,
            rows_so_far: 10,
            bytes_so_far: 100,
            chunk: 1,
            http: None,
        }),
        EventBody::Retrying(Retrying::new(
            "a",
            Some("s1".into()),
            None,
            &AttemptRecord {
                level: RetryLevel::Request,
                attempt: 2,
                class: ErrorClass::Throttled,
                message: "429".into(),
                wait_ms: 1500,
            },
        )),
        EventBody::HopCommitted(HopCommitted {
            replication: "a".into(),
            hop: 0,
            target: "lake".into(),
            lake_snapshot: Some(7),
        }),
        EventBody::ReplicationCompleted(sample_replication("a", ReplicationStatus::Succeeded)),
        EventBody::Throttle(Throttle {
            connection: "record_queue".into(),
            wait_ms: 500,
            reason: "rate_limit".into(),
        }),
        EventBody::SchemaDrift(SchemaDrift::new(
            "a",
            "s1",
            None,
            sample_member().schema.as_ref().expect("schema"),
        )),
        EventBody::Warning(Warning {
            replication: None,
            stream_id: Some("s1".into()),
            code: "skipped_empty".into(),
            message: "no rows".into(),
        }),
        EventBody::RunCompleted(RunCompleted {
            status: RunStatus::PartialFailure,
            exit_code: 1,
            counts: RunCounts {
                succeeded: 1,
                failed: 1,
                ..Default::default()
            },
            report_path: Some("/tmp/report.json".into()),
        }),
    ]
}

#[test]
fn event_snapshots() {
    let cases = [
        (
            EventBody::ReplicationStarted(ReplicationStarted {
                replication: "a".into(),
                op: "op_a".into(),
                streams: vec!["s1".into(), "s2".into()],
            }),
            r#""type":"replication.started","replication":"a","op":"op_a","streams":["s1","s2"]}"#,
        ),
        (
            EventBody::StreamProgress(StreamProgress {
                replication: "a".into(),
                stream_id: "s1".into(),
                member: None,
                phase: Phase::Stage,
                rows_so_far: 10,
                bytes_so_far: 100,
                chunk: 3,
                http: None,
            }),
            r#""type":"stream.progress","replication":"a","stream_id":"s1","phase":"stage","rows_so_far":10,"bytes_so_far":100,"chunk":3}"#,
        ),
        (
            EventBody::Retrying(Retrying::new(
                "a",
                None,
                None,
                &AttemptRecord {
                    level: RetryLevel::Commit,
                    attempt: 1,
                    class: ErrorClass::Conflict,
                    message: "conflict".into(),
                    wait_ms: 100,
                },
            )),
            r#""type":"retrying","replication":"a","level":"commit","attempt":1,"error_class":"conflict","message":"conflict","wait_ms":100}"#,
        ),
        (
            EventBody::HopCommitted(HopCommitted {
                replication: "a".into(),
                hop: 1,
                target: "serve".into(),
                lake_snapshot: None,
            }),
            r#""type":"hop.committed","replication":"a","hop":1,"target":"serve"}"#,
        ),
        (
            EventBody::Throttle(Throttle {
                connection: "rq".into(),
                wait_ms: 5,
                reason: "credits".into(),
            }),
            r#""type":"throttle","connection":"rq","wait_ms":5,"reason":"credits"}"#,
        ),
        (
            EventBody::Warning(Warning {
                replication: Some("a".into()),
                stream_id: None,
                code: "c".into(),
                message: "m".into(),
            }),
            r#""type":"warning","replication":"a","code":"c","message":"m"}"#,
        ),
        (
            EventBody::RunCompleted(RunCompleted {
                status: RunStatus::Succeeded,
                exit_code: 0,
                counts: RunCounts {
                    succeeded: 2,
                    ..Default::default()
                },
                report_path: None,
            }),
            r#""type":"run.completed","status":"succeeded","exit_code":0,"counts":{"succeeded":2,"failed":0,"skipped":0,"killed":0,"not_started":0},"report_path":null}"#,
        ),
        (
            EventBody::RunStarted(RunStarted {
                env: None,
                selection: RunSelection::default(),
                plan_hash: "h".into(),
                atlatl_version: "0.1.0".into(),
                duckdb_version: Some("1.5".into()),
            }),
            r#""type":"run.started","env":null,"selection":{"replications":[],"streams":[],"tags":[],"with_deps":false},"plan_hash":"h","atlatl_version":"0.1.0","duckdb_version":"1.5"}"#,
        ),
    ];
    for (body, tail) in cases {
        assert_eq!(at(body), format!("{HEAD}{tail}"));
    }
}

#[test]
fn every_event_type_round_trips_with_its_type() {
    for body in every_event() {
        let s = at(body.clone());
        let v: Value = serde_json::from_str(&s).expect("json");
        assert_eq!(v["v"], 1);
        assert_eq!(v["type"], body.type_name());
        let back: Event = serde_json::from_str(&s).expect("de");
        assert_eq!(back.body, body);
    }
}

#[test]
fn replication_completed_carries_the_report_entry() {
    let v: Value = serde_json::from_str(&at(EventBody::ReplicationCompleted(sample_replication(
        "a",
        ReplicationStatus::Succeeded,
    ))))
    .expect("json");
    assert_eq!(v["type"], "replication.completed");
    assert_eq!(v["status"], "succeeded");
    let s = &v["streams"][0];
    assert_eq!(s["asset_key"], "br/warehouse/est_v2/est_core_consf");
    assert_eq!(s["members"][0]["rows"]["inserted"], 7);
    assert_eq!(s["members"][0]["rejects"]["open"], 1);
    assert_eq!(s["members"][0]["state"]["after"]["watermark"], "2026-09-27");
}

#[test]
fn sink_writes_ndjson_lines_to_a_file() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("events.ndjson");
    let target: EventTarget = path.to_str().expect("utf8").parse().expect("target");
    let sink = EventSink::open(Some(&target), "01RUN").expect("open");
    for body in every_event() {
        assert!(sink.emit(body));
    }
    // Flushed per line: readable before the sink is dropped.
    let text = std::fs::read_to_string(&path).expect("read");
    let lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).expect("line"))
        .collect();
    assert_eq!(lines.len(), every_event().len());
    for (l, b) in lines.iter().zip(every_event()) {
        assert_eq!(l["v"], 1);
        assert_eq!(l["run_id"], "01RUN");
        assert_eq!(l["type"], b.type_name());
        assert!(l["ts"].as_str().expect("ts").ends_with('Z'));
    }
}

#[cfg(unix)]
#[test]
fn sink_writes_to_an_inherited_pipe_fd() {
    use std::os::fd::IntoRawFd;
    let (mut rx, tx) = io::pipe().expect("pipe");
    let fd = tx.into_raw_fd();
    let target: EventTarget = format!("fd:{fd}").parse().expect("target");
    assert_eq!(target, EventTarget::Fd(fd));
    let sink = EventSink::open(Some(&target), "01RUN").expect("open fd");
    let clone = sink.clone();
    let t = std::thread::spawn(move || {
        for i in 0..50 {
            clone.emit(EventBody::Warning(Warning {
                replication: None,
                stream_id: None,
                code: format!("w{i}"),
                message: "x".repeat(200),
            }));
        }
    });
    for i in 0..50 {
        sink.emit(EventBody::Throttle(Throttle {
            connection: format!("c{i}"),
            wait_ms: i,
            reason: "r".into(),
        }));
    }
    t.join().expect("join");
    drop(sink);
    // EOF proves the sink owned (and closed) the inherited descriptor.
    let mut out = String::new();
    rx.read_to_string(&mut out).expect("read");
    let lines: Vec<Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).expect("whole lines, no interleaving"))
        .collect();
    assert_eq!(lines.len(), 100);
    assert_eq!(lines.iter().filter(|l| l["type"] == "warning").count(), 50);
}

#[test]
fn event_targets_parse() {
    assert_eq!("-".parse::<EventTarget>(), Ok(EventTarget::Stdout));
    assert_eq!("fd:3".parse::<EventTarget>(), Ok(EventTarget::Fd(3)));
    assert_eq!(
        "out/ev.ndjson".parse::<EventTarget>(),
        Ok(EventTarget::Path("out/ev.ndjson".into()))
    );
    assert!("fd:x".parse::<EventTarget>().is_err());
    assert!("fd:-1".parse::<EventTarget>().is_err());
    assert!("".parse::<EventTarget>().is_err());
    assert_eq!(EventTarget::Fd(3).to_string(), "fd:3");
}

#[cfg(unix)]
#[test]
fn closed_fd_is_an_error() {
    let target = EventTarget::Fd(987_654);
    assert!(EventSink::open(Some(&target), "r").is_err());
}

#[test]
fn disabled_sink_builds_nothing() {
    let sink = EventSink::disabled();
    assert!(!sink.is_enabled());
    assert!(!sink.emit_with(|| panic!("must not build events when disabled")));
    assert!(!EventSink::open(None, "r").expect("none").is_enabled());
}

#[test]
fn write_errors_disable_the_sink() {
    struct Broken;
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let sink = EventSink::to_writer(Broken, "r");
    assert!(sink.is_enabled());
    assert!(!sink.emit(every_event().remove(0)));
    assert!(!sink.is_enabled());
}

fn progress(stream: &str, rows: u64) -> StreamProgress {
    StreamProgress {
        replication: "a".into(),
        stream_id: stream.into(),
        member: None,
        phase: Phase::Extract,
        rows_so_far: rows,
        bytes_so_far: 0,
        chunk: 0,
        http: None,
    }
}

#[tokio::test(start_paused = true)]
async fn progress_is_rate_limited_per_stream() {
    let buf = Buf::default();
    let sink = EventSink::to_writer(buf.clone(), "r");
    assert!(sink.progress(progress("s1", 1)));
    assert!(!sink.progress(progress("s1", 2)));
    assert!(sink.progress(progress("s2", 1)), "limit is per stream");
    tokio::time::advance(Duration::from_millis(999)).await;
    assert!(!sink.progress(progress("s1", 3)));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(sink.progress(progress("s1", 4)));
    assert!(!sink.progress(progress("s1", 5)));
    assert!(
        sink.emit(EventBody::StreamProgress(progress("s1", 6))),
        "emit bypasses"
    );
    let rows: Vec<(String, u64)> = buf
        .lines()
        .iter()
        .map(|l| {
            (
                l["stream_id"].as_str().expect("id").to_owned(),
                l["rows_so_far"].as_u64().expect("rows"),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            ("s1".into(), 1),
            ("s2".into(), 1),
            ("s1".into(), 4),
            ("s1".into(), 6)
        ]
    );
}

#[test]
fn events_are_redacted() {
    let s = secret("ev");
    let buf = Buf::default();
    let sink = EventSink::to_writer(buf.clone(), "r");
    sink.emit(EventBody::Warning(Warning {
        replication: None,
        stream_id: None,
        code: "c".into(),
        message: format!("GET https://api.x/q?apikey=plain&x=1 with \"{s}\""),
    }));
    let text = buf.text();
    assert!(!text.contains(&s), "{text}");
    assert!(!text.contains("plain"), "{text}");
    let v: Value = serde_json::from_str(text.trim()).expect("still valid json");
    assert_eq!(
        v["message"],
        "GET https://api.x/q?apikey=****&x=1 with \"****\""
    );
}

fn log_with(format: log::LogFormat, f: impl FnOnce()) -> String {
    let buf = Buf::default();
    let sub = log::subscriber(
        format,
        tracing_subscriber::EnvFilter::new("debug"),
        buf.clone(),
        false,
    );
    tracing::subscriber::with_default(sub, f);
    buf.text()
}

#[test]
fn logs_are_redacted_in_both_formats() {
    let s = secret("log");
    let quoted = secret("lo\"g");
    for format in [log::LogFormat::Text, log::LogFormat::Json] {
        let out = log_with(format, || {
            let _run = log::run_span("01RUN").entered();
            let _rep = log::replication_span("a", 2).entered();
            tracing::info!(token = %s, "connecting with {s} and {quoted}");
            tracing::debug!(sql = %format!("SELECT '{s}'"), "statement");
        });
        assert!(!out.contains(&s), "{format:?}: {out}");
        assert!(!out.contains("lo\\\"g-"), "{format:?}: {out}");
        assert!(!out.contains("lo\"g-"), "{format:?}: {out}");
        assert_eq!(out.lines().count(), 2, "{out}");
        assert!(out.contains("****"));
        if format == log::LogFormat::Json {
            for l in out.lines() {
                let v: Value = serde_json::from_str(l).expect("json log line");
                let spans = v["spans"].as_array().expect("spans");
                assert_eq!(spans[0]["run_id"], "01RUN");
                assert_eq!(spans[1]["replication"], "a");
                assert_eq!(spans[1]["attempt"], 2);
            }
        } else {
            assert!(out.contains("run_id=01RUN"), "{out}");
        }
    }
}

#[test]
fn log_filter_applies() {
    let buf = Buf::default();
    let sub = log::subscriber(
        log::LogFormat::Text,
        tracing_subscriber::EnvFilter::new("warn"),
        buf.clone(),
        false,
    );
    tracing::subscriber::with_default(sub, || {
        tracing::info!("hidden");
        tracing::warn!("shown");
    });
    let out = buf.text();
    assert!(out.contains("shown") && !out.contains("hidden"), "{out}");
}

#[test]
fn log_format_parses() {
    assert_eq!("json".parse(), Ok(log::LogFormat::Json));
    assert_eq!("text".parse(), Ok(log::LogFormat::Text));
    assert!("yaml".parse::<log::LogFormat>().is_err());
}

#[test]
fn report_is_redacted_on_write() {
    let s = secret("rep");
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("report.json");
    let h = ReportHandle::new(sample_report());
    h.replication("b", |r| {
        r.error = Some(AtlatlError::transient(format!("password={s} host=x")).into());
    });
    h.member(
        "a",
        "warehouse/est_v2/est_core_consf",
        "est_v2.est_core_cons_af",
        |m| {
            m.rejects.samples.push(json!({"raw": s.clone()}));
            m.state.after = Some(json!({s.clone(): 1}));
        },
    )
    .expect("member");
    let r = h.write(&path).expect("write");
    assert!(r.replications[1].error.is_some());
    let text = std::fs::read_to_string(&path).expect("read");
    assert!(!text.contains(&s), "{text}");
    let v: Value = serde_json::from_str(&text).expect("json");
    assert_eq!(
        v["replications"][1]["error"]["message"],
        "password=**** host=x"
    );
    assert_eq!(v["engine"]["memory_mode"], "low");
}

#[test]
fn atomic_write_never_exposes_a_partial_report() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("report.json");
    let old = sample_report();
    old.write(&path).expect("first");
    let before = std::fs::read(&path).expect("read");

    // Crash between staging and rename: the old report is intact.
    let mut new = sample_report();
    new.run_id = "02NEW".into();
    let bytes = new.to_redacted_json().expect("json");
    let tmp = stage_tmp(&path, &bytes).expect("stage");
    assert_eq!(
        tmp.parent(),
        path.parent(),
        "same directory, so rename is atomic"
    );
    assert_eq!(std::fs::read(&path).expect("read"), before);
    assert_eq!(std::fs::read(&tmp).expect("tmp"), bytes);

    commit_tmp(&tmp, &path).expect("commit");
    let v: Value = serde_json::from_slice(&std::fs::read(&path).expect("read")).expect("json");
    assert_eq!(v["run_id"], "02NEW");
    let names: Vec<_> = std::fs::read_dir(dir.path())
        .expect("ls")
        .map(|e| e.expect("entry").file_name())
        .collect();
    assert_eq!(names, ["report.json"], "no temp files left behind");
}

#[test]
fn failed_write_leaves_nothing() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("missing").join("report.json");
    assert!(sample_report().write(&path).is_err());
    assert!(!path.exists());
}

fn with(replications: &[ReplicationStatus]) -> RunReport {
    let mut r = RunReport::new(RunMeta::default());
    for (i, s) in replications.iter().enumerate() {
        r.replications.push(ReplicationReport {
            status: *s,
            ..ReplicationReport::new(format!("r{i}"), "op")
        });
    }
    r
}

#[test]
fn exit_codes_follow_spec() {
    use ReplicationStatus::*;
    let cases: Vec<(RunReport, RunStatus, u8)> = vec![
        (with(&[]), RunStatus::Succeeded, 0),
        (
            with(&[Succeeded, UpToDate, Skipped]),
            RunStatus::Succeeded,
            0,
        ),
        (with(&[Succeeded, Failed]), RunStatus::PartialFailure, 1),
        (with(&[Failed, Skipped]), RunStatus::Failed, 1),
        (with(&[Killed]), RunStatus::Failed, 1),
        (with(&[UpToDate, NotStarted]), RunStatus::PartialFailure, 1),
        (
            {
                let mut r = with(&[Succeeded, Failed]);
                r.replications[1].error = Some(ReportError::panic("boom", None));
                r
            },
            RunStatus::PartialFailure,
            3,
        ),
        (
            {
                let mut r = with(&[]);
                r.error = Some(AtlatlError::config("bad yaml").into());
                r
            },
            RunStatus::ConfigError,
            2,
        ),
        (
            {
                let mut r = with(&[Succeeded]);
                r.error = Some(AtlatlError::internal("report dir vanished").into());
                r
            },
            RunStatus::InternalError,
            3,
        ),
        (
            {
                let mut r = with(&[Succeeded, Killed]);
                r.killed_by = Some(KillReason::Sigint);
                r
            },
            RunStatus::Killed,
            130,
        ),
        (
            {
                let mut r = with(&[Succeeded, Failed]);
                r.replications[1].error = Some(ReportError::panic("boom", None));
                r.killed_by = Some(KillReason::Sigterm);
                r
            },
            RunStatus::Killed,
            143,
        ),
        (
            {
                let mut r = with(&[Killed]);
                r.killed_by = Some(KillReason::Timeout);
                r
            },
            RunStatus::Killed,
            1,
        ),
    ];
    for (i, (mut r, status, code)) in cases.into_iter().enumerate() {
        r.finalize();
        assert_eq!((r.status, r.exit_code), (status, code), "case {i}");
    }
}

#[test]
fn builder_tracks_replications_across_tasks() {
    let mut base = RunReport::new(RunMeta {
        run_id: "01RUN".into(),
        ..Default::default()
    });
    for name in ["a", "b", "c", "d"] {
        let mut r = ReplicationReport::new(name, "op");
        r.streams.push(StreamReport::new("s", "k/s", false));
        r.streams.push(StreamReport::new("feed", "k/feed", true));
        base.replications.push(r);
    }
    let h = ReportHandle::new(base);
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let h = h.clone();
            std::thread::spawn(move || {
                for i in 0..100 {
                    h.member("a", "s", &format!("m{}", i % 4), |m| {
                        m.rows.extracted += 1;
                        m.chunks += 1;
                    })
                    .expect("member");
                }
                t
            })
        })
        .collect();
    for t in threads {
        t.join().expect("join");
    }
    assert!(h.start_replication("a"));
    assert!(!h.start_replication("nope"));
    let done = h
        .finish_replication("a", ReplicationStatus::Succeeded, None)
        .expect("a");
    assert_eq!(done.attempts, 1);
    assert_eq!(done.streams[0].status, StreamStatus::Succeeded);
    assert_eq!(done.streams[1].status, StreamStatus::FeedOnly);
    assert_eq!(done.streams[0].members.len(), 4);
    assert_eq!(done.streams[0].rows().extracted, 800);

    h.start_replication("b");
    h.start_replication("b");
    h.replication("b", |r| {
        r.stream_mut("s").expect("s").status = StreamStatus::Failed
    });
    let b = h
        .finish_replication(
            "b",
            ReplicationStatus::Failed,
            Some(AtlatlError::data("dup").into()),
        )
        .expect("b");
    assert_eq!(b.attempts, 2);
    assert_eq!(b.streams[0].status, StreamStatus::Failed);
    assert_eq!(b.error.as_ref().map(|e| e.class), Some(ErrorClass::Data));

    h.start_replication("c"); // in flight when the run ends
    let r = h.finish();
    let st: Vec<_> = r.replications.iter().map(|r| r.status).collect();
    assert_eq!(
        st,
        [
            ReplicationStatus::Succeeded,
            ReplicationStatus::Failed,
            ReplicationStatus::Killed,
            ReplicationStatus::NotStarted
        ]
    );
    assert_eq!(
        r.replications[2].streams[0].status,
        StreamStatus::RolledBack
    );
    assert_eq!(r.status, RunStatus::PartialFailure);
    assert_eq!(r.exit_code, 1);
    assert_eq!(
        r.counts(),
        RunCounts {
            succeeded: 1,
            failed: 1,
            killed: 1,
            not_started: 1,
            skipped: 0
        }
    );
    assert!(r.finished_at.is_some());
}

#[test]
fn committed_hop_keeps_lake_streams_on_serve_failure() {
    let mut r = ReplicationReport::new("x", "op");
    r.hops = vec![
        HopReport {
            target: "lake".into(),
            status: HopStatus::Committed,
            lake_snapshot: Some(1),
            error: None,
        },
        HopReport {
            target: "serve".into(),
            status: HopStatus::Failed,
            lake_snapshot: None,
            error: None,
        },
    ];
    let mut lake = StreamReport::new("lake_s", "k/lake", false);
    lake.status = StreamStatus::Succeeded;
    let mut serve = StreamReport::new("serve_s", "k/serve", false);
    serve.status = StreamStatus::Failed;
    r.streams = vec![lake, serve];
    let mut run = RunReport::new(RunMeta::default());
    run.replications.push(r);
    let h = ReportHandle::new(run);
    let r = h
        .finish_replication("x", ReplicationStatus::Failed, None)
        .expect("x");
    assert_eq!(r.streams[0].status, StreamStatus::Succeeded);
    assert_eq!(r.streams[1].status, StreamStatus::Failed);
    let run = h.finish();
    assert_eq!(
        (run.status, run.exit_code),
        (RunStatus::PartialFailure, 1),
        "the committed lake hop is a partial success"
    );
}

#[test]
fn kill_write_is_best_effort_and_bounded() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("report.json");
    let h = ReportHandle::new(with(&[
        ReplicationStatus::Succeeded,
        ReplicationStatus::NotStarted,
        ReplicationStatus::NotStarted,
    ]));
    h.start_replication("r1");
    let code = h.write_on_kill(Some(&path), KillReason::Sigterm, Duration::from_secs(2));
    assert_eq!(code, 143);
    let v: Value = serde_json::from_slice(&std::fs::read(&path).expect("read")).expect("json");
    assert_eq!(v["status"], "killed");
    assert_eq!(v["killed_by"], "sigterm");
    assert_eq!(v["exit_code"], 143);
    let st: Vec<_> = v["replications"]
        .as_array()
        .expect("reps")
        .iter()
        .map(|r| r["status"].as_str().expect("s").to_owned())
        .collect();
    assert_eq!(st, ["succeeded", "killed", "not_started"]);

    // A lock held elsewhere doesn't hang the kill path.
    let h2 = h.clone();
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (rel_tx, rel_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        h2.update(|_| {
            held_tx.send(()).expect("send");
            let _ = rel_rx.recv_timeout(Duration::from_secs(5));
        })
    });
    held_rx.recv().expect("held");
    let t = std::time::Instant::now();
    let other = dir.path().join("other.json");
    let code = h.write_on_kill(Some(&other), KillReason::Sigint, Duration::from_millis(50));
    assert!(t.elapsed() < Duration::from_secs(1));
    assert_eq!(code, 130);
    assert!(!other.exists());
    rel_tx.send(()).expect("release");
    holder.join().expect("join");
}

#[test]
fn from_plan_prepopulates_replications() {
    let dir = tempfile::tempdir().expect("tmp");
    let files = [
        (
            "atlatl.yaml",
            r#"version: 1
environments: {names: [dev], default: dev, base: dev}
state: {lake: lake, locks: lake.catalog}
connections:
  lake:
    type: ducklake
    alias: catalog
    catalog: {type: postgres, host: localhost, database: meta, user: u}
    data_path: /tmp/lake/
    storage: {type: local}
"#,
        ),
        (
            "replications/r.yaml",
            r#"replications:
  r1:
    source: lake
    target: lake
    defaults: {mode: full-refresh, object: "br.{{ member.table }}"}
    streams:
      - id: s/wild
        asset: {key: br/s/wild}
        from: {tables: "a.t_*"}
      - id: s/one
        asset: {key: br/s/one}
        from: {table: a.one}
"#,
        ),
    ];
    for (p, t) in files {
        let p = dir.path().join(p);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, t).expect("write");
    }
    let (project, d) = crate::config::load::load(dir.path());
    assert!(!d.has_errors(), "{:?}", d.items);
    let req = crate::plan::PlanRequest {
        static_plan: true,
        ..crate::plan::PlanRequest::all()
    };
    let (plan, _) = crate::plan::build(
        &project,
        &req,
        Arc::new(crate::template::MapEnv::new::<_, &str, &str>([])),
        &crate::redact::Redactor::new(),
    )
    .expect("plan");
    let r = RunReport::from_plan(&plan, Some("ext".into()));
    assert_eq!(r.plan_hash.as_deref(), Some(plan.plan_hash.as_str()));
    assert_eq!(r.external_run_id.as_deref(), Some("ext"));
    assert_eq!(r.replications.len(), 1);
    let rep = &r.replications[0];
    assert_eq!(rep.status, ReplicationStatus::NotStarted);
    let streams: Vec<_> = rep
        .streams
        .iter()
        .map(|s| {
            (
                s.stream_id.as_str(),
                s.asset_key.as_str(),
                s.members
                    .iter()
                    .map(|m| m.member.as_str())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    assert_eq!(
        streams,
        [
            ("s/wild", "br/s/wild", vec![]),
            ("s/one", "br/s/one", vec!["a.one"])
        ]
    );
}

#[test]
fn schemas_accept_samples_and_reject_bad_values() {
    let rs = serde_json::to_value(report::schema()).expect("schema");
    let sample = serde_json::to_value(sample_report()).expect("report");
    validate(&rs, &sample).expect("report validates");
    let mut bad = sample.clone();
    bad["status"] = json!("exploded");
    assert!(validate(&rs, &bad).is_err());
    let mut bad = sample.clone();
    bad["replications"][0]["streams"][0]["status"] = json!("running");
    assert!(validate(&rs, &bad).is_err());
    let mut bad = sample;
    bad.as_object_mut().expect("obj").remove("run_id");
    assert!(validate(&rs, &bad).is_err());

    let es = serde_json::to_value(events::schema()).expect("schema");
    for body in every_event() {
        let v: Value = serde_json::from_str(&at(body)).expect("event");
        validate(&es, &v).unwrap_or_else(|e| panic!("{}: {e}", v["type"]));
    }
    let mut bad: Value = serde_json::from_str(&at(every_event().remove(1))).expect("event");
    bad["type"] = json!("replication.exploded");
    assert!(validate(&es, &bad).is_err());
}

#[test]
fn redact_value_masks_nested_strings_and_keys() {
    let s = secret("val");
    let mut v = json!({"a": [s.clone(), {"b": format!("x{s}x")}], s.clone(): 1, "n": 5});
    redact_value(&mut v);
    assert_eq!(
        v,
        json!({"a": ["****", {"b": "x****x"}], "****": 1, "n": 5})
    );
}
