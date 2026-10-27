use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use atlatl_core::config::model::{EngineConfig, MemoryMode};
use atlatl_core::config::types::{Auto, ByteSize};
use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_engine::engine::{KILLED, STATEMENT_TIMEOUT};
use atlatl_engine::{
    Batch, Budget, BudgetParams, DUCKDB_VERSION, DbResultExt, Detected, Engine, EngineOptions,
    StmtOptions, StreamOptions, append_temp, check_version,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const MIB: u64 = 1 << 20;
const LONG: &str = "SELECT count(*) FROM range(100000000000) t(i) WHERE hash(i) % 7 = 3";

struct TestEngine {
    engine: Engine,
    _spill: tempfile::TempDir,
}

impl std::ops::Deref for TestEngine {
    type Target = Engine;
    fn deref(&self) -> &Engine {
        &self.engine
    }
}

fn budget(spill: &tempfile::TempDir, threads: u32, channel: u64) -> Budget {
    budget_in(spill, threads, channel, None)
}

fn budget_in(
    spill: &tempfile::TempDir,
    threads: u32,
    channel: u64,
    memory_mode: Option<MemoryMode>,
) -> Budget {
    let cfg = EngineConfig {
        memory_mode,
        memory: Some(Auto::Value(ByteSize(2048 * MIB))),
        threads: Some(Auto::Value(threads)),
        channel_bytes: Some(ByteSize(channel)),
        temp_directory: Some(spill.path().to_string_lossy().into_owned()),
        max_temp_directory_size: Some(ByteSize(1024 * MIB)),
        ..Default::default()
    };
    Budget::compute(&cfg, None, &Detected::detect(), &BudgetParams::default()).expect("budget")
}

fn engine_with(pool: usize, channel: u64) -> TestEngine {
    let spill = tempfile::tempdir().expect("tempdir");
    let mut opts = EngineOptions::new(budget(&spill, 2, channel));
    opts.extension_dir = Some(atlatl_testkit::extension_dir());
    opts.pool_size = Some(pool);
    TestEngine {
        engine: Engine::open(opts).expect("open"),
        _spill: spill,
    }
}

fn engine() -> TestEngine {
    engine_with(4, 256 * MIB)
}

fn scalar(
    sql: &'static str,
) -> impl FnOnce(&mut duckdb::Connection) -> atlatl_core::error::Result<String> {
    move |c| c.query_row(sql, [], |r| r.get::<_, String>(0)).db()
}

#[tokio::test]
async fn settings_applied_at_open() {
    let e = engine();
    let b = e.budget().clone();
    let settings: Vec<(String, String)> = e
        .with_conn(|c| {
            let mut s = c
                .prepare(
                    "SELECT name, value FROM duckdb_settings() WHERE name IN ('memory_limit', \
                     'threads', 'temp_directory', 'max_temp_directory_size', \
                     'preserve_insertion_order', 'autoinstall_known_extensions', \
                     'autoload_known_extensions', 'allow_unsigned_extensions', \
                     'allow_community_extensions', 'extension_directory', 'TimeZone', \
                     'allocator_bulk_deallocation_flush_threshold') ORDER BY name",
                )
                .db()?;
            s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .db()?
                .collect::<Result<_, _>>()
                .db()
        })
        .await
        .expect("settings");
    let get = |k: &str| {
        settings
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
            .unwrap_or_else(|| panic!("no {k} in {settings:?}"))
    };
    // DuckDB truncates to one decimal.
    let (unit, name) = if b.memory_limit >= 1 << 30 {
        (1u64 << 30, "GiB")
    } else {
        (MIB, "MiB")
    };
    let human = format!(
        "{}.{} {name}",
        b.memory_limit / unit,
        b.memory_limit % unit * 10 / unit
    );
    assert_eq!(get("memory_limit"), human, "{b:?}");
    assert_eq!(get("threads"), "2");
    assert_eq!(
        get("temp_directory"),
        b.temp_directory.to_str().expect("utf8")
    );
    assert_eq!(get("max_temp_directory_size"), "1.0 GiB");
    assert_eq!(get("preserve_insertion_order"), "false");
    assert_eq!(get("autoinstall_known_extensions"), "false");
    assert_eq!(get("autoload_known_extensions"), "false");
    assert_eq!(get("allow_unsigned_extensions"), "false");
    assert_eq!(get("allow_community_extensions"), "false");
    assert_eq!(
        get("extension_directory"),
        atlatl_testkit::extension_dir().to_str().expect("utf8")
    );
    assert_eq!(get("TimeZone"), "UTC");
    // `memory_mode: high` leaves DuckDB's default.
    assert_eq!(
        get("allocator_bulk_deallocation_flush_threshold"),
        "512.0 MiB"
    );

    let mut s = e.session().expect("session");
    assert_eq!(
        s.run(scalar("SELECT current_setting('TimeZone')"))
            .await
            .expect("tz"),
        "UTC"
    );
    let st = e
        .stream(
            "SELECT current_setting('TimeZone') AS tz",
            StreamOptions::default(),
        )
        .await
        .expect("stream");
    let batches = st.collect().await.expect("collect");
    let tz = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("str");
    assert_eq!(tz.value(0), "UTC");

    let v = e
        .with_conn(scalar("SELECT library_version FROM pragma_version()"))
        .await
        .expect("version");
    assert_eq!(v, DUCKDB_VERSION);

    e.with_conn(|c| c.execute_batch("LOAD ducklake; LOAD httpfs;").db())
        .await
        .expect("offline extensions load");
    let err = e
        .with_conn(|c| c.execute_batch("LOAD spatial").db())
        .await
        .expect_err("no download");
    assert_eq!(err.class, ErrorClass::Config, "{err}");
}

#[test]
fn version_check() {
    check_version(DUCKDB_VERSION).expect("same");
    let e = check_version("v1.4.0").expect_err("mismatch");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(e.message.contains("v1.4.0") && e.message.contains(DUCKDB_VERSION));

    // The crate pin `=1.MMmmpp.x` encodes DuckDB `vM.m.p`.
    let root = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.toml"))
        .expect("workspace Cargo.toml");
    let pin = root
        .lines()
        .find_map(|l| l.strip_prefix("duckdb = { version = \"=1."))
        .and_then(|r| r.split('.').next())
        .expect("duckdb pin");
    let n: u32 = pin.parse().expect("digits");
    let encoded = format!("v{}.{}.{}", n / 10_000, n / 100 % 100, n % 100);
    assert_eq!(encoded, DUCKDB_VERSION);
}

#[test]
fn extension_dir_resolution() {
    let p = std::path::Path::new("/opt/ext");
    assert_eq!(
        atlatl_engine::resolve_extension_dir(Some(p)).expect("explicit"),
        p
    );
    if std::env::var_os(atlatl_engine::engine::EXTENSION_DIR_ENV).is_none() {
        let d = atlatl_engine::resolve_extension_dir(None).expect("exe-relative");
        assert!(d.ends_with("../share/atlatl/extensions"), "{}", d.display());
    }
}

#[tokio::test]
async fn bulk_flush_threshold_only_in_low_mode_with_worker_threads() {
    for (mode, threads, want) in [
        (MemoryMode::High, 1, "512.0 MiB"),
        (MemoryMode::High, 4, "512.0 MiB"),
        (MemoryMode::Low, 1, "512.0 MiB"),
        (MemoryMode::Low, 2, "4.0 MiB"),
    ] {
        let spill = tempfile::tempdir().expect("tempdir");
        let mut opts = EngineOptions::new(budget_in(&spill, threads, MIB, Some(mode)));
        opts.extension_dir = Some(atlatl_testkit::extension_dir());
        let e = Engine::open(opts).expect("open");
        let v = e
            .with_conn(scalar(
                "SELECT current_setting('allocator_bulk_deallocation_flush_threshold')",
            ))
            .await
            .expect("setting");
        assert_eq!(v, want, "{mode:?} at {threads} threads");
    }
}

#[test]
fn open_rejects_bad_settings() {
    let spill = tempfile::tempdir().expect("tempdir");
    let mut opts = EngineOptions::new(budget(&spill, 2, MIB));
    opts.budget.temp_directory = spill.path().join("x\0y");
    let e = Engine::open(opts).expect_err("NUL in path");
    assert_eq!(e.class, ErrorClass::Config);
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_pool_is_bounded_and_off_the_runtime() {
    let e = engine_with(2, 256 * MIB);
    assert_eq!(e.pool_size(), 2);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let ticks = Arc::new(AtomicUsize::new(0));
    let t = ticks.clone();
    let ticker = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            t.fetch_add(1, Ordering::SeqCst);
        }
    });
    let started = Instant::now();
    let jobs: Vec<_> = (0..6)
        .map(|_| {
            let (e, active, peak) = (e.engine.clone(), active.clone(), peak.clone());
            tokio::spawn(async move {
                e.with_conn(move |c| {
                    let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(n, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(150));
                    active.fetch_sub(1, Ordering::SeqCst);
                    c.query_row("SELECT 1", [], |r| r.get::<_, i32>(0)).db()
                })
                .await
            })
        })
        .collect();
    for j in jobs {
        assert_eq!(j.await.expect("join").expect("run"), 1);
    }
    ticker.abort();
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert!(started.elapsed() >= Duration::from_millis(450));
    assert!(ticks.load(Ordering::SeqCst) >= 20, "runtime was blocked");
}

#[tokio::test]
async fn timeout_interrupts_and_connection_survives() {
    let e = engine_with(1, 256 * MIB);
    let t = Instant::now();
    let err = e
        .with_conn_opts(&StmtOptions::timeout(Duration::from_millis(200)), |c| {
            c.query_row(LONG, [], |r| r.get::<_, i64>(0)).db()
        })
        .await
        .expect_err("timeout");
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Timeout, Some(STATEMENT_TIMEOUT)),
        "{err}"
    );
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    assert_eq!(
        e.with_conn(scalar("SELECT '42'")).await.expect("reuse"),
        "42"
    );

    let mut s = e.session().expect("session");
    s.run(|c| {
        c.execute_batch("CREATE TEMP TABLE keep AS SELECT 7 AS v")
            .db()
    })
    .await
    .expect("temp");
    let err = s
        .run_opts(&StmtOptions::timeout(Duration::from_millis(100)), |c| {
            c.query_row(LONG, [], |r| r.get::<_, i64>(0)).db()
        })
        .await
        .expect_err("timeout");
    assert_eq!(err.class, ErrorClass::Timeout);
    let v = s
        .run(|c| {
            c.query_row("SELECT v FROM keep", [], |r| r.get::<_, i32>(0))
                .db()
        })
        .await
        .expect("session survives with its temp table");
    assert_eq!(v, 7);
}

#[tokio::test]
async fn timeout_covers_every_statement_in_the_call() {
    let e = engine();
    let t = Instant::now();
    let r = e
        .with_conn_opts(&StmtOptions::timeout(Duration::from_millis(150)), |c| {
            let mut errs = 0;
            for _ in 0..3 {
                if c.query_row(LONG, [], |r| r.get::<_, i64>(0)).is_err() {
                    errs += 1;
                }
            }
            Ok(errs)
        })
        .await
        .expect("closure swallowed the errors");
    assert_eq!(r, 3);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
}

#[tokio::test]
async fn stale_interrupt_does_not_leak_into_next_statement() {
    let e = engine();
    let v = e
        .with_conn(|c| {
            c.interrupt_handle().interrupt();
            c.query_row("SELECT 5", [], |r| r.get::<_, i32>(0)).db()
        })
        .await
        .expect("idle interrupt is forgotten");
    assert_eq!(v, 5);
}

#[tokio::test]
async fn cancel_token_kills() {
    let e = engine();
    let token = CancellationToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        t2.cancel();
    });
    let opts = StmtOptions {
        timeout: Some(Duration::from_secs(60)),
        cancel: Some(token),
    };
    let err = e
        .with_conn_opts(&opts, |c| {
            c.query_row(LONG, [], |r| r.get::<_, i64>(0)).db()
        })
        .await
        .expect_err("killed");
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Killed, Some(KILLED)),
        "{err}"
    );
    assert_eq!(
        e.with_conn(scalar("SELECT 'ok'"))
            .await
            .expect("engine alive"),
        "ok"
    );
}

#[tokio::test]
async fn engine_kill_stops_running_and_new_work() {
    let e = engine();
    let k = e.engine.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        k.kill();
    });
    let err = e
        .with_conn(|c| c.query_row(LONG, [], |r| r.get::<_, i64>(0)).db())
        .await
        .expect_err("killed");
    assert_eq!(err.class, ErrorClass::Killed, "{err}");
    let err = e
        .with_conn(scalar("SELECT 'x'"))
        .await
        .expect_err("killed engine");
    assert_eq!(err.class, ErrorClass::Killed);
    let err = e
        .stream("SELECT 1", StreamOptions::default())
        .await
        .expect_err("killed engine");
    assert_eq!(err.class, ErrorClass::Killed);
}

#[tokio::test]
async fn panic_in_closure_is_internal_panic_and_pool_recovers() {
    let e = engine();
    let err = e
        .with_conn(|_| -> atlatl_core::error::Result<()> { panic!("boom in closure") })
        .await
        .expect_err("panicked");
    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(err.code.as_deref(), Some("panic"));
    assert!(err.message.contains("boom in closure"), "{err}");
    assert_eq!(e.with_conn(scalar("SELECT 'x'")).await.expect("ok"), "x");
}

async fn drain(mut s: atlatl_engine::BatchStream) -> (u64, Option<AtlatlError>) {
    let mut rows = 0;
    while let Some(b) = s.next().await {
        match b {
            Ok(b) => rows += b.num_rows() as u64,
            Err(e) => return (rows, Some(e)),
        }
    }
    (rows, None)
}

#[tokio::test]
async fn stream_happy_path() {
    let e = engine();
    let s = e
        .stream(
            "SELECT i, i::VARCHAR AS s FROM range(1000000) t(i)",
            StreamOptions::default(),
        )
        .await
        .expect("stream");
    let schema = s.schema();
    assert_eq!(schema.field(0).name(), "i");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).name(), "s");
    let (rows, err) = drain(s).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(rows, 1_000_000);

    let s = e
        .stream("SELECT 1 AS a WHERE false", StreamOptions::default())
        .await
        .expect("empty");
    assert_eq!(s.schema().fields().len(), 1);
    assert!(s.collect().await.expect("collect").is_empty());
}

#[tokio::test]
async fn stream_surfaces_mid_stream_error() {
    let e = engine();
    let s = e
        .stream(
            "SELECT CASE WHEN i = 500000 THEN error('boom at 500k') ELSE i END AS i FROM range(2000000) t(i)",
            StreamOptions::default(),
        )
        .await
        .expect("stream starts");
    let (rows, err) = drain(s).await;
    let err = err.expect("the error is the last item");
    assert!(rows < 2_000_000);
    assert!(err.message.contains("boom at 500k"), "{err}");
    assert_eq!(err.class, ErrorClass::Data);

    let err = e
        .stream("SELEC 1", StreamOptions::default())
        .await
        .expect_err("prepare error");
    assert_eq!(err.class, ErrorClass::Config, "{err}");
    let err = e
        .stream("SELECT * FROM nope", StreamOptions::default())
        .await
        .expect_err("binder error");
    assert_eq!(err.class, ErrorClass::Config, "{err}");
}

#[tokio::test]
async fn stream_timeout_and_kill() {
    let e = engine();
    let opts = StreamOptions {
        stmt: StmtOptions::timeout(Duration::from_millis(200)),
        ..Default::default()
    };
    let t = Instant::now();
    let err = match e.stream(LONG, opts).await {
        Ok(s) => drain(s).await.1.expect("timeout error"),
        Err(e) => e,
    };
    assert_eq!(
        (err.class, err.code.as_deref()),
        (ErrorClass::Timeout, Some(STATEMENT_TIMEOUT)),
        "{err}"
    );
    assert!(t.elapsed() < Duration::from_secs(3));

    // Killed while the producer is parked on a full channel.
    let token = CancellationToken::new();
    let opts = StreamOptions {
        stmt: StmtOptions {
            timeout: None,
            cancel: Some(token.clone()),
        },
        capacity: 1,
    };
    let mut s = e
        .stream("SELECT i FROM range(100000000) t(i)", opts)
        .await
        .expect("stream");
    let first = s.next().await.expect("item").expect("batch");
    assert!(first.num_rows() > 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();
    let (_, err) = drain(s).await;
    let err = err.expect("killed, not a silent end");
    assert_eq!(err.class, ErrorClass::Killed, "{err}");
}

#[tokio::test]
async fn stream_backpressure_by_bytes() {
    let e = engine_with(4, 256 * 1024);
    let budget = e.byte_budget().clone();
    let mut s = e
        .stream(
            "SELECT i, i * 2 AS j FROM range(3000000) t(i)",
            StreamOptions {
                capacity: 64,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let mut held = vec![];
    loop {
        match tokio::time::timeout(Duration::from_millis(300), s.next()).await {
            Ok(Some(b)) => held.push(b.expect("batch")),
            Ok(None) => panic!("stream ended while batches were held"),
            Err(_) => break,
        }
        assert!(budget.in_use() <= budget.total());
    }
    assert!(!held.is_empty());
    let held_bytes: usize = held.iter().map(|b| b.get_array_memory_size()).sum();
    assert!(
        held_bytes <= budget.total(),
        "{held_bytes} > {}",
        budget.total()
    );
    assert!(
        budget.in_use() > budget.total() / 2,
        "producer stalled early: {}",
        budget.in_use()
    );
    let mut rows: u64 = held.iter().map(|b| b.num_rows() as u64).sum();
    drop(held);
    let (rest, err) = drain(s).await;
    assert!(err.is_none(), "{err:?}");
    rows += rest;
    assert_eq!(rows, 3_000_000);
    assert_eq!(budget.in_use(), 0);
}

#[tokio::test]
async fn dropping_a_stream_releases_its_slot() {
    let e = engine_with(1, 256 * MIB);
    let mut s = e
        .stream(
            "SELECT i FROM range(100000000) t(i)",
            StreamOptions {
                capacity: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    s.next().await.expect("item").expect("batch");
    drop(s);
    let v = tokio::time::timeout(Duration::from_secs(5), e.with_conn(scalar("SELECT 'free'")))
        .await
        .expect("slot released")
        .expect("run");
    assert_eq!(v, "free");
}

#[tokio::test]
async fn dropping_a_stream_interrupts_a_long_first_batch() {
    let e = engine_with(1, 256 * MIB);
    // A blocking aggregate executes before the first batch (and the schema) is ready.
    let r = tokio::time::timeout(
        Duration::from_millis(200),
        e.stream(LONG, StreamOptions::default()),
    )
    .await;
    assert!(r.is_err(), "the aggregate cannot finish in 200ms");
    let v = tokio::time::timeout(Duration::from_secs(5), e.with_conn(scalar("SELECT 'free'")))
        .await
        .expect("slot released")
        .expect("run");
    assert_eq!(v, "free");
}

fn sample(offset: i64, n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(offset..offset + n as i64)),
            Arc::new(StringArray::from_iter(
                (0..n).map(|i| (i % 10 != 0).then(|| format!("n{i}"))),
            )),
        ],
    )
    .expect("batch")
}

#[tokio::test]
async fn arrow_append_into_temp_table() {
    let e = engine();
    let mut s = e.session().expect("session");
    s.run(|c| {
        c.execute_batch("CREATE TEMP TABLE stg (id BIGINT, name VARCHAR)")
            .db()
    })
    .await
    .expect("create");

    let (tx, rx) = mpsc::channel(4);
    let budget = e.byte_budget().clone();
    let producer = tokio::spawn(async move {
        for k in 0..50 {
            let b = budget.reserve(sample(k * 10_000, 10_000)).await;
            tx.send(Ok(b)).await.expect("send");
        }
    });
    let n = s.append("stg", rx).await.expect("append");
    producer.await.expect("producer");
    assert_eq!(n, 500_000);

    // A byte budget far below the flush size can't stall the load.
    let small = atlatl_engine::ByteBudget::new(64 * 1024);
    let (tx, rx) = mpsc::channel(64);
    let producer = tokio::spawn(async move {
        for k in 0..20 {
            let b = small.reserve(sample(1_000_000 + k * 10_000, 10_000)).await;
            tx.send(Ok(b)).await.expect("send");
        }
    });
    let n = tokio::time::timeout(Duration::from_secs(20), s.append("stg", rx))
        .await
        .expect("no deadlock")
        .expect("append");
    producer.await.expect("producer");
    assert_eq!(n, 200_000);
    s.run(|c| c.execute_batch("DELETE FROM stg WHERE id >= 1000000").db())
        .await
        .expect("cleanup");
    let (count, nulls, max): (i64, i64, i64) = s
        .run(|c| {
            c.query_row(
                "SELECT count(*), count(*) FILTER (name IS NULL), max(id) FROM stg",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .db()
        })
        .await
        .expect("check");
    assert_eq!((count, nulls, max), (500_000, 50_000, 499_999));
    assert_eq!(e.byte_budget().in_use(), 0);

    let (tx, rx) = mpsc::channel(4);
    tx.send(Ok(Batch::unbudgeted(sample(0, 10))))
        .await
        .expect("send");
    tx.send(Err(AtlatlError::transient("source broke")))
        .await
        .expect("send");
    drop(tx);
    let err = s.append("stg", rx).await.expect_err("producer error");
    assert_eq!(err.class, ErrorClass::Transient);

    let n = e
        .with_conn(|c| {
            c.execute_batch("CREATE TEMP TABLE t2 (id BIGINT, name VARCHAR)")
                .db()?;
            let n = append_temp(c, "t2", [sample(0, 3), sample(3, 4)])?;
            c.execute_batch("DROP TABLE t2").db()?;
            Ok(n)
        })
        .await
        .expect("append_temp");
    assert_eq!(n, 7);
}

#[tokio::test]
async fn memory_usage_reports() {
    let e = engine();
    e.memory_usage().await.expect("memory usage");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_canon_is_visible_on_every_connection() {
    let e = engine();
    let v: (String, bool, Option<String>) = e
        .with_conn(|c| {
            c.query_row(
                "SELECT atlatl_json_canon('{\"b\": [1, 2], \"a\": 1}'), atlatl_json_canon('{\"a\":1,\"b\":[1,2]}') = atlatl_json_canon('{\"b\":[1,2],\"a\":1}'), atlatl_json_canon(NULL)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|e| atlatl_core::AtlatlError::internal(e.to_string()))
        })
        .await
        .expect("query");
    assert_eq!(v, ("{\"a\":1,\"b\":[1,2]}".to_owned(), true, None));
    let mut s = e.session().expect("session");
    let n: i64 = s
        .run(|c| c.query_row("SELECT count(*) FROM (SELECT atlatl_json_canon(x) FROM (VALUES ('[1]'), (NULL), ('{')) t(x))", [], |r| r.get(0)).map_err(|e| atlatl_core::AtlatlError::internal(e.to_string())))
        .await
        .expect("session");
    assert_eq!(n, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_canon_survives_deep_nesting_in_sql() {
    let e = engine();
    let v: (bool, bool, bool) = e
        .with_conn(|c| {
            c.query_row(
                "WITH d AS (SELECT repeat('[', 200000) || '1' || repeat(']', 200000) AS a, repeat('[', 200000) || '2' || repeat(']', 200000) AS b)
                 SELECT atlatl_json_canon(a) IS DISTINCT FROM atlatl_json_canon(b), atlatl_json_canon(a) IS DISTINCT FROM atlatl_json_canon(NULL), atlatl_json_canon(a) = atlatl_json_canon(a) FROM d",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|e| atlatl_core::AtlatlError::internal(e.to_string()))
        })
        .await
        .expect("query");
    assert_eq!(
        v,
        (true, true, true),
        "deep documents compare by raw text, never as NULL"
    );
}
