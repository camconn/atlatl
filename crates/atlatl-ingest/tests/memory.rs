//! The memory harness behind `BudgetParams` (spike S3 §4/§6): a chunked 10M-row merge applied in
//! one transaction, straight from the pinned source (04 §7), on an engine sized by
//! `Budget::compute`. Peak RSS must stay within `engine.memory`. Slow and host-sensitive, so
//! ignored; run it to recalibrate `BudgetParams`:
//!
//! `cargo test -p atlatl-ingest --release --test memory -- --ignored --nocapture`
//!
//! `ATLATL_MEMTEST_ROWS` (10M), `_MEMORY_MIB` (1536), `_THREADS` (2), `_CHUNKS` (10), `_MODE`
//! (`engine.memory_mode`: `high`, or `low`) and `_SHARE` (`engine.duckdb_share`, unset) override.
//! Diagnostics: `_SQL` runs after attach (e.g. allocator settings), `_TXN_PER_CHUNK=1` commits each
//! chunk on its own, `_RELEASE=0` turns off `alloc::release` (between chunks, after long calls
//! and periodic; spike S3 §6.2), `_PURGE_MS` sets the periodic release interval during calls
//! (`low`'s default, in either mode; 0 turns it off). Each run prints per-chunk RSS against
//! `duckdb_memory()` and jemalloc/glibc heap stats, and the same sampled every 200 ms.
//! For the cgroup view, wrap it in `systemd-run --user --scope -p MemoryMax=… -p MemorySwapMax=0`,
//! with `TMPDIR` on disk: tmpfs pages the test writes are charged to its cgroup.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use atlatl_core::config::model::{ChunkBy, ChunkConfig, EngineConfig, MemoryMode};
use atlatl_core::config::types::{Auto, ByteSize};
use atlatl_engine::alloc;
use atlatl_engine::{Budget, BudgetParams, DbResultExt, Detected, Engine, EngineOptions};
use atlatl_ingest::chunk;
use atlatl_ingest::modes::{Apply, Counts, Kind};
use atlatl_testkit::LocalLake;

const MIB: u64 = 1 << 20;

fn mib(b: u64) -> u64 {
    b / MIB
}

fn show(s: &alloc::Stats) -> String {
    let m = |b: Option<u64>| b.map_or("-".into(), |b| mib(b).to_string());
    format!(
        "jemalloc allocated {} resident {} dirty {} muzzy {}, glibc in use {} free {}",
        m(s.jemalloc_allocated),
        m(s.jemalloc_resident),
        m(s.jemalloc_dirty),
        m(s.jemalloc_muzzy),
        m(s.glibc_in_use),
        m(s.glibc_free)
    )
}

fn env<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(format!("ATLATL_MEMTEST_{name}"))
        .ok()
        .and_then(|v| v.parse().ok())
}

fn kib_field(file: &str, key: &str) -> u64 {
    std::fs::read_to_string(file)
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(key))?
                .split_whitespace()
                .nth(1)?
                .parse::<u64>()
                .ok()
        })
        .map_or(0, |kib| kib * 1024)
}

fn status(key: &str) -> u64 {
    kib_field("/proc/self/status", key)
}

/// `MADV_FREE`d pages jemalloc has released: still in RSS until the kernel reclaims them under
/// pressure, so they count toward `VmHWM` but cannot trigger a cgroup OOM kill.
fn lazy_free() -> u64 {
    kib_field("/proc/self/smaps_rollup", "LazyFree:")
}

/// This process's cgroup `memory.peak` (meaningful under a `systemd-run --scope`).
fn cgroup_peak() -> Option<u64> {
    let cg = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = cg.lines().find_map(|l| l.strip_prefix("0::"))?;
    std::fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.peak"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn peak_rss() -> u64 {
    status("VmHWM:")
}

/// Resets `VmHWM` so the next phase's peak is its own.
fn reset_peak() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

fn tracked(c: &duckdb::Connection) -> atlatl_core::error::Result<u64> {
    c.query_row(
        "SELECT coalesce(sum(memory_usage_bytes), 0)::UBIGINT FROM duckdb_memory()",
        [],
        |r| r.get(0),
    )
    .db()
}

fn top_tags(c: &duckdb::Connection) -> atlatl_core::error::Result<String> {
    let mut st = c
        .prepare(
            "SELECT tag, memory_usage_bytes // 1048576, temporary_storage_bytes // 1048576
             FROM duckdb_memory() WHERE memory_usage_bytes + temporary_storage_bytes > 0
             ORDER BY memory_usage_bytes DESC",
        )
        .db()?;
    let rows = st
        .query_map([], |r| {
            Ok(format!(
                "{}={}M/{}M spilled",
                r.get::<_, String>(0)?,
                r.get::<_, u64>(1)?,
                r.get::<_, u64>(2)?
            ))
        })
        .db()?;
    Ok(rows.collect::<Result<Vec<_>, _>>().db()?.join(", "))
}

/// Max over 200 ms samples.
#[derive(Debug, Default, Clone, Copy)]
struct Sampled {
    rss: u64,
    /// DuckDB's `duckdb_memory()`: what `memory_limit` bounds.
    tracked: u64,
    /// RSS minus tracked.
    outside: u64,
    lazy_free: u64,
    je_resident: u64,
    je_dirty: u64,
    glibc_in_use: u64,
    glibc_held: u64,
}

fn sampler(engine: Engine, stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<Sampled> {
    tokio::spawn(async move {
        let mut s = Sampled::default();
        while !stop.load(Ordering::Relaxed) {
            let t = engine.memory_usage().await.unwrap_or(0);
            let rss = status("VmRSS:");
            let a = alloc::stats();
            s.rss = s.rss.max(rss);
            s.tracked = s.tracked.max(t);
            s.outside = s.outside.max(rss.saturating_sub(t));
            s.lazy_free = s.lazy_free.max(lazy_free());
            s.je_resident = s.je_resident.max(a.jemalloc_resident.unwrap_or(0));
            s.je_dirty = s.je_dirty.max(a.jemalloc_dirty.unwrap_or(0));
            s.glibc_in_use = s.glibc_in_use.max(a.glibc_in_use.unwrap_or(0));
            s.glibc_held = s
                .glibc_held
                .max(a.glibc_in_use.unwrap_or(0) + a.glibc_free.unwrap_or(0));
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        s
    })
}

/// After one chunk: peak and current RSS, `LazyFree`, tracked, allocator stats, and the RSS
/// [`alloc::release`] took back (and how long it took).
struct ChunkStats {
    peak: u64,
    rss: u64,
    lazy_free: u64,
    tracked: u64,
    alloc: alloc::Stats,
    released: Option<(u64, Duration)>,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "slow: 10M-row merge; run to recalibrate BudgetParams"]
async fn chunked_merge_stays_within_the_budget() {
    let rows = env("ROWS").unwrap_or(10_000_000u64);
    let chunks = env("CHUNKS").unwrap_or(10u32);
    let spill = tempfile::tempdir().expect("spill");
    let cfg = EngineConfig {
        memory: Some(Auto::Value(ByteSize(
            env("MEMORY_MIB").unwrap_or(1536u64) * MIB,
        ))),
        threads: Some(Auto::Value(env("THREADS").unwrap_or(2u32))),
        duckdb_share: env("SHARE"),
        temp_directory: Some(spill.path().to_string_lossy().into_owned()),
        memory_mode: std::env::var("ATLATL_MEMTEST_MODE")
            .ok()
            .map(|m| match m.as_str() {
                "high" => MemoryMode::High,
                "low" => MemoryMode::Low,
                m => panic!("ATLATL_MEMTEST_MODE={m}: expected high or low"),
            }),
        ..Default::default()
    };
    let mut budget =
        Budget::compute(&cfg, None, &Detected::detect(), &BudgetParams::default()).expect("budget");
    if let Some(ms) = env::<u64>("PURGE_MS") {
        budget.purge_every_ms = (ms > 0).then_some(ms);
    }
    let (memory, limit, threads) = (budget.memory, budget.memory_limit, budget.threads);
    let (mode, purge_ms) = (budget.memory_mode, budget.purge_every_ms);
    let mut opts = EngineOptions::new(budget);
    opts.extension_dir = Some(atlatl_testkit::extension_dir());
    let engine = Engine::open(opts).expect("engine");
    let lake = LocalLake::file().expect("lake");
    let src = format!("read_parquet('{}/src.parquet')", lake.dir.path().display());

    let mut s = engine.session().expect("session");
    let txn_per_chunk = env("TXN_PER_CHUNK").unwrap_or(0u8) == 1;
    let release = env("RELEASE").unwrap_or(1u8) == 1;
    alloc::set_enabled(release);
    let attach = format!(
        "LOAD ducklake; {}; {}",
        lake.attach_sql("lake"),
        env::<String>("SQL").unwrap_or_default()
    );
    s.run(move |c| c.execute_batch(&attach).db())
        .await
        .expect("attach");
    let rss_open = status("VmRSS:");

    let stop = Arc::new(AtomicBool::new(false));
    let setup_sampler = sampler(engine.clone(), stop.clone());
    let setup = format!(
        "CREATE SCHEMA lake.s;
         CREATE TABLE lake.s.big AS SELECT i AS id, (i * 7) % 1000 AS a, md5(i::VARCHAR) AS b,
           md5((i + 1)::VARCHAR) AS c, DATE '2026-01-01' AS d FROM range({rows}) t(i);
         COPY (SELECT i AS id, (i * 7) % 1000 AS a,
                 CASE WHEN i % 2 = 0 THEN md5(i::VARCHAR) || 'u' ELSE md5(i::VARCHAR) END AS b,
                 md5((i + 1)::VARCHAR) AS c, DATE '2026-01-01' AS d
               FROM range({total}) t(i)) TO '{dir}/src.parquet';",
        total = rows + rows / 10,
        dir = lake.dir.path().display(),
    );
    s.run(move |c| c.execute_batch(&setup).db())
        .await
        .expect("setup");
    stop.store(true, Ordering::Relaxed);
    let setup_sampled = setup_sampler.await.expect("sampler");
    let setup_peak = peak_rss();
    let rss_after_setup = status("VmRSS:");
    reset_peak();

    let stop = Arc::new(AtomicBool::new(false));
    let merge_sampler = sampler(engine.clone(), stop.clone());
    let bulk: String = s
        .run(|c| {
            c.query_row(
                "SELECT current_setting('allocator_bulk_deallocation_flush_threshold')",
                [],
                |r| r.get(0),
            )
            .db()
        })
        .await
        .expect("bulk setting");
    let purges_before = engine.purges();
    let t = Instant::now();
    let (counts, snapshots, per_chunk, tags, rss_commit, after_commit) = s
        .run(move |c| {
            let snap = |c: &duckdb::Connection| -> atlatl_core::error::Result<i64> {
                c.query_row(
                    "SELECT id FROM ducklake_current_snapshot('lake')",
                    [],
                    |r| r.get(0),
                )
                .db()
            };
            let s0 = snap(c)?;
            let preds = chunk::predicates(
                c,
                &src,
                &ChunkConfig {
                    by: ChunkBy::Hash,
                    size: None,
                    columns: Some(vec!["id".into()]),
                    buckets: Some(chunks),
                    column: None,
                    step: None,
                },
            )?;
            let columns: Vec<(String, String)> = ["id", "a", "b", "c", "d"]
                .iter()
                .map(|n| (n.to_string(), format!("\"{n}\"")))
                .collect();
            let pk = ["id".to_owned()];
            let mut total = Counts::default();
            let mut per_chunk = vec![];
            if !txn_per_chunk {
                c.execute_batch("BEGIN").db()?;
            }
            for p in &preds {
                let rel = chunk::relation(&src, p);
                let a = Apply {
                    kind: Kind::Incremental,
                    target: "lake.s.big",
                    source: &rel,
                    columns: &columns,
                    primary_key: &pk,
                    null_safe: false,
                };
                total.add(a.run(c)?);
                let (rss, lf) = (status("VmRSS:"), lazy_free());
                let alloc = alloc::stats();
                // As the commit path does between chunks (atlatl-run `lake.rs`).
                let released = release.then(|| {
                    let t = Instant::now();
                    alloc::release();
                    (rss.saturating_sub(status("VmRSS:")), t.elapsed())
                });
                per_chunk.push(ChunkStats {
                    peak: peak_rss(),
                    rss,
                    lazy_free: lf,
                    tracked: tracked(c)?,
                    alloc,
                    released,
                });
            }
            let tags = top_tags(c)?;
            if !txn_per_chunk {
                c.execute_batch("COMMIT").db()?;
            }
            // The engine releases once this long call returns.
            let (rss, stats) = (status("VmRSS:"), alloc::stats());
            Ok((total, snap(c)? - s0, per_chunk, tags, rss, stats))
        })
        .await
        .expect("chunked merge");
    stop.store(true, Ordering::Relaxed);
    let merge_sampled = merge_sampler.await.expect("sampler");
    let merge_time = t.elapsed();
    let purges = engine.purges() - purges_before;
    let merge_peak = peak_rss();
    let rss_released = status("VmRSS:");
    tokio::time::sleep(Duration::from_secs(5)).await;
    let rss_idle = status("VmRSS:");
    let peak = setup_peak.max(merge_peak);
    eprintln!(
        "memtest: rows={rows} chunks={chunks} threads={threads} mode={mode:?} memory={} MiB \
         memory_limit={} MiB release={release} purge_every_ms={purge_ms:?} ({purges} purges) \
         bulk_flush={bulk} peak RSS {} MiB (setup {}, merge {}), outside {} MiB (merge {}), RSS \
         open {} / after setup {}, merge {merge_time:?}, {counts:?}",
        mib(memory),
        mib(limit),
        mib(peak),
        mib(setup_peak),
        mib(merge_peak),
        mib(peak.saturating_sub(limit)),
        mib(merge_peak.saturating_sub(limit)),
        mib(rss_open),
        mib(rss_after_setup),
    );
    for (phase, x) in [("setup", setup_sampled), ("merge", merge_sampled)] {
        eprintln!(
            "memtest: {phase} sampled max RSS {} MiB, tracked {}, RSS-tracked {}, LazyFree {}, \
             jemalloc resident {} dirty {}, glibc in use {} held {}",
            mib(x.rss),
            mib(x.tracked),
            mib(x.outside),
            mib(x.lazy_free),
            mib(x.je_resident),
            mib(x.je_dirty),
            mib(x.glibc_in_use),
            mib(x.glibc_held)
        );
    }
    for (i, c) in per_chunk.iter().enumerate() {
        let released = c.released.map_or(String::new(), |(b, d)| {
            format!("; release freed {} MiB in {d:.1?}", mib(b))
        });
        eprintln!(
            "memtest: chunk {i}: peak {} MiB, RSS {}, LazyFree {}, tracked {}; {}{released}",
            mib(c.peak),
            mib(c.rss),
            mib(c.lazy_free),
            mib(c.tracked),
            show(&c.alloc),
        );
    }
    eprintln!("memtest: duckdb_memory before COMMIT: {tags}");
    eprintln!(
        "memtest: after COMMIT RSS {} MiB, released {}, 5 s later {}; {}; jemalloc (dirty_decay_ms, \
         muzzy_decay_ms, narenas, background_thread) {:?}; cgroup memory.peak {:?} MiB",
        mib(rss_commit),
        mib(rss_released),
        mib(rss_idle),
        show(&after_commit),
        alloc::jemalloc_config(),
        cgroup_peak().map(mib)
    );
    let want = if txn_per_chunk { i64::from(chunks) } else { 1 };
    assert_eq!(snapshots, want, "one snapshot for the whole chunked merge");
    assert_eq!(
        (counts.updated, counts.inserted, counts.unchanged),
        (rows.div_ceil(2), rows / 10, rows / 2)
    );
    assert!(
        peak <= memory,
        "peak RSS {} MiB exceeds engine.memory {} MiB: BudgetParams underestimates the overhead",
        mib(peak),
        mib(memory)
    );
}
