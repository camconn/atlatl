//! `alloc` against the official libduckdb, in a process of its own so no other test allocates
//! while it measures.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use atlatl_core::config::model::{EngineConfig, MemoryMode};
use atlatl_core::config::types::{Auto, ByteSize};
use atlatl_engine::alloc::{self, Released};
use atlatl_engine::budget::PURGE_EVERY_MS;
use atlatl_engine::engine::RELEASE_AFTER;
use atlatl_engine::{
    Budget, BudgetParams, DbResultExt, Detected, Engine, EngineOptions, StreamOptions,
};

const MIB: u64 = 1 << 20;

/// `alloc::releases()` is process-wide.
fn serial() -> MutexGuard<'static, ()> {
    static M: Mutex<()> = Mutex::new(());
    M.lock().unwrap_or_else(PoisonError::into_inner)
}

fn rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("status")
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())
        .expect("VmRSS")
        * 1024
}

#[test]
fn release_returns_what_duckdb_freed() {
    let _g = serial();
    let c = duckdb::Connection::open_in_memory().expect("open");
    let (dirty_decay_ms, _, _, background) = alloc::jemalloc_config().expect("duckdb_je_mallctl");
    let v = alloc::jemalloc_version().expect("version");
    assert!(v.starts_with("5."), "{v}");
    // What makes the retention (spike S3 §6.2); if upstream changes it, re-measure.
    assert!(
        dirty_decay_ms > 0 && !background,
        "{dirty_decay_ms} {background}"
    );

    // A hash aggregate and a sort in buffer-managed (jemalloc) memory, freed when they finish.
    c.execute_batch(
        "SET threads = 2;
         CREATE TABLE t AS SELECT i, md5(i::VARCHAR) AS s FROM range(3000000) r(i);
         CREATE TABLE u AS SELECT s, count(*) AS n FROM t GROUP BY s ORDER BY s;
         DROP TABLE u; DROP TABLE t;",
    )
    .expect("work");
    let before = alloc::stats();
    let dirty = before.jemalloc_dirty.expect("dirty");
    assert!(dirty > 32 * MIB, "{} MiB dirty", dirty / MIB);
    assert!(before.glibc_in_use.is_some() && before.glibc_free.is_some());
    let rss_before = rss();

    let n = alloc::releases();
    assert_eq!(
        alloc::release(),
        Released {
            jemalloc: true,
            glibc: true
        }
    );
    assert_eq!(alloc::releases(), n + 1);
    let after = alloc::stats();
    let freed = rss_before.saturating_sub(rss());
    eprintln!("dirty {} MiB, RSS fell {} MiB", dirty / MIB, freed / MIB);
    assert!(
        after.jemalloc_dirty.expect("dirty") < MIB,
        "{:?}",
        after.jemalloc_dirty
    );
    assert!(
        freed >= dirty / 2,
        "RSS fell {} MiB with {} MiB dirty",
        freed / MIB,
        dirty / MIB
    );
    assert!(after.jemalloc_resident < before.jemalloc_resident);
    drop(c);
}

fn engine(spill: &tempfile::TempDir, memory_mode: MemoryMode) -> Engine {
    let cfg = EngineConfig {
        memory: Some(Auto::Value(ByteSize(2048 * MIB))),
        threads: Some(Auto::Value(2)),
        temp_directory: Some(spill.path().to_string_lossy().into_owned()),
        memory_mode: Some(memory_mode),
        ..Default::default()
    };
    let b =
        Budget::compute(&cfg, None, &Detected::detect(), &BudgetParams::default()).expect("budget");
    let mut opts = EngineOptions::new(b);
    opts.extension_dir = Some(atlatl_testkit::extension_dir());
    Engine::open(opts).expect("open")
}

#[test]
fn long_calls_release_when_they_finish() {
    let spill = tempfile::tempdir().expect("spill");
    let e = engine(&spill, MemoryMode::High);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let _g = serial();

    let n = alloc::releases();
    rt.block_on(e.with_conn(|c| c.execute_batch("SELECT 1").db()))
        .expect("short");
    assert_eq!(alloc::releases(), n, "a short call doesn't release");

    let mut s = e.session().expect("session");
    rt.block_on(s.run(|_| {
        std::thread::sleep(RELEASE_AFTER + Duration::from_millis(50));
        Ok(())
    }))
    .expect("long");
    assert_eq!(alloc::releases(), n + 1);

    alloc::set_enabled(false);
    assert_eq!(alloc::release(), Released::default());
    assert_eq!(alloc::releases(), n + 1);
    alloc::set_enabled(true);
}

/// `Engine::purges` is per engine, so this holds with other engines busy in the process.
#[test]
fn low_mode_purges_during_calls_and_high_does_not() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let every = Duration::from_millis(PURGE_EVERY_MS);
    let busy = |e: &Engine| {
        let mut s = e.session().expect("session");
        rt.block_on(s.run(move |_| {
            std::thread::sleep(every * 5);
            Ok(())
        }))
        .expect("long call");
    };

    let spill = tempfile::tempdir().expect("spill");
    let high = engine(&spill, MemoryMode::High);
    assert_eq!(high.budget().purge_every_ms, None);
    busy(&high);
    assert_eq!(high.purges(), 0);

    let low = engine(&spill, MemoryMode::Low);
    assert_eq!(low.budget().purge_every_ms, Some(PURGE_EVERY_MS));
    std::thread::sleep(every * 3);
    assert_eq!(low.purges(), 0, "idle: nothing in flight");
    let _g = serial();
    let n = alloc::releases();
    busy(&low);
    let during = low.purges();
    assert!((2..=6).contains(&during), "{during} purges in 5 intervals");
    assert!(alloc::releases() > n + during, "purges go through release");

    // A stream counts while its producer is blocked on an unread channel.
    let st = rt
        .block_on(low.stream(
            "SELECT i FROM range(10000000) t(i)",
            StreamOptions {
                capacity: 1,
                ..Default::default()
            },
        ))
        .expect("stream");
    std::thread::sleep(every * 4);
    assert!(
        low.purges() >= during + 2,
        "{} after the stream",
        low.purges()
    );
    drop(st);
    std::thread::sleep(every * 2);
    let idle = low.purges();
    std::thread::sleep(every * 3);
    assert_eq!(low.purges(), idle, "no purges once calls finish");
    assert_eq!(high.purges(), 0);
}
