#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use atlatl_core::{AtlatlError, ErrorClass};
use atlatl_engine::error::{INTERRUPTED, OUT_OF_MEMORY, classify};
use duckdb::{Config, Connection};

fn conn() -> Connection {
    let cfg = Config::default()
        .with("autoinstall_known_extensions", "false")
        .unwrap()
        .with("autoload_known_extensions", "false")
        .unwrap();
    Connection::open_in_memory_with_flags(cfg).unwrap()
}

fn fail(c: &Connection, sql: &str) -> AtlatlError {
    let e = c.execute_batch(sql).expect_err(sql);
    classify(&e)
}

#[track_caller]
fn assert_class(e: &AtlatlError, class: ErrorClass, prefix: &str) {
    assert_eq!(e.class, class, "{}", e.message);
    assert!(e.message.starts_with(prefix), "{}", e.message);
}

#[test]
fn parser_error_is_config() {
    let e = fail(&conn(), "SELEC 1");
    assert_class(&e, ErrorClass::Config, "Parser Error: ");
}

#[test]
fn binder_error_is_config() {
    let e = fail(&conn(), "SELECT nope FROM (SELECT 1 AS a)");
    assert_class(&e, ErrorClass::Config, "Binder Error: ");
}

#[test]
fn missing_table_is_config() {
    let e = fail(&conn(), "SELECT * FROM nope");
    assert_class(&e, ErrorClass::Config, "Catalog Error: ");
}

#[test]
fn conversion_error_is_data() {
    let e = fail(&conn(), "SELECT 'x'::INT");
    assert_class(&e, ErrorClass::Data, "Conversion Error: ");
}

#[test]
fn constraint_violation_is_data() {
    let e = fail(
        &conn(),
        "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);",
    );
    assert_class(&e, ErrorClass::Data, "Constraint Error: ");
}

#[test]
fn missing_extension_is_config() {
    let dir = tempfile::tempdir().unwrap();
    let c = conn();
    c.execute_batch(&format!(
        "SET extension_directory = '{}'",
        dir.path().display()
    ))
    .unwrap();
    let e = fail(&c, "LOAD ducklake");
    assert_class(&e, ErrorClass::Config, "IO Error: Extension ");
    let e = fail(&c, "SELECT * FROM ducklake_snapshots('lake')");
    assert_class(&e, ErrorClass::Config, "Catalog Error: ");
}

#[test]
fn interrupt_is_timeout() {
    let c = conn();
    let h = c.interrupt_handle();
    let done = Arc::new(AtomicBool::new(false));
    let t = {
        let done = done.clone();
        thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(20));
                h.interrupt();
            }
        })
    };
    let r = c.query_row(
        "SELECT count(*) FROM range(100000000) a, range(100000) b WHERE a.range + b.range < 0",
        [],
        |r| r.get::<_, i64>(0),
    );
    done.store(true, Ordering::Relaxed);
    t.join().unwrap();
    let e = classify(&r.expect_err("query should be interrupted"));
    assert_class(&e, ErrorClass::Timeout, "INTERRUPT Error: ");
    assert_eq!(e.code.as_deref(), Some(INTERRUPTED));
    assert_eq!(
        c.query_row("SELECT 1", [], |r| r.get::<_, i32>(0)).unwrap(),
        1
    );
}

#[test]
fn out_of_memory_is_internal() {
    let c = conn();
    c.execute_batch("SET memory_limit = '8MB'; SET threads = 1; SET temp_directory = '';")
        .unwrap();
    let e = fail(
        &c,
        "SELECT length(string_agg(i::VARCHAR || repeat('x', 100), ',')) FROM range(10000000) t(i)",
    );
    assert_class(&e, ErrorClass::Internal, "Out of Memory Error: ");
    assert_eq!(e.code.as_deref(), Some(OUT_OF_MEMORY));
}
