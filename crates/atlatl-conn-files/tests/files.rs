use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use atlatl_conn_files::{
    Compression, Downloader, FileSet, FilesRead, FilesSource, render_selector,
};
use atlatl_core::config::model::{
    BackoffKind, ChunkConfig, ColumnsConfig, FilesSelector, Naming, OnNoMatch, StorageConfig,
};
use atlatl_core::error::{AtlatlError, ErrorClass};
use atlatl_core::retry::RetryPolicy;
use atlatl_core::template::{Context, MapEnv, Templates};
use duckdb::Connection;
use serde_json::json;

fn engine() -> Connection {
    atlatl_testkit::lake::open_engine().expect("engine")
}

fn sel(v: serde_json::Value) -> FilesSelector {
    serde_json::from_value(v).expect("selector")
}

fn read(v: serde_json::Value) -> FilesRead {
    FilesRead::new(sel(v))
}

fn discover(c: &Connection, r: FilesRead, work: &Path) -> Result<FileSet, AtlatlError> {
    FilesSource::new(r)?.discover(c, work)
}

fn rows(c: &Connection, table: &str, order: &str) -> Vec<Vec<Option<String>>> {
    let mut s = c
        .prepare(&format!(
            "SELECT CAST(COLUMNS(*) AS VARCHAR) FROM {table} ORDER BY {order}"
        ))
        .expect("prepare");
    let mut out = Vec::new();
    let mut q = s.query([]).expect("query");
    while let Some(r) = q.next().expect("row") {
        let n = r.as_ref().column_count();
        out.push((0..n).map(|i| r.get(i).expect("value")).collect());
    }
    out
}

fn col_names(c: &Connection, table: &str) -> Vec<String> {
    atlatl_ingest::staging::describe(c, table)
        .expect("describe")
        .into_iter()
        .map(|c| c.name)
        .collect()
}

fn s(v: &str) -> Option<String> {
    Some(v.into())
}

fn stage(c: &Connection, r: FilesRead, work: &Path) -> (FileSet, u64) {
    let fs = discover(c, r, work).expect("discover");
    let n = fs.stage(c, "stg").expect("stage");
    (fs, n)
}

fn gzip(b: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(b).expect("gz");
    e.finish().expect("gz")
}

fn zstd(b: &[u8]) -> Vec<u8> {
    zstd::encode_all(b, 3).expect("zstd")
}

fn write_zip(path: &Path, members: &[(&str, &[u8])]) {
    let mut z = zip::ZipWriter::new(std::fs::File::create(path).expect("zip"));
    for (name, body) in members {
        z.start_file(*name, zip::write::SimpleFileOptions::default())
            .expect("member");
        z.write_all(body).expect("write");
    }
    z.finish().expect("finish");
}

fn write_tar(path: &Path, members: &[(&str, &[u8])], gz: bool) {
    let mut b = tar::Builder::new(Vec::new());
    for (name, body) in members {
        let mut h = tar::Header::new_gnu();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, *body).expect("append");
    }
    let bytes = b.into_inner().expect("tar");
    std::fs::write(path, if gz { gzip(&bytes) } else { bytes }).expect("write");
}

const CSV: &str = "id,name,note\n1,alpha,\n2,\"\",NULL\n3,\"NULL\",x\n";

#[test]
fn csv_nulls_default_to_nothing() {
    let d = tempfile::tempdir().expect("tmp");
    std::fs::write(d.path().join("a.csv"), CSV).expect("write");
    let c = engine();
    let (fs, n) = stage(&c, read(json!({"url": d.path().join("a.csv")})), d.path());
    assert_eq!(n, 3);
    assert_eq!(fs.format, atlatl_core::config::model::FileFormat::Csv);
    assert_eq!(
        rows(&c, "stg", "id"),
        [
            [s("1"), s("alpha"), s("")],
            [s("2"), s(""), s("NULL")],
            [s("3"), s("NULL"), s("x")],
        ]
    );
    let (_, _) = stage(
        &c,
        read(json!({"url": d.path().join("a.csv"), "csv": {"null_values": ["NULL"]}})),
        d.path(),
    );
    assert_eq!(
        rows(&c, "stg", "id"),
        [
            [s("1"), s("alpha"), s("")],
            [s("2"), s(""), None],
            [s("3"), None, s("x")],
        ]
    );
}

#[test]
fn csv_options_and_declared_types() {
    let d = tempfile::tempdir().expect("tmp");
    std::fs::write(d.path().join("a.txt"), "1|a b\n2|'c|d'\n").expect("write");
    let c = engine();
    let mut r = read(json!({
        "url": d.path().join("a.txt"), "format": "csv",
        "csv": {"header": false, "delimiter": "|", "quote": "'"}
    }));
    r.columns = Some(
        serde_json::from_value::<ColumnsConfig>(
            json!({"column0": {"type": "BIGINT"}, "*": {"type": "text"}}),
        )
        .expect("columns"),
    );
    let (_, n) = stage(&c, r, d.path());
    assert_eq!(n, 2);
    let t = atlatl_ingest::staging::describe(&c, "stg").expect("describe");
    assert_eq!(t[0].kind, "BIGINT");
    assert_eq!(t[1].kind, "VARCHAR");
    assert_eq!(
        rows(&c, "stg", "1"),
        [[s("1"), s("a b")], [s("2"), s("c|d")]]
    );

    let mut r = read(json!({"url": d.path().join("a.txt"), "format": "csv"}));
    r.columns = Some(serde_json::from_value(json!({"*": {"type": "BIGINT"}})).expect("columns"));
    let e = discover(&c, r, d.path()).expect_err("star type");
    assert_eq!(e.class, ErrorClass::Config);
}

#[test]
fn parquet_json_jsonl() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    let c = engine();
    c.execute_batch(&format!(
        "COPY (SELECT range AS id, 'v' || range AS v, 1.5::DECIMAL(10,2) AS amt FROM range(4)) TO '{}'",
        p.join("t.parquet").display()
    ))
    .expect("parquet");
    let (_, n) = stage(&c, read(json!({"url": p.join("t.parquet")})), p);
    assert_eq!(n, 4);
    let t = atlatl_ingest::staging::describe(&c, "stg").expect("describe");
    assert_eq!(t[0].kind, "BIGINT");
    assert_eq!(t[2].kind, "DECIMAL(10,2)");

    std::fs::write(
        p.join("a.json"),
        r#"[{"id": 1, "amt": 1234.5678, "obj": {"k": [1, 2]}}, {"id": 2, "amt": null, "obj": null}]"#,
    )
    .expect("json");
    let (_, n) = stage(&c, read(json!({"url": p.join("a.json")})), p);
    assert_eq!(n, 2);
    assert_eq!(
        rows(&c, "stg", "id"),
        [
            [s("1"), s("1234.5678"), s(r#"{"k":[1,2]}"#)],
            [s("2"), None, None]
        ]
    );
    assert!(
        atlatl_ingest::staging::describe(&c, "stg")
            .expect("describe")
            .iter()
            .all(|c| c.kind == "VARCHAR")
    );

    std::fs::write(
        p.join("b.ndjson"),
        "{\"a\": \"x\"}\n{\"a\": \"y\", \"b\": true}\n",
    )
    .expect("jsonl");
    let (_, n) = stage(&c, read(json!({"url": p.join("b.ndjson")})), p);
    assert_eq!(n, 2);
    assert_eq!(rows(&c, "stg", "a"), [[s("x"), None], [s("y"), s("true")]]);

    std::fs::write(p.join("c.data"), "{\"a\": 1}\n").expect("override");
    let e = discover(&c, read(json!({"url": p.join("c.data")})), p).expect_err("no format");
    assert_eq!(e.class, ErrorClass::Config);
    let (fs, n) = stage(
        &c,
        read(json!({"url": p.join("c.data"), "format": "jsonl"})),
        p,
    );
    assert_eq!(
        (fs.format, n),
        (atlatl_core::config::model::FileFormat::Jsonl, 1)
    );
}

#[test]
fn compression_by_magic_bytes() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    std::fs::write(p.join("g.csv.gz"), gzip(CSV.as_bytes())).expect("gz");
    std::fs::write(p.join("z.csv.zst"), zstd(CSV.as_bytes())).expect("zst");
    // Misnamed: gzip without the suffix, zstd named .gz, plain named .gz.
    std::fs::write(p.join("misnamed.csv"), gzip(CSV.as_bytes())).expect("gz");
    std::fs::write(p.join("really_zstd.csv.gz"), zstd(CSV.as_bytes())).expect("zst");
    std::fs::write(p.join("plain.csv.gz"), CSV).expect("plain");
    let c = engine();
    for (f, want) in [
        ("g.csv.gz", Compression::Gzip),
        ("z.csv.zst", Compression::Zstd),
        ("misnamed.csv", Compression::Gzip),
        ("really_zstd.csv.gz", Compression::Zstd),
        ("plain.csv.gz", Compression::None),
    ] {
        let (fs, n) = stage(&c, read(json!({"url": p.join(f)})), p);
        assert_eq!((fs.files[0].compression, n), (want, 3), "{f}");
        assert_eq!(rows(&c, "stg", "id")[2], [s("3"), s("NULL"), s("x")], "{f}");
    }
}

#[test]
fn torn_streams_are_decode_errors() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    let big: String = std::iter::once("id,v\n".to_owned())
        .chain((0..20_000).map(|i| format!("{i},value{i}\n")))
        .collect();
    let g = gzip(big.as_bytes());
    let z = zstd(big.as_bytes());
    std::fs::write(p.join("torn.csv.gz"), &g[..g.len() - 4]).expect("gz");
    std::fs::write(p.join("torn.csv.zst"), &z[..z.len() / 2]).expect("zst");
    std::fs::write(p.join("ragged.csv"), "a,b\n1,2\n3,4,5\n").expect("csv");
    let c = engine();
    for f in ["torn.csv.gz", "torn.csv.zst", "ragged.csv"] {
        let r = discover(
            &c,
            read(json!({"url": p.join(f), "csv": {"delimiter": ","}})),
            p,
        )
        .and_then(|fs| fs.stage(&c, "stg"));
        let e = r.expect_err(f);
        assert_eq!(e.class, ErrorClass::Decode, "{f}: {e}");
    }
    c.execute_batch(&format!(
        "COPY (SELECT 1 AS a) TO '{}'",
        p.join("t.parquet").display()
    ))
    .expect("parquet");
    let b = std::fs::read(p.join("t.parquet")).expect("read");
    std::fs::write(p.join("torn.parquet"), &b[..b.len() - 6]).expect("write");
    let e = discover(&c, read(json!({"url": p.join("torn.parquet")})), p).expect_err("parquet");
    assert_eq!(e.class, ErrorClass::Decode, "{e}");
}

#[test]
fn zip_members() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    let z = p.join("US_BULK.zip");
    write_zip(
        &z,
        &[
            ("US_FIN_INCOME.csv", b"ticker,val\nAAPL,1\n"),
            ("US_INDU_INCOME.csv", b"ticker,val\nMSFT,2\nIBM,3\n"),
            ("US_INCOME_KEY.csv", b"tag,description\nval,Value\n"),
            ("nested/US_FIN_EXTRA.csv.gz", &gzip(b"ticker,val\nGOOG,4\n")),
        ],
    );
    let c = engine();
    let one =
        |m: &str| read(json!({"url": format!("file://{}", z.display()), "archive": {"member": m}}));

    let (fs, n) = stage(&c, one("US_FIN_INCOME.csv"), p);
    assert_eq!(n, 1);
    assert_eq!(fs.names(), [format!("{}!US_FIN_INCOME.csv", z.display())]);

    let (fs, n) = stage(&c, one("*_INCOME.csv"), p);
    assert_eq!(n, 3);
    assert_eq!(fs.names().len(), 2);
    assert!(fs.names()[0].ends_with("!US_FIN_INCOME.csv"));

    let (fs, n) = stage(&c, one("US_FIN_EXTRA.csv.gz"), p);
    assert_eq!((fs.files[0].compression, n), (Compression::Gzip, 1));

    let e = discover(&c, one("US_*.csv"), p).expect_err("different schemas");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(e.message.contains("union_by_name"), "{e}");

    let (_, n) = stage(
        &c,
        read(json!({"url": z, "archive": {"member": "US_*.csv"}, "union_by_name": true})),
        p,
    );
    assert_eq!(n, 4);
    assert_eq!(
        col_names(&c, "stg"),
        ["ticker", "val", "tag", "description"]
    );

    let e = discover(&c, one("NOPE.csv"), p).expect_err("no member");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(e.message.contains("US_INCOME_KEY.csv"), "{e}");

    let e = discover(&c, read(json!({"url": z})), p).expect_err("zip without member");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(e.message.contains("archive.member"), "{e}");
}

#[test]
fn tar_members() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    let members: &[(&str, &[u8])] = &[
        ("data/a.csv", b"k,v\n1,x\n"),
        ("data/b.csv", b"k,v\n2,y\n"),
        ("README", b"hi"),
    ];
    write_tar(&p.join("x.tar"), members, false);
    write_tar(&p.join("x.tgz"), members, true);
    let c = engine();
    for f in ["x.tar", "x.tgz"] {
        let (fs, n) = stage(
            &c,
            read(json!({"url": p.join(f), "archive": {"member": "data/*.csv"}})),
            &p.join(format!("work_{f}")),
        );
        assert_eq!(n, 2, "{f}");
        assert_eq!(fs.files.len(), 2);
        assert_eq!(rows(&c, "stg", "k"), [[s("1"), s("x")], [s("2"), s("y")]]);
    }
    let g = std::fs::read(p.join("x.tgz")).expect("read");
    std::fs::write(p.join("torn.tgz"), &g[..g.len() - 4]).expect("write");
    let e = discover(
        &c,
        read(json!({"url": p.join("torn.tgz"), "archive": {"member": "data/a.csv"}})),
        &p.join("w"),
    )
    .expect_err("torn tar.gz");
    assert_eq!(e.class, ErrorClass::Decode, "{e}");
}

#[test]
fn clean_csv_naming() {
    let d = tempfile::tempdir().expect("tmp");
    std::fs::write(
        d.path().join("h.csv"),
        "Fiscal Year,Ticker,EPS (Diluted),1Q Rev,Café,Ticker\n2024,AAPL,1.2,3,x,dup\n",
    )
    .expect("write");
    let c = engine();
    let mut r = read(json!({"url": d.path().join("h.csv")}));
    r.naming = Naming::CleanCsv;
    r.columns =
        Some(serde_json::from_value(json!({"fiscal_year": {"type": "INTEGER"}})).expect("cols"));
    let (fs, _) = stage(&c, r, d.path());
    let want = [
        "fiscal_year",
        "ticker",
        "eps_diluted",
        "_1q_rev",
        "cafe",
        "ticker1",
    ];
    assert_eq!(col_names(&c, "stg"), want);
    assert_eq!(fs.names().len(), 1);
    assert_eq!(
        fs.columns()[2],
        ("EPS (Diluted)".into(), "eps_diluted".into())
    );
    assert_eq!(
        atlatl_ingest::staging::describe(&c, "stg").expect("d")[0].kind,
        "INTEGER"
    );

    let (_, _) = stage(&c, read(json!({"url": d.path().join("h.csv")})), d.path());
    assert_eq!(
        col_names(&c, "stg"),
        [
            "Fiscal Year",
            "Ticker",
            "EPS (Diluted)",
            "1Q Rev",
            "Café",
            "Ticker_1"
        ]
    );
}

#[test]
fn zero_matches() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    let c = engine();
    let e = discover(&c, read(json!({"url": p, "glob": "*.csv"})), p).expect_err("empty glob");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(e.message.contains("no files match"), "{e}");
    let e = discover(&c, read(json!({"url": p.join("missing.csv")})), p).expect_err("missing");
    assert_eq!(e.class, ErrorClass::Config);

    let mut r = read(json!({"url": p, "glob": "*.csv"}));
    r.on_no_match = OnNoMatch::Warn;
    let fs = discover(&c, r, p).expect("warn");
    assert!(fs.is_empty());
    assert_eq!(fs.warnings.len(), 1);

    std::fs::write(p.join("b.csv"), "x\n2\n").expect("b");
    std::fs::write(p.join("a.csv"), "x\n1\n").expect("a");
    let (fs, n) = stage(
        &c,
        read(json!({"url": format!("{}/", p.display()), "glob": "*.csv"})),
        p,
    );
    assert_eq!(n, 2);
    let names = fs.names();
    assert!(
        names[0].ends_with("a.csv") && names[1].ends_with("b.csv"),
        "{names:?}"
    );
}

#[test]
fn relative_paths_use_root() {
    let d = tempfile::tempdir().expect("tmp");
    std::fs::create_dir_all(d.path().join("in")).expect("dir");
    std::fs::write(d.path().join("in/a.csv"), "x\n1\n").expect("a");
    let c = engine();
    let mut r = read(json!({"url": "in/a.csv"}));
    r.root = Some(d.path().to_owned());
    let (_, n) = stage(&c, r, d.path());
    assert_eq!(n, 1);
}

#[test]
fn hive_paths_from_vars() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    let c = engine();
    for (day, n) in [("2024-01-01", 2), ("2024-01-02", 3)] {
        let dir = p.join(format!("t/date={day}"));
        std::fs::create_dir_all(&dir).expect("dir");
        c.execute_batch(&format!(
            "COPY (SELECT range AS id FROM range({n})) TO '{}'",
            dir.join("part.parquet").display()
        ))
        .expect("copy");
    }
    let t = Templates::new();
    let ctx = Context::load(Arc::new(MapEnv::new::<_, &str, &str>([])), p, None);
    let vars: indexmap::IndexMap<String, serde_json::Value> =
        serde_json::from_value(json!({"root": p, "date": "2024-01-02"})).expect("vars");
    let sel = render_selector(
        &sel(json!({"url": "{{ vars.root }}/t/date={{ vars.date }}/*.parquet"})),
        &t,
        &ctx,
        Some(&vars),
    )
    .expect("render");
    assert_eq!(
        sel.url,
        format!("{}/t/date=2024-01-02/*.parquet", p.display())
    );

    let mut r = FilesRead::new(sel.clone());
    r.hive_partitioning = true;
    let (_, n) = stage(&c, r, p);
    assert_eq!(n, 3);
    assert_eq!(col_names(&c, "stg"), ["id", "date"]);
    assert_eq!(rows(&c, "stg", "id")[0], [s("0"), s("2024-01-02")]);

    let (_, _) = stage(&c, FilesRead::new(sel), p);
    assert_eq!(col_names(&c, "stg"), ["id"]);

    let e = render_selector(
        &self::sel(json!({"url": "{{ vars.nope }}"})),
        &t,
        &ctx,
        Some(&vars),
    )
    .expect_err("undefined");
    assert_eq!(e.class, ErrorClass::Config);
}

// ----------------------------------------------------------------------------- https

#[derive(Clone, Copy, Debug)]
enum Step {
    Status(u16),
    DropHalf,
    Serve,
}

struct Server {
    url: String,
    log: Arc<Mutex<Vec<String>>>,
}

fn serve(body: Vec<u8>, script: Vec<Step>) -> Server {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!(
        "http://{}/files/US_BULK.zip?X-Amz-Signature=secret",
        l.local_addr().expect("addr")
    );
    let log = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();
    std::thread::spawn(move || {
        let mut script = script.into_iter();
        for conn in l.incoming() {
            let Ok(mut s) = conn else { return };
            let step = script.next().unwrap_or(Step::Serve);
            handle(&mut s, &body, step, &log2);
        }
    });
    Server { url, log }
}

fn handle(s: &mut TcpStream, body: &[u8], step: Step, log: &Mutex<Vec<String>>) {
    let mut req = Vec::new();
    let mut b = [0u8; 1];
    while !req.ends_with(b"\r\n\r\n") {
        match s.read(&mut b) {
            Ok(1) => req.push(b[0]),
            _ => return,
        }
    }
    let req = String::from_utf8_lossy(&req).to_string();
    let range = req
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("range: bytes=")
                .map(str::to_owned)
        })
        .and_then(|r| r.trim_end_matches('-').parse::<usize>().ok());
    let if_range = req.to_ascii_lowercase().contains("if-range: \"v1\"");
    log.lock()
        .expect("log")
        .push(format!("{step:?} range={range:?} if_range={if_range}"));
    let len = body.len();
    let head = |status: &str, extra: String, n: usize| {
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {n}\r\nETag: \"v1\"\r\nConnection: close\r\n{extra}\r\n"
        )
    };
    let _ = match step {
        Step::Status(code) => s.write_all(
            format!("HTTP/1.1 {code} X\r\nContent-Length: 0\r\nRetry-After: 0\r\nConnection: close\r\n\r\n").as_bytes(),
        ),
        Step::DropHalf => s
            .write_all(head("200 OK", String::new(), len).as_bytes())
            .and_then(|_| s.write_all(&body[..len / 2])),
        Step::Serve => match range {
            Some(start) if start > 0 => s
                .write_all(
                    head(
                        "206 Partial Content",
                        format!("Content-Range: bytes {start}-{}/{len}\r\n", len - 1),
                        len - start,
                    )
                    .as_bytes(),
                )
                .and_then(|_| s.write_all(&body[start..])),
            _ => s
                .write_all(head("200 OK", String::new(), len).as_bytes())
                .and_then(|_| s.write_all(body)),
        },
    };
    let _ = s.flush();
    let _ = s.shutdown(std::net::Shutdown::Both);
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 4,
        backoff: BackoffKind::Constant,
        base: Duration::from_millis(10),
        max: Duration::from_millis(10),
        ..Default::default()
    }
}

fn bulk_zip(p: &Path) -> Vec<u8> {
    let rows: String = (0..5000).map(|i| format!("T{i},{i}\n")).collect();
    let z = p.join("src.zip");
    write_zip(
        &z,
        &[(
            "US_FIN_INCOME.csv",
            format!("ticker,val\n{rows}").as_bytes(),
        )],
    );
    std::fs::read(z).expect("zip")
}

#[tokio::test]
async fn https_download_resumes() {
    let d = tempfile::tempdir().expect("tmp");
    let body = bulk_zip(d.path());
    let srv = serve(body.clone(), vec![Step::DropHalf]);
    let dl = Downloader::new(fast_retry()).expect("dl");
    let mut src = FilesSource::new(read(json!({
        "url": srv.url, "archive": {"member": "US_FIN_INCOME.csv"}
    })))
    .expect("source");
    assert!(src.needs_fetch());
    let got = src
        .fetch(&dl, d.path())
        .await
        .expect("fetch")
        .expect("download");
    assert_eq!((got.attempts, got.resumed), (2, 1));
    assert_eq!(got.bytes, body.len() as u64);
    assert_eq!(std::fs::read(&got.path).expect("read"), body);
    assert!(got.retries[0].contains("transient"), "{:?}", got.retries);
    assert!(!got.retries[0].contains("secret"), "{:?}", got.retries);
    let log = srv.log.lock().expect("log").clone();
    assert_eq!(
        log,
        [
            "DropHalf range=None if_range=false".to_owned(),
            format!("Serve range=Some({}) if_range=true", body.len() / 2)
        ]
    );

    let c = engine();
    let fs = src.discover(&c, d.path()).expect("discover");
    let shown = srv.url.split('?').next().expect("url");
    assert_eq!(fs.names(), [format!("{shown}!US_FIN_INCOME.csv")]);
    assert_eq!(fs.stage(&c, "stg").expect("stage"), 5000);
}

#[tokio::test]
async fn https_errors_follow_the_taxonomy() {
    let d = tempfile::tempdir().expect("tmp");
    let dl = Downloader::new(fast_retry()).expect("dl");

    let srv = serve(
        b"x\n1\n".to_vec(),
        vec![Step::Status(503), Step::Status(429)],
    );
    let got = dl
        .download(&srv.url, &d.path().join("a.csv"))
        .await
        .expect("retried");
    assert_eq!((got.attempts, got.resumed), (3, 0));

    let srv = serve(Vec::new(), vec![Step::Status(404)]);
    let e = dl
        .download(&srv.url, &d.path().join("b.csv"))
        .await
        .expect_err("404");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(!e.message.contains("secret"), "{e}");
    assert_eq!(srv.log.lock().expect("log").len(), 1);

    let srv = serve(Vec::new(), vec![Step::Status(403)]);
    let e = dl
        .download(&srv.url, &d.path().join("c.csv"))
        .await
        .expect_err("403");
    assert_eq!(e.class, ErrorClass::Auth);

    let srv = serve(Vec::new(), vec![Step::Status(500); 10]);
    let e = dl
        .download(&srv.url, &d.path().join("d.csv"))
        .await
        .expect_err("500s");
    assert_eq!(e.class, ErrorClass::Transient);
    assert!(e.message.contains("after 4 attempts"), "{e}");
    assert!(!d.path().join("d.csv").exists());
}

#[tokio::test]
async fn https_download_sends_the_connection_headers() {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}/graph/x.csv", l.local_addr().expect("addr"));
    let seen = Arc::new(Mutex::new(String::new()));
    let seen2 = seen.clone();
    std::thread::spawn(move || {
        let Some(Ok(mut s)) = l.incoming().next() else {
            return;
        };
        let mut req = Vec::new();
        let mut b = [0u8; 1];
        while !req.ends_with(b"\r\n\r\n") && s.read(&mut b).is_ok_and(|n| n == 1) {
            req.push(b[0]);
        }
        *seen2.lock().expect("seen") = String::from_utf8_lossy(&req).to_ascii_lowercase();
        let _ =
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\na\n1\n");
    });
    let d = tempfile::tempdir().expect("tmp");
    let dl = Downloader::with_headers(
        fast_retry(),
        &[("User-Agent".into(), "curl/8 (compatible; test)".into())],
    )
    .expect("dl");
    dl.download(&url, &d.path().join("x.csv"))
        .await
        .expect("download");
    assert!(
        seen.lock()
            .expect("seen")
            .contains("user-agent: curl/8 (compatible; test)\r\n"),
        "{}",
        seen.lock().expect("seen")
    );
    let e = Downloader::with_headers(fast_retry(), &[("Bad Name".into(), "x".into())])
        .expect_err("invalid header name");
    assert_eq!(e.class, ErrorClass::Config);
}

// ----------------------------------------------------------------------------- s3

fn rustfs_storage() -> StorageConfig {
    let s3 = atlatl_testkit::rustfs();
    serde_json::from_value(json!({
        "type": "s3", "provider": "config", "key_id": s3.key_id, "secret": s3.secret,
        "region": s3.region, "endpoint": format!("http://{}", s3.endpoint), "url_style": "path"
    }))
    .expect("storage")
}

fn put(bucket: &str, key: &str, body: &str) {
    let (st, resp) = atlatl_testkit::rustfs()
        .request("PUT", &format!("/{bucket}/{key}"), body)
        .expect("put");
    assert_eq!(st, 200, "{resp}");
}

fn s3_read(v: serde_json::Value) -> FilesRead {
    let mut r = read(v);
    r.storage = Some(rustfs_storage());
    r
}

#[test]
fn s3_csv_and_parquet() {
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    put(&bucket, "in/a.csv", "id,name\n1,x\n2,\n");
    put(&bucket, "in/b.csv", "id,name\n3,NULL\n");
    let w = engine();
    w.execute_batch("LOAD httpfs").expect("httpfs");
    w.execute_batch(&s3.duckdb_secret("w", &bucket))
        .expect("secret");
    w.execute_batch(&format!(
        "COPY (SELECT range AS id FROM range(7)) TO 's3://{bucket}/pq/t.parquet'"
    ))
    .expect("parquet");

    let d = tempfile::tempdir().expect("tmp");
    let c = engine();
    let (fs, n) = stage(
        &c,
        s3_read(
            json!({"url": format!("s3://{bucket}/in"), "glob": "*.csv", "csv": {"null_values": ["NULL"]}}),
        ),
        d.path(),
    );
    assert_eq!(n, 3);
    assert_eq!(
        fs.names(),
        [
            format!("s3://{bucket}/in/a.csv"),
            format!("s3://{bucket}/in/b.csv")
        ]
    );
    assert!(fs.files.iter().all(|f| f.size.is_some_and(|s| s > 0)));
    assert_eq!(
        rows(&c, "stg", "id"),
        [[s("1"), s("x")], [s("2"), s("")], [s("3"), None]]
    );

    let (_, n) = stage(
        &c,
        s3_read(json!({"url": format!("s3://{bucket}/pq/t.parquet")})),
        d.path(),
    );
    assert_eq!(n, 7);

    let e = discover(
        &c,
        s3_read(json!({"url": format!("s3://{bucket}/nope/*.csv")})),
        d.path(),
    )
    .expect_err("no match");
    assert_eq!(e.class, ErrorClass::Config);
}

#[test]
fn s3_manifest_chunks() {
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    let w = engine();
    w.execute_batch("LOAD httpfs").expect("httpfs");
    w.execute_batch(&s3.duckdb_secret("w", &bucket))
        .expect("secret");
    let mut entries = Vec::new();
    for (i, n) in [3, 4, 5].into_iter().enumerate() {
        let url = format!("s3://{bucket}/unload/run1/{i:04}_part_00.parquet");
        w.execute_batch(&format!(
            "COPY (SELECT range + {} AS id, 'r' || range AS v FROM range({n})) TO '{url}' (FORMAT parquet)",
            i * 100
        ))
        .expect("copy");
        let size: i64 = w
            .query_row(&format!("SELECT size FROM read_blob('{url}')"), [], |r| {
                r.get(0)
            })
            .expect("size");
        entries.push(json!({"url": url, "meta": {"content_length": size, "record_count": n}}));
    }
    let manifest = json!({"entries": entries, "meta": {"record_count": 12}}).to_string();
    put(&bucket, "unload/run1/manifest", &manifest);

    let d = tempfile::tempdir().expect("tmp");
    let c = engine();
    let mut r = s3_read(json!({"url": format!("s3://{bucket}/unload/run1/manifest")}));
    r.manifest = true;
    let fs = discover(&c, r.clone(), d.path()).expect("discover");
    assert_eq!(fs.format, atlatl_core::config::model::FileFormat::Parquet);
    assert_eq!(fs.files.len(), 3);
    assert_eq!(fs.expected_rows(), Some(12));
    assert_eq!(fs.stage(&c, "stg").expect("stage"), 12);

    let sizes: Vec<u64> = fs.files.iter().map(|f| f.size.expect("size")).collect();
    let chunk: ChunkConfig =
        serde_json::from_value(json!({"by": "files", "size": sizes[0] + sizes[1]})).expect("chunk");
    let chunks = fs.chunks(&chunk).expect("chunks");
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].files.len(), 2);
    assert_eq!(chunks[0].expected_rows, Some(7));
    assert_eq!(chunks[1].expected_rows, Some(5));
    let mut total = 0;
    for ch in &chunks {
        let n: i64 = c
            .query_row(&format!("SELECT count(*) FROM ({})", ch.select), [], |r| {
                r.get(0)
            })
            .expect("chunk count");
        assert_eq!(Some(n as u64), ch.expected_rows);
        total += n;
    }
    assert_eq!(total, 12);

    let bad = json!({"entries": [{"url": format!("s3://{bucket}/unload/run1/0000_part_00.parquet"), "meta": {"record_count": 99}}]});
    put(&bucket, "unload/bad/manifest", &bad.to_string());
    let mut r = s3_read(json!({"url": format!("s3://{bucket}/unload/bad/manifest")}));
    r.manifest = true;
    let e = discover(&c, r, d.path())
        .and_then(|fs| fs.stage(&c, "stg"))
        .expect_err("row mismatch");
    assert_eq!(e.class, ErrorClass::Decode, "{e}");
}

#[test]
fn concurrent_reads_share_a_bucket_secret() {
    let s3 = atlatl_testkit::rustfs();
    let bucket = s3.fresh_bucket().expect("bucket");
    put(&bucket, "in/a.csv", "id\n1\n");
    let c = engine();
    let d = tempfile::tempdir().expect("tmp");
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..8)
            .map(|_| {
                let (c, work, bucket) = (c.try_clone().expect("clone"), d.path(), &bucket);
                sc.spawn(move || {
                    for _ in 0..10 {
                        let r = s3_read(json!({"url": format!("s3://{bucket}/in/a.csv")}));
                        discover(&c, r, work).expect("one process, many readers of a bucket");
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().expect("thread");
        }
    });
}

#[test]
fn from_stream_and_csv_hive() {
    use atlatl_core::config::de::Spanned;
    use atlatl_core::config::model::{FilesConn, FromConfig, StreamConfig};
    use atlatl_core::config::node::Span;

    let d = tempfile::tempdir().expect("tmp");
    let dir = d.path().join("in/Region=us");
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(dir.join("x.csv"), "Some Id,v\n1,\n").expect("csv");
    let span = Span {
        file: Arc::from(Path::new("t.yaml")),
        line: 1,
        col: 1,
    };
    let stream = StreamConfig {
        from: Some(Spanned::new(
            FromConfig {
                files: Some(sel(json!({"url": "in", "glob": "*/*.csv"}))),
                on_no_match: Some(OnNoMatch::Warn),
                ..Default::default()
            },
            span,
        )),
        naming: Some(Naming::CleanCsv),
        ..Default::default()
    };
    let conn: FilesConn = serde_json::from_value(json!({"root": d.path()})).expect("conn");
    let mut r = FilesRead::from_stream(&stream, Some(&conn)).expect("read");
    assert_eq!(r.on_no_match, OnNoMatch::Warn);
    r.hive_partitioning = true;
    let c = engine();
    let (_, n) = stage(&c, r, d.path());
    assert_eq!(n, 1);
    assert_eq!(col_names(&c, "stg"), ["some_id", "v", "region"]);
    assert_eq!(rows(&c, "stg", "1"), [[s("1"), s(""), s("us")]]);

    let e = FilesRead::from_stream(&StreamConfig::default(), None).expect_err("no from");
    assert_eq!(e.class, ErrorClass::Config);
}

#[test]
fn mixed_selections_are_errors() {
    let d = tempfile::tempdir().expect("tmp");
    let p = d.path();
    std::fs::write(p.join("a.csv"), "x\n1\n").expect("a");
    std::fs::write(p.join("b.json"), "[{\"x\": 1}]").expect("b");
    std::fs::write(p.join("c.csv.gz"), gzip(b"x\n2\n")).expect("c");
    let c = engine();
    let e = discover(&c, read(json!({"url": p, "glob": "*"})), p).expect_err("formats");
    assert_eq!(e.class, ErrorClass::Config);
    assert!(e.message.contains("mix formats"), "{e}");
    let e = discover(&c, read(json!({"url": p, "glob": "*.csv*"})), p).expect_err("compression");
    assert!(e.message.contains("mix compressions"), "{e}");
}
