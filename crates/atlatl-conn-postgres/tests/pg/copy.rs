use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, DictionaryArray,
    Float32Array, Float64Array, Int8Array, Int32Array, Int64Array, LargeListBuilder,
    LargeStringArray, LargeStringBuilder, ListBuilder, NullArray, RecordBatch, StringArray,
    StringBuilder, StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow::array::{Int32Builder, Int64Builder};
use arrow::datatypes::{Field, Int8Type, Schema};
use atlatl_conn_postgres::{CopyEncoder, CopyWriter, Session, column_oids, connect};
use atlatl_core::ErrorClass;
use atlatl_testkit::Postgres;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use crate::fresh;

on_both!(
    every_type_roundtrips,
    sliced_batches,
    many_rows_flush_in_chunks,
    server_rejects_bad_json_as_data,
    numeric_matches_postgres_text_parsing,
);

const ROWS: usize = 5;

struct Col {
    name: &'static str,
    pg: &'static str,
    array: ArrayRef,
    expect: [Option<&'static str>; ROWS],
}

fn col(
    name: &'static str,
    pg: &'static str,
    array: ArrayRef,
    expect: [Option<&'static str>; ROWS],
) -> Col {
    assert_eq!(array.len(), ROWS, "{name}");
    Col {
        name,
        pg,
        array,
        expect,
    }
}

const DAY_US: i64 = 86_400_000_000;
/// 100000-01-01, days since 1970-01-01.
const Y100K_DAYS: i32 = 35_804_722;

fn text_lists() -> ArrayRef {
    let mut b = ListBuilder::new(StringBuilder::new());
    b.values().append_value("a");
    b.values().append_value("b,c");
    b.values().append_null();
    b.values().append_value("🦀 ünï");
    b.append(true);
    b.append(true);
    b.append_null();
    b.values().append_value("\"q\" {x}");
    b.append(true);
    b.values().append_null();
    b.append(true);
    Arc::new(b.finish())
}

fn large_text_lists() -> ArrayRef {
    let mut b = LargeListBuilder::new(LargeStringBuilder::new());
    b.values().append_value("x");
    b.append(true);
    b.append_null();
    b.append(true);
    b.values().append_value("");
    b.values().append_value("NULL");
    b.append(true);
    b.values().append_value("y");
    b.append(true);
    Arc::new(b.finish())
}

fn int_lists() -> ArrayRef {
    let mut b = ListBuilder::new(Int64Builder::new());
    b.values().append_value(1);
    b.values().append_null();
    b.values().append_value(i64::MIN);
    b.values().append_value(i64::MAX);
    b.append(true);
    b.append(true);
    b.append_null();
    b.values().append_value(0);
    b.append(true);
    b.values().append_value(-7);
    b.append(true);
    Arc::new(b.finish())
}

fn int32_lists() -> ArrayRef {
    let mut b = ListBuilder::new(Int32Builder::new());
    b.values().append_value(i32::MIN);
    b.append(true);
    b.append_null();
    b.append(true);
    b.values().append_value(i32::MAX);
    b.values().append_value(3);
    b.append(true);
    b.append(true);
    Arc::new(b.finish())
}

fn dec(values: [Option<i128>; ROWS], p: u8, s: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(p, s)
            .expect("decimal"),
    )
}

fn columns() -> Vec<Col> {
    let max38 = 10i128.pow(38) - 1;
    let y100k_s = i64::from(Y100K_DAYS) * 86_400;
    let pre2000_us = 946_684_800_000_000 - 1;
    vec![
        col(
            "i2",
            "int2",
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(i8::MAX),
                None,
                Some(0),
                Some(-1),
            ])),
            [Some("-128"), Some("127"), None, Some("0"), Some("-1")],
        ),
        col(
            "i4",
            "int4",
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                Some(i32::MAX),
                None,
                Some(0),
                Some(1),
            ])),
            [
                Some("-2147483648"),
                Some("2147483647"),
                None,
                Some("0"),
                Some("1"),
            ],
        ),
        col(
            "i8",
            "int8",
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(i64::MAX),
                None,
                Some(0),
                Some(42),
            ])),
            [
                Some("-9223372036854775808"),
                Some("9223372036854775807"),
                None,
                Some("0"),
                Some("42"),
            ],
        ),
        col(
            "i4_from_i8",
            "int8",
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                None,
                None,
                None,
                Some(5),
            ])),
            [Some("-2147483648"), None, None, None, Some("5")],
        ),
        col(
            "u1",
            "int2",
            Arc::new(UInt8Array::from(vec![Some(255), Some(0), None, None, None])),
            [Some("255"), Some("0"), None, None, None],
        ),
        col(
            "u2",
            "int4",
            Arc::new(UInt16Array::from(vec![
                Some(u16::MAX),
                None,
                None,
                None,
                None,
            ])),
            [Some("65535"), None, None, None, None],
        ),
        col(
            "u4",
            "int8",
            Arc::new(UInt32Array::from(vec![
                Some(u32::MAX),
                None,
                None,
                None,
                None,
            ])),
            [Some("4294967295"), None, None, None, None],
        ),
        col(
            "f4",
            "float4",
            Arc::new(Float32Array::from(vec![
                Some(1.5),
                Some(-0.0),
                Some(f32::NAN),
                Some(f32::INFINITY),
                None,
            ])),
            [Some("1.5"), Some("-0"), Some("NaN"), Some("Infinity"), None],
        ),
        col(
            "f4_to_f8",
            "float8",
            Arc::new(Float32Array::from(vec![Some(0.1), None, None, None, None])),
            [Some("0.10000000149011612"), None, None, None, None],
        ),
        col(
            "f8",
            "float8",
            Arc::new(Float64Array::from(vec![
                Some(1e308),
                Some(-2.5),
                Some(f64::MIN_POSITIVE),
                Some(f64::NEG_INFINITY),
                None,
            ])),
            [
                Some("1e+308"),
                Some("-2.5"),
                Some("2.2250738585072014e-308"),
                Some("-Infinity"),
                None,
            ],
        ),
        col(
            "n_38_6",
            "numeric",
            dec(
                [
                    Some(0),
                    Some(12_345_678_901_234_567_890_123_456),
                    Some(-1),
                    None,
                    Some(-12_345_678_901_234_567_890_123_456),
                ],
                38,
                6,
            ),
            [
                Some("0.000000"),
                Some("12345678901234567890.123456"),
                Some("-0.000001"),
                None,
                Some("-12345678901234567890.123456"),
            ],
        ),
        col(
            "n_4_4",
            "numeric",
            dec([Some(-1), Some(1), Some(0), Some(9999), None], 4, 4),
            [
                Some("-0.0001"),
                Some("0.0001"),
                Some("0.0000"),
                Some("0.9999"),
                None,
            ],
        ),
        col(
            "n_38_0",
            "numeric",
            dec(
                [Some(max38), Some(-max38), Some(0), Some(10_000), None],
                38,
                0,
            ),
            [
                Some("99999999999999999999999999999999999999"),
                Some("-99999999999999999999999999999999999999"),
                Some("0"),
                Some("10000"),
                None,
            ],
        ),
        col(
            "n_38_38",
            "numeric(38,38)",
            dec([Some(max38), Some(-1), Some(0), None, Some(1)], 38, 38),
            [
                Some("0.99999999999999999999999999999999999999"),
                Some("-0.00000000000000000000000000000000000001"),
                Some("0.00000000000000000000000000000000000000"),
                None,
                Some("0.00000000000000000000000000000000000001"),
            ],
        ),
        col(
            "n_18_4",
            "numeric(18,4)",
            dec(
                [Some(123_456_789_012_345_678), None, None, None, None],
                18,
                4,
            ),
            [Some("12345678901234.5678"), None, None, None, None],
        ),
        col(
            "n_from_i64",
            "numeric",
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(i64::MAX),
                Some(0),
                None,
                None,
            ])),
            [
                Some("-9223372036854775808"),
                Some("9223372036854775807"),
                Some("0"),
                None,
                None,
            ],
        ),
        col(
            "t",
            "text",
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("héllo 🦀 世界"),
                Some("a,b\"c\\d\ne"),
                None,
                Some("NULL"),
            ])),
            [
                Some(""),
                Some("héllo 🦀 世界"),
                Some("a,b\"c\\d\ne"),
                None,
                Some("NULL"),
            ],
        ),
        col(
            "lt",
            "text",
            Arc::new(LargeStringArray::from(vec![
                Some("large"),
                None,
                None,
                None,
                Some("ß"),
            ])),
            [Some("large"), None, None, None, Some("ß")],
        ),
        col(
            "vc",
            "varchar",
            Arc::new(StringViewArray::from(vec![
                Some("view"),
                Some("a string longer than twelve bytes"),
                None,
                None,
                None,
            ])),
            [
                Some("view"),
                Some("a string longer than twelve bytes"),
                None,
                None,
                None,
            ],
        ),
        col(
            "enum_dict",
            "text",
            Arc::new(
                vec![Some("BUY"), Some("SELL"), None, Some("BUY"), Some("HOLD")]
                    .into_iter()
                    .collect::<DictionaryArray<Int8Type>>(),
            ),
            [Some("BUY"), Some("SELL"), None, Some("BUY"), Some("HOLD")],
        ),
        col(
            "tags",
            "text[]",
            text_lists(),
            [
                Some(r#"{a,"b,c",NULL,"🦀 ünï"}"#),
                Some("{}"),
                None,
                Some(r#"{"\"q\" {x}"}"#),
                Some("{NULL}"),
            ],
        ),
        col(
            "ltags",
            "varchar[]",
            large_text_lists(),
            [
                Some("{x}"),
                None,
                Some("{}"),
                Some(r#"{"","NULL"}"#),
                Some("{y}"),
            ],
        ),
        col(
            "ids",
            "int8[]",
            int_lists(),
            [
                Some("{1,NULL,-9223372036854775808,9223372036854775807}"),
                Some("{}"),
                None,
                Some("{0}"),
                Some("{-7}"),
            ],
        ),
        col(
            "ids32",
            "int8[]",
            int32_lists(),
            [
                Some("{-2147483648}"),
                None,
                Some("{}"),
                Some("{2147483647,3}"),
                Some("{}"),
            ],
        ),
        col(
            "j",
            "jsonb",
            Arc::new(StringArray::from(vec![
                Some(r#"{"b": 1, "a": [1, 2.50, "x"]}"#),
                Some("null"),
                None,
                Some(r#""ünï""#),
                Some("[]"),
            ])),
            [
                Some(r#"{"a": [1, 2.50, "x"], "b": 1}"#),
                Some("null"),
                None,
                Some(r#""ünï""#),
                Some("[]"),
            ],
        ),
        col(
            "js",
            "json",
            Arc::new(StringArray::from(vec![
                Some(r#"{"b":1,  "a":2}"#),
                None,
                None,
                None,
                None,
            ])),
            [Some(r#"{"b":1,  "a":2}"#), None, None, None, None],
        ),
        col(
            "ts_us_tz",
            "timestamptz",
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(0),
                    Some(946_684_800_000_000),
                    Some(pre2000_us),
                    Some(-719_162 * DAY_US),
                    Some(2_932_897 * DAY_US - 1),
                ])
                .with_timezone("UTC"),
            ),
            [
                Some("1970-01-01 00:00:00+00"),
                Some("2000-01-01 00:00:00+00"),
                Some("1999-12-31 23:59:59.999999+00"),
                Some("0001-01-01 00:00:00+00"),
                Some("9999-12-31 23:59:59.999999+00"),
            ],
        ),
        col(
            "ts_ms_tz",
            "timestamptz",
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(-1), Some(1_500), None, None, None])
                    .with_timezone("+02:00"),
            ),
            [
                Some("1969-12-31 23:59:59.999+00"),
                Some("1970-01-01 00:00:01.5+00"),
                None,
                None,
                None,
            ],
        ),
        col(
            "ts_ns",
            "timestamp",
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1),
                Some(1_500),
                Some(-2_208_988_800_000_000_000),
                None,
                Some(i64::MAX),
            ])),
            [
                Some("1969-12-31 23:59:59.999999"),
                Some("1970-01-01 00:00:00.000001"),
                Some("1900-01-01 00:00:00"),
                None,
                Some("2262-04-11 23:47:16.854775"),
            ],
        ),
        col(
            "ts_s",
            "timestamp",
            Arc::new(TimestampSecondArray::from(vec![
                Some(y100k_s),
                Some(-1),
                None,
                None,
                None,
            ])),
            [
                Some("100000-01-01 00:00:00"),
                Some("1969-12-31 23:59:59"),
                None,
                None,
                None,
            ],
        ),
        col(
            "d32",
            "date",
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(10_957),
                Some(10_956),
                Some(-719_162),
                Some(Y100K_DAYS),
            ])),
            [
                Some("1970-01-01"),
                Some("2000-01-01"),
                Some("1999-12-31"),
                Some("0001-01-01"),
                Some("100000-01-01"),
            ],
        ),
        col(
            "d64",
            "date",
            Arc::new(Date64Array::from(vec![
                Some(2_932_896 * 86_400_000),
                Some(-86_400_000),
                None,
                None,
                None,
            ])),
            [Some("9999-12-31"), Some("1969-12-31"), None, None, None],
        ),
        col(
            "b",
            "bool",
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
                None,
            ])),
            [Some("true"), Some("false"), None, Some("true"), None],
        ),
        col(
            "nothing",
            "int8",
            Arc::new(NullArray::new(ROWS)),
            [None; ROWS],
        ),
    ]
}

async fn session(pg: &Postgres) -> Session {
    let s = connect(&fresh(pg)).await.expect("connect");
    s.client()
        .batch_execute("SET TimeZone = 'UTC'")
        .await
        .expect("tz");
    s
}

async fn copy(
    s: &Session,
    table: &str,
    batches: &[RecordBatch],
) -> atlatl_core::error::Result<u64> {
    let oids: Vec<u32> = column_oids(s.client(), table)
        .await?
        .into_iter()
        .map(|(_, o)| o)
        .collect();
    let enc = CopyEncoder::from_oids(&batches[0].schema(), &oids)?;
    let mut w = CopyWriter::open(
        s.client(),
        &format!("COPY {table} FROM STDIN (FORMAT binary)"),
        enc,
    )
    .await?;
    for b in batches {
        w.write(b).await?;
    }
    w.finish().await
}

fn batch(cols: &[Col]) -> RecordBatch {
    let mut fields = vec![Field::new("id", arrow::datatypes::DataType::Int64, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64))];
    for c in cols {
        fields.push(Field::new(c.name, c.array.data_type().clone(), true));
        arrays.push(c.array.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("batch")
}

async fn create(s: &Session, table: &str, cols: &[Col]) {
    let defs: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {}", c.name, c.pg))
        .collect();
    s.client()
        .batch_execute(&format!(
            "CREATE TEMP TABLE {table} (id int8, {})",
            defs.join(", ")
        ))
        .await
        .expect("create");
}

async fn texts(s: &Session, table: &str, c: &str) -> Vec<Option<String>> {
    s.client()
        .query(&format!("SELECT {c}::text FROM {table} ORDER BY id"), &[])
        .await
        .expect("select")
        .iter()
        .map(|r| r.get(0))
        .collect()
}

async fn every_type_roundtrips(pg: &'static Postgres) {
    let s = session(pg).await;
    let cols = columns();
    create(&s, "stg", &cols).await;
    let n = copy(&s, "stg", &[batch(&cols)]).await.expect("copy");
    assert_eq!(n, ROWS as u64);
    for c in &cols {
        let want: Vec<Option<String>> = c.expect.iter().map(|v| v.map(str::to_owned)).collect();
        assert_eq!(
            texts(&s, "stg", c.name).await,
            want,
            "column {} ({})",
            c.name,
            c.pg
        );
    }
}

async fn sliced_batches(pg: &'static Postgres) {
    let s = session(pg).await;
    let cols: Vec<Col> = columns()
        .into_iter()
        .filter(|c| ["tags", "ids", "t", "n_38_6", "enum_dict"].contains(&c.name))
        .collect();
    create(&s, "stg", &cols).await;
    let b = batch(&cols);
    let n = copy(
        &s,
        "stg",
        &[b.slice(3, 2), b.slice(0, 0), b.slice(1, 2), b.slice(0, 1)],
    )
    .await
    .expect("copy");
    assert_eq!(n, ROWS as u64);
    for c in &cols {
        let want: Vec<Option<String>> = c.expect.iter().map(|v| v.map(str::to_owned)).collect();
        assert_eq!(texts(&s, "stg", c.name).await, want, "column {}", c.name);
    }
}

async fn many_rows_flush_in_chunks(pg: &'static Postgres) {
    let s = session(pg).await;
    s.client()
        .batch_execute("CREATE TEMP TABLE big (id int8, t text)")
        .await
        .expect("create");
    let n = 60_000i64;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", arrow::datatypes::DataType::Int64, false),
        Field::new("t", arrow::datatypes::DataType::Utf8, true),
    ]));
    let batches: Vec<RecordBatch> = (0..3)
        .map(|k| {
            let ids = Int64Array::from_iter_values(k * n / 3..(k + 1) * n / 3);
            let t = StringArray::from_iter(ids.values().iter().map(|i| {
                (i % 7 != 0).then(|| format!("row {i} {}", "x".repeat((i % 50) as usize)))
            }));
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(t)]).expect("batch")
        })
        .collect();
    assert_eq!(copy(&s, "big", &batches).await.expect("copy"), n as u64);
    let r = s
        .client()
        .query_one(
            "SELECT count(*), sum(id)::int8, count(t), \
             sum(length(t))::int8 FROM big",
            &[],
        )
        .await
        .expect("stats");
    let (count, sum, nonnull): (i64, i64, i64) = (r.get(0), r.get(1), r.get(2));
    assert_eq!((count, sum), (n, n * (n - 1) / 2));
    assert_eq!(nonnull, (0..n).filter(|i| i % 7 != 0).count() as i64);
    let chars: i64 = r.get(3);
    let want: i64 = (0..n)
        .filter(|i| i % 7 != 0)
        .map(|i| format!("row {i} ").len() as i64 + i % 50)
        .sum();
    assert_eq!(chars, want);
}

async fn server_rejects_bad_json_as_data(pg: &'static Postgres) {
    let s = session(pg).await;
    s.client()
        .batch_execute("CREATE TEMP TABLE j (v jsonb)")
        .await
        .expect("create");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        arrow::datatypes::DataType::Utf8,
        true,
    )]));
    let b = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("{\"ok\": 1}"),
            Some("{not json"),
        ]))],
    )
    .expect("batch");
    let e = copy(&s, "j", &[b]).await.expect_err("bad json");
    assert_eq!(e.class, ErrorClass::Data, "{e}");
    assert_eq!(e.code.as_deref(), Some("22P02"), "{e}");
}

fn decimal_text(v: i128, scale: i8) -> String {
    let d = v.unsigned_abs().to_string();
    let sign = if v < 0 { "-" } else { "" };
    if scale <= 0 {
        if v == 0 {
            return "0".into();
        }
        return format!("{sign}{d}{}", "0".repeat(usize::from(scale.unsigned_abs())));
    }
    let s = scale as usize;
    let d = format!("{d:0>w$}", w = s + 1);
    let (i, f) = d.split_at(d.len() - s);
    format!("{sign}{i}.{f}")
}

/// Binary-copied numerics equal Postgres's own parse of their text, and print back identically.
async fn numeric_matches_postgres_text_parsing(pg: &'static Postgres) {
    let s = session(pg).await;
    s.client()
        .batch_execute("CREATE TEMP TABLE n (id int8, v numeric, txt text)")
        .await
        .expect("create");
    let digits = prop_oneof![1u32..=38, Just(38u32),];
    let value = (digits, any::<bool>(), any::<u128>()).prop_map(|(d, neg, r)| {
        let m = (r % 10u128.pow(d)) as i128;
        if neg { -m } else { m }
    });
    let case = proptest::collection::vec((value, 0i8..=38), 1..64);
    let mut runner = TestRunner::new(Config {
        cases: 48,
        failure_persistence: None,
        ..Config::default()
    });
    let handle = tokio::runtime::Handle::current();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", arrow::datatypes::DataType::Int64, false),
        Field::new("v", arrow::datatypes::DataType::Decimal128(38, 0), true),
        Field::new("txt", arrow::datatypes::DataType::Utf8, false),
    ]));
    tokio::task::block_in_place(|| {
        runner
            .run(&case, |vals| {
                handle.block_on(async {
                    s.client()
                        .batch_execute("TRUNCATE n")
                        .await
                        .expect("truncate");
                    // One scale per batch column: group values by scale.
                    for scale in 0i8..=38 {
                        let group: Vec<i128> = vals
                            .iter()
                            .filter(|(_, sc)| *sc == scale)
                            .map(|(v, _)| *v)
                            .collect();
                        if group.is_empty() {
                            continue;
                        }
                        let schema = Arc::new(Schema::new(vec![
                            schema.field(0).clone(),
                            Field::new(
                                "v",
                                arrow::datatypes::DataType::Decimal128(38, scale),
                                true,
                            ),
                            schema.field(2).clone(),
                        ]));
                        let b = RecordBatch::try_new(
                            schema,
                            vec![
                                Arc::new(Int64Array::from_iter_values(0..group.len() as i64)),
                                Arc::new(
                                    Decimal128Array::from(group.clone())
                                        .with_precision_and_scale(38, scale)
                                        .expect("decimal"),
                                ),
                                Arc::new(StringArray::from_iter_values(
                                    group.iter().map(|v| decimal_text(*v, scale)),
                                )),
                            ],
                        )
                        .expect("batch");
                        copy(&s, "n", &[b]).await.expect("copy");
                    }
                    let bad = s
                        .client()
                        .query(
                            "SELECT v::text, txt FROM n \
                             WHERE v IS DISTINCT FROM txt::numeric OR v::text <> txt",
                            &[],
                        )
                        .await
                        .expect("compare");
                    let bad: Vec<(String, String)> =
                        bad.iter().map(|r| (r.get(0), r.get(1))).collect();
                    prop_assert!(bad.is_empty(), "binary vs text: {bad:?}");
                    Ok(())
                })
            })
            .expect("numeric property");
    });
}
