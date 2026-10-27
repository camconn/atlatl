//! Arrow → Postgres binary `COPY … FROM STDIN (FORMAT binary)` (spec 08 §6.1).

use std::pin::Pin;

use arrow::array::{
    Array, AsArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, GenericListArray,
    LargeStringArray, OffsetSizeTrait, RecordBatch, StringArray, StringViewArray, new_empty_array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Schema, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use atlatl_core::AtlatlError;
use atlatl_core::error::Result;
use bytes::{BufMut, Bytes, BytesMut};
use futures::SinkExt;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{CopyInSink, GenericClient};

use crate::error::wrap;

const SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
const FLUSH_AT: usize = 1 << 20;
/// 2000-01-01 − 1970-01-01.
const PG_EPOCH_US: i64 = 946_684_800_000_000;
const PG_EPOCH_DAYS: i32 = 10_957;
const NUMERIC_NEG: u16 = 0x4000;

/// Encodes batches of one schema into COPY rows for the given target column types.
#[derive(Clone, Debug)]
pub struct CopyEncoder {
    names: Vec<String>,
    types: Vec<DataType>,
    targets: Vec<Type>,
}

impl CopyEncoder {
    /// Fails with `config` when an Arrow type can't be written into its target column.
    pub fn new(schema: &Schema, targets: &[Type]) -> Result<Self> {
        if schema.fields().len() != targets.len() {
            return Err(AtlatlError::config(format!(
                "COPY has {} arrow columns but {} target columns",
                schema.fields().len(),
                targets.len()
            )));
        }
        for (f, t) in schema.fields().iter().zip(targets) {
            let empty = new_empty_array(f.data_type());
            cell(empty.as_ref(), t)
                .map_err(|m| AtlatlError::config(format!("column `{}`: {m}", f.name())))?;
        }
        Ok(Self {
            names: schema.fields().iter().map(|f| f.name().clone()).collect(),
            types: schema
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect(),
            targets: targets.to_vec(),
        })
    }

    /// Targets by type OID; only built-in types can be staged.
    pub fn from_oids(schema: &Schema, oids: &[u32]) -> Result<Self> {
        let types = oids
            .iter()
            .zip(schema.fields())
            .map(|(&o, f)| {
                Type::from_oid(o).ok_or_else(|| {
                    AtlatlError::config(format!(
                        "column `{}`: target type oid {o} is not a built-in type",
                        f.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if oids.len() != schema.fields().len() {
            return Err(AtlatlError::config(format!(
                "COPY has {} arrow columns but {} target columns",
                schema.fields().len(),
                oids.len()
            )));
        }
        Self::new(schema, &types)
    }

    pub fn targets(&self) -> &[Type] {
        &self.targets
    }

    pub fn header(buf: &mut BytesMut) {
        buf.put_slice(SIGNATURE);
        buf.put_i32(0);
        buf.put_i32(0);
    }

    pub fn trailer(buf: &mut BytesMut) {
        buf.put_i16(-1);
    }

    /// Appends every row of `batch`.
    pub fn encode(&self, batch: &RecordBatch, buf: &mut BytesMut) -> Result<()> {
        let cells = self.cells(batch)?;
        self.rows(&cells, 0..batch.num_rows(), buf)
    }

    fn cells<'a>(&self, batch: &'a RecordBatch) -> Result<Vec<Cell<'a>>> {
        if batch.num_columns() != self.types.len() {
            return Err(AtlatlError::internal(format!(
                "batch has {} columns, the COPY encoder {}",
                batch.num_columns(),
                self.types.len()
            )));
        }
        batch
            .columns()
            .iter()
            .zip(&self.types)
            .zip(&self.targets)
            .zip(&self.names)
            .map(|(((a, dt), t), name)| {
                if a.data_type() != dt {
                    return Err(AtlatlError::internal(format!(
                        "column `{name}` is {} in this batch, {dt} in the schema",
                        a.data_type()
                    )));
                }
                cell(a.as_ref(), t)
                    .map_err(|m| AtlatlError::config(format!("column `{name}`: {m}")))
            })
            .collect()
    }

    fn rows(
        &self,
        cells: &[Cell<'_>],
        rows: std::ops::Range<usize>,
        buf: &mut BytesMut,
    ) -> Result<()> {
        let n = i16::try_from(cells.len())
            .map_err(|_| AtlatlError::config("COPY supports at most 32767 columns"))?;
        for r in rows {
            buf.put_i16(n);
            for (c, cell) in cells.iter().enumerate() {
                cell.put(r, buf).map_err(|m| {
                    AtlatlError::data(format!("column `{}` row {r}: {m}", self.names[c]))
                })?;
            }
        }
        Ok(())
    }
}

/// Streams batches into a `COPY … FROM STDIN (FORMAT binary)` sink.
pub struct CopyWriter {
    sink: Pin<Box<CopyInSink<Bytes>>>,
    enc: CopyEncoder,
    buf: BytesMut,
}

impl CopyWriter {
    pub fn new(sink: CopyInSink<Bytes>, enc: CopyEncoder) -> Self {
        let mut buf = BytesMut::with_capacity(FLUSH_AT + (FLUSH_AT >> 2));
        CopyEncoder::header(&mut buf);
        Self {
            sink: Box::pin(sink),
            enc,
            buf,
        }
    }

    /// Opens `copy_sql` (a `COPY … FROM STDIN (FORMAT binary)`) on `c`.
    pub async fn open(c: &impl GenericClient, copy_sql: &str, enc: CopyEncoder) -> Result<Self> {
        let sink = c.client().copy_in(copy_sql).await.map_err(wrap)?;
        Ok(Self::new(sink, enc))
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let cells = self.enc.cells(batch)?;
        let mut r = 0;
        let n = batch.num_rows();
        while r < n {
            let end = (r + 256).min(n);
            self.enc.rows(&cells, r..end, &mut self.buf)?;
            r = end;
            if self.buf.len() >= FLUSH_AT {
                let chunk = self.buf.split().freeze();
                self.sink.send(chunk).await.map_err(wrap)?;
            }
        }
        Ok(())
    }

    /// Returns the number of rows the server copied.
    pub async fn finish(mut self) -> Result<u64> {
        CopyEncoder::trailer(&mut self.buf);
        let chunk = self.buf.split().freeze();
        self.sink.send(chunk).await.map_err(wrap)?;
        self.sink.as_mut().finish().await.map_err(wrap)
    }
}

/// `(name, type oid)` of a table's columns in order, e.g. a `pg_temp` staging table.
pub async fn column_oids(c: &impl GenericClient, table: &str) -> Result<Vec<(String, u32)>> {
    let rows = c
        .query(
            "SELECT attname::text, atttypid FROM pg_attribute \
             WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            &[&table],
        )
        .await
        .map_err(wrap)?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Whether [`CopyEncoder`] can write arrow `dt` into a column of type `t`.
pub fn copyable(dt: &DataType, t: &Type) -> bool {
    cell(new_empty_array(dt).as_ref(), t).is_ok()
}

struct Cell<'a> {
    nulls: Option<NullBuffer>,
    col: Col<'a>,
}

enum Col<'a> {
    Null,
    Bool(&'a BooleanArray),
    Int(Ints<'a>, u8),
    IntNumeric(Ints<'a>),
    F32(&'a Float32Array, bool),
    F64(&'a Float64Array),
    Decimal(&'a Decimal128Array, i8),
    Str(Strs<'a>, bool),
    Ts(&'a [i64], TimeUnit),
    Date32(&'a [i32]),
    Date64(&'a [i64]),
    Dict(Vec<usize>, Box<Cell<'a>>),
    List(Offsets<'a>, Box<Cell<'a>>, u32),
}

enum Ints<'a> {
    I8(&'a [i8]),
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
}

impl Ints<'_> {
    fn get(&self, i: usize) -> i128 {
        match self {
            Ints::I8(v) => v[i].into(),
            Ints::I16(v) => v[i].into(),
            Ints::I32(v) => v[i].into(),
            Ints::I64(v) => v[i].into(),
            Ints::U8(v) => v[i].into(),
            Ints::U16(v) => v[i].into(),
            Ints::U32(v) => v[i].into(),
            Ints::U64(v) => v[i].into(),
        }
    }

    /// Bytes needed for every value.
    fn width(&self) -> u8 {
        match self {
            Ints::I8(_) | Ints::I16(_) | Ints::U8(_) => 2,
            Ints::I32(_) | Ints::U16(_) => 4,
            Ints::I64(_) | Ints::U32(_) => 8,
            Ints::U64(_) => 16,
        }
    }
}

enum Strs<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl Strs<'_> {
    fn get(&self, i: usize) -> &str {
        match self {
            Strs::Utf8(a) => a.value(i),
            Strs::Large(a) => a.value(i),
            Strs::View(a) => a.value(i),
        }
    }
}

enum Offsets<'a> {
    Small(&'a [i32]),
    Large(&'a [i64]),
}

impl Offsets<'_> {
    fn range(&self, i: usize) -> std::ops::Range<usize> {
        match self {
            Offsets::Small(o) => o[i] as usize..o[i + 1] as usize,
            Offsets::Large(o) => o[i] as usize..o[i + 1] as usize,
        }
    }
}

fn ints(a: &dyn Array) -> Option<Ints<'_>> {
    Some(match a.data_type() {
        DataType::Int8 => Ints::I8(a.as_primitive::<Int8Type>().values()),
        DataType::Int16 => Ints::I16(a.as_primitive::<Int16Type>().values()),
        DataType::Int32 => Ints::I32(a.as_primitive::<Int32Type>().values()),
        DataType::Int64 => Ints::I64(a.as_primitive::<Int64Type>().values()),
        DataType::UInt8 => Ints::U8(a.as_primitive::<UInt8Type>().values()),
        DataType::UInt16 => Ints::U16(a.as_primitive::<UInt16Type>().values()),
        DataType::UInt32 => Ints::U32(a.as_primitive::<UInt32Type>().values()),
        DataType::UInt64 => Ints::U64(a.as_primitive::<UInt64Type>().values()),
        _ => return None,
    })
}

fn list<O: OffsetSizeTrait>(a: &GenericListArray<O>) -> (&dyn Array, &[O]) {
    (a.values().as_ref(), a.value_offsets())
}

/// Validates `a`'s type against `t` and borrows its buffers for encoding.
fn cell<'a>(a: &'a dyn Array, t: &Type) -> std::result::Result<Cell<'a>, String> {
    let dt = a.data_type();
    let nope = || format!("arrow {dt} cannot be copied into a postgres `{t}` column");
    let col = match dt {
        DataType::Null => Col::Null,
        DataType::Boolean if *t == Type::BOOL => Col::Bool(a.as_boolean()),
        _ if dt.is_integer() => {
            let v = ints(a).ok_or_else(nope)?;
            let w = v.width();
            let target = [(Type::INT2, 2), (Type::INT4, 4), (Type::INT8, 8)]
                .into_iter()
                .find(|(ty, _)| ty == t);
            match target {
                _ if *t == Type::NUMERIC => Col::IntNumeric(v),
                Some((_, tw)) if w <= tw => Col::Int(v, tw),
                _ => return Err(nope()),
            }
        }
        DataType::Float32 if *t == Type::FLOAT4 => Col::F32(a.as_primitive(), false),
        DataType::Float32 if *t == Type::FLOAT8 => Col::F32(a.as_primitive(), true),
        DataType::Float64 if *t == Type::FLOAT8 => Col::F64(a.as_primitive()),
        DataType::Decimal128(_, s) if *t == Type::NUMERIC => {
            Col::Decimal(a.as_primitive::<Decimal128Type>(), *s)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let s = match dt {
                DataType::Utf8 => Strs::Utf8(a.as_string()),
                DataType::LargeUtf8 => Strs::Large(a.as_string()),
                _ => Strs::View(a.as_string_view()),
            };
            if *t == Type::JSONB {
                Col::Str(s, true)
            } else if [Type::TEXT, Type::VARCHAR, Type::BPCHAR, Type::JSON].contains(t) {
                Col::Str(s, false)
            } else {
                return Err(nope());
            }
        }
        DataType::Timestamp(u, tz)
            if *t
                == if tz.is_some() {
                    Type::TIMESTAMPTZ
                } else {
                    Type::TIMESTAMP
                } =>
        {
            let v = match u {
                TimeUnit::Second => a.as_primitive::<TimestampSecondType>().values(),
                TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().values(),
                TimeUnit::Microsecond => a.as_primitive::<TimestampMicrosecondType>().values(),
                TimeUnit::Nanosecond => a.as_primitive::<TimestampNanosecondType>().values(),
            };
            Col::Ts(v, *u)
        }
        DataType::Date32 if *t == Type::DATE => {
            Col::Date32(a.as_primitive::<Date32Type>().values())
        }
        DataType::Date64 if *t == Type::DATE => {
            Col::Date64(a.as_primitive::<Date64Type>().values())
        }
        DataType::Dictionary(..) => {
            let d = a.as_any_dictionary();
            let values = cell(d.values().as_ref(), t)?;
            // All keys are null when there are no values.
            let keys = if d.values().is_empty() {
                Vec::new()
            } else {
                d.normalized_keys()
            };
            Col::Dict(keys, Box::new(values))
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let Kind::Array(elem) = t.kind() else {
                return Err(nope());
            };
            let (child, offsets) = match dt {
                DataType::List(_) => {
                    let (c, o) = list(a.as_list::<i32>());
                    (c, Offsets::Small(o))
                }
                _ => {
                    let (c, o) = list(a.as_list::<i64>());
                    (c, Offsets::Large(o))
                }
            };
            if matches!(elem.kind(), Kind::Array(_))
                || matches!(
                    child.data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                )
            {
                return Err(format!("{}; nested arrays are not supported", nope()));
            }
            let e = cell(child, elem).map_err(|m| format!("{}: elements: {m}", nope()))?;
            Col::List(offsets, Box::new(e), elem.oid())
        }
        _ => return Err(nope()),
    };
    Ok(Cell {
        nulls: a.logical_nulls(),
        col,
    })
}

impl Cell<'_> {
    fn is_null(&self, i: usize) -> bool {
        self.nulls.as_ref().is_some_and(|n| n.is_null(i))
    }

    fn put(&self, i: usize, buf: &mut BytesMut) -> std::result::Result<(), String> {
        if self.is_null(i) {
            buf.put_i32(-1);
            return Ok(());
        }
        match &self.col {
            Col::Null => buf.put_i32(-1),
            Col::Bool(a) => {
                buf.put_i32(1);
                buf.put_u8(a.value(i).into());
            }
            Col::Int(v, w) => {
                let x = v.get(i);
                buf.put_i32((*w).into());
                match w {
                    2 => buf.put_i16(x as i16),
                    4 => buf.put_i32(x as i32),
                    _ => buf.put_i64(x as i64),
                }
            }
            Col::IntNumeric(v) => put_numeric(buf, v.get(i), 0),
            Col::F32(a, false) => {
                buf.put_i32(4);
                buf.put_f32(a.value(i));
            }
            Col::F32(a, true) => {
                buf.put_i32(8);
                buf.put_f64(a.value(i).into());
            }
            Col::F64(a) => {
                buf.put_i32(8);
                buf.put_f64(a.value(i));
            }
            Col::Decimal(a, s) => put_numeric(buf, a.value(i), *s),
            Col::Str(s, jsonb) => {
                let v = s.get(i).as_bytes();
                let len = i32::try_from(v.len() + usize::from(*jsonb))
                    .map_err(|_| format!("value of {} bytes is too large", v.len()))?;
                buf.put_i32(len);
                if *jsonb {
                    buf.put_u8(1);
                }
                buf.put_slice(v);
            }
            Col::Ts(v, u) => {
                let x = v[i];
                let us = match u {
                    TimeUnit::Second => x.checked_mul(1_000_000),
                    TimeUnit::Millisecond => x.checked_mul(1_000),
                    TimeUnit::Microsecond => Some(x),
                    TimeUnit::Nanosecond => Some(x.div_euclid(1_000)),
                }
                .and_then(|us| us.checked_sub(PG_EPOCH_US))
                .ok_or_else(|| format!("timestamp {x} ({u:?}) is out of range"))?;
                buf.put_i32(8);
                buf.put_i64(us);
            }
            Col::Date32(v) => {
                let d = v[i]
                    .checked_sub(PG_EPOCH_DAYS)
                    .ok_or_else(|| format!("date {} is out of range", v[i]))?;
                buf.put_i32(4);
                buf.put_i32(d);
            }
            Col::Date64(v) => {
                let d = i32::try_from(v[i].div_euclid(86_400_000))
                    .ok()
                    .and_then(|d| d.checked_sub(PG_EPOCH_DAYS))
                    .ok_or_else(|| format!("date {} ms is out of range", v[i]))?;
                buf.put_i32(4);
                buf.put_i32(d);
            }
            Col::Dict(keys, values) => values.put(keys[i], buf)?,
            Col::List(offsets, elems, oid) => {
                let r = offsets.range(i);
                let at = buf.len();
                buf.put_i32(0);
                if r.is_empty() {
                    buf.put_i32(0);
                    buf.put_i32(0);
                    buf.put_u32(*oid);
                } else {
                    let n = i32::try_from(r.len()).map_err(|_| "array is too long".to_owned())?;
                    buf.put_i32(1);
                    buf.put_i32(r.clone().any(|j| elems.is_null(j)).into());
                    buf.put_u32(*oid);
                    buf.put_i32(n);
                    buf.put_i32(1);
                    for j in r {
                        elems.put(j, buf)?;
                    }
                }
                let len = i32::try_from(buf.len() - at - 4)
                    .map_err(|_| "array value is too large".to_owned())?;
                buf[at..at + 4].copy_from_slice(&len.to_be_bytes());
            }
        }
        Ok(())
    }
}

/// `v × 10^-scale` in the NUMERIC wire format: ndigits, weight, sign, dscale, then base-10000
/// digits, most significant first.
fn put_numeric(buf: &mut BytesMut, v: i128, scale: i8) {
    let (weight, groups) = numeric_groups(v, scale);
    buf.put_i32(8 + 2 * groups.len() as i32);
    buf.put_i16(groups.len() as i16);
    buf.put_i16(weight);
    buf.put_u16(if v < 0 { NUMERIC_NEG } else { 0 });
    buf.put_u16(scale.max(0) as u16);
    for g in groups {
        buf.put_i16(g);
    }
}

/// `(weight, digits)` with leading and trailing zero digits stripped; zero is `(0, [])`.
fn numeric_groups(v: i128, scale: i8) -> (i16, Vec<i16>) {
    const POW: [i16; 4] = [1, 10, 100, 1000];
    let s = i32::from(scale);
    // A decimal digit at position k (from the right) has exponent k - s; group g spans 4g..4g+3.
    let gmin = (-s).div_euclid(4);
    let mut acc = [0i16; 12];
    let mut m = v.unsigned_abs();
    let mut k = 0i32;
    while m > 0 {
        let e = k - s;
        acc[(e.div_euclid(4) - gmin) as usize] += (m % 10) as i16 * POW[e.rem_euclid(4) as usize];
        m /= 10;
        k += 1;
    }
    let Some(hi) = acc.iter().rposition(|&g| g != 0) else {
        return (0, Vec::new());
    };
    let lo = acc.iter().position(|&g| g != 0).unwrap_or(hi);
    (
        (gmin + hi as i32) as i16,
        acc[lo..=hi].iter().rev().copied().collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Postgres's text output for a numeric with `dscale = max(scale, 0)`.
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

    /// The inverse of [`put_numeric`], as Postgres's `get_str_from_var` renders it.
    fn wire_text(b: &[u8]) -> String {
        let rd = |o: usize| i16::from_be_bytes([b[o], b[o + 1]]);
        let len = i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
        let (n, weight, sign, dscale) = (rd(4), rd(6), rd(8) as u16, rd(10) as usize);
        assert_eq!(len, 8 + 2 * n as usize);
        let digits: Vec<i16> = (0..n as usize).map(|i| rd(12 + 2 * i)).collect();
        assert!(digits.iter().all(|d| (0..10_000).contains(d)));
        assert!(digits.first().is_none_or(|&d| d != 0));
        assert!(digits.last().is_none_or(|&d| d != 0));
        let at = |w: i32| -> i16 {
            let idx = i32::from(weight) - w;
            if idx < 0 || idx >= i32::from(n) {
                0
            } else {
                digits[idx as usize]
            }
        };
        let mut int = String::new();
        for w in (0..=i32::from(weight).max(0)).rev() {
            int.push_str(&format!("{:04}", at(w)));
        }
        let int = int.trim_start_matches('0');
        let int = if int.is_empty() { "0" } else { int };
        let mut frac = String::new();
        let mut w = -1;
        while frac.len() < dscale {
            frac.push_str(&format!("{:04}", at(w)));
            w -= 1;
        }
        frac.truncate(dscale);
        let neg = if sign == NUMERIC_NEG { "-" } else { "" };
        if dscale == 0 {
            format!("{neg}{int}")
        } else {
            format!("{neg}{int}.{frac}")
        }
    }

    fn encoded(v: i128, s: i8) -> Vec<u8> {
        let mut b = BytesMut::new();
        put_numeric(&mut b, v, s);
        b.to_vec()
    }

    #[test]
    fn numeric_wire() {
        // (value, scale, weight, digits)
        let cases: &[(i128, i8, i16, &[i16])] = &[
            (0, 0, 0, &[]),
            (0, 4, 0, &[]),
            (1, 0, 0, &[1]),
            (-1, 4, -1, &[1]),
            (10_000, 0, 1, &[1]),
            (123_456_789, 4, 1, &[1, 2345, 6789]),
            (
                12_345_678_901_234_567_890_123_456,
                6,
                4,
                &[1234, 5678, 9012, 3456, 7890, 1234, 5600],
            ),
            (5, -8, 2, &[5]),
            (1_2345_0000_0001, 0, 3, &[1, 2345, 0, 1]),
        ];
        for &(v, s, weight, digits) in cases {
            assert_eq!(numeric_groups(v, s), (weight, digits.to_vec()), "{v}e-{s}");
        }
        let b = encoded(-1, 4);
        assert_eq!(&b[8..12], &[0x40, 0, 0, 4], "sign and dscale");
        assert_eq!(wire_text(&b), "-0.0001");
    }

    #[test]
    fn numeric_text_examples() {
        let max38 = 10i128.pow(38) - 1;
        for (v, s, t) in [
            (0, 0, "0"),
            (0, 3, "0.000"),
            (-1, 4, "-0.0001"),
            (
                12_345_678_901_234_567_890_123_456,
                6,
                "12345678901234567890.123456",
            ),
            (max38, 0, "99999999999999999999999999999999999999"),
            (-max38, 38, "-0.99999999999999999999999999999999999999"),
            (max38, 19, "9999999999999999999.9999999999999999999"),
            (7, -3, "7000"),
        ] {
            assert_eq!(decimal_text(v, s), t);
            assert_eq!(wire_text(&encoded(v, s)), t);
        }
    }

    fn schema(dt: DataType) -> Schema {
        Schema::new(vec![arrow::datatypes::Field::new("c", dt, true)])
    }

    fn list_of(dt: DataType) -> DataType {
        DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item", dt, true,
        )))
    }

    #[test]
    fn type_mismatches_are_config() {
        let tz = Some("UTC".into());
        let us = TimeUnit::Microsecond;
        let bad = [
            (DataType::Utf8, Type::INT8),
            (DataType::Int64, Type::INT4),
            (DataType::Int32, Type::INT2),
            (DataType::UInt32, Type::INT4),
            (DataType::UInt64, Type::INT8),
            (DataType::Float64, Type::FLOAT4),
            (DataType::Float64, Type::NUMERIC),
            (DataType::Decimal128(38, 2), Type::INT8),
            (DataType::Boolean, Type::INT2),
            (DataType::Timestamp(us, tz.clone()), Type::TIMESTAMP),
            (DataType::Timestamp(us, None), Type::TIMESTAMPTZ),
            (DataType::Date32, Type::TIMESTAMP),
            (DataType::Int64, Type::TEXT),
            (list_of(DataType::Utf8), Type::INT8_ARRAY),
            (list_of(DataType::Int64), Type::TEXT),
            (list_of(DataType::Utf8), Type::TEXT),
            (list_of(list_of(DataType::Int64)), Type::INT8_ARRAY),
            (DataType::Binary, Type::BYTEA),
        ];
        for (dt, t) in bad {
            let e = CopyEncoder::new(&schema(dt.clone()), std::slice::from_ref(&t))
                .expect_err("mismatch");
            assert_eq!(e.class, atlatl_core::ErrorClass::Config, "{dt} -> {t}: {e}");
            assert!(e.message.contains("column `c`"), "{e}");
        }
        let good = [
            (DataType::Int8, Type::INT2),
            (DataType::UInt16, Type::INT4),
            (DataType::UInt32, Type::INT8),
            (DataType::UInt64, Type::NUMERIC),
            (DataType::Float32, Type::FLOAT8),
            (DataType::Utf8View, Type::JSONB),
            (DataType::LargeUtf8, Type::VARCHAR),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, tz),
                Type::TIMESTAMPTZ,
            ),
            (DataType::Date64, Type::DATE),
            (DataType::Null, Type::JSONB),
            (list_of(DataType::Int16), Type::INT8_ARRAY),
            (
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                Type::TEXT,
            ),
        ];
        for (dt, t) in good {
            CopyEncoder::new(&schema(dt.clone()), std::slice::from_ref(&t))
                .unwrap_or_else(|e| panic!("{dt} -> {t}: {e}"));
        }
        let e = CopyEncoder::new(&schema(DataType::Int64), &[]).expect_err("count");
        assert_eq!(e.class, atlatl_core::ErrorClass::Config);
        let e = CopyEncoder::from_oids(&schema(DataType::Utf8), &[99_999]).expect_err("oid");
        assert!(e.message.contains("oid 99999"), "{e}");
    }

    #[test]
    fn out_of_range_values_are_data() {
        use arrow::array::{Date64Array, TimestampSecondArray};
        let cases: [(std::sync::Arc<dyn Array>, Type); 2] = [
            (
                std::sync::Arc::new(TimestampSecondArray::from(vec![0, i64::MAX])),
                Type::TIMESTAMP,
            ),
            (
                std::sync::Arc::new(Date64Array::from(vec![0, i64::MIN])),
                Type::DATE,
            ),
        ];
        for (a, t) in cases {
            let s = std::sync::Arc::new(schema(a.data_type().clone()));
            let b = RecordBatch::try_new(s.clone(), vec![a]).expect("batch");
            let e = CopyEncoder::new(&s, &[t])
                .expect("encoder")
                .encode(&b, &mut BytesMut::new())
                .expect_err("overflow");
            assert_eq!(e.class, atlatl_core::ErrorClass::Data, "{e}");
            assert!(e.message.contains("column `c` row 1"), "{e}");
        }
    }

    #[test]
    fn wire_layout() {
        use arrow::array::{Int16Array, ListBuilder, StringBuilder};
        let mut l = ListBuilder::new(StringBuilder::new());
        l.values().append_value("ab");
        l.values().append_null();
        l.append(true);
        l.append(true);
        let s = std::sync::Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("i", DataType::Int16, true),
            arrow::datatypes::Field::new("l", list_of(DataType::Utf8), true),
        ]));
        let b = RecordBatch::try_new(
            s.clone(),
            vec![
                std::sync::Arc::new(Int16Array::from(vec![Some(-2), None])),
                std::sync::Arc::new(l.finish()),
            ],
        )
        .expect("batch");
        let mut buf = BytesMut::new();
        CopyEncoder::header(&mut buf);
        CopyEncoder::new(&s, &[Type::INT8, Type::TEXT_ARRAY])
            .expect("encoder")
            .encode(&b, &mut buf)
            .expect("encode");
        CopyEncoder::trailer(&mut buf);
        let mut want: Vec<u8> = b"PGCOPY\n\xff\r\n\0".to_vec();
        want.extend([0; 8]);
        want.extend([0, 2]);
        want.extend(8i32.to_be_bytes());
        want.extend((-2i64).to_be_bytes());
        let arr: Vec<u8> = [
            &1i32.to_be_bytes()[..],
            &1i32.to_be_bytes(),
            &25u32.to_be_bytes(),
            &2i32.to_be_bytes(),
            &1i32.to_be_bytes(),
            &2i32.to_be_bytes(),
            b"ab",
            &(-1i32).to_be_bytes(),
        ]
        .concat();
        want.extend((arr.len() as i32).to_be_bytes());
        want.extend(arr);
        want.extend([0, 2]);
        want.extend((-1i32).to_be_bytes());
        let empty: Vec<u8> = [
            &0i32.to_be_bytes()[..],
            &0i32.to_be_bytes(),
            &25u32.to_be_bytes(),
        ]
        .concat();
        want.extend((empty.len() as i32).to_be_bytes());
        want.extend(empty);
        want.extend((-1i16).to_be_bytes());
        assert_eq!(buf.to_vec(), want);
    }

    proptest! {
        #[test]
        fn numeric_roundtrips(v in -(10i128.pow(38) - 1)..10i128.pow(38), s in -10i8..=38) {
            prop_assert_eq!(wire_text(&encoded(v, s)), decimal_text(v, s));
        }

        #[test]
        fn numeric_extremes(v in prop_oneof![Just(i128::MIN), Just(i128::MAX), any::<i128>()], s in any::<i8>()) {
            prop_assert_eq!(wire_text(&encoded(v, s)), decimal_text(v, s));
        }
    }
}
