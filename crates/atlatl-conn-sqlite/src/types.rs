//! Arrow → SQLite storage mapping shared by SQLite, D1 and Turso (06 §6).

use std::hash::{Hash, Hasher};

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryViewArray, BooleanArray, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, FixedSizeListArray, LargeStringArray, MapArray, StringArray,
    StringViewArray, StructArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use atlatl_core::AtlatlError;
use atlatl_core::error::Result;
use base64::Engine as _;
use chrono::{DateTime, NaiveTime};

/// A SQLite value, as bound and as read back.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Real(f) if f.fract() == 0.0 => Some(*f as i64),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Bytes this value adds to a request or row.
    pub fn size(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Integer(_) | Value::Real(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Integer(i) => (*i).into(),
            Value::Real(f) => {
                serde_json::Number::from_f64(*f).map_or(serde_json::Value::Null, Into::into)
            }
            Value::Text(s) => s.clone().into(),
            Value::Blob(b) => base64::engine::general_purpose::STANDARD.encode(b).into(),
        }
    }
}

/// Equality and hashing for keys: reals by bit pattern.
#[derive(Clone, Debug)]
pub struct Key(pub Vec<Value>);

impl PartialEq for Key {
    fn eq(&self, o: &Self) -> bool {
        self.0.len() == o.0.len()
            && self.0.iter().zip(&o.0).all(|(a, b)| match (a, b) {
                (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
                _ => a == b,
            })
    }
}

impl Eq for Key {}

impl Hash for Key {
    fn hash<H: Hasher>(&self, h: &mut H) {
        for v in &self.0 {
            match v {
                Value::Null => 0u8.hash(h),
                Value::Integer(i) => (1u8, i).hash(h),
                Value::Real(f) => (2u8, f.to_bits()).hash(h),
                Value::Text(s) => (3u8, s).hash(h),
                Value::Blob(b) => (4u8, b).hash(h),
            }
        }
    }
}

/// How a served column is stored. The default follows the Arrow type; `columns.<c>.target_type`
/// overrides it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Store {
    Integer,
    Real,
    Text,
    Blob,
    /// Lists, structs and maps (or text taken as JSON) as compact JSON text.
    Json,
    /// Timestamps and dates as whole seconds since the epoch.
    EpochS,
    EpochMs,
}

impl Store {
    pub const NAMES: &[&str] = &[
        "integer", "real", "text", "blob", "json", "epoch_s", "epoch_ms",
    ];

    pub fn parse(target_type: &str) -> Result<Self> {
        Ok(match target_type.to_ascii_lowercase().as_str() {
            "integer" => Store::Integer,
            "real" => Store::Real,
            "text" => Store::Text,
            "blob" => Store::Blob,
            "json" => Store::Json,
            "epoch_s" => Store::EpochS,
            "epoch_ms" => Store::EpochMs,
            other => {
                return Err(AtlatlError::config(format!(
                    "target_type `{other}` is not a SQLite storage mapping (use one of: {})",
                    Self::NAMES.join(", ")
                )));
            }
        })
    }

    /// The default mapping (06 §6).
    pub fn default_for(dt: &DataType) -> Result<Self> {
        use DataType as D;
        Ok(match dt {
            D::Boolean
            | D::Int8
            | D::Int16
            | D::Int32
            | D::Int64
            | D::UInt8
            | D::UInt16
            | D::UInt32
            | D::UInt64 => Store::Integer,
            D::Float16 | D::Float32 | D::Float64 => Store::Real,
            D::Decimal128(..) | D::Decimal256(..) => Store::Text,
            D::Utf8 | D::LargeUtf8 | D::Utf8View | D::Null => Store::Text,
            D::Timestamp(..) | D::Date32 | D::Date64 | D::Time32(_) | D::Time64(_) => Store::Text,
            D::Binary | D::LargeBinary | D::BinaryView | D::FixedSizeBinary(_) => Store::Blob,
            D::List(_) | D::LargeList(_) | D::FixedSizeList(..) | D::Struct(_) | D::Map(..) => {
                Store::Json
            }
            D::Dictionary(_, v) => Self::default_for(v)?,
            other => {
                return Err(AtlatlError::config(format!(
                    "arrow {other} has no SQLite mapping; add a `transform` to a supported type"
                )));
            }
        })
    }

    /// The declared column type for `create_tables`.
    pub fn decl(self) -> &'static str {
        match self {
            Store::Integer | Store::EpochS | Store::EpochMs => "INTEGER",
            Store::Real => "REAL",
            Store::Text | Store::Json => "TEXT",
            Store::Blob => "BLOB",
        }
    }
}

/// The values of column `a` for rows `0..a.len()` under `store`. `name` is for errors.
pub fn column_values(a: &ArrayRef, store: Store, name: &str) -> Result<Vec<Value>> {
    let a = match a.data_type() {
        DataType::Dictionary(_, v) => {
            arrow::compute::cast(a, v).map_err(|e| AtlatlError::internal(e.to_string()))?
        }
        _ => a.clone(),
    };
    (0..a.len())
        .map(|i| cell(a.as_ref(), i, store, name))
        .collect()
}

fn mismatch(dt: &DataType, store: Store, name: &str) -> AtlatlError {
    AtlatlError::config(format!(
        "column `{name}`: arrow {dt} cannot be stored as {store:?}"
    ))
}

fn int_of(a: &dyn Array, i: usize) -> Option<std::result::Result<i64, String>> {
    use DataType as D;
    Some(Ok(match a.data_type() {
        D::Int8 => a.as_primitive::<Int8Type>().value(i).into(),
        D::Int16 => a.as_primitive::<Int16Type>().value(i).into(),
        D::Int32 => a.as_primitive::<Int32Type>().value(i).into(),
        D::Int64 => a.as_primitive::<Int64Type>().value(i),
        D::UInt8 => a.as_primitive::<UInt8Type>().value(i).into(),
        D::UInt16 => a.as_primitive::<UInt16Type>().value(i).into(),
        D::UInt32 => a.as_primitive::<UInt32Type>().value(i).into(),
        D::UInt64 => {
            let v = a.as_primitive::<UInt64Type>().value(i);
            return Some(
                i64::try_from(v).map_err(|_| format!("{v} exceeds SQLite's 64-bit signed INTEGER")),
            );
        }
        D::Boolean => i64::from(a.as_any().downcast_ref::<BooleanArray>()?.value(i)),
        _ => return None,
    }))
}

fn real_of(a: &dyn Array, i: usize) -> Option<f64> {
    use DataType as D;
    Some(match a.data_type() {
        D::Float16 => a.as_primitive::<Float16Type>().value(i).to_f64(),
        D::Float32 => a.as_primitive::<Float32Type>().value(i).into(),
        D::Float64 => a.as_primitive::<Float64Type>().value(i),
        D::Decimal128(..) | D::Decimal256(..) => decimal_text(a, i)?.parse().ok()?,
        _ => int_of(a, i)?.ok()? as f64,
    })
}

fn decimal_text(a: &dyn Array, i: usize) -> Option<String> {
    Some(match a.data_type() {
        DataType::Decimal128(..) => a
            .as_any()
            .downcast_ref::<Decimal128Array>()?
            .value_as_string(i),
        DataType::Decimal256(..) => a
            .as_any()
            .downcast_ref::<Decimal256Array>()?
            .value_as_string(i),
        _ => return None,
    })
}

fn text_of(a: &dyn Array, i: usize) -> Option<String> {
    use DataType as D;
    Some(match a.data_type() {
        D::Utf8 => a
            .as_any()
            .downcast_ref::<StringArray>()?
            .value(i)
            .to_owned(),
        D::LargeUtf8 => a
            .as_any()
            .downcast_ref::<LargeStringArray>()?
            .value(i)
            .to_owned(),
        D::Utf8View => a
            .as_any()
            .downcast_ref::<StringViewArray>()?
            .value(i)
            .to_owned(),
        _ => return None,
    })
}

fn bytes_of(a: &dyn Array, i: usize) -> Option<Vec<u8>> {
    use DataType as D;
    Some(match a.data_type() {
        D::Binary => a.as_binary::<i32>().value(i).to_vec(),
        D::LargeBinary => a.as_binary::<i64>().value(i).to_vec(),
        D::BinaryView => a
            .as_any()
            .downcast_ref::<BinaryViewArray>()?
            .value(i)
            .to_vec(),
        D::FixedSizeBinary(_) => a
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()?
            .value(i)
            .to_vec(),
        _ => return None,
    })
}

/// Seconds and nanoseconds since the epoch of a timestamp or date cell, and its unit.
fn instant_of(a: &dyn Array, i: usize) -> Option<(i64, u32, Option<TimeUnit>)> {
    use DataType as D;
    let split = |v: i64, per: i64| {
        (
            v.div_euclid(per),
            (v.rem_euclid(per) * (1_000_000_000 / per)) as u32,
        )
    };
    Some(match a.data_type() {
        D::Timestamp(u, _) => {
            let v = match u {
                TimeUnit::Second => a
                    .as_primitive::<arrow::datatypes::TimestampSecondType>()
                    .value(i),
                TimeUnit::Millisecond => a
                    .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                    .value(i),
                TimeUnit::Microsecond => a
                    .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                    .value(i),
                TimeUnit::Nanosecond => a
                    .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                    .value(i),
            };
            let per = match u {
                TimeUnit::Second => 1,
                TimeUnit::Millisecond => 1_000,
                TimeUnit::Microsecond => 1_000_000,
                TimeUnit::Nanosecond => 1_000_000_000,
            };
            let (s, n) = split(v, per);
            (s, n, Some(*u))
        }
        D::Date32 => (
            i64::from(a.as_primitive::<Date32Type>().value(i)) * 86_400,
            0,
            None,
        ),
        D::Date64 => {
            let (s, n) = split(a.as_primitive::<Date64Type>().value(i), 1_000);
            (s, n, None)
        }
        _ => return None,
    })
}

/// ISO-8601 text: fixed width per unit (so text order is time order); `Z` only for zoned types.
fn temporal_text(a: &dyn Array, i: usize) -> Option<std::result::Result<String, String>> {
    use DataType as D;
    let dt = a.data_type();
    if let D::Time32(_) | D::Time64(_) = dt {
        let (secs, nanos, frac) = match dt {
            D::Time32(TimeUnit::Second) => (
                i64::from(a.as_primitive::<Time32SecondType>().value(i)),
                0,
                "",
            ),
            D::Time32(_) => {
                let v = i64::from(a.as_primitive::<Time32MillisecondType>().value(i));
                (v / 1_000, (v % 1_000) as u32 * 1_000_000, "%.3f")
            }
            D::Time64(TimeUnit::Microsecond) => {
                let v = a.as_primitive::<Time64MicrosecondType>().value(i);
                (v / 1_000_000, (v % 1_000_000) as u32 * 1_000, "%.6f")
            }
            _ => {
                let v = a.as_primitive::<Time64NanosecondType>().value(i);
                (v / 1_000_000_000, (v % 1_000_000_000) as u32, "%.9f")
            }
        };
        return Some(
            NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos)
                .map(|t| t.format(&format!("%H:%M:%S{frac}")).to_string())
                .ok_or_else(|| format!("time {secs}s is out of range")),
        );
    }
    let (s, n, unit) = instant_of(a, i)?;
    let Some(t) = DateTime::from_timestamp(s, n) else {
        return Some(Err(format!("timestamp {s}s is out of range")));
    };
    Some(Ok(match (dt, unit) {
        (D::Date32 | D::Date64, _) => t.date_naive().format("%Y-%m-%d").to_string(),
        (D::Timestamp(_, tz), Some(u)) => {
            let frac = match u {
                TimeUnit::Second => "",
                TimeUnit::Millisecond => "%.3f",
                TimeUnit::Microsecond => "%.6f",
                TimeUnit::Nanosecond => "%.9f",
            };
            let z = if tz.is_some() { "Z" } else { "" };
            t.format(&format!("%Y-%m-%dT%H:%M:%S{frac}{z}")).to_string()
        }
        _ => return None,
    }))
}

/// ISO-8601 UTC text for a stamp (soft deletes, cursors).
pub fn iso(t: std::time::SystemTime) -> String {
    let d: DateTime<chrono::Utc> = t.into();
    d.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

pub fn parse_iso(s: &str) -> Option<std::time::SystemTime> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(Into::into)
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()
                .map(|n| n.and_utc().into())
        })
}

fn cell(a: &dyn Array, i: usize, store: Store, name: &str) -> Result<Value> {
    if a.is_null(i) {
        return Ok(Value::Null);
    }
    let dt = a.data_type();
    let data = |m: String| AtlatlError::data(format!("column `{name}`: {m}"));
    Ok(match store {
        Store::Integer => match int_of(a, i) {
            Some(v) => Value::Integer(v.map_err(data)?),
            None => match decimal_text(a, i) {
                Some(t) if !t.contains('.') => Value::Integer(
                    t.parse()
                        .map_err(|_| data(format!("{t} exceeds SQLite's 64-bit signed INTEGER")))?,
                ),
                _ => return Err(mismatch(dt, store, name)),
            },
        },
        Store::Real => match real_of(a, i) {
            Some(f) if f.is_nan() => Value::Null,
            Some(f) => Value::Real(f),
            None => return Err(mismatch(dt, store, name)),
        },
        Store::Text => {
            if let Some(s) = text_of(a, i).or_else(|| decimal_text(a, i)) {
                Value::Text(s)
            } else if let Some(t) = temporal_text(a, i) {
                Value::Text(t.map_err(data)?)
            } else if let DataType::Boolean = dt {
                Value::Text(a.as_boolean().value(i).to_string())
            } else if let Some(v) = int_of(a, i) {
                Value::Text(v.map_or_else(
                    |_| a.as_primitive::<UInt64Type>().value(i).to_string(),
                    |v| v.to_string(),
                ))
            } else if let Some(f) = real_of(a, i) {
                Value::Text(f.to_string())
            } else if nested(dt) {
                Value::Text(json_of(a, i, name)?.to_string())
            } else {
                return Err(mismatch(dt, store, name));
            }
        }
        Store::Blob => match bytes_of(a, i).or_else(|| text_of(a, i).map(String::into_bytes)) {
            Some(b) => Value::Blob(b),
            None => return Err(mismatch(dt, store, name)),
        },
        Store::Json => match text_of(a, i) {
            Some(s) => Value::Text(s),
            None => Value::Text(json_of(a, i, name)?.to_string()),
        },
        Store::EpochS | Store::EpochMs => match instant_of(a, i) {
            Some((s, n, _)) => Value::Integer(if store == Store::EpochS {
                s
            } else {
                s.saturating_mul(1_000)
                    .saturating_add(i64::from(n / 1_000_000))
            }),
            None => return Err(mismatch(dt, store, name)),
        },
    })
}

fn nested(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::Struct(_)
            | DataType::Map(..)
    )
}

/// A cell as JSON: nested types recursively, decimals and temporals as strings (lossless),
/// binary as base64, non-finite floats as null.
fn json_of(a: &dyn Array, i: usize, name: &str) -> Result<serde_json::Value> {
    use serde_json::Value as J;
    if a.is_null(i) {
        return Ok(J::Null);
    }
    let dt = a.data_type();
    let list = |child: ArrayRef| -> Result<J> {
        (0..child.len())
            .map(|j| json_of(child.as_ref(), j, name))
            .collect::<Result<Vec<_>>>()
            .map(J::Array)
    };
    Ok(match dt {
        DataType::Boolean => J::Bool(a.as_boolean().value(i)),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => real_of(a, i)
            .and_then(serde_json::Number::from_f64)
            .map_or(J::Null, J::Number),
        DataType::List(_) => list(a.as_list::<i32>().value(i))?,
        DataType::LargeList(_) => list(a.as_list::<i64>().value(i))?,
        DataType::FixedSizeList(..) => list(
            a.as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| mismatch(dt, Store::Json, name))?
                .value(i),
        )?,
        DataType::Struct(fields) => {
            let s = a
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| mismatch(dt, Store::Json, name))?;
            let mut m = serde_json::Map::new();
            for (f, c) in fields.iter().zip(s.columns()) {
                m.insert(f.name().clone(), json_of(c.as_ref(), i, name)?);
            }
            J::Object(m)
        }
        DataType::Map(..) => {
            let m = a
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| mismatch(dt, Store::Json, name))?;
            let e = m.value(i);
            let (k, v) = (e.column(0), e.column(1));
            let keys: Vec<J> = (0..k.len())
                .map(|j| json_of(k.as_ref(), j, name))
                .collect::<Result<_>>()?;
            if keys.iter().all(J::is_string) {
                let mut o = serde_json::Map::new();
                for (j, key) in keys.into_iter().enumerate() {
                    o.insert(
                        key.as_str().unwrap_or_default().to_owned(),
                        json_of(v.as_ref(), j, name)?,
                    );
                }
                J::Object(o)
            } else {
                J::Array(
                    keys.into_iter()
                        .enumerate()
                        .map(|(j, key)| Ok(J::Array(vec![key, json_of(v.as_ref(), j, name)?])))
                        .collect::<Result<_>>()?,
                )
            }
        }
        DataType::Dictionary(_, v) => {
            let c = arrow::compute::cast(&a.slice(i, 1), v)
                .map_err(|e| AtlatlError::internal(e.to_string()))?;
            json_of(c.as_ref(), 0, name)?
        }
        _ => {
            if let Some(v) = int_of(a, i) {
                match v {
                    Ok(v) => v.into(),
                    Err(_) => a.as_primitive::<UInt64Type>().value(i).into(),
                }
            } else if let Some(s) = text_of(a, i).or_else(|| decimal_text(a, i)) {
                J::String(s)
            } else if let Some(t) = temporal_text(a, i) {
                J::String(t.map_err(|m| AtlatlError::data(format!("column `{name}`: {m}")))?)
            } else if let Some(b) = bytes_of(a, i) {
                J::String(base64::engine::general_purpose::STANDARD.encode(b))
            } else {
                return Err(mismatch(dt, Store::Json, name));
            }
        }
    })
}

/// SQLite column affinity of a declared type (sqlite.org/datatype3.html §3.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

pub fn affinity(decl: &str) -> Affinity {
    let d = decl.to_ascii_uppercase();
    if d.contains("INT") {
        Affinity::Integer
    } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
        Affinity::Text
    } else if d.contains("BLOB") || d.is_empty() {
        Affinity::Blob
    } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
        Affinity::Real
    } else {
        Affinity::Numeric
    }
}

/// `v` as SQLite would store it in a column of affinity `a` (for comparing with read-back rows).
pub fn apply_affinity(a: Affinity, v: Value) -> Value {
    let numeric = |s: &str| -> Option<Value> {
        let t = s.trim();
        if let Ok(i) = t.parse::<i64>() {
            return Some(Value::Integer(i));
        }
        t.parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .map(Value::Real)
    };
    let integral =
        |f: f64| (f.fract() == 0.0 && f.abs() < 9.2e18).then_some(Value::Integer(f as i64));
    match (a, v) {
        (Affinity::Text, Value::Integer(i)) => Value::Text(i.to_string()),
        (Affinity::Text, Value::Real(f)) => Value::Text(if f.fract() == 0.0 && f.abs() < 1e15 {
            format!("{f:.1}")
        } else {
            f.to_string()
        }),
        (Affinity::Real, Value::Integer(i)) => Value::Real(i as f64),
        (Affinity::Real, Value::Text(s)) => match numeric(&s) {
            Some(Value::Integer(i)) => Value::Real(i as f64),
            Some(r) => r,
            None => Value::Text(s),
        },
        (Affinity::Integer | Affinity::Numeric, Value::Real(f)) => {
            integral(f).unwrap_or(Value::Real(f))
        }
        (Affinity::Integer | Affinity::Numeric, Value::Text(s)) => match numeric(&s) {
            Some(Value::Real(f)) => integral(f).unwrap_or(Value::Real(f)),
            Some(n) => n,
            None => Value::Text(s),
        },
        (_, v) => v,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
        TimestampMicrosecondArray, UInt64Array,
    };
    use arrow::datatypes::{Field, Fields, Int32Type};

    use super::*;

    fn vals(a: ArrayRef, s: Store) -> Vec<Value> {
        column_values(&a, s, "c").expect("values")
    }

    #[test]
    fn defaults() {
        let d = |dt: DataType| Store::default_for(&dt).expect("mapping");
        assert_eq!(d(DataType::Boolean), Store::Integer);
        assert_eq!(d(DataType::Float32), Store::Real);
        assert_eq!(d(DataType::Decimal128(18, 4)), Store::Text);
        assert_eq!(
            d(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            Store::Text
        );
        assert_eq!(
            d(DataType::List(Arc::new(Field::new(
                "i",
                DataType::Int32,
                true
            )))),
            Store::Json
        );
        assert_eq!(d(DataType::Binary), Store::Blob);
        assert!(Store::default_for(&DataType::Duration(TimeUnit::Second)).is_err());
        assert_eq!(Store::parse("EPOCH_MS").expect("parse"), Store::EpochMs);
        assert!(
            Store::parse("varchar")
                .expect_err("bad")
                .message
                .contains("epoch_s")
        );
    }

    #[test]
    fn scalars() {
        assert_eq!(
            vals(
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Store::Integer
            ),
            [Value::Integer(1), Value::Null]
        );
        assert_eq!(
            vals(
                Arc::new(BooleanArray::from(vec![true, false])),
                Store::Integer
            ),
            [Value::Integer(1), Value::Integer(0)]
        );
        assert_eq!(
            vals(
                Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
                Store::Real
            ),
            [Value::Real(1.5), Value::Null]
        );
        let big: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        assert_eq!(
            column_values(&big, Store::Integer, "c")
                .expect_err("overflow")
                .class,
            atlatl_core::error::ErrorClass::Data
        );
        assert_eq!(vals(big, Store::Text), [Value::Text(u64::MAX.to_string())]);
        let dec: ArrayRef = Arc::new(
            Decimal128Array::from(vec![12345, -5])
                .with_precision_and_scale(10, 2)
                .expect("dec"),
        );
        assert_eq!(
            vals(dec.clone(), Store::Text),
            [Value::Text("123.45".into()), Value::Text("-0.05".into())]
        );
        assert_eq!(
            vals(dec, Store::Real),
            [Value::Real(123.45), Value::Real(-0.05)]
        );
        assert_eq!(
            vals(Arc::new(StringArray::from(vec!["x"])), Store::Text),
            [Value::Text("x".into())]
        );
    }

    #[test]
    fn temporals() {
        let ts: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![1_700_000_000_123_456, -1]).with_timezone("UTC"),
        );
        assert_eq!(
            vals(ts.clone(), Store::Text),
            [
                Value::Text("2023-11-14T22:13:20.123456Z".into()),
                Value::Text("1969-12-31T23:59:59.999999Z".into())
            ]
        );
        assert_eq!(
            vals(ts.clone(), Store::EpochS),
            [Value::Integer(1_700_000_000), Value::Integer(-1)]
        );
        assert_eq!(
            vals(ts, Store::EpochMs),
            [Value::Integer(1_700_000_000_123), Value::Integer(-1)]
        );
        let naive: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0]));
        assert_eq!(
            vals(naive, Store::Text),
            [Value::Text("1970-01-01T00:00:00.000000".into())]
        );
        let d: ArrayRef = Arc::new(Date32Array::from(vec![19_000]));
        assert_eq!(
            vals(d.clone(), Store::Text),
            [Value::Text("2022-01-08".into())]
        );
        assert_eq!(vals(d, Store::EpochS), [Value::Integer(19_000 * 86_400)]);
        assert!(
            column_values(
                &(Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                Store::EpochS,
                "c"
            )
            .is_err()
        );
    }

    #[test]
    fn nested_json() {
        let l: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
        ]));
        assert_eq!(
            vals(l, Store::Json),
            [Value::Text("[1,null]".into()), Value::Null]
        );
        let s: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("b", DataType::Utf8, true),
                Field::new("a", DataType::Decimal128(5, 1), true),
            ]),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(
                    Decimal128Array::from(vec![15])
                        .with_precision_and_scale(5, 1)
                        .expect("dec"),
                ),
            ],
            None,
        ));
        assert_eq!(
            vals(s, Store::Json),
            [Value::Text(r#"{"b":"x","a":"1.5"}"#.into())],
            "field order kept; decimals lossless"
        );
    }

    #[test]
    fn affinities() {
        assert_eq!(affinity("VARCHAR(10)"), Affinity::Text);
        assert_eq!(affinity("BIGINT"), Affinity::Integer);
        assert_eq!(affinity(""), Affinity::Blob);
        assert_eq!(affinity("DOUBLE"), Affinity::Real);
        assert_eq!(affinity("DATETIME"), Affinity::Numeric);
        assert_eq!(
            apply_affinity(Affinity::Real, Value::Integer(1)),
            Value::Real(1.0)
        );
        assert_eq!(
            apply_affinity(Affinity::Integer, Value::Text("12".into())),
            Value::Integer(12)
        );
        assert_eq!(
            apply_affinity(Affinity::Numeric, Value::Text("1.50".into())),
            Value::Real(1.5)
        );
        assert_eq!(
            apply_affinity(Affinity::Numeric, Value::Text("2024-01-01".into())),
            Value::Text("2024-01-01".into())
        );
        assert_eq!(
            apply_affinity(Affinity::Text, Value::Integer(7)),
            Value::Text("7".into())
        );
        assert_eq!(Key(vec![Value::Real(0.1)]), Key(vec![Value::Real(0.1)]));
    }
}
