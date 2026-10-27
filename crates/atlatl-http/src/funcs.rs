//! Expression functions (07 §9): dates, `range`, encoders and signers.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use chrono::{
    DateTime, Datelike, Duration as CDuration, Months, NaiveDate, NaiveDateTime, TimeZone,
    Timelike, Utc,
};
use hmac::{Hmac, KeyInit, Mac};
use minijinja::value::{DynObject, Object, ObjectRepr, ValueKind};
use minijinja::{Error, ErrorKind, Value};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::{Digest, Sha256, Sha512};

pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(Utc::now)
}

pub fn fixed_clock(rfc3339: &str) -> Clock {
    let t = DateTime::parse_from_rfc3339(rfc3339)
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or_default();
    Arc::new(move || t)
}

fn op_err(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidOperation, msg.into())
}

/// A UTC instant. Renders as RFC 3339; compares with other dates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Date(pub DateTime<Utc>);

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fmt = if self.0.nanosecond() == 0 {
            "%Y-%m-%dT%H:%M:%SZ"
        } else {
            "%Y-%m-%dT%H:%M:%S%.6fZ"
        };
        write!(f, "{}", self.0.format(fmt))
    }
}

impl Object for Date {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Plain
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let t = self.0;
        Some(Value::from(match key.as_str()? {
            "year" => i64::from(t.year()),
            "month" => i64::from(t.month()),
            "day" => i64::from(t.day()),
            "hour" => i64::from(t.hour()),
            "minute" => i64::from(t.minute()),
            "second" => i64::from(t.second()),
            "epoch" => t.timestamp(),
            _ => return None,
        }))
    }

    fn custom_cmp(self: &Arc<Self>, other: &DynObject) -> Option<Ordering> {
        other.downcast_ref::<Date>().map(|o| self.0.cmp(&o.0))
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

pub fn date_value(t: DateTime<Utc>) -> Value {
    Value::from_object(Date(t))
}

fn parse_auto(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    for f in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(t) = NaiveDateTime::parse_from_str(s, f) {
            return Some(t.and_utc());
        }
    }
    for f in ["%Y-%m-%d %H:%M:%S%.f%z", "%Y-%m-%dT%H:%M:%S%.f%z"] {
        if let Ok(t) = DateTime::parse_from_str(s, f) {
            return Some(t.with_timezone(&Utc));
        }
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
}

/// A date from a date value, an ISO-8601 string or an epoch-seconds integer.
pub fn to_date(v: &Value) -> Result<DateTime<Utc>, Error> {
    if let Some(d) = v.downcast_object_ref::<Date>() {
        return Ok(d.0);
    }
    match v.kind() {
        ValueKind::String => {
            let s = v.as_str().unwrap_or_default();
            parse_auto(s).ok_or_else(|| op_err(format!("cannot parse `{s}` as a date")))
        }
        ValueKind::Number if v.is_integer() => i64::try_from(v.clone())
            .ok()
            .and_then(|n| Utc.timestamp_opt(n, 0).single())
            .ok_or_else(|| op_err(format!("{v} is not a valid epoch timestamp"))),
        _ => Err(op_err(format!("expected a date, found {}", v.kind()))),
    }
}

pub fn date_parse(v: &Value, fmt: &str) -> Result<DateTime<Utc>, Error> {
    if fmt == "auto" || v.downcast_object_ref::<Date>().is_some() {
        return to_date(v);
    }
    let s = v
        .as_str()
        .ok_or_else(|| op_err(format!("date_parse expects a string, found {}", v.kind())))?;
    if let Ok(t) = DateTime::parse_from_str(s, fmt) {
        return Ok(t.with_timezone(&Utc));
    }
    if let Ok(t) = NaiveDateTime::parse_from_str(s, fmt) {
        return Ok(t.and_utc());
    }
    NaiveDate::parse_from_str(s, fmt)
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
        .ok_or_else(|| op_err(format!("cannot parse `{s}` with format `{fmt}`")))
}

pub fn date_format(v: &Value, fmt: &str) -> Result<Value, Error> {
    let t = to_date(v)?;
    let mut out = String::new();
    fmt::write(&mut out, format_args!("{}", t.format(fmt)))
        .map_err(|_| op_err(format!("invalid date format `{fmt}`")))?;
    Ok(Value::from(out))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

fn unit_name(u: &str) -> Result<Unit, Error> {
    let l = u.trim().to_ascii_lowercase();
    let l = l.strip_suffix('s').filter(|x| !x.is_empty()).unwrap_or(&l);
    Ok(match l {
        "second" => Unit::Second,
        "minute" => Unit::Minute,
        "hour" => Unit::Hour,
        "day" => Unit::Day,
        "week" => Unit::Week,
        "month" => Unit::Month,
        "quarter" => Unit::Quarter,
        "year" => Unit::Year,
        _ => {
            return Err(op_err(format!(
                "unknown date unit `{u}` (second, minute, hour, day, week, month, quarter, year)"
            )));
        }
    })
}

fn add_months(t: DateTime<Utc>, n: i64) -> Result<DateTime<Utc>, Error> {
    let m = Months::new(u32::try_from(n.unsigned_abs()).map_err(|_| op_err("date overflow"))?);
    if n >= 0 {
        t.checked_add_months(m)
    } else {
        t.checked_sub_months(m)
    }
    .ok_or_else(|| op_err("date overflow"))
}

fn add(t: DateTime<Utc>, n: i64, u: Unit) -> Result<DateTime<Utc>, Error> {
    let fixed = |d: Option<CDuration>| {
        d.and_then(|d| t.checked_add_signed(d))
            .ok_or_else(|| op_err("date overflow"))
    };
    match u {
        Unit::Second => fixed(CDuration::try_seconds(n)),
        Unit::Minute => fixed(CDuration::try_minutes(n)),
        Unit::Hour => fixed(CDuration::try_hours(n)),
        Unit::Day => fixed(CDuration::try_days(n)),
        Unit::Week => fixed(CDuration::try_weeks(n)),
        Unit::Month => add_months(t, n),
        Unit::Quarter => add_months(t, n.saturating_mul(3)),
        Unit::Year => add_months(t, n.saturating_mul(12)),
    }
}

pub fn date_add(v: &Value, n: i64, unit: &str) -> Result<DateTime<Utc>, Error> {
    add(to_date(v)?, n, unit_name(unit)?)
}

pub fn date_trunc(v: &Value, unit: &str) -> Result<DateTime<Utc>, Error> {
    let t = to_date(v)?;
    let day = |d: NaiveDate| d.and_hms_opt(0, 0, 0).map(|x| x.and_utc());
    let r = match unit_name(unit)? {
        Unit::Second => t.with_nanosecond(0),
        Unit::Minute => t.with_nanosecond(0).and_then(|x| x.with_second(0)),
        Unit::Hour => t
            .with_nanosecond(0)
            .and_then(|x| x.with_second(0))
            .and_then(|x| x.with_minute(0)),
        Unit::Day => day(t.date_naive()),
        Unit::Week => {
            day(t.date_naive() - CDuration::days(i64::from(t.weekday().num_days_from_monday())))
        }
        Unit::Month => NaiveDate::from_ymd_opt(t.year(), t.month(), 1).and_then(day),
        Unit::Quarter => {
            NaiveDate::from_ymd_opt(t.year(), (t.month0() / 3) * 3 + 1, 1).and_then(day)
        }
        Unit::Year => NaiveDate::from_ymd_opt(t.year(), 1, 1).and_then(day),
    };
    r.ok_or_else(|| op_err("date_trunc overflow"))
}

const RANGE_CAP: usize = 1_000_000;

fn date_step(s: &str) -> Result<(i64, Unit), Error> {
    let bad = || {
        op_err(format!(
            "invalid range step `{s}` (e.g. `1d`, `7d`, `1w`, `1mo`, `3mo`, `1q`, `1y`, `6h`)"
        ))
    };
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
    let (num, unit) = t.split_at(split);
    let n: i64 = num.parse().map_err(|_| bad())?;
    if n < 1 {
        return Err(bad());
    }
    let unit = unit.trim();
    if unit == "M" {
        return Ok((n, Unit::Month));
    }
    let l = unit.to_ascii_lowercase();
    if l == "m" {
        return Err(op_err(format!(
            "ambiguous range step `{s}`: use `min` for minutes or `mo` for months"
        )));
    }
    let l = if l.len() > 1 {
        l.strip_suffix('s').unwrap_or(&l)
    } else {
        &l
    };
    let u = match l {
        "h" | "hour" => Unit::Hour,
        "min" | "minute" => Unit::Minute,
        "d" | "day" => Unit::Day,
        "w" | "week" => Unit::Week,
        "mo" | "month" => Unit::Month,
        "q" | "quarter" => Unit::Quarter,
        "y" | "year" => Unit::Year,
        _ => return Err(bad()),
    };
    Ok((n, u))
}

fn as_int(v: &Value) -> Option<i64> {
    if v.is_integer() {
        return i64::try_from(v.clone()).ok();
    }
    None
}

/// `range(start, end[, step])`: end-inclusive over ints or dates (07 §9).
pub fn range(start: Value, end: Value, step: Option<Value>) -> Result<Value, Error> {
    if let (Some(a), Some(b)) = (as_int(&start), as_int(&end)) {
        let step = match &step {
            None => 1,
            Some(s) => as_int(s)
                .or_else(|| s.as_str().and_then(|x| x.trim().parse().ok()))
                .ok_or_else(|| {
                    op_err(format!(
                        "integer range step must be an integer, found `{s}`"
                    ))
                })?,
        };
        if step == 0 {
            return Err(op_err("range step must not be 0"));
        }
        let mut out = Vec::new();
        let mut x = a;
        while (step > 0 && x <= b) || (step < 0 && x >= b) {
            out.push(Value::from(x));
            if out.len() > RANGE_CAP {
                return Err(op_err(format!("range yields more than {RANGE_CAP} items")));
            }
            match x.checked_add(step) {
                Some(n) => x = n,
                None => break,
            }
        }
        return Ok(Value::from(out));
    }
    let a = to_date(&start)?;
    let b = to_date(&end)?;
    let (n, unit) = match &step {
        None => (1, Unit::Day),
        Some(s) => date_step(s.as_str().ok_or_else(|| {
            op_err(format!(
                "date range step must be a string like `1d`, found `{s}`"
            ))
        })?)?,
    };
    let mut out = Vec::new();
    for k in 0.. {
        let t = add(a, n.saturating_mul(k), unit)?;
        if t > b {
            break;
        }
        out.push(date_value(t));
        if out.len() > RANGE_CAP {
            return Err(op_err(format!("range yields more than {RANGE_CAP} items")));
        }
    }
    Ok(Value::from(out))
}

pub fn is_empty(v: &Value) -> bool {
    match v.kind() {
        ValueKind::Undefined | ValueKind::None => true,
        ValueKind::String => v.as_str().is_some_and(str::is_empty),
        ValueKind::Seq | ValueKind::Map => v.len() == Some(0),
        _ => false,
    }
}

/// RFC 3986 unreserved characters stay; everything else is percent-encoded.
const COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub fn urlencode_str(s: &str) -> String {
    utf8_percent_encode(s, COMPONENT).to_string()
}

pub fn bytes_of(v: &Value) -> Vec<u8> {
    match v.as_bytes() {
        Some(b) if v.kind() == ValueKind::Bytes => b.to_vec(),
        _ => crate::expr::plain(v).into_bytes(),
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = fmt::Write::write_fmt(&mut s, format_args!("{x:02x}"));
            s
        })
}

pub fn base64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

pub fn sha256(b: &[u8]) -> Vec<u8> {
    Sha256::digest(b).to_vec()
}

pub fn hmac(alg: &str, key: &[u8], msg: &[u8]) -> Result<Vec<u8>, String> {
    match alg.to_ascii_lowercase().replace('-', "").as_str() {
        "sha256" => {
            let mut m = Hmac::<Sha256>::new_from_slice(key).map_err(|e| e.to_string())?;
            m.update(msg);
            Ok(m.finalize().into_bytes().to_vec())
        }
        "sha512" => {
            let mut m = Hmac::<Sha512>::new_from_slice(key).map_err(|e| e.to_string())?;
            m.update(msg);
            Ok(m.finalize().into_bytes().to_vec())
        }
        other => Err(format!(
            "unsupported hmac algorithm `{other}` (sha256, sha512)"
        )),
    }
}

pub fn hmac_fn(alg: String, key: Value, msg: Value) -> Result<Value, Error> {
    hmac(&alg, &bytes_of(&key), &bytes_of(&msg))
        .map(Value::from_bytes)
        .map_err(op_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{Engine, Scope};

    fn e() -> Engine {
        Engine::new(fixed_clock("2026-09-28T12:34:56Z"))
    }

    fn s(x: &str) -> String {
        crate::expr::plain(&e().eval(x, &Scope::new()).expect(x))
    }

    #[test]
    fn now_is_utc_and_dates_render_iso() {
        assert_eq!(s("now()"), "2026-09-28T12:34:56Z");
        assert_eq!(s("date('2015-01-01')"), "2015-01-01T00:00:00Z");
        assert_eq!(
            s("date('2015-01-01T10:00:00+02:00')"),
            "2015-01-01T08:00:00Z"
        );
        assert_eq!(
            s("date_parse('01/02/2020', '%m/%d/%Y')"),
            "2020-01-02T00:00:00Z"
        );
        assert_eq!(
            s("date_parse('2020-01-02 03:04', 'auto')"),
            "2020-01-02T03:04:00Z"
        );
        assert_eq!(
            s("now() | date_format('%Y-%m-%dT%H:%M:%SZ')"),
            "2026-09-28T12:34:56Z"
        );
        assert_eq!(s("date('2020-03-04').year"), "2020");
        assert!(e().eval("date('nope')", &Scope::new()).is_err());
        assert!(e().eval("date_format(now(), '%Q')", &Scope::new()).is_err());
    }

    #[test]
    fn date_add_clamps_months() {
        assert_eq!(
            s("date_add(date('2024-01-31'), 1, 'month')"),
            "2024-02-29T00:00:00Z"
        );
        assert_eq!(
            s("date_add(date('2023-01-31'), 1, 'Month')"),
            "2023-02-28T00:00:00Z"
        );
        assert_eq!(
            s("date_add(date('2024-02-29'), 1, 'year')"),
            "2025-02-28T00:00:00Z"
        );
        assert_eq!(
            s("date_add(date('2024-05-31'), -1, 'quarters')"),
            "2024-02-29T00:00:00Z"
        );
        assert_eq!(
            s(
                "date('2024-01-01') | date_add(3, 'month') | date_add(-1, 'day') | date_format('%Y-%m-%d')"
            ),
            "2024-03-31"
        );
        assert_eq!(
            s("date_add(date('2024-01-01'), 90, 'minutes')"),
            "2024-01-01T01:30:00Z"
        );
        assert!(
            e().eval("date_add(now(), 1, 'fortnight')", &Scope::new())
                .is_err()
        );
    }

    #[test]
    fn date_trunc_units() {
        assert_eq!(
            s("date_trunc(date('2026-08-19T10:11:12Z'), 'quarter')"),
            "2026-07-01T00:00:00Z"
        );
        assert_eq!(
            s("date_trunc(date('2026-08-19T10:11:12Z'), 'week')"),
            "2026-08-17T00:00:00Z"
        );
        assert_eq!(
            s("date_trunc(date('2026-08-19T10:11:12Z'), 'hour')"),
            "2026-08-19T10:00:00Z"
        );
        assert_eq!(
            s("date_trunc(date_add(now(), -6, 'month'), 'quarter')"),
            "2026-01-01T00:00:00Z"
        );
    }

    #[test]
    fn date_comparisons() {
        assert_eq!(s("date('2020-01-02') > date('2020-01-01')"), "true");
        assert_eq!(
            s("date('2020-01-01') == date('2020-01-01T00:00:00Z')"),
            "true"
        );
        assert_eq!(s("date('2020-01-01') < now()"), "true");
    }

    #[test]
    fn int_ranges() {
        assert_eq!(s("range(1, 5)"), "[1,2,3,4,5]");
        assert_eq!(s("range(0, 10, 5)"), "[0,5,10]");
        assert_eq!(s("range(0, 2500, '1000')"), "[0,1000,2000]");
        assert_eq!(s("range(3, 1, -1)"), "[3,2,1]");
        assert_eq!(s("range(5, 1)"), "[]");
        assert!(e().eval("range(1, 2, 0)", &Scope::new()).is_err());
    }

    #[test]
    fn date_ranges() {
        let d = |x: &str| {
            let v = e().eval(x, &Scope::new()).expect(x);
            v.try_iter()
                .expect("seq")
                .map(|t| date_format(&t, "%Y-%m-%d").expect("fmt").to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            d("range('2024-01-01', '2024-01-03')"),
            ["2024-01-01", "2024-01-02", "2024-01-03"]
        );
        assert_eq!(
            d("range(date('2024-01-30'), date('2024-04-30'), '1mo')"),
            ["2024-01-30", "2024-02-29", "2024-03-30", "2024-04-30"]
        );
        assert_eq!(
            d("range(date('2024-01-01'), date('2024-12-31'), '1q')"),
            ["2024-01-01", "2024-04-01", "2024-07-01", "2024-10-01"]
        );
        assert_eq!(
            d("range(date('2024-01-01'), date('2024-01-20'), '7d')"),
            ["2024-01-01", "2024-01-08", "2024-01-15"]
        );
        assert_eq!(
            d("range(date('2015-01-01'), date('2015-12-31'), '3MO')"),
            ["2015-01-01", "2015-04-01", "2015-07-01", "2015-10-01"]
        );
        assert_eq!(
            d("range(date('2024-01-31'), date('2024-03-31'), '1M')"),
            ["2024-01-31", "2024-02-29", "2024-03-31"]
        );
        assert_eq!(
            d("range(date('2020-02-29'), date('2022-03-01'), '1years')"),
            ["2020-02-29", "2021-02-28", "2022-02-28"]
        );
        assert_eq!(
            d("range(date('2024-02-01'), date('2024-01-01'), '1d')"),
            Vec::<String>::new()
        );
        assert_eq!(
            d("range(date('2024-01-01'), date('2024-01-01T02:00:00Z'), '1h')").len(),
            3
        );
        for bad in ["'1m'", "'0d'", "'d'", "'1s'", "1"] {
            let x = format!("range(date('2024-01-01'), date('2024-02-01'), {bad})");
            assert!(e().eval(&x, &Scope::new()).is_err(), "{bad}");
        }
    }
}
