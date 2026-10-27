//! pgoutput binary values (`binary 'true'`, PG 14+), rendered as the text the walsender sends
//! under the session settings in `wire.rs` (UTC, ISO dates, `postgres` intervals, shortest
//! floats, hex bytea). Staging, keys and casts are then the same for both formats.

use std::collections::HashMap;
use std::fmt::Write;

use atlatl_conn_postgres::{Session, wrap};
use atlatl_core::AtlatlError;
use atlatl_core::error::Result;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Plan {
    Bool,
    Int2,
    Int4,
    Int8,
    Oid,
    Float4,
    Float8,
    Numeric,
    Text,
    Bytea,
    Json,
    Jsonb,
    Uuid,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
    Array { elem: Box<Plan>, delim: char },
}

fn builtin(oid: u32) -> Option<Plan> {
    Some(match oid {
        16 => Plan::Bool,
        17 => Plan::Bytea,
        19 | 25 | 1042 | 1043 => Plan::Text,
        20 => Plan::Int8,
        21 => Plan::Int2,
        23 => Plan::Int4,
        26 => Plan::Oid,
        114 => Plan::Json,
        700 => Plan::Float4,
        701 => Plan::Float8,
        1082 => Plan::Date,
        1083 => Plan::Time,
        1114 => Plan::Timestamp,
        1184 => Plan::TimestampTz,
        1186 => Plan::Interval,
        1266 => Plan::TimeTz,
        1700 => Plan::Numeric,
        2950 => Plan::Uuid,
        3802 => Plan::Jsonb,
        _ => return None,
    })
}

struct TypeRow {
    typtype: char,
    base: u32,
    elem: u32,
    category: char,
    delim: char,
}

/// Decoders by type OID. Arrays, enums and domains are resolved through `pg_type`.
#[derive(Default)]
pub(crate) struct Plans(HashMap<u32, Option<Plan>>);

impl Plans {
    pub fn get(&self, oid: u32) -> Option<&Plan> {
        self.0.get(&oid).and_then(Option::as_ref)
    }

    /// Resolves `oids` and returns the ones without a decoder.
    pub async fn resolve(&mut self, sql: &Session, oids: &[u32]) -> Result<Vec<u32>> {
        let mut want: Vec<u32> = oids
            .iter()
            .copied()
            .filter(|o| !self.0.contains_key(o))
            .collect();
        want.sort_unstable();
        want.dedup();
        let mut rows: HashMap<u32, TypeRow> = HashMap::new();
        let mut frontier: Vec<u32> = want
            .iter()
            .copied()
            .filter(|o| builtin(*o).is_none())
            .collect();
        while !frontier.is_empty() {
            let ids: Vec<i64> = frontier.iter().map(|&o| i64::from(o)).collect();
            frontier.clear();
            let got = sql
                .client()
                .query(
                    "SELECT oid::int8, typtype::text, typbasetype::int8, typelem::int8,
                            typcategory::text, typdelim::text
                     FROM pg_type WHERE oid::int8 = ANY($1)",
                    &[&ids],
                )
                .await
                .map_err(wrap)?;
            for r in got {
                let ch = |i: usize| r.get::<_, String>(i).chars().next().unwrap_or('\0');
                let t = TypeRow {
                    typtype: ch(1),
                    base: r.get::<_, i64>(2) as u32,
                    elem: r.get::<_, i64>(3) as u32,
                    category: ch(4),
                    delim: ch(5),
                };
                for next in [t.base, t.elem] {
                    if next != 0
                        && builtin(next).is_none()
                        && !self.0.contains_key(&next)
                        && !rows.contains_key(&next)
                    {
                        frontier.push(next);
                    }
                }
                rows.insert(r.get::<_, i64>(0) as u32, t);
            }
        }
        for o in want {
            let p = self.plan(o, &rows, 0);
            self.0.insert(o, p);
        }
        let mut missing: Vec<u32> = oids
            .iter()
            .copied()
            .filter(|o| self.get(*o).is_none())
            .collect();
        missing.sort_unstable();
        missing.dedup();
        Ok(missing)
    }

    fn plan(&self, oid: u32, rows: &HashMap<u32, TypeRow>, depth: u8) -> Option<Plan> {
        if depth > 8 {
            return None;
        }
        if let Some(p) = builtin(oid) {
            return Some(p);
        }
        if let Some(p) = self.0.get(&oid) {
            return p.clone();
        }
        let t = rows.get(&oid)?;
        match t.typtype {
            'e' => Some(Plan::Text),
            'd' => self.plan(t.base, rows, depth + 1),
            // int2vector and oidvector print space-separated, not as arrays.
            'b' if t.category == 'A' && t.elem != 0 && oid != 22 && oid != 30 => {
                Some(Plan::Array {
                    elem: Box::new(self.plan(t.elem, rows, depth + 1)?),
                    delim: t.delim,
                })
            }
            _ => None,
        }
    }
}

fn bad(what: &str) -> AtlatlError {
    AtlatlError::internal(format!("pgoutput: malformed binary {what} value")).code("pgoutput")
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(bad(what));
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }

    fn i16(&mut self, what: &str) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2, what)?.try_into().map_err(|_| bad(what))?,
        ))
    }

    fn i32(&mut self, what: &str) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4, what)?.try_into().map_err(|_| bad(what))?,
        ))
    }

    fn i64(&mut self, what: &str) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.take(8, what)?.try_into().map_err(|_| bad(what))?,
        ))
    }

    fn done(&self, what: &str) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(bad(what))
        }
    }
}

/// The Postgres text form of binary value `b`.
pub(crate) fn render(p: &Plan, b: &[u8]) -> Result<String> {
    let mut out = String::new();
    write_value(&mut out, p, b)?;
    Ok(out)
}

fn exact<const N: usize>(b: &[u8], what: &str) -> Result<[u8; N]> {
    b.try_into().map_err(|_| bad(what))
}

fn write_value(out: &mut String, p: &Plan, b: &[u8]) -> Result<()> {
    match p {
        Plan::Bool => out.push(if exact::<1>(b, "bool")?[0] != 0 {
            't'
        } else {
            'f'
        }),
        Plan::Int2 => write_int(out, i16::from_be_bytes(exact(b, "int2")?)),
        Plan::Int4 => write_int(out, i32::from_be_bytes(exact(b, "int4")?)),
        Plan::Int8 => write_int(out, i64::from_be_bytes(exact(b, "int8")?)),
        Plan::Oid => write_int(out, u32::from_be_bytes(exact(b, "oid")?)),
        Plan::Float4 => {
            let v = f32::from_be_bytes(exact(b, "float4")?);
            write_float(
                out,
                v.is_nan(),
                v.is_sign_negative(),
                v.is_infinite(),
                &format!("{v:e}"),
                6,
            );
        }
        Plan::Float8 => {
            let v = f64::from_be_bytes(exact(b, "float8")?);
            write_float(
                out,
                v.is_nan(),
                v.is_sign_negative(),
                v.is_infinite(),
                &format!("{v:e}"),
                15,
            );
        }
        Plan::Numeric => write_numeric(out, b)?,
        Plan::Text | Plan::Json => out.push_str(std::str::from_utf8(b).map_err(|_| bad("text"))?),
        Plan::Jsonb => match b.split_first() {
            Some((1, rest)) => out.push_str(std::str::from_utf8(rest).map_err(|_| bad("jsonb"))?),
            _ => return Err(bad("jsonb")),
        },
        Plan::Bytea => {
            out.push_str("\\x");
            for x in b {
                let _ = write!(out, "{x:02x}");
            }
        }
        Plan::Uuid => {
            let u = exact::<16>(b, "uuid")?;
            for (i, x) in u.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    out.push('-');
                }
                let _ = write!(out, "{x:02x}");
            }
        }
        Plan::Date => {
            let d = i32::from_be_bytes(exact(b, "date")?);
            match d {
                i32::MAX => out.push_str("infinity"),
                i32::MIN => out.push_str("-infinity"),
                d => {
                    let bc = write_date(out, i64::from(d) + POSTGRES_EPOCH_JDATE);
                    if bc {
                        out.push_str(" BC");
                    }
                }
            }
        }
        Plan::Time => write_time(out, i64::from_be_bytes(exact(b, "time")?)),
        Plan::TimeTz => {
            let mut r = Reader(b);
            let us = r.i64("timetz")?;
            let zone = r.i32("timetz")?;
            r.done("timetz")?;
            write_time(out, us);
            write_zone(out, zone);
        }
        Plan::Timestamp | Plan::TimestampTz => {
            let us = i64::from_be_bytes(exact(b, "timestamp")?);
            match us {
                i64::MAX => out.push_str("infinity"),
                i64::MIN => out.push_str("-infinity"),
                us => {
                    let bc = write_date(out, us.div_euclid(USECS_PER_DAY) + POSTGRES_EPOCH_JDATE);
                    out.push(' ');
                    write_time(out, us.rem_euclid(USECS_PER_DAY));
                    if *p == Plan::TimestampTz {
                        out.push_str("+00");
                    }
                    if bc {
                        out.push_str(" BC");
                    }
                }
            }
        }
        Plan::Interval => write_interval(out, b)?,
        Plan::Array { elem, delim } => write_array(out, elem, *delim, b)?,
    }
    Ok(())
}

fn write_int(out: &mut String, v: impl std::fmt::Display) {
    let _ = write!(out, "{v}");
}

/// Shortest round-trip digits in Postgres's layout: fixed for decimal exponents in
/// `[-4, fixed_below)`, otherwise `d.ddde±XX`.
fn write_float(out: &mut String, nan: bool, neg: bool, inf: bool, sci: &str, fixed_below: i32) {
    if nan {
        out.push_str("NaN");
        return;
    }
    if inf {
        out.push_str(if neg { "-Infinity" } else { "Infinity" });
        return;
    }
    let sci = sci.strip_prefix('-').unwrap_or(sci);
    if neg {
        out.push('-');
    }
    let (mant, exp) = sci.split_once('e').unwrap_or((sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let n = digits.len() as i32;
    if (-4..fixed_below).contains(&exp) {
        if exp < 0 {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', (-exp - 1) as usize));
            out.push_str(&digits);
        } else if n <= exp + 1 {
            out.push_str(&digits);
            out.extend(std::iter::repeat_n('0', (exp + 1 - n) as usize));
        } else {
            let (a, b) = digits.split_at((exp + 1) as usize);
            out.push_str(a);
            out.push('.');
            out.push_str(b);
        }
    } else {
        let (a, b) = digits.split_at(1);
        out.push_str(a);
        if !b.is_empty() {
            out.push('.');
            out.push_str(b);
        }
        let _ = write!(out, "e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs());
    }
}

fn write_numeric(out: &mut String, b: &[u8]) -> Result<()> {
    const W: &str = "numeric";
    let mut r = Reader(b);
    let ndigits = r.i16(W)?;
    let weight = i32::from(r.i16(W)?);
    let sign = r.i16(W)? as u16;
    let dscale = r.i16(W)?;
    let digits = (0..ndigits.max(0))
        .map(|_| r.i16(W))
        .collect::<Result<Vec<_>>>()?;
    r.done(W)?;
    match sign {
        0xC000 => {
            out.push_str("NaN");
            return Ok(());
        }
        0xD000 => {
            out.push_str("Infinity");
            return Ok(());
        }
        0xF000 => {
            out.push_str("-Infinity");
            return Ok(());
        }
        0x4000 => out.push('-'),
        0x0000 => {}
        _ => return Err(bad(W)),
    }
    let digit = |i: i32| -> i16 {
        usize::try_from(i)
            .ok()
            .and_then(|i| digits.get(i).copied())
            .unwrap_or(0)
    };
    if weight < 0 {
        out.push('0');
    } else {
        for i in 0..=weight {
            if i == 0 {
                let _ = write!(out, "{}", digit(i));
            } else {
                let _ = write!(out, "{:04}", digit(i));
            }
        }
    }
    if dscale > 0 {
        out.push('.');
        let mut frac = String::new();
        let mut j = 1;
        while (frac.len() as i16) < dscale {
            let _ = write!(frac, "{:04}", digit(weight + j));
            j += 1;
        }
        frac.truncate(dscale as usize);
        out.push_str(&frac);
    }
    Ok(())
}

const POSTGRES_EPOCH_JDATE: i64 = 2_451_545;
const USECS_PER_DAY: i64 = 86_400_000_000;
const USECS_PER_HOUR: i64 = 3_600_000_000;
const USECS_PER_MINUTE: i64 = 60_000_000;
const USECS_PER_SEC: i64 = 1_000_000;

/// Postgres's `j2date`.
fn j2date(jd: i64) -> (i64, i64, i64) {
    let mut julian = jd + 32044;
    let mut quad = julian / 146_097;
    let extra = (julian - quad * 146_097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146_097;
    quad = julian / 1461;
    julian -= quad * 1461;
    let mut y = julian * 4 / 1461;
    julian = if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    } + 123;
    y += quad * 4;
    let quad = julian * 2141 / 65536;
    (y - 4800, (quad + 10) % 12 + 1, julian - 7834 * quad / 256)
}

/// Writes the ISO date and returns whether it is BC (the caller appends ` BC` last).
fn write_date(out: &mut String, jd: i64) -> bool {
    let (y, m, d) = j2date(jd);
    let bc = y <= 0;
    let _ = write!(out, "{:04}-{m:02}-{d:02}", if bc { 1 - y } else { y });
    bc
}

fn write_frac(out: &mut String, f: i64) {
    if f != 0 {
        let s = format!("{:06}", f.abs());
        out.push('.');
        out.push_str(s.trim_end_matches('0'));
    }
}

fn write_time(out: &mut String, us: i64) {
    let _ = write!(
        out,
        "{:02}:{:02}:{:02}",
        us / USECS_PER_HOUR,
        us % USECS_PER_HOUR / USECS_PER_MINUTE,
        us % USECS_PER_MINUTE / USECS_PER_SEC
    );
    write_frac(out, us % USECS_PER_SEC);
}

/// `zone` is seconds west of UTC, as Postgres stores it.
fn write_zone(out: &mut String, zone: i32) {
    let a = zone.unsigned_abs();
    let (h, m, s) = (a / 3600, a / 60 % 60, a % 60);
    let _ = write!(out, "{}{h:02}", if zone <= 0 { '+' } else { '-' });
    if s != 0 {
        let _ = write!(out, ":{m:02}:{s:02}");
    } else if m != 0 {
        let _ = write!(out, ":{m:02}");
    }
}

/// `IntervalStyle = postgres` (`EncodeInterval`).
fn write_interval(out: &mut String, b: &[u8]) -> Result<()> {
    const W: &str = "interval";
    let mut r = Reader(b);
    let time = r.i64(W)?;
    let day = r.i32(W)?;
    let month = r.i32(W)?;
    r.done(W)?;
    if (time, day, month) == (i64::MAX, i32::MAX, i32::MAX) {
        out.push_str("infinity");
        return Ok(());
    }
    if (time, day, month) == (i64::MIN, i32::MIN, i32::MIN) {
        out.push_str("-infinity");
        return Ok(());
    }
    let (mut is_zero, mut is_before) = (true, false);
    let mut part = |out: &mut String, v: i64, unit: &str| {
        if v == 0 {
            return;
        }
        let _ = write!(
            out,
            "{}{}{v} {unit}{}",
            if is_zero { "" } else { " " },
            if is_before && v > 0 { "+" } else { "" },
            if v != 1 { "s" } else { "" }
        );
        is_before = v < 0;
        is_zero = false;
    };
    part(out, i64::from(month / 12), "year");
    part(out, i64::from(month % 12), "mon");
    part(out, i64::from(day), "day");
    let hour = time / USECS_PER_HOUR;
    let min = time % USECS_PER_HOUR / USECS_PER_MINUTE;
    let sec = time % USECS_PER_MINUTE / USECS_PER_SEC;
    let fsec = time % USECS_PER_SEC;
    if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
        let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;
        let _ = write!(
            out,
            "{}{}{:02}:{:02}:{:02}",
            if is_zero { "" } else { " " },
            if minus {
                "-"
            } else if is_before {
                "+"
            } else {
                ""
            },
            hour.abs(),
            min.abs(),
            sec.abs()
        );
        write_frac(out, fsec);
    }
    Ok(())
}

/// `array_out`: nested braces, `[lo:hi]=` when a lower bound isn't 1, and quoting.
fn write_array(out: &mut String, elem: &Plan, delim: char, b: &[u8]) -> Result<()> {
    const W: &str = "array";
    let mut r = Reader(b);
    let ndim = r.i32(W)?;
    let _flags = r.i32(W)?;
    let _elem_oid = r.i32(W)?;
    if ndim == 0 {
        r.done(W)?;
        out.push_str("{}");
        return Ok(());
    }
    if !(1..=6).contains(&ndim) {
        return Err(bad(W));
    }
    let mut dims = Vec::with_capacity(ndim as usize);
    for _ in 0..ndim {
        let len = r.i32(W)?;
        let lb = r.i32(W)?;
        if len < 0 {
            return Err(bad(W));
        }
        dims.push((len, lb));
    }
    if dims.iter().any(|&(_, lb)| lb != 1) {
        for &(len, lb) in &dims {
            let _ = write!(out, "[{lb}:{}]", i64::from(lb) + i64::from(len) - 1);
        }
        out.push('=');
    }
    write_dim(out, elem, delim, &dims, &mut r)?;
    r.done(W)
}

fn write_dim(
    out: &mut String,
    elem: &Plan,
    delim: char,
    dims: &[(i32, i32)],
    r: &mut Reader<'_>,
) -> Result<()> {
    out.push('{');
    for i in 0..dims[0].0 {
        if i > 0 {
            out.push(delim);
        }
        if dims.len() > 1 {
            write_dim(out, elem, delim, &dims[1..], r)?;
            continue;
        }
        let len = r.i32("array")?;
        if len < 0 {
            out.push_str("NULL");
            continue;
        }
        let v = render(elem, r.take(len as usize, "array")?)?;
        let quote = v.is_empty()
            || v.eq_ignore_ascii_case("NULL")
            || v.chars().any(|c| {
                matches!(
                    c,
                    '"' | '\\' | '{' | '}' | ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c'
                ) || c == delim
            });
        if quote {
            out.push('"');
            for c in v.chars() {
                if c == '"' || c == '\\' {
                    out.push('\\');
                }
                out.push(c);
            }
            out.push('"');
        } else {
            out.push_str(&v);
        }
    }
    out.push('}');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(ndigits: i16, weight: i16, sign: u16, dscale: i16, digits: &[i16]) -> Vec<u8> {
        let mut b = Vec::new();
        for v in [ndigits, weight, sign as i16, dscale] {
            b.extend(v.to_be_bytes());
        }
        for d in digits {
            b.extend(d.to_be_bytes());
        }
        b
    }

    #[test]
    fn numerics() {
        let cases: &[(Vec<u8>, &str)] = &[
            (num(2, 0, 0, 3, &[12, 3450]), "12.345"),
            (num(1, -1, 0x4000, 3, &[5000]), "-0.500"),
            (num(0, 0, 0, 2, &[]), "0.00"),
            (num(1, 2, 0, 0, &[7]), "700000000"),
            (num(1, -2, 0, 6, &[1200]), "0.000012"),
            (num(0, 0, 0xC000, 0, &[]), "NaN"),
            (num(0, 0, 0xF000, 0, &[]), "-Infinity"),
        ];
        for (b, want) in cases {
            assert_eq!(render(&Plan::Numeric, b).expect("numeric"), *want);
        }
    }

    #[test]
    fn dates_and_times() {
        let date = |d: i32| render(&Plan::Date, &d.to_be_bytes()).expect("date");
        assert_eq!(date(0), "2000-01-01");
        assert_eq!(date(8796), "2024-01-31");
        assert_eq!(date(-730_120), "0001-12-31 BC");
        assert_eq!(date(i32::MAX), "infinity");
        let ts = |us: i64, p: &Plan| render(p, &us.to_be_bytes()).expect("ts");
        assert_eq!(ts(-1, &Plan::TimestampTz), "1999-12-31 23:59:59.999999+00");
        assert_eq!(
            ts(USECS_PER_HOUR + 500_000, &Plan::Timestamp),
            "2000-01-01 01:00:00.5"
        );
        let mut tz = (13 * USECS_PER_HOUR).to_be_bytes().to_vec();
        tz.extend((-(5 * 3600 + 30 * 60_i32)).to_be_bytes());
        assert_eq!(
            render(&Plan::TimeTz, &tz).expect("timetz"),
            "13:00:00+05:30"
        );
    }

    #[test]
    fn intervals() {
        let iv = |time: i64, day: i32, month: i32| {
            let mut b = time.to_be_bytes().to_vec();
            b.extend(day.to_be_bytes());
            b.extend(month.to_be_bytes());
            render(&Plan::Interval, &b).expect("interval")
        };
        assert_eq!(iv(0, 0, 0), "00:00:00");
        assert_eq!(iv(0, 3, 14), "1 year 2 mons 3 days");
        assert_eq!(
            iv(
                -(4 * USECS_PER_HOUR + 5 * USECS_PER_MINUTE + 6 * USECS_PER_SEC),
                3,
                -14
            ),
            "-1 years -2 mons +3 days -04:05:06"
        );
        assert_eq!(iv(1_500_000, 1, 0), "1 day 00:00:01.5");
    }

    #[test]
    fn floats() {
        let f8 = |v: f64| render(&Plan::Float8, &v.to_be_bytes()).expect("f8");
        assert_eq!(f8(1.5), "1.5");
        assert_eq!(f8(1e21), "1e+21");
        assert_eq!(f8(1.25e-7), "1.25e-07");
        assert_eq!(f8(0.0001), "0.0001");
        assert_eq!(f8(-0.0), "-0");
        assert_eq!(f8(f64::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn arrays_quote_like_array_out() {
        let mut b = Vec::new();
        for v in [1i32, 1, 25, 4, 1] {
            b.extend(v.to_be_bytes());
        }
        for s in ["a", "", "x y", "NULL"] {
            b.extend((s.len() as i32).to_be_bytes());
            b.extend(s.as_bytes());
        }
        let p = Plan::Array {
            elem: Box::new(Plan::Text),
            delim: ',',
        };
        assert_eq!(render(&p, &b).expect("array"), r#"{a,"","x y","NULL"}"#);
        assert!(render(&p, &b[..b.len() - 1]).is_err());
    }

    struct Raw(Vec<u8>);

    impl<'a> tokio_postgres::types::FromSql<'a> for Raw {
        fn from_sql(
            _: &tokio_postgres::types::Type,
            raw: &'a [u8],
        ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            Ok(Raw(raw.to_vec()))
        }

        fn accepts(_: &tokio_postgres::types::Type) -> bool {
            true
        }
    }

    /// Every decoder against the server's own text output (simple-query results use the type
    /// output functions) under the walsender's settings.
    async fn renders_like_the_server(pg: &atlatl_testkit::Postgres) {
        let db = pg.fresh_db().expect("db");
        let opts = atlatl_conn_postgres::ConnectOptions::new(
            &pg.host,
            pg.port,
            &db,
            &pg.user,
            Some(pg.password.clone()),
        );
        let sql = atlatl_conn_postgres::connect(&opts).await.expect("connect");
        let c = sql.client();
        c.batch_execute(
            "SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres';
             SET extra_float_digits = 3; SET bytea_output = 'hex';
             CREATE TYPE mood AS ENUM ('happy', 'sad', 'a b');
             CREATE DOMAIN posint AS int4 CHECK (VALUE > 0);",
        )
        .await
        .expect("setup");
        let version: i32 = c
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .await
            .expect("version")
            .get(0);
        let mut exprs = vec![
            "true",
            "false",
            "32767::int2",
            "(-2147483648)::int4",
            "9007199254740993::int8",
            "4294967295::oid",
            "1.5::float4",
            "123456::float4",
            "1234567::float4",
            "1e-5::float4",
            "0.0001::float4",
            "3.4028235e38::float4",
            "'NaN'::float4",
            "(-0.1)::float4",
            "1.5::float8",
            "1e14::float8",
            "1e15::float8",
            "123456789012345678::float8",
            "0.0001::float8",
            "0.00001::float8",
            "1.25e-7::float8",
            "'-0'::float8",
            "'Infinity'::float8",
            "'-Infinity'::float8",
            "5e-324::float8",
            "0.1::float8",
            "1.7976931348623157e308::float8",
            "(2^53)::float8",
            "12.345::numeric(12,3)",
            "(-0.5)::numeric(12,3)",
            "0::numeric",
            "0.00::numeric",
            "700000000::numeric",
            "0.000012::numeric",
            "'NaN'::numeric",
            "123456789012345678901234567890.123456789::numeric",
            "(-1e-20)::numeric",
            "'text'::text",
            "''::text",
            "'bp'::char(4)",
            "'v'::varchar(3)",
            "'n'::name",
            "'é 😀'::text",
            "'\\x0001ff'::bytea",
            "''::bytea",
            "'{\"k\": [1, 2]}'::json",
            "'{\"k\": [1, 2], \"a\": null}'::jsonb",
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
            "'2024-01-31'::date",
            "'0044-03-15 BC'::date",
            "'4713-01-01 BC'::date",
            "'infinity'::date",
            "'-infinity'::date",
            "'10000-01-01'::date",
            "'2000-02-29'::date",
            "'13:00:00.25'::time",
            "'24:00:00'::time",
            "'00:00:00.000001'::time",
            "'13:00:00+05:30'::timetz",
            "'13:00:00-08'::timetz",
            "'13:00:00+05:30:15'::timetz",
            "'2024-03-10 12:00:00.123456'::timestamp",
            "'0044-03-15 12:00 BC'::timestamp",
            "'infinity'::timestamp",
            "'1970-01-01 00:00:00'::timestamp",
            "'2024-03-10 12:00:00.123456+00'::timestamptz",
            "'1999-12-31 23:59:59.999999+00'::timestamptz",
            "'-infinity'::timestamptz",
            "'0044-03-15 12:00+00 BC'::timestamptz",
            "'294276-12-31 23:59:59+00'::timestamptz",
            "'1 year 2 mons 3 days 04:05:06.789'::interval",
            "'-1 years -2 mons +3 days -04:05:06'::interval",
            "'0'::interval",
            "'-00:00:01.5'::interval",
            "'100 hours'::interval",
            "'1 mon -1 day'::interval",
            "'-1 day +02:00'::interval",
            "'1 year -1 mon'::interval",
            "'-1 mon 1 day'::interval",
            "'1 day 1 microsecond'::interval",
            "ARRAY[1, NULL, 3]::int4[]",
            "ARRAY['a', '', 'x y', 'NULL', 'q\"t', 'b\\s', '{}', 'c,d']::text[]",
            "'{{1,2},{3,4}}'::int2[]",
            "'[0:1]={5,6}'::int4[]",
            "'{}'::int4[]",
            "ARRAY['2024-01-01 00:00:00+00']::timestamptz[]",
            "ARRAY[1.5, NULL]::numeric[]",
            "ARRAY[true, false]",
            "ARRAY['1 day'::interval]",
            "ARRAY['{\"a\": 1}'::jsonb]",
            "'happy'::mood",
            "ARRAY['happy', 'a b']::mood[]",
            "5::posint",
            "ARRAY[5]::posint[]",
        ];
        if version >= 140000 {
            exprs.extend(["'Infinity'::numeric", "'-Infinity'::numeric"]);
        }
        if version >= 170000 {
            exprs.extend(["'infinity'::interval", "'-infinity'::interval"]);
        }
        let mut plans = Plans::default();
        for e in exprs {
            let q = format!("SELECT {e}");
            let text = c
                .simple_query(&q)
                .await
                .expect("simple")
                .into_iter()
                .find_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{e}: no row"));
            let row = c.query_one(&q, &[]).await.expect("query");
            let oid = row.columns()[0].type_().oid();
            let missing = plans.resolve(&sql, &[oid]).await.expect("resolve");
            assert!(missing.is_empty(), "{e}: no plan for {oid}");
            let raw: Raw = row.get(0);
            let got = render(plans.get(oid).expect("plan"), &raw.0)
                .unwrap_or_else(|err| panic!("{e}: {err}"));
            assert_eq!(got, text, "{e} (oid {oid})");
        }
        for unsupported in [
            "'1.2.3.4'::inet",
            "'(1,2)'::point",
            "'1'::money",
            "'1 2'::int2vector",
        ] {
            let row = c
                .query_one(&format!("SELECT {unsupported}"), &[])
                .await
                .expect("q");
            let oid = row.columns()[0].type_().oid();
            assert_eq!(
                plans.resolve(&sql, &[oid]).await.expect("resolve"),
                [oid],
                "{unsupported}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renders_like_pg12() {
        renders_like_the_server(atlatl_testkit::pg12()).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renders_like_pg16() {
        renders_like_the_server(atlatl_testkit::pg16()).await
    }

    /// PG 14 is the first with binary pgoutput; 17 adds infinite intervals.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renders_like_pg14_and_pg17() {
        for v in [14, 17] {
            let pg = atlatl_testkit::Postgres::builder(v)
                .start()
                .unwrap_or_else(|e| panic!("postgres {v}: {e}"));
            renders_like_the_server(&pg).await
        }
    }
}
