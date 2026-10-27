//! Binary result values rendered as the text DuckDB casts back. Redshift's text format rounds
//! `float8` computed on compute nodes to 15 digits whatever `extra_float_digits` says; the
//! binary format carries the exact value.

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use chrono::{Duration, NaiveDate, NaiveTime};
use tokio_postgres::types::{FromSql, Type};

/// Any column's raw binary value.
pub struct Raw<'a>(pub &'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(raw))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

fn bad(ty: &Type, what: &str) -> AtlatlError {
    AtlatlError::new(
        ErrorClass::Decode,
        format!(
            "Redshift export: malformed binary `{}` value ({what})",
            ty.name()
        ),
    )
}

fn be<const N: usize>(ty: &Type, raw: &[u8], at: usize) -> Result<[u8; N]> {
    raw.get(at..at + N)
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| bad(ty, "too short"))
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or_default()
}

fn float(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else {
        // Shortest representation that round-trips.
        format!("{v:e}")
    }
}

fn timestamp(ty: &Type, us: i64) -> Result<String> {
    match us {
        i64::MAX => return Ok("infinity".into()),
        i64::MIN => return Ok("-infinity".into()),
        _ => {}
    }
    let t = epoch()
        .and_hms_opt(0, 0, 0)
        .and_then(|e| e.checked_add_signed(Duration::microseconds(us)))
        .ok_or_else(|| bad(ty, "out of range"))?;
    Ok(t.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

fn time_of_day(ty: &Type, us: i64) -> Result<String> {
    if !(0..=MICROS_PER_DAY).contains(&us) {
        return Err(bad(ty, "out of range"));
    }
    if us == MICROS_PER_DAY {
        return Ok("24:00:00".into());
    }
    let t = NaiveTime::from_num_seconds_from_midnight_opt(
        (us / 1_000_000) as u32,
        ((us % 1_000_000) * 1000) as u32,
    )
    .ok_or_else(|| bad(ty, "out of range"))?;
    Ok(t.format("%H:%M:%S%.6f").to_string())
}

/// Postgres binary `numeric`: ndigits, weight, sign, dscale, then base-10000 digits.
fn numeric(ty: &Type, raw: &[u8]) -> Result<String> {
    let ndigits = i16::from_be_bytes(be(ty, raw, 0)?).max(0) as usize;
    let weight = i16::from_be_bytes(be(ty, raw, 2)?) as i64;
    let sign = u16::from_be_bytes(be(ty, raw, 4)?);
    let dscale = u16::from_be_bytes(be(ty, raw, 6)?) as usize;
    match sign {
        0xC000 => return Ok("NaN".into()),
        0xD000 => return Ok("Infinity".into()),
        0xF000 => return Ok("-Infinity".into()),
        0x0000 | 0x4000 => {}
        _ => return Err(bad(ty, "sign")),
    }
    let digits: Vec<i64> = (0..ndigits)
        .map(|i| Ok(i16::from_be_bytes(be(ty, raw, 8 + 2 * i)?) as i64))
        .collect::<Result<_>>()?;
    let digit = |pos: i64| -> i64 {
        // `pos` counts base-10000 groups from the most significant (`weight`).
        let i = weight - pos;
        if i < 0 || i as usize >= digits.len() {
            0
        } else {
            digits[i as usize]
        }
    };
    let mut int = String::new();
    for pos in (0..=weight.max(0)).rev() {
        if int.is_empty() {
            let d = digit(pos);
            if d != 0 || pos == 0 {
                int.push_str(&d.to_string());
            }
        } else {
            int.push_str(&format!("{:04}", digit(pos)));
        }
    }
    let mut frac = String::new();
    let mut pos = -1i64;
    while frac.len() < dscale {
        frac.push_str(&format!("{:04}", digit(pos)));
        pos -= 1;
    }
    frac.truncate(dscale);
    let negative = sign == 0x4000 && digits.iter().any(|d| *d != 0);
    Ok(format!(
        "{}{int}{}{frac}",
        if negative { "-" } else { "" },
        if dscale > 0 { "." } else { "" }
    ))
}

/// The value as text DuckDB casts to the column's type.
pub fn render(ty: &Type, raw: &[u8]) -> Result<String> {
    Ok(match *ty {
        Type::BOOL => (if raw.first() == Some(&1) { "t" } else { "f" }).into(),
        Type::INT2 => i16::from_be_bytes(be(ty, raw, 0)?).to_string(),
        Type::INT4 => i32::from_be_bytes(be(ty, raw, 0)?).to_string(),
        Type::INT8 => i64::from_be_bytes(be(ty, raw, 0)?).to_string(),
        Type::FLOAT4 => match f32::from_be_bytes(be(ty, raw, 0)?) {
            // At f32's own shortest precision, not widened.
            v if v.is_finite() => format!("{v:e}"),
            v => float(v as f64),
        },
        Type::FLOAT8 => float(f64::from_be_bytes(be(ty, raw, 0)?)),
        Type::NUMERIC => numeric(ty, raw)?,
        Type::DATE => match i32::from_be_bytes(be(ty, raw, 0)?) {
            i32::MAX => "infinity".into(),
            i32::MIN => "-infinity".into(),
            d => epoch()
                .checked_add_signed(Duration::days(d as i64))
                .ok_or_else(|| bad(ty, "out of range"))?
                .format("%Y-%m-%d")
                .to_string(),
        },
        Type::TIMESTAMP => timestamp(ty, i64::from_be_bytes(be(ty, raw, 0)?))?,
        Type::TIMESTAMPTZ => {
            let t = timestamp(ty, i64::from_be_bytes(be(ty, raw, 0)?))?;
            if t.ends_with("infinity") { t } else { format!("{t}+00") }
        }
        Type::TIME => time_of_day(ty, i64::from_be_bytes(be(ty, raw, 0)?))?,
        Type::TIMETZ => {
            let t = time_of_day(ty, i64::from_be_bytes(be(ty, raw, 0)?))?;
            // Seconds west of UTC.
            let west = i32::from_be_bytes(be(ty, raw, 8)?);
            let east = -west;
            let (s, a) = (if east < 0 { '-' } else { '+' }, east.unsigned_abs());
            format!("{t}{s}{:02}:{:02}:{:02}", a / 3600, a / 60 % 60, a % 60)
        }
        _ => std::str::from_utf8(raw)
            .map_err(|_| {
                AtlatlError::config(format!(
                    "Redshift export: `{}` values aren't text in the binary format; exclude the column",
                    ty.name()
                ))
            })?
            .to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut v = Vec::new();
        for x in [
            ndigits.to_be_bytes(),
            weight.to_be_bytes(),
            sign.to_be_bytes(),
            dscale.to_be_bytes(),
        ] {
            v.extend(x);
        }
        for d in digits {
            v.extend(d.to_be_bytes());
        }
        v
    }

    #[test]
    fn numerics() {
        let n = |b: Vec<u8>| numeric(&Type::NUMERIC, &b).expect("numeric");
        // 12345678901234567890.1234567890
        assert_eq!(
            n(num(
                8,
                4,
                0,
                10,
                &[1234, 5678, 9012, 3456, 7890, 1234, 5678, 9000]
            )),
            "12345678901234567890.1234567890"
        );
        assert_eq!(n(num(2, 0, 0x4000, 4, &[1, 2345])), "-1.2345");
        assert_eq!(n(num(1, -1, 0x4000, 4, &[1])), "-0.0001");
        assert_eq!(n(num(0, 0, 0, 2, &[])), "0.00");
        assert_eq!(n(num(1, 1, 0, 0, &[5])), "50000");
        assert_eq!(n(num(1, 0, 0, 0, &[7])), "7");
        assert_eq!(n(num(0, 0, 0xC000, 0, &[])), "NaN");
        assert!(numeric(&Type::NUMERIC, &[0, 1]).is_err());
    }

    #[test]
    fn scalars() {
        let r = |t: &Type, b: &[u8]| render(t, b).expect("render");
        assert_eq!(
            r(&Type::FLOAT8, &2.3338936666666665f64.to_be_bytes()),
            "2.3338936666666665e0"
        );
        assert_eq!(r(&Type::FLOAT8, &1e-300f64.to_be_bytes()), "1e-300");
        assert_eq!(r(&Type::FLOAT8, &f64::NAN.to_be_bytes()), "NaN");
        assert_eq!(
            r(&Type::FLOAT8, &f64::NEG_INFINITY.to_be_bytes()),
            "-Infinity"
        );
        assert_eq!(r(&Type::FLOAT4, &0.1f32.to_be_bytes()), "1e-1");
        assert_eq!(r(&Type::BOOL, &[1]), "t");
        assert_eq!(r(&Type::INT8, &(-5i64).to_be_bytes()), "-5");
        assert_eq!(r(&Type::DATE, &(-1i32).to_be_bytes()), "1999-12-31");
        let us = 3_600_000_000i64 + 123_456;
        assert_eq!(
            r(&Type::TIMESTAMP, &us.to_be_bytes()),
            "2000-01-01 01:00:00.123456"
        );
        assert_eq!(
            r(&Type::TIMESTAMPTZ, &us.to_be_bytes()),
            "2000-01-01 01:00:00.123456+00"
        );
        assert_eq!(r(&Type::TIME, &us.to_be_bytes()), "01:00:00.123456");
        let mut tz = us.to_be_bytes().to_vec();
        tz.extend((-19800i32).to_be_bytes());
        assert_eq!(r(&Type::TIMETZ, &tz), "01:00:00.123456+05:30:00");
        assert_eq!(r(&Type::VARCHAR, b"it's"), "it's");
        assert!(render(&Type::BYTEA, &[0xff, 0xfe]).is_err());
        assert!(render(&Type::INT8, &[0]).is_err());
    }
}
