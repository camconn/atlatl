//! Run ids (ULID) and UTC timestamps, without extra dependencies.

use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A 26-character ULID: 48-bit millisecond timestamp plus 80 random bits.
pub fn ulid_at(t: SystemTime, rng: &mut impl Rng) -> String {
    let ms = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() & ((1 << 48) - 1);
    let mut rand = [0u8; 10];
    rng.fill_bytes(&mut rand);
    let mut v: u128 = ms << 80;
    for (i, b) in rand.iter().enumerate() {
        v |= (*b as u128) << (8 * (9 - i));
    }
    (0..26)
        .rev()
        .map(|i| CROCKFORD[((v >> (5 * i)) & 31) as usize] as char)
        .collect()
}

pub fn ulid() -> String {
    ulid_at(SystemTime::now(), &mut rand::rng())
}

/// RFC 3339 in UTC with millisecond precision, e.g. `2026-09-28T12:00:00.000Z`.
pub fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60,
        d.subsec_millis()
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::SeedableRng;

    use super::*;

    #[test]
    fn ulid_shape_and_order() {
        let mut r = rand::rngs::StdRng::seed_from_u64(1);
        let a = ulid_at(
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_000),
            &mut r,
        );
        let b = ulid_at(
            UNIX_EPOCH + Duration::from_millis(1_700_000_000_001),
            &mut r,
        );
        assert_eq!(a.len(), 26);
        assert!(a.bytes().all(|c| CROCKFORD.contains(&c)));
        assert!(a < b, "lexicographic order follows time");
        assert_eq!(&a[..10], "01HF7YAT00");
    }

    #[test]
    fn timestamps() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123)),
            "2023-11-14T22:13:20.123Z"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(951_782_400)),
            "2000-02-29T00:00:00.000Z"
        );
    }
}
