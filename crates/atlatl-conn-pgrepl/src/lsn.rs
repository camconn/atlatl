use std::fmt;
use std::str::FromStr;

use atlatl_core::AtlatlError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A WAL position (`XLogRecPtr`), printed as Postgres does (`16/B374D848`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);
}

impl FromStr for Lsn {
    type Err = AtlatlError;

    fn from_str(s: &str) -> Result<Self, AtlatlError> {
        let bad = || AtlatlError::internal(format!("invalid LSN `{s}`"));
        let (hi, lo) = s.split_once('/').ok_or_else(bad)?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| bad())?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| bad())?;
        if hi > u32::MAX as u64 || lo > u32::MAX as u64 {
            return Err(bad());
        }
        Ok(Self((hi << 32) | lo))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Serialize for Lsn {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let l: Lsn = "16/B374D848".parse().expect("lsn");
        assert_eq!(l.0, 0x16_B374_D848);
        assert_eq!(l.to_string(), "16/B374D848");
        assert_eq!(Lsn::ZERO.to_string(), "0/0");
        assert!("x".parse::<Lsn>().is_err());
        assert!("1/2/3".parse::<Lsn>().is_err());
        assert!("100000000/0".parse::<Lsn>().is_err());
        let j = serde_json::to_string(&l).expect("ser");
        assert_eq!(j, "\"16/B374D848\"");
        assert_eq!(serde_json::from_str::<Lsn>(&j).expect("de"), l);
        assert!(Lsn(1) < Lsn(0x1_0000_0000));
    }
}
