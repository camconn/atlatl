//! Small value types shared by the config model: durations, byte sizes, `auto`, secrets.

use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `250ms`, `2s`, `5m`, `2h`, `3d`, or `0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Dur(pub Duration);

impl Dur {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s == "0" {
            return Ok(Dur(Duration::ZERO));
        }
        let split = s
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or_else(|| format!("invalid duration `{s}`: missing unit (ms, s, m, h, d)"))?;
        let (num, unit) = s.split_at(split);
        let n: f64 = num.parse().map_err(|_| format!("invalid duration `{s}`"))?;
        let secs = match unit.trim() {
            "ms" => n / 1000.0,
            "s" => n,
            "m" | "min" => n * 60.0,
            "h" => n * 3600.0,
            "d" => n * 86400.0,
            u => {
                return Err(format!(
                    "invalid duration unit `{u}` in `{s}` (ms, s, m, h, d)"
                ));
            }
        };
        Ok(Dur(Duration::from_secs_f64(secs)))
    }
}

impl fmt::Display for Dur {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = self.0.as_millis();
        if ms == 0 {
            f.write_str("0")
        } else if ms.is_multiple_of(86_400_000) {
            write!(f, "{}d", ms / 86_400_000)
        } else if ms.is_multiple_of(3_600_000) {
            write!(f, "{}h", ms / 3_600_000)
        } else if ms.is_multiple_of(60_000) {
            write!(f, "{}m", ms / 60_000)
        } else if ms.is_multiple_of(1000) {
            write!(f, "{}s", ms / 1000)
        } else {
            write!(f, "{ms}ms")
        }
    }
}

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Dur;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration such as `2s`, `5m` or `250ms`")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<Dur, E> {
                Dur::parse(s).map_err(E::custom)
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Dur, E> {
                if v == 0 {
                    Ok(Dur(Duration::ZERO))
                } else {
                    Err(E::custom("a non-zero duration needs a unit, e.g. `30s`"))
                }
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Dur, E> {
                self.visit_i64(v as i64)
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for Dur {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl JsonSchema for Dur {
    fn schema_name() -> Cow<'static, str> {
        "Duration".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "oneOf": [
                {"type": "string", "pattern": "^[0-9]+(\\.[0-9]+)?(ms|s|m|min|h|d)$"},
                {"const": 0}
            ],
            "description": "Duration: `250ms`, `2s`, `5m`, `2h`, `3d`, or 0"
        })
    }
}

/// `256MB`, `2GB`, `50GiB`. Decimal units are powers of 1000, `*iB` powers of 1024.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let split = s
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(s.len());
        let (num, unit) = s.split_at(split);
        let n: f64 = num.parse().map_err(|_| format!("invalid size `{s}`"))?;
        let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "kb" => 1000,
            "mb" => 1000u64.pow(2),
            "gb" => 1000u64.pow(3),
            "tb" => 1000u64.pow(4),
            "kib" => 1 << 10,
            "mib" => 1 << 20,
            "gib" => 1 << 30,
            "tib" => 1 << 40,
            u => return Err(format!("invalid size unit `{u}` in `{s}`")),
        };
        Ok(ByteSize((n * mult as f64).round() as u64))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}B", self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = ByteSize;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a size such as `256MB` or `2GiB`")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<ByteSize, E> {
                ByteSize::parse(s).map_err(E::custom)
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| E::custom("size must be positive"))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(self.0)
    }
}

impl JsonSchema for ByteSize {
    fn schema_name() -> Cow<'static, str> {
        "ByteSize".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "oneOf": [
                {"type": "string", "pattern": "^[0-9]+(\\.[0-9]+)?\\s*([KMGT]i?B|B)?$"},
                {"type": "integer", "minimum": 0}
            ]
        })
    }
}

/// `auto` or an explicit value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Auto<T> {
    #[default]
    Auto,
    Value(T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Auto<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
            type Value = Auto<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`auto` or a value")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                if s == "auto" {
                    return Ok(Auto::Auto);
                }
                T::deserialize(de::value::StrDeserializer::<E>::new(s)).map(Auto::Value)
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                T::deserialize(de::value::U64Deserializer::<E>::new(v)).map(Auto::Value)
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                T::deserialize(de::value::I64Deserializer::<E>::new(v)).map(Auto::Value)
            }
        }
        d.deserialize_any(V(std::marker::PhantomData))
    }
}

impl<T: Serialize> Serialize for Auto<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Auto::Auto => s.serialize_str("auto"),
            Auto::Value(v) => v.serialize(s),
        }
    }
}

impl<T: JsonSchema> JsonSchema for Auto<T> {
    fn schema_name() -> Cow<'static, str> {
        format!("Auto_{}", T::schema_name()).into()
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        let inner = g.subschema_for::<T>();
        json_schema!({"oneOf": [{"const": "auto"}, inner]})
    }
}

/// A secret-bearing value: a literal (lint warning), a template, or `{secret: <ref>}`.
#[derive(Clone, Debug, PartialEq)]
pub enum SecretValue {
    Literal(String),
    Ref(SecretRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SecretRef {
    Env(String),
    File(String),
    AwsSecretsManager {
        arn: String,
        json_key: Option<String>,
    },
}

impl SecretRef {
    pub fn parse(s: &str) -> Result<Self, String> {
        let (scheme, rest) = s
            .split_once(':')
            .ok_or_else(|| format!("invalid secret reference `{s}`: expected `env:NAME`, `file:/path` or `aws-sm:<arn>`"))?;
        if rest.is_empty() {
            return Err(format!("invalid secret reference `{s}`: empty target"));
        }
        match scheme {
            "env" => Ok(SecretRef::Env(rest.to_owned())),
            "file" => Ok(SecretRef::File(rest.to_owned())),
            "aws-sm" => {
                let (arn, key) = match rest.split_once('#') {
                    Some((a, k)) => (a.to_owned(), Some(k.to_owned())),
                    None => (rest.to_owned(), None),
                };
                Ok(SecretRef::AwsSecretsManager { arn, json_key: key })
            }
            other => Err(format!(
                "unknown secret scheme `{other}:` (supported: env, file, aws-sm)"
            )),
        }
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretRef::Env(n) => write!(f, "env:{n}"),
            SecretRef::File(p) => write!(f, "file:{p}"),
            SecretRef::AwsSecretsManager {
                arn,
                json_key: None,
            } => write!(f, "aws-sm:{arn}"),
            SecretRef::AwsSecretsManager {
                arn,
                json_key: Some(k),
            } => write!(f, "aws-sm:{arn}#{k}"),
        }
    }
}

impl<'de> Deserialize<'de> for SecretValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = SecretValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string or `{secret: env:NAME | file:/path | aws-sm:<arn>}`")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<SecretValue, E> {
                Ok(SecretValue::Literal(s.to_owned()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<SecretValue, E> {
                Ok(SecretValue::Literal(v.to_string()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<SecretValue, E> {
                Ok(SecretValue::Literal(v.to_string()))
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut m: A) -> Result<SecretValue, A::Error> {
                let mut r = None;
                while let Some(k) = m.next_key::<String>()? {
                    if k != "secret" {
                        return Err(de::Error::unknown_field(&k, &["secret"]));
                    }
                    let s: String = m.next_value()?;
                    r = Some(SecretRef::parse(&s).map_err(de::Error::custom)?);
                }
                r.map(SecretValue::Ref)
                    .ok_or_else(|| de::Error::missing_field("secret"))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for SecretValue {
    /// Never serializes a literal; references are safe to show.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            SecretValue::Literal(_) => s.serialize_str("****"),
            SecretValue::Ref(r) => s.collect_str(&format_args!("{{secret: {r}}}")),
        }
    }
}

impl JsonSchema for SecretValue {
    fn schema_name() -> Cow<'static, str> {
        "SecretValue".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "oneOf": [
                {"type": "string", "description": "literal (discouraged) or template"},
                {
                    "type": "object",
                    "properties": {"secret": {"type": "string", "pattern": "^(env|file|aws-sm):.+$"}},
                    "required": ["secret"],
                    "additionalProperties": false
                }
            ]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(
            Dur::parse("250ms").map(|d| d.0),
            Ok(Duration::from_millis(250))
        );
        assert_eq!(Dur::parse("5m").map(|d| d.0), Ok(Duration::from_secs(300)));
        assert_eq!(
            Dur::parse("1.5h").map(|d| d.0),
            Ok(Duration::from_secs(5400))
        );
        assert!(Dur::parse("5").is_err());
        assert!(Dur::parse("5y").is_err());
        assert_eq!(Dur(Duration::from_secs(300)).to_string(), "5m");
        assert_eq!(Dur(Duration::from_millis(1500)).to_string(), "1500ms");
    }

    #[test]
    fn sizes() {
        assert_eq!(ByteSize::parse("256MB"), Ok(ByteSize(256_000_000)));
        assert_eq!(ByteSize::parse("2GiB"), Ok(ByteSize(2 << 30)));
        assert_eq!(ByteSize::parse("50 GB"), Ok(ByteSize(50_000_000_000)));
        assert!(ByteSize::parse("5 parsecs").is_err());
    }

    #[test]
    fn secret_refs() {
        assert_eq!(SecretRef::parse("env:X"), Ok(SecretRef::Env("X".into())));
        assert_eq!(
            SecretRef::parse("aws-sm:arn:aws:secretsmanager:us-east-1:1:secret:x#key"),
            Ok(SecretRef::AwsSecretsManager {
                arn: "arn:aws:secretsmanager:us-east-1:1:secret:x".into(),
                json_key: Some("key".into())
            })
        );
        assert!(SecretRef::parse("vault:x").is_err());
        assert!(SecretRef::parse("env:").is_err());
    }
}
