//! `serde::Deserializer` over [`Node`]. Errors carry the span of the innermost node or key they
//! arose at. `Spanned<T>` captures a value's span; `Raw<T>` captures the node itself for
//! deferred (post-merge) deserialization.

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, DeserializeSeed, IntoDeserializer, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::diag::ConfigError;
use super::node::{Entry, Node, Span, Value};

const SPANNED: &str = "$__atlatl_spanned";
const RAW: &str = "$__atlatl_raw";

thread_local! {
    static CAPTURED_SPAN: RefCell<Option<Span>> = const { RefCell::new(None) };
    static CAPTURED_NODE: RefCell<Option<Node>> = const { RefCell::new(None) };
}

pub fn from_node<'a, T: Deserialize<'a>>(node: &'a Node) -> Result<T, ConfigError> {
    T::deserialize(NodeDe(node))
}

pub struct NodeDe<'a>(pub &'a Node);

impl NodeDe<'_> {
    fn err(&self, e: ConfigError) -> ConfigError {
        e.at(self.0.span.clone())
    }

    fn invalid<V>(&self, exp: &dyn de::Expected) -> Result<V, ConfigError> {
        let unexp = match &self.0.value {
            Value::Null => de::Unexpected::Unit,
            Value::Scalar(s) => de::Unexpected::Str(&s.text),
            Value::Seq(_) => de::Unexpected::Seq,
            Value::Map(_) => de::Unexpected::Map,
        };
        Err(self.err(de::Error::invalid_type(unexp, exp)))
    }
}

macro_rules! de_int {
    ($name:ident, $visit:ident, $ty:ty) => {
        fn $name<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
            match &self.0.value {
                Value::Scalar(s) => match s.as_i64().and_then(|i| <$ty>::try_from(i).ok()) {
                    Some(i) => v.$visit(i).map_err(|e| self.err(e)),
                    None => self.invalid(&v),
                },
                _ => self.invalid(&v),
            }
        }
    };
}

impl<'de> Deserializer<'de> for NodeDe<'_> {
    type Error = ConfigError;

    fn deserialize_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        let r = match &self.0.value {
            Value::Null => v.visit_unit(),
            Value::Scalar(s) => {
                if let Some(b) = s.as_bool() {
                    v.visit_bool(b)
                } else if let Some(i) = s.as_i64() {
                    v.visit_i64(i)
                } else if let Some(u) = s.as_u64() {
                    v.visit_u64(u)
                } else if let Some(f) = s.as_f64() {
                    v.visit_f64(f)
                } else {
                    v.visit_str(&s.text)
                }
            }
            Value::Seq(items) => v.visit_seq(SeqAccess(items.iter())),
            Value::Map(m) => v.visit_map(MapAccess {
                iter: m.iter(),
                value: None,
            }),
        };
        r.map_err(|e| self.err(e))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Scalar(s) => match s.as_bool() {
                Some(b) => v.visit_bool(b).map_err(|e| self.err(e)),
                None => self.invalid(&v),
            },
            _ => self.invalid(&v),
        }
    }

    de_int!(deserialize_i8, visit_i8, i8);
    de_int!(deserialize_i16, visit_i16, i16);
    de_int!(deserialize_i32, visit_i32, i32);
    de_int!(deserialize_i64, visit_i64, i64);
    de_int!(deserialize_u8, visit_u8, u8);
    de_int!(deserialize_u16, visit_u16, u16);
    de_int!(deserialize_u32, visit_u32, u32);

    fn deserialize_u64<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Scalar(s) => match s.as_u64() {
                Some(u) => v.visit_u64(u).map_err(|e| self.err(e)),
                None => self.invalid(&v),
            },
            _ => self.invalid(&v),
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_f64(v)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Scalar(s) => match s.as_f64() {
                Some(f) => v.visit_f64(f).map_err(|e| self.err(e)),
                None => self.invalid(&v),
            },
            _ => self.invalid(&v),
        }
    }

    fn deserialize_char<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_str(v)
    }

    fn deserialize_str<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Scalar(s) => v.visit_str(&s.text).map_err(|e| self.err(e)),
            _ => self.invalid(&v),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_str(v)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_str(v)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_str(v)
    }

    fn deserialize_option<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Null => v.visit_none().map_err(|e| self.err(e)),
            _ => v.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Null => v.visit_unit().map_err(|e| self.err(e)),
            _ => self.invalid(&v),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        v: V,
    ) -> Result<V::Value, ConfigError> {
        self.deserialize_unit(v)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        v: V,
    ) -> Result<V::Value, ConfigError> {
        v.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Seq(items) => v
                .visit_seq(SeqAccess(items.iter()))
                .map_err(|e| self.err(e)),
            _ => self.invalid(&v),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_seq(v)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        v: V,
    ) -> Result<V::Value, ConfigError> {
        self.deserialize_seq(v)
    }

    fn deserialize_map<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Map(m) => v
                .visit_map(MapAccess {
                    iter: m.iter(),
                    value: None,
                })
                .map_err(|e| self.err(e)),
            _ => self.invalid(&v),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, ConfigError> {
        match name {
            SPANNED => {
                CAPTURED_SPAN.with(|c| *c.borrow_mut() = Some(self.0.span.clone()));
                v.visit_newtype_struct(self)
            }
            RAW => {
                CAPTURED_NODE.with(|c| *c.borrow_mut() = Some(self.0.clone()));
                v.visit_unit()
            }
            _ => {
                let _ = fields;
                self.deserialize_map(v)
            }
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, ConfigError> {
        match &self.0.value {
            Value::Scalar(s) => v
                .visit_enum(s.text.as_str().into_deserializer())
                .map_err(|e: ConfigError| self.err(e)),
            Value::Map(m) if m.len() == 1 => {
                let (k, e) = m.iter().next().expect("len 1");
                v.visit_enum(EnumAccess { key: k, entry: e })
                    .map_err(|e| self.err(e))
            }
            _ => self.invalid(&v),
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_str(v)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, ConfigError> {
        v.visit_unit()
    }
}

struct SeqAccess<'a>(std::slice::Iter<'a, Node>);

impl<'de> de::SeqAccess<'de> for SeqAccess<'_> {
    type Error = ConfigError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, ConfigError> {
        match self.0.next() {
            Some(n) => seed
                .deserialize(NodeDe(n))
                .map(Some)
                .map_err(|e| e.at(n.span.clone())),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}

struct MapAccess<'a> {
    iter: indexmap::map::Iter<'a, String, Entry>,
    value: Option<&'a Entry>,
}

impl<'de> de::MapAccess<'de> for MapAccess<'_> {
    type Error = ConfigError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, ConfigError> {
        match self.iter.next() {
            Some((k, e)) => {
                self.value = Some(e);
                seed.deserialize(k.as_str().into_deserializer())
                    .map(Some)
                    .map_err(|err: ConfigError| err.at(e.key_span.clone()))
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, ConfigError> {
        let e = self
            .value
            .take()
            .ok_or_else(|| ConfigError::new("value without key"))?;
        seed.deserialize(NodeDe(&e.value))
            .map_err(|err| err.at(e.value.span.clone()))
    }
}

struct EnumAccess<'a> {
    key: &'a str,
    entry: &'a Entry,
}

impl<'de, 'a> de::EnumAccess<'de> for EnumAccess<'a> {
    type Error = ConfigError;
    type Variant = NodeDe<'a>;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, NodeDe<'a>), ConfigError> {
        let v = seed
            .deserialize(self.key.into_deserializer())
            .map_err(|e: ConfigError| e.at(self.entry.key_span.clone()))?;
        Ok((v, NodeDe(&self.entry.value)))
    }
}

impl<'de> de::VariantAccess<'de> for NodeDe<'_> {
    type Error = ConfigError;

    fn unit_variant(self) -> Result<(), ConfigError> {
        if self.0.is_null() {
            Ok(())
        } else {
            Err(ConfigError::new("expected no value for this variant").at(self.0.span.clone()))
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, ConfigError> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, v: V) -> Result<V::Value, ConfigError> {
        self.deserialize_seq(v)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, ConfigError> {
        self.deserialize_map(v)
    }
}

/// A value plus the span it was read from.
#[derive(Clone, Debug)]
pub struct Spanned<T> {
    pub value: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }
}

impl<T> std::ops::Deref for Spanned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: PartialEq> PartialEq for Spanned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: fmt::Display> fmt::Display for Spanned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<T: Serialize> Serialize for Spanned<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(s)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Spanned<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
            type Value = Spanned<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a spanned value")
            }
            fn visit_newtype_struct<D: Deserializer<'de>>(
                self,
                d: D,
            ) -> Result<Self::Value, D::Error> {
                let span = CAPTURED_SPAN
                    .with(|c| c.borrow_mut().take())
                    .ok_or_else(|| {
                        de::Error::custom("Spanned<T> requires the atlatl node deserializer")
                    })?;
                Ok(Spanned {
                    value: T::deserialize(d)?,
                    span,
                })
            }
        }
        d.deserialize_struct(SPANNED, &[], V(PhantomData))
    }
}

impl<T: schemars::JsonSchema> schemars::JsonSchema for Spanned<T> {
    fn inline_schema() -> bool {
        true
    }
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }
    fn json_schema(g: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(g)
    }
}

/// The raw node, to be deserialized as `T` after merging.
#[derive(Clone, Debug)]
pub struct Raw<T> {
    pub node: Node,
    _t: PhantomData<fn() -> T>,
}

impl<T> Raw<T> {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            _t: PhantomData,
        }
    }
}

impl<'de, T> Deserialize<'de> for Raw<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(PhantomData<T>);
        impl<'de, T> Visitor<'de> for V<T> {
            type Value = Raw<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a config node")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                let node = CAPTURED_NODE
                    .with(|c| c.borrow_mut().take())
                    .ok_or_else(|| E::custom("Raw<T> requires the atlatl node deserializer"))?;
                Ok(Raw::new(node))
            }
        }
        d.deserialize_struct(RAW, &[], V(PhantomData))
    }
}

impl<T: schemars::JsonSchema> schemars::JsonSchema for Raw<T> {
    fn inline_schema() -> bool {
        T::inline_schema()
    }
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }
    fn json_schema(g: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(g)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::config::node::parse_yaml;

    fn node(s: &str) -> Node {
        parse_yaml(s, Arc::from(Path::new("t.yaml"))).expect("yaml")
    }

    #[derive(Deserialize, Debug)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct S {
        name: String,
        count: Option<u32>,
        inner: Option<Inner>,
        mode: Option<Mode>,
        tag: Option<Spanned<String>>,
    }

    #[derive(Deserialize, Debug)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct Inner {
        flag: bool,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    enum Mode {
        FullRefresh,
        Incremental,
    }

    #[test]
    fn unknown_field_points_at_key() {
        let n = node("name: a\ninner:\n  flag: true\n  flga: 1\n");
        let e = from_node::<S>(&n).expect_err("unknown");
        assert!(e.message.contains("unknown field `flga`"), "{e}");
        let s = e.span.expect("span");
        assert_eq!((s.line, s.col), (4, 3));
    }

    #[test]
    fn type_errors_point_at_value() {
        let n = node("name: a\ncount: lots\n");
        let e = from_node::<S>(&n).expect_err("type");
        assert_eq!(e.span.as_ref().map(|s| (s.line, s.col)), Some((2, 8)));
        assert!(e.message.contains("invalid type"), "{e}");
    }

    #[test]
    fn missing_field_points_at_map() {
        let n = node("x:\n  count: 1\n");
        #[derive(Deserialize, Debug)]
        #[allow(dead_code)]
        struct W {
            x: S,
        }
        let e = from_node::<W>(&n).expect_err("missing");
        assert!(e.message.contains("missing field `name`"), "{e}");
        assert_eq!(e.span.map(|s| s.line), Some(2));
    }

    #[test]
    fn enums_and_spanned() {
        let n = node("name: 1234\nmode: full-refresh\ntag: hello\n");
        let s = from_node::<S>(&n).expect("ok");
        assert_eq!(s.name, "1234");
        assert_eq!(s.mode, Some(Mode::FullRefresh));
        let tag = s.tag.expect("tag");
        assert_eq!((tag.value.as_str(), tag.span.line), ("hello", 3));
        let e = from_node::<S>(&node("name: a\nmode: fullrefresh\n")).expect_err("variant");
        assert!(e.message.contains("unknown variant"), "{e}");
        assert_eq!(e.span.map(|s| s.line), Some(2));
    }

    #[test]
    fn quoted_numbers_are_not_ints() {
        let e = from_node::<S>(&node("name: a\ncount: \"3\"\n")).expect_err("quoted");
        assert!(e.message.contains("invalid type"), "{e}");
    }

    #[test]
    fn raw_captures_node() {
        #[derive(Deserialize)]
        struct W {
            later: Raw<Inner>,
        }
        let w = from_node::<W>(&node("later:\n  flag: true\n")).expect("ok");
        assert_eq!(w.later.node.span.line, 2);
        let inner: Inner = from_node(&w.later.node).expect("inner");
        assert!(inner.flag);
    }
}
