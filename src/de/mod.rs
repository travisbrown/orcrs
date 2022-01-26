use serde::de::{Deserializer, Visitor};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowDeError {
    field: Option<u64>,
    kind: RowDeErrorKind,
}

impl std::fmt::Display for RowDeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(field) = self.field {
            write!(f, "field {}: {}", field, self.kind)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl std::error::Error for RowDeError {}

impl serde::de::Error for RowDeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self {
            field: None,
            kind: RowDeErrorKind::SerdeMessage(msg.to_string()),
        }
    }
}

#[derive(thiserror::Error, Clone, Debug, Eq, PartialEq)]
pub enum RowDeErrorKind {
    #[error("Unsupported target")]
    Unsupported(String),
    #[error("Serde error")]
    SerdeMessage(String),
    #[error("Unknown error")]
    Unknown,
}

struct RowDe {}

impl RowDe {
    fn error(&self, kind: RowDeErrorKind) -> RowDeError {
        RowDeError { field: None, kind }
    }
}

impl<'de> Deserializer<'de> for RowDe {
    type Error = RowDeError;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("any".to_string())))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("bool".to_string())))
    }

    fn deserialize_i8<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("i8".to_string())))
    }

    fn deserialize_i16<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("i16".to_string())))
    }

    fn deserialize_i32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("i32".to_string())))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("i64".to_string())))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("u8".to_string())))
    }

    fn deserialize_u16<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("u16".to_string())))
    }

    fn deserialize_u32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("u32".to_string())))
    }

    fn deserialize_u64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("u64".to_string())))
    }

    fn deserialize_f32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("f32".to_string())))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("f64".to_string())))
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("char".to_string())))
    }

    fn deserialize_str<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("str".to_string())))
    }

    fn deserialize_string<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("string".to_string())))
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("bytes".to_string())))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("byte_buf".to_string())))
    }

    fn deserialize_option<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("option".to_string())))
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("unit".to_string())))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("unit_struct".to_string())))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("newtype_struct".to_string())))
    }

    fn deserialize_seq<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("seq".to_string())))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("tuple".to_string())))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("tuple_struct".to_string())))
    }

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("map".to_string())))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("struct".to_string())))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("enum".to_string())))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("identifier".to_string())))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(RowDeErrorKind::Unsupported("ignored_any".to_string())))
    }
}
