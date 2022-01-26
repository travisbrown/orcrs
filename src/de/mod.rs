use crate::value::Value;
use serde::de::{Deserialize, DeserializeSeed, Deserializer, SeqAccess, Visitor};

pub(crate) fn get_field_names<'de, T: Deserialize<'de>>() -> &'static [&'static str] {
    serde_aux::serde_introspection::serde_introspect::<T>()
}

#[derive(Debug)]
pub struct Error {
    field: Option<usize>,
    kind: ErrorKind,
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { field: None, kind }
    }
}

impl From<crate::parser::Error> for Error {
    fn from(error: crate::parser::Error) -> Self {
        Self {
            field: None,
            kind: ErrorKind::Parser(error),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(field) = self.field {
            write!(f, "field {}: {}", field, self.kind)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self {
            field: None,
            kind: ErrorKind::SerdeMessage(msg.to_string()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorKind {
    #[error("Unsupported target")]
    Unsupported(String),
    #[error("Invalid field names")]
    InvalidFieldNames(Vec<String>),
    #[error("Serde error")]
    SerdeMessage(String),
    #[error("Invalid column")]
    InvalidColumn,
    #[error("Invalid value")]
    InvalidValue,
    #[error("Parser error")]
    Parser(crate::parser::Error),
}

pub(crate) struct RowDe<'a> {
    row: &'a [Value<'a>],
    current_field: usize,
}

impl<'a> RowDe<'a> {
    pub(crate) fn new(row: &'a [Value<'a>]) -> Self {
        Self {
            row,
            current_field: 0,
        }
    }

    fn error(&self, kind: ErrorKind) -> Error {
        Error {
            field: Some(self.current_field),
            kind,
        }
    }
}

impl<'a, 'de: 'a> SeqAccess<'de> for &mut RowDe<'a> {
    type Error = Error;

    fn next_element_seed<U: DeserializeSeed<'de>>(
        &mut self,
        seed: U,
    ) -> Result<Option<U::Value>, Self::Error> {
        if self.current_field == self.row.len() {
            Ok(None)
        } else {
            seed.deserialize(&mut **self).map(Some)
        }
    }
}

impl<'a, 'de: 'a> Deserializer<'de> for &mut RowDe<'a> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("any".to_string())))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self
            .row
            .get(self.current_field)
            .and_then(|value| value.as_bool())
        {
            Some(value) => {
                self.current_field += 1;
                visitor.visit_bool(value)
            }
            None => Err(self.error(ErrorKind::InvalidValue)),
        }
    }

    fn deserialize_i8<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("i8".to_string())))
    }

    fn deserialize_i16<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("i16".to_string())))
    }

    fn deserialize_i32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("i32".to_string())))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("i64".to_string())))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("u8".to_string())))
    }

    fn deserialize_u16<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("u16".to_string())))
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self
            .row
            .get(self.current_field)
            .and_then(|value| value.as_u64())
            .and_then(|value| u32::try_from(value).ok())
        {
            Some(value) => {
                self.current_field += 1;
                visitor.visit_u32(value)
            }
            None => Err(self.error(ErrorKind::InvalidValue)),
        }
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self
            .row
            .get(self.current_field)
            .and_then(|value| value.as_u64())
        {
            Some(value) => {
                self.current_field += 1;
                visitor.visit_u64(value)
            }
            None => Err(self.error(ErrorKind::InvalidValue)),
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("f32".to_string())))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("f64".to_string())))
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("char".to_string())))
    }

    fn deserialize_str<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("str".to_string())))
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self
            .row
            .get(self.current_field)
            .and_then(|value| value.as_string())
        {
            Some(value) => {
                self.current_field += 1;
                visitor.visit_string(value)
            }
            None => Err(self.error(ErrorKind::InvalidValue)),
        }
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("bytes".to_string())))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("byte_buf".to_string())))
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.get(self.current_field) {
            Some(Value::Null) => {
                self.current_field += 1;
                visitor.visit_none()
            }
            Some(_) => visitor.visit_some(self),
            None => Err(self.error(ErrorKind::InvalidValue)),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("unit".to_string())))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("unit_struct".to_string())))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("newtype_struct".to_string())))
    }

    fn deserialize_seq<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("seq".to_string())))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("tuple".to_string())))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("tuple_struct".to_string())))
    }

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("map".to_string())))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_seq(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("enum".to_string())))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("identifier".to_string())))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(self.error(ErrorKind::Unsupported("ignored_any".to_string())))
    }
}
