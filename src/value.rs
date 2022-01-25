#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Value<'a> {
    Bool(bool),
    U64(u64),
    Utf8(&'a str),
    Null,
}

impl<'a> Value<'a> {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_nullable_bool(&self) -> Option<Option<bool>> {
        match self {
            Self::Bool(value) => Some(Some(*value)),
            Self::Null => Some(None),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_nullable_u64(&self) -> Option<Option<u64>> {
        match self {
            Self::U64(value) => Some(Some(*value)),
            Self::Null => Some(None),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Utf8(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            Self::Utf8(value) => Some(value.to_string()),
            _ => None,
        }
    }

    pub fn as_nullable_str(&self) -> Option<Option<&str>> {
        match self {
            Self::Utf8(value) => Some(Some(value)),
            Self::Null => Some(None),
            _ => None,
        }
    }

    pub fn as_nullable_string(&self) -> Option<Option<String>> {
        match self {
            Self::Utf8(value) => Some(Some(value.to_string())),
            Self::Null => Some(None),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }
}
