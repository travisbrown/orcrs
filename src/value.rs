#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Value<'a> {
    Bool(bool),
    U64(u64),
    Utf8(&'a str),
    Null,
}
