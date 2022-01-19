use crate::proto::orc_proto::ColumnEncoding_Kind;

pub mod byte;
pub mod intv1;
pub mod intv2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegerRleVersion {
    V1,
    V2,
}

impl From<ColumnEncoding_Kind> for IntegerRleVersion {
    fn from(kind: ColumnEncoding_Kind) -> Self {
        match kind {
            ColumnEncoding_Kind::DIRECT => Self::V1,
            ColumnEncoding_Kind::DIRECT_V2 => Self::V2,
            ColumnEncoding_Kind::DICTIONARY => Self::V1,
            ColumnEncoding_Kind::DICTIONARY_V2 => Self::V2,
        }
    }
}
