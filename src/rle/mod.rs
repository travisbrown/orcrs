use crate::proto::orc_proto::column_encoding::Kind;

pub mod byte;
pub mod intv1;
pub mod intv2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegerRleVersion {
    V1,
    V2,
}

impl From<Kind> for IntegerRleVersion {
    fn from(kind: Kind) -> Self {
        match kind {
            Kind::DIRECT => Self::V1,
            Kind::DIRECT_V2 => Self::V2,
            Kind::DICTIONARY => Self::V1,
            Kind::DICTIONARY_V2 => Self::V2,
        }
    }
}
