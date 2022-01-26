use crate::proto::orc_proto::{
    ColumnEncoding_Kind, CompressionKind, Footer, PostScript, Stream_Kind, StripeFooter, Type_Kind,
};
use crate::{
    column::{BoolWriter, Column, PresentInfo, PresentInfoWriter},
    compress::{self, Decompressor},
    rle::{byte::ByteWriter, IntegerRleVersion},
    value::Value,
};
use protobuf::Message;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const POSTSCRIPT_BUFFER_LEN: usize = 256;
const POSTSCRIPT_LEN_LEN: u64 = 1;
const SUPPORTED_COMPRESSION_KINDS: [CompressionKind; 3] = [
    CompressionKind::ZSTD,
    CompressionKind::ZLIB,
    CompressionKind::NONE,
];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Protobuf error")]
    Protobuf(#[from] protobuf::error::ProtobufError),
    #[error("Compression error")]
    Compress(#[from] compress::Error),
    #[error("Unsupported type")]
    UnsupportedType(Type_Kind),
    #[error("Invalid parser state")]
    InvalidState,
    #[error("Invalid ORC file metadata")]
    InvalidMetadata,
    #[error("Invalid column index")]
    InvalidColumnIndex(usize),
    #[error("Invalid value")]
    InvalidValue {
        stripe_index: usize,
        column_index: usize,
        row_index: usize,
    },
    #[error("Invalid integer encoding")]
    InvalidIntegerEncoding,
    #[error("Invalid dictionary size")]
    InvalidDictionarySize { expected: u32, actual: u32 },
}

#[derive(Debug)]
pub struct StripeInfo {
    row_count: usize,
    data_start: u64,
    data_len: u64,
    columns: Vec<ColumnInfo>,
}

impl StripeInfo {
    pub fn get_column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_row_count(&self) -> usize {
        self.row_count
    }

    pub fn get_data_len(&self) -> u64 {
        self.data_len
    }
}

#[derive(Debug)]
enum ColumnInfo {
    Bool {
        offset: u64,
        present_len: Option<u64>,
        data_len: u64,
    },
    U64 {
        offset: u64,
        present_len: Option<u64>,
        data_len: u64,
        version: IntegerRleVersion,
    },
    Utf8Direct {
        offset: u64,
        present_len: Option<u64>,
        data_len: u64,
        length_len: u64,
        version: IntegerRleVersion,
    },
    Utf8Dictionary {
        offset: u64,
        present_len: Option<u64>,
        data_len: u64,
        dictionary_data_len: u64,
        length_len: u64,
        version: IntegerRleVersion,
        dictionary_size: u32,
    },
}

pub struct OrcFile {
    file: Option<File>,
    pub file_len: u64,
    postscript: PostScript,
    footer: Footer,
    type_kinds: Vec<Type_Kind>,
    field_names: Vec<String>,
    field_name_map: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct ColumnDataStreamInfo {
    present_len: u64,
    data_len: u64,
    dictionary_data_len: u64,
    length_len: u64,
}

impl ColumnDataStreamInfo {
    fn len(&self) -> u64 {
        self.present_len + self.data_len + self.dictionary_data_len + self.length_len
    }
}

impl OrcFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<OrcFile, Error> {
        let metadata = std::fs::metadata(path.as_ref())?;
        let file_len = metadata.len();

        let mut file = File::open(path)?;
        let (postscript, postscript_len) = Self::read_postscript(&mut file, file_len)?;

        if !SUPPORTED_COMPRESSION_KINDS.contains(&postscript.get_compression()) {
            Err(compress::Error::UnsupportedCompression(postscript.get_compression()).into())
        } else {
            let (footer, file) = Self::read_footer(
                file,
                &postscript.get_compression(),
                postscript_len,
                postscript.get_footerLength(),
            )?;

            let type_kinds = Self::extract_column_type_kinds(&footer)?;
            let field_names = footer
                .get_types()
                .get(0)
                .ok_or(Error::InvalidMetadata)?
                .get_fieldNames()
                .to_vec();

            let mut field_names_with_indices = field_names
                .iter()
                .enumerate()
                .map(|(i, field_name)| (field_name.to_string(), i))
                .collect::<Vec<_>>();

            // A field name may be repeated, in which case the map points to the first instance.
            field_names_with_indices.reverse();

            let field_name_map = field_names_with_indices.into_iter().collect();

            Ok(OrcFile {
                file: Some(file),
                file_len,
                postscript,
                footer,
                type_kinds,
                field_names,
                field_name_map,
            })
        }
    }

    pub fn get_field_names(&self) -> &[String] {
        &self.field_names
    }

    pub fn map_rows<T, E: From<Error>, F>(
        &mut self,
        columns: &[usize],
        f: F,
    ) -> Result<MappedRows<'_, F>, Error>
    where
        F: FnMut(&[Value<'_>]) -> Result<T, E>,
    {
        let stripe_info = self.get_stripe_info()?;

        Ok(MappedRows::new(self, stripe_info, columns.to_vec(), f))
    }

    pub fn deserialize<T: serde::de::DeserializeOwned>(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<T, crate::de::Error>> + '_> {
        let required_field_names = crate::de::get_field_names::<T>();
        let mut missing_field_names = vec![];
        let mut field_name_indices = Vec::with_capacity(required_field_names.len());

        for field_name in required_field_names {
            match self.field_name_map.get(*field_name) {
                Some(index) => {
                    field_name_indices.push(*index);
                }
                None => {
                    missing_field_names.push(field_name.to_string());
                }
            }
        }

        if missing_field_names.is_empty() {
            match self.map_rows(&field_name_indices, |row| {
                T::deserialize(&mut crate::de::RowDe::new(row))
            }) {
                Ok(iter) => Box::new(iter),
                Err(error) => Box::new(std::iter::once_with(|| Err(error.into()))),
            }
        } else {
            Box::new(std::iter::once_with(|| {
                Err(crate::de::ErrorKind::InvalidFieldNames(missing_field_names).into())
            }))
        }
    }

    fn read_null_runs(
        &mut self,
        start: u64,
        len: u64,
        row_count: usize,
    ) -> Result<Vec<u64>, Error> {
        let pos = SeekFrom::Start(start);
        let mut decompressor = Decompressor::open(
            self.take_file()?,
            self.postscript.get_compression(),
            pos,
            len,
        )?;
        let present_info_writer = PresentInfoWriter::new(row_count);
        let mut byte_writer = ByteWriter::new(present_info_writer);
        std::io::copy(&mut decompressor, &mut byte_writer)?;
        self.file = Some(decompressor.into_inner());
        Ok(byte_writer.into_inner().into_inner())
    }

    fn read_u64s(
        &mut self,
        start: u64,
        len: u64,
        version: IntegerRleVersion,
        signed: bool,
    ) -> Result<Vec<u64>, Error> {
        let pos = SeekFrom::Start(start);
        let mut decompressor = Decompressor::open(
            self.take_file()?,
            self.postscript.get_compression(),
            pos,
            len,
        )?;

        let mut bytes = vec![];
        decompressor.read_to_end(&mut bytes)?;

        let values = if version == IntegerRleVersion::V1 {
            crate::rle::intv1::decode_u64s(&bytes, None, signed)
        } else {
            crate::rle::intv2::decode_u64s(&bytes, None, signed)
        }
        .ok_or(Error::InvalidIntegerEncoding)?;

        self.file = Some(decompressor.into_inner());

        Ok(values)
    }

    pub fn read_column(&mut self, stripe: &StripeInfo, column_id: usize) -> Result<Column, Error> {
        if let Some(column_info) = stripe.columns.get(column_id) {
            match column_info {
                ColumnInfo::Bool {
                    offset,
                    present_len,
                    data_len,
                } => {
                    let null_runs = match present_len {
                        Some(len) => Some(self.read_null_runs(
                            stripe.data_start + offset,
                            *len,
                            stripe.row_count,
                        )?),

                        None => None,
                    };

                    let present_info = PresentInfo::new(null_runs);

                    let data_pos =
                        SeekFrom::Start(stripe.data_start + offset + present_len.unwrap_or(0));
                    let mut decompressor = Decompressor::open(
                        self.take_file()?,
                        self.postscript.get_compression(),
                        data_pos,
                        *data_len,
                    )?;

                    let bool_writer = BoolWriter::new(stripe.row_count, present_info);
                    let mut byte_writer = ByteWriter::new(bool_writer);
                    std::io::copy(&mut decompressor, &mut byte_writer)?;
                    self.file = Some(decompressor.into_inner());
                    Ok(byte_writer.into_inner().finish())
                }
                ColumnInfo::U64 {
                    offset,
                    present_len,
                    data_len,
                    version,
                } => {
                    let null_runs = match present_len {
                        Some(len) => Some(self.read_null_runs(
                            stripe.data_start + offset,
                            *len,
                            stripe.row_count,
                        )?),
                        None => None,
                    };

                    let values = self.read_u64s(
                        stripe.data_start + offset + present_len.unwrap_or(0),
                        *data_len,
                        *version,
                        true,
                    )?;

                    Ok(Column::make_u64_column(
                        values,
                        &null_runs.unwrap_or_default(),
                    ))
                }
                ColumnInfo::Utf8Dictionary {
                    offset,
                    present_len,
                    data_len,
                    dictionary_data_len,
                    length_len,
                    version,
                    dictionary_size,
                } => {
                    let null_runs = match present_len {
                        Some(len) => Some(self.read_null_runs(
                            stripe.data_start + offset,
                            *len,
                            stripe.row_count,
                        )?),
                        None => None,
                    };

                    let data = self.read_u64s(
                        stripe.data_start + offset + present_len.unwrap_or(0),
                        *data_len,
                        *version,
                        false,
                    )?;

                    let lengths = self.read_u64s(
                        stripe.data_start + offset + present_len.unwrap_or(0) + data_len,
                        *length_len,
                        *version,
                        false,
                    )?;

                    let pos = SeekFrom::Start(
                        stripe.data_start
                            + offset
                            + present_len.unwrap_or(0)
                            + data_len
                            + length_len,
                    );
                    let mut decompressor = Decompressor::open(
                        self.take_file()?,
                        self.postscript.get_compression(),
                        pos,
                        *dictionary_data_len,
                    )?;

                    let mut dictionary_bytes = vec![];
                    decompressor.read_to_end(&mut dictionary_bytes)?;

                    self.file = Some(decompressor.into_inner());

                    if *dictionary_size != lengths.len() as u32 {
                        Err(Error::InvalidDictionarySize {
                            expected: *dictionary_size,
                            actual: lengths.len() as u32,
                        })
                    } else {
                        Ok(Column::make_utf8_dictionary_column(
                            null_runs,
                            data,
                            dictionary_bytes,
                            lengths,
                        ))
                    }
                }
                ColumnInfo::Utf8Direct {
                    offset,
                    present_len,
                    data_len,
                    length_len,
                    version,
                } => {
                    let null_runs = match present_len {
                        Some(len) => Some(self.read_null_runs(
                            stripe.data_start + offset,
                            *len,
                            stripe.row_count,
                        )?),
                        None => None,
                    };

                    let pos =
                        SeekFrom::Start(stripe.data_start + offset + present_len.unwrap_or(0));
                    let mut decompressor = Decompressor::open(
                        self.take_file()?,
                        self.postscript.get_compression(),
                        pos,
                        *data_len,
                    )?;

                    let mut data_bytes = vec![];
                    decompressor.read_to_end(&mut data_bytes)?;

                    self.file = Some(decompressor.into_inner());

                    let lengths = self.read_u64s(
                        stripe.data_start + offset + present_len.unwrap_or(0) + data_len,
                        *length_len,
                        *version,
                        false,
                    )?;

                    Ok(Column::make_utf8_direct_column(
                        null_runs, data_bytes, lengths,
                    ))
                }
            }
        } else {
            Err(Error::InvalidColumnIndex(column_id))
        }
    }

    fn read_message<M: Message>(&mut self, pos: SeekFrom, len: u64) -> Result<M, Error> {
        let file = self.take_file()?;
        let (message, file) =
            Self::read_message_from_file(file, &self.postscript.get_compression(), pos, len)?;
        self.file = Some(file);
        Ok(message)
    }

    fn take_file(&mut self) -> Result<File, Error> {
        self.file.take().ok_or(Error::InvalidState)
    }

    fn read_message_from_file<M: Message>(
        file: File,
        compression: &CompressionKind,
        pos: SeekFrom,
        len: u64,
    ) -> Result<(M, File), Error> {
        let mut decompressor = Decompressor::open(file, *compression, pos, len)?;
        let message = Message::parse_from_reader(&mut decompressor)?;
        let file = decompressor.into_inner();

        Ok((message, file))
    }

    fn read_postscript(file: &mut File, file_len: u64) -> Result<(PostScript, u8), Error> {
        let bytes_to_read = std::cmp::min(POSTSCRIPT_BUFFER_LEN, file_len as usize) as usize;

        let mut buffer = Vec::with_capacity(bytes_to_read);
        file.seek(SeekFrom::End(-(bytes_to_read as i64)))?;
        file.read_to_end(&mut buffer)?;

        let postscript_len = buffer[bytes_to_read - 1];
        let postscript_start = bytes_to_read - 1 - postscript_len as usize;
        let postscript_bytes = &buffer[postscript_start..bytes_to_read - 1];

        Ok((
            PostScript::parse_from_bytes(postscript_bytes)?,
            postscript_len,
        ))
    }

    fn read_footer(
        file: File,
        compression: &CompressionKind,
        postscript_len: u8,
        footer_len: u64,
    ) -> Result<(Footer, File), Error> {
        let footer_offset = (postscript_len as u64 + footer_len + POSTSCRIPT_LEN_LEN) as i64;

        Self::read_message_from_file(file, compression, SeekFrom::End(-footer_offset), footer_len)
    }

    fn extract_column_type_kinds(footer: &Footer) -> Result<Vec<Type_Kind>, Error> {
        // We currently only support structs with scalar fields (and only a few types).
        footer
            .types
            .iter()
            .skip(1)
            .map(|type_value| {
                let kind = type_value.get_kind();
                if kind == Type_Kind::LONG
                    || kind == Type_Kind::INT
                    || kind == Type_Kind::STRING
                    || kind == Type_Kind::BOOLEAN
                {
                    Ok(kind)
                } else {
                    Err(Error::UnsupportedType(kind))
                }
            })
            .collect()
    }

    pub fn get_postscript(&self) -> &PostScript {
        &self.postscript
    }

    pub fn get_footer(&self) -> &Footer {
        &self.footer
    }

    pub fn get_stripe_footers(&mut self) -> Result<Vec<StripeFooter>, Error> {
        let stripe_count = self.footer.stripes.len();
        let mut stripe_footers = Vec::with_capacity(stripe_count);

        for i in 0..stripe_count {
            let stripe_info = &self.footer.stripes[i];
            let footer_start = stripe_info.get_offset()
                + stripe_info.get_indexLength()
                + stripe_info.get_dataLength();
            let footer_len = stripe_info.get_footerLength();

            let stripe_footer = self.read_message(SeekFrom::Start(footer_start), footer_len)?;

            stripe_footers.push(stripe_footer);
        }

        Ok(stripe_footers)
    }

    pub fn get_stripe_info(&mut self) -> Result<Vec<StripeInfo>, Error> {
        let stripe_footers = self.get_stripe_footers()?;

        stripe_footers
            .iter()
            .enumerate()
            .map(|(i, stripe_footer)| {
                let stripe_orig_info = &self.footer.stripes[i];
                let row_count = stripe_orig_info.get_numberOfRows() as usize;
                let data_start = stripe_orig_info.get_offset() + stripe_orig_info.get_indexLength();
                let data_len = stripe_orig_info.get_dataLength();

                let column_count = stripe_footer.columns.len();
                let mut column_data_stream_infos =
                    vec![ColumnDataStreamInfo::default(); column_count];

                for stream in stripe_footer.get_streams() {
                    let kind = stream.get_kind();
                    let column_id = stream.get_column() as usize;
                    let length = stream.get_length();
                    match kind {
                        Stream_Kind::DATA => {
                            column_data_stream_infos[column_id - 1].data_len = length;
                        }
                        Stream_Kind::LENGTH => {
                            column_data_stream_infos[column_id - 1].length_len = length;
                        }
                        Stream_Kind::PRESENT => {
                            column_data_stream_infos[column_id - 1].present_len = length;
                        }
                        Stream_Kind::DICTIONARY_DATA => {
                            column_data_stream_infos[column_id - 1].dictionary_data_len = length;
                        }
                        _ => {}
                    }
                }

                let mut current_offset = 0;

                let columns = stripe_footer
                    .get_columns()
                    .iter()
                    .skip(1) // Skip the struct column
                    .zip(&self.type_kinds)
                    .zip(column_data_stream_infos)
                    .map(|((column_encoding, type_kind), stream_info)| {
                        let result = match (type_kind, column_encoding.get_kind()) {
                            (Type_Kind::LONG | Type_Kind::INT, encoding_kind) => {
                                if stream_info.dictionary_data_len != 0
                                    || stream_info.length_len != 0
                                    || (encoding_kind != ColumnEncoding_Kind::DIRECT
                                        && encoding_kind != ColumnEncoding_Kind::DIRECT_V2)
                                {
                                    Err(Error::InvalidMetadata)
                                } else {
                                    Ok(ColumnInfo::U64 {
                                        offset: current_offset,
                                        present_len: if stream_info.present_len == 0 {
                                            None
                                        } else {
                                            Some(stream_info.present_len)
                                        },
                                        data_len: stream_info.data_len,
                                        version: encoding_kind.into(),
                                    })
                                }
                            }
                            (Type_Kind::BOOLEAN, ColumnEncoding_Kind::DIRECT) => {
                                if stream_info.dictionary_data_len != 0
                                    || stream_info.length_len != 0
                                {
                                    Err(Error::InvalidMetadata)
                                } else {
                                    Ok(ColumnInfo::Bool {
                                        offset: current_offset,
                                        present_len: if stream_info.present_len == 0 {
                                            None
                                        } else {
                                            Some(stream_info.present_len)
                                        },
                                        data_len: stream_info.data_len,
                                    })
                                }
                            }
                            (
                                Type_Kind::STRING,
                                encoding_kind @ (ColumnEncoding_Kind::DIRECT
                                | ColumnEncoding_Kind::DIRECT_V2),
                            ) => {
                                if stream_info.dictionary_data_len != 0 {
                                    Err(Error::InvalidMetadata)
                                } else {
                                    Ok(ColumnInfo::Utf8Direct {
                                        offset: current_offset,
                                        present_len: if stream_info.present_len == 0 {
                                            None
                                        } else {
                                            Some(stream_info.present_len)
                                        },
                                        data_len: stream_info.data_len,
                                        length_len: stream_info.length_len,
                                        version: encoding_kind.into(),
                                    })
                                }
                            }
                            (
                                Type_Kind::STRING,
                                encoding_kind @ (ColumnEncoding_Kind::DICTIONARY
                                | ColumnEncoding_Kind::DICTIONARY_V2),
                            ) => Ok(ColumnInfo::Utf8Dictionary {
                                offset: current_offset,
                                present_len: if stream_info.present_len == 0 {
                                    None
                                } else {
                                    Some(stream_info.present_len)
                                },
                                data_len: stream_info.data_len,
                                dictionary_data_len: stream_info.dictionary_data_len,
                                length_len: stream_info.length_len,
                                version: encoding_kind.into(),
                                dictionary_size: column_encoding.get_dictionarySize(),
                            }),
                            (kind, _) => Err(Error::UnsupportedType(*kind)),
                        };
                        current_offset += stream_info.len();
                        result
                    })
                    .collect::<Result<Vec<ColumnInfo>, Error>>()?;

                Ok(StripeInfo {
                    row_count,
                    data_start,
                    data_len,
                    columns,
                })
            })
            .collect()
    }
}

pub struct MappedRows<'a, F> {
    file: &'a mut OrcFile,
    stripe_info: Vec<StripeInfo>,
    columns: Vec<usize>,
    f: F,
    data: Vec<Column>,
    current_stripe: usize,
    current_row: usize,
}

impl<'a, F> MappedRows<'a, F> {
    fn new(
        file: &'a mut OrcFile,
        stripe_info: Vec<StripeInfo>,
        columns: Vec<usize>,
        f: F,
    ) -> MappedRows<'a, F> {
        Self {
            file,
            stripe_info,
            columns,
            f,
            data: vec![],
            current_stripe: 0,
            current_row: 0,
        }
    }
}

impl<T, E, F> Iterator for MappedRows<'_, F>
where
    E: From<Error>,
    F: FnMut(&[Value<'_>]) -> Result<T, E>,
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_stripe >= self.stripe_info.len() {
            None
        } else {
            let stripe_info = &self.stripe_info[self.current_stripe];

            if self.current_row >= stripe_info.get_row_count() {
                self.data.clear();
                self.current_stripe += 1;
                self.current_row = 0;
                self.next()
            } else {
                if self.current_row == 0 {
                    for i in &self.columns {
                        let column = match self.file.read_column(stripe_info, *i) {
                            Ok(column) => column,
                            Err(error) => {
                                // Unrecoverable error.
                                self.current_stripe = self.stripe_info.len();
                                return Some(Err(E::from(error)));
                            }
                        };
                        self.data.push(column);
                    }
                }

                let mut values = Vec::with_capacity(self.data.len());

                for (column, column_index) in self.data.iter().zip(&self.columns) {
                    match column.get(self.current_row) {
                        Some(value) => values.push(value),
                        None => {
                            let error = Error::InvalidValue {
                                stripe_index: self.current_stripe,
                                column_index: *column_index,
                                row_index: self.current_row,
                            };

                            // Unrecoverable error.
                            self.current_stripe = self.stripe_info.len();
                            return Some(Err(E::from(error)));
                        }
                    }
                }

                self.current_row += 1;
                Some((self.f)(&values))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::orc_proto::{CompressionKind, PostScript},
        value::Value,
    };
    use serde_derive::Deserialize;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    const TS_10K_EXAMPLE_PATH: &str = "examples/ts-10k-zstd-2020-09-20.orc";
    const TS_1K_ZLIB_PATH: &str = "examples/ts-1k-zlib-2020-09-20.orc";
    const TS_1K_NONE_PATH: &str = "examples/ts-1k-none-2020-09-20.orc";
    const TS_1K_JSON_PATH: &str = "examples/ts-1k-2020-09-20.ndjson";
    const TS_FIELD_NAMES: [&str; 11] = [
        "id",
        "status_id",
        "timestamp",
        "screen_name",
        "name",
        "url",
        "location",
        "description",
        "profile_image_url",
        "verified",
        "followers_count",
    ];

    #[test]
    fn get_postscript() {
        let orc_file = OrcFile::open(TS_10K_EXAMPLE_PATH).unwrap();
        let postscript = orc_file.get_postscript();

        let mut expected = PostScript::default();
        expected.set_footerLength(1065);
        expected.set_compression(CompressionKind::ZSTD);
        expected.set_compressionBlockSize(262144);
        expected.set_version(vec![0, 12]);
        expected.set_metadataLength(909);
        expected.set_writerVersion(9);
        expected.set_magic("ORC".to_string());

        assert_eq!(*postscript, expected);
    }

    #[test]
    fn get_footer() {
        let orc_file = OrcFile::open(TS_10K_EXAMPLE_PATH).unwrap();
        let footer = orc_file.get_footer();

        assert_eq!(footer.get_headerLength(), 3);
        assert_eq!(footer.get_contentLength(), 937322);
        assert_eq!(footer.get_stripes().len(), 1);
    }

    #[test]
    fn read_u64_column() {
        let mut orc_file = OrcFile::open(TS_10K_EXAMPLE_PATH).unwrap();
        let mut user_ids = HashSet::new();

        for stripe in orc_file.get_stripe_info().unwrap() {
            let column = orc_file.read_column(&stripe, 0).unwrap();

            for row_index in 0..stripe.get_row_count() as usize {
                match column.get(row_index).unwrap() {
                    Value::U64(value) => {
                        user_ids.insert(value);
                    }
                    other => {
                        panic!("Unexpected value: {:?}", other);
                    }
                }
            }
        }

        assert_eq!(user_ids.len(), 8830);
    }

    #[test]
    fn read_utf8_direct_column() {
        let mut orc_file = OrcFile::open(TS_10K_EXAMPLE_PATH).unwrap();
        let mut names = HashSet::new();
        let mut name_null_count = 0;

        for stripe in orc_file.get_stripe_info().unwrap() {
            let column = orc_file.read_column(&stripe, 4).unwrap();

            for row_index in 0..stripe.get_row_count() as usize {
                match column.get(row_index).unwrap() {
                    Value::Utf8(value) => {
                        names.insert(value.to_string());
                    }
                    Value::Null => {
                        name_null_count += 1;
                    }
                    other => {
                        panic!("Unexpected value: {:?}", other);
                    }
                }
            }
        }

        assert_eq!(names.len(), 8670);
        assert_eq!(name_null_count, 0);
    }

    #[test]
    fn read_utf8_dictionary_column() {
        let mut orc_file = OrcFile::open(TS_10K_EXAMPLE_PATH).unwrap();
        let mut locations = HashSet::new();
        let mut location_null_count = 0;

        for stripe in orc_file.get_stripe_info().unwrap() {
            let column = orc_file.read_column(&stripe, 6).unwrap();

            for row_index in 0..stripe.get_row_count() as usize {
                match column.get(row_index).unwrap() {
                    Value::Utf8(value) => {
                        locations.insert(value.to_string());
                    }
                    Value::Null => {
                        location_null_count += 1;
                    }
                    other => {
                        panic!("Unexpected value: {:?}", other);
                    }
                }
            }
        }

        assert_eq!(locations.len(), 3391);
        assert_eq!(location_null_count, 4898);
    }

    #[test]
    fn read_bool_column() {
        let mut orc_file = OrcFile::open(TS_10K_EXAMPLE_PATH).unwrap();
        let mut verified_count = 0;

        for stripe in orc_file.get_stripe_info().unwrap() {
            let column = orc_file.read_column(&stripe, 9).unwrap();

            for row_index in 0..stripe.get_row_count() as usize {
                match column.get(row_index).unwrap() {
                    Value::Bool(value) => {
                        if value {
                            verified_count += 1;
                        }
                    }
                    Value::Null => {}
                    other => {
                        panic!("Unexpected value: {:?}", other);
                    }
                }
            }
        }

        assert_eq!(verified_count, 543);
    }

    #[test]
    fn test_map_rows_error() {
        let mut orc_file = OrcFile::open(TS_1K_ZLIB_PATH).unwrap();

        let result = orc_file
            .map_rows(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], |values| {
                let id = values.get(0).and_then(|value| value.as_u64()).unwrap();

                // Let a couple of rows through successfully.
                if id == 762674860875276288 || id == 1237648870118580224 {
                    Ok(id)
                } else {
                    Err(Error::InvalidState)
                }
            })
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 1743);
        assert_eq!(*result[0].as_ref().unwrap(), 762674860875276288);
        assert_eq!(*result[1].as_ref().unwrap(), 1237648870118580224);

        for value in &result[2..] {
            assert!(value.is_err());
        }
    }

    #[test]
    fn test_compression_ts_1k_zlib() {
        test_compression_ts_1k(CompressionKind::ZLIB);
    }

    #[test]
    fn test_compression_ts_1k_none() {
        test_compression_ts_1k(CompressionKind::NONE);
    }

    fn test_compression_ts_1k(compression: CompressionKind) {
        let orc_file_path = match compression {
            CompressionKind::ZLIB => TS_1K_ZLIB_PATH,
            CompressionKind::NONE => TS_1K_NONE_PATH,
            other => panic!("No example data for compression type {:?}", other),
        };
        let mut orc_file = OrcFile::open(orc_file_path).unwrap();

        assert_eq!(orc_file.get_field_names(), TS_FIELD_NAMES);

        let user_rows = orc_file
            .map_rows(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], |values| {
                let id = values.get(0).and_then(|value| value.as_u64()).unwrap();
                let status_id = values.get(1).and_then(|value| value.as_u64()).unwrap();
                let timestamp = values.get(2).and_then(|value| value.as_u64()).unwrap();
                let screen_name = values.get(3).and_then(|value| value.as_string()).unwrap();
                let name = values
                    .get(4)
                    .and_then(|value| value.as_nullable_string())
                    .unwrap();
                let url = values
                    .get(5)
                    .and_then(|value| value.as_nullable_string())
                    .unwrap();
                let location = values
                    .get(6)
                    .and_then(|value| value.as_nullable_string())
                    .unwrap();
                let description = values
                    .get(7)
                    .and_then(|value| value.as_nullable_string())
                    .unwrap();
                let profile_image_url = values
                    .get(8)
                    .and_then(|value| value.as_nullable_string())
                    .unwrap();
                let verified = values
                    .get(9)
                    .and_then(|value| value.as_nullable_bool())
                    .unwrap();
                let followers_count = values
                    .get(10)
                    .and_then(|value| value.as_nullable_u64())
                    .unwrap();

                Ok(UserRow {
                    id,
                    status_id,
                    timestamp,
                    screen_name,
                    name,
                    url,
                    location,
                    description,
                    profile_image_url,
                    verified,
                    followers_count: followers_count.map(|v| v as u32),
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, Error>>()
            .unwrap();

        for (result, expected) in user_rows.iter().zip(load_ts_1k_json()) {
            assert_eq!(*result, expected);
        }
    }

    #[test]
    fn test_deserialize_ts_1k_zlib() {
        test_deserialize_ts_1k(CompressionKind::ZLIB);
    }

    #[test]
    fn test_deserialize_ts_1k_none() {
        test_deserialize_ts_1k(CompressionKind::NONE);
    }

    fn test_deserialize_ts_1k(compression: CompressionKind) {
        let orc_file_path = match compression {
            CompressionKind::ZLIB => TS_1K_ZLIB_PATH,
            CompressionKind::NONE => TS_1K_NONE_PATH,
            other => panic!("No example data for compression type {:?}", other),
        };

        let mut orc_file = OrcFile::open(orc_file_path).unwrap();

        let result = orc_file
            .deserialize::<UserRow>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for (result, expected) in result.iter().zip(load_ts_1k_json()) {
            assert_eq!(*result, expected);
        }
    }

    #[derive(Deserialize, Debug, Eq, PartialEq)]
    struct UserRow {
        id: u64,
        status_id: u64,
        timestamp: u64,
        screen_name: String,
        name: Option<String>,
        url: Option<String>,
        location: Option<String>,
        description: Option<String>,
        profile_image_url: Option<String>,
        verified: Option<bool>,
        followers_count: Option<u32>,
    }

    fn load_ts_1k_json() -> Vec<UserRow> {
        let reader = BufReader::new(File::open(TS_1K_JSON_PATH).unwrap());

        reader
            .lines()
            .map(|line| {
                serde_json::from_str(&line.as_ref().unwrap()).expect(&format!("bad: {:?}", line))
            })
            .collect()
    }
}
