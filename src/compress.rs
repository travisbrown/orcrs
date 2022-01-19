use crate::proto::orc_proto::CompressionKind;
use flate2::read::DeflateDecoder;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Take};
use zstd::stream::read::Decoder as ZstdDecoder;

// The compression header will always be three bytes.
const COMPRESSION_HEADER_LEN: usize = 3;
// No compression is typically only used for small messages.
const NONE_COMPRESSION_BUFFER_CAPACITY: usize = 512;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Unsupported compression")]
    UnsupportedCompression(CompressionKind),
    #[error("Expected length mismatch")]
    ExpectedLenMismatch(u64, u64),
    #[error("Invalid state")]
    InvalidState,
}

pub struct Decompressor {
    decoder: Option<Decoder>,
    compression: CompressionKind,
    remaining: u64,
}

impl Decompressor {
    pub fn open(
        mut file: File,
        compression: CompressionKind,
        pos: SeekFrom,
        len: u64,
    ) -> Result<Decompressor, Error> {
        file.seek(pos)?;

        let (is_original, chunk_len) = Self::read_header(&mut file)?;

        let chunk_compression = if is_original {
            CompressionKind::NONE
        } else {
            compression
        };
        let file = file.take(chunk_len);
        let decoder = Self::open_decoder(file, chunk_compression)?;

        Ok(Decompressor {
            decoder: Some(decoder),
            compression,
            remaining: len - (chunk_len + 3),
        })
    }

    fn read_header(file: &mut File) -> Result<(bool, u64), std::io::Error> {
        let mut header_buffer = [0; COMPRESSION_HEADER_LEN];

        file.read_exact(&mut header_buffer)?;

        let is_original = (header_buffer[0] & 0x01) == 1;
        let header_value = ((header_buffer[2] as u64) << 15)
            | ((header_buffer[1] as u64) << 7)
            | ((header_buffer[0] as u64) >> 1);

        Ok((is_original, header_value))
    }

    fn open_decoder(
        file: Take<File>,
        compression: CompressionKind,
    ) -> Result<Decoder, std::io::Error> {
        match compression {
            CompressionKind::ZSTD => Ok(Decoder::Zstd(ZstdDecoder::new(file)?)),
            CompressionKind::ZLIB => Ok(Decoder::Zlib(DeflateDecoder::new(file))),
            CompressionKind::NONE => Ok(Decoder::None(BufReader::with_capacity(
                NONE_COMPRESSION_BUFFER_CAPACITY,
                file,
            ))),
            other => {
                panic!(
                    "We should have already checked that this compression type ({:?}) was supported",
                    other
                )
            }
        }
    }

    pub fn into_inner(mut self) -> File {
        self.decoder.take().unwrap().into_inner()
    }
}

impl Read for Decompressor {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let mut decoder = self.decoder.as_mut().expect("Invalid state");

        let bytes_read = match &mut decoder {
            Decoder::Zstd(decoder) => decoder.read(buf),
            Decoder::Zlib(decoder) => decoder.read(buf),
            Decoder::None(reader) => reader.read(buf),
        }?;

        if bytes_read == 0 && self.remaining != 0 {
            let mut file = self.decoder.take().expect("Invalid state").into_inner();

            let (is_original, chunk_len) = Self::read_header(&mut file)?;

            let chunk_compression = if is_original {
                CompressionKind::NONE
            } else {
                self.compression
            };
            let file = file.take(chunk_len);
            let decoder = Self::open_decoder(file, chunk_compression)?;
            self.decoder = Some(decoder);
            self.remaining -= chunk_len + 3;

            self.read(buf)
        } else {
            Ok(bytes_read)
        }
    }
}

enum Decoder {
    Zstd(ZstdDecoder<'static, BufReader<Take<File>>>),
    Zlib(DeflateDecoder<Take<File>>),
    None(BufReader<Take<File>>),
}

impl Decoder {
    fn into_inner(self) -> File {
        let take = match self {
            Decoder::Zstd(decoder) => decoder.finish().into_inner(),
            Decoder::Zlib(decoder) => decoder.into_inner(),
            Decoder::None(reader) => reader.into_inner(),
        };

        take.into_inner()
    }
}
