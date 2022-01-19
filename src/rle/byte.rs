use std::io::{Error, Write};

const MIN_REPEAT_LEN: u8 = 3;

pub struct ByteWriter<W: Write> {
    writer: W,
    state: ByteWriterState,
}

enum ByteWriterState {
    AwaitingControl,
    AwaitingRepeated(u8),
    LiteralRemaining(u8),
}

impl<W: Write> ByteWriter<W> {
    pub fn new(writer: W) -> ByteWriter<W> {
        ByteWriter {
            writer,
            state: ByteWriterState::AwaitingControl,
        }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> Write for ByteWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let mut i = 0;

        while i < buf.len() {
            let next_state = match &self.state {
                ByteWriterState::AwaitingControl => {
                    let next_state = if buf[i] < 128 {
                        ByteWriterState::AwaitingRepeated(buf[i] + MIN_REPEAT_LEN)
                    } else {
                        ByteWriterState::LiteralRemaining(buf[i].wrapping_neg())
                    };
                    i += 1;
                    next_state
                }
                ByteWriterState::AwaitingRepeated(repeat_len) => {
                    let repeated = vec![buf[i]; *repeat_len as usize];
                    self.writer.write_all(&repeated)?;
                    i += 1;
                    ByteWriterState::AwaitingControl
                }
                ByteWriterState::LiteralRemaining(literal_remaining) => {
                    let buf_remaining = buf.len() - i;

                    if buf_remaining >= *literal_remaining as usize {
                        self.writer
                            .write_all(&buf[i..i + *literal_remaining as usize])?;
                        i += *literal_remaining as usize;
                        ByteWriterState::AwaitingControl
                    } else {
                        self.writer.write_all(&buf[i..])?;
                        i = buf.len();
                        ByteWriterState::LiteralRemaining(*literal_remaining - buf_remaining as u8)
                    }
                }
            };
            self.state = next_state;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn zeros() {
        let input = &[97, 0];
        let expected = vec![0; 100];

        let mut result = ByteWriter::new(vec![]);
        result.write_all(input).unwrap();
        assert_eq!(result.into_inner(), expected);
    }

    #[test]
    fn four_ones() {
        let input = &[1, 1];
        let expected = vec![1, 1, 1, 1];

        let mut result = ByteWriter::new(vec![]);
        result.write_all(input).unwrap();
        assert_eq!(result.into_inner(), expected);
    }

    #[test]
    fn literal() {
        let input = &[0xfe, 0x44, 0x45];
        let expected = vec![0x44, 0x45];

        let mut result = ByteWriter::new(vec![]);
        result.write_all(input).unwrap();
        assert_eq!(result.into_inner(), expected);
    }
}
