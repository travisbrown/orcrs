use integer_encoding::VarInt;

const DEFAULT_CAPACITY: usize = 1024;

pub fn decode_u64s(bytes: &[u8], expected_len: Option<usize>, signed: bool) -> Option<Vec<u64>> {
    let mut values = Vec::with_capacity(expected_len.unwrap_or(DEFAULT_CAPACITY));
    let mut current = bytes;

    while !current.is_empty() {
        let read_len = append_next_u64s(current, &mut values, signed)?;
        current = &current[read_len..];
    }

    Some(values)
}

fn append_next_u64s(bytes: &[u8], current_values: &mut Vec<u64>, signed: bool) -> Option<usize> {
    let (header, mut current) = parse_header(bytes)?;
    current_values.reserve(header.value_count());

    match header {
        Header::ShortRepeat {
            width,
            repeat_count,
        } => {
            let expected = current + width as usize;
            if bytes.len() < expected {
                None
            } else {
                let value = read_u64_be_bytes(&bytes[current..], width)?;
                for _ in 0..repeat_count as usize {
                    current_values.push(value);
                }
                Some(expected)
            }
        }
        Header::Direct { width, len } => {
            let expected = current + bits_to_bytes(width as u64 * len as u64);
            if bytes.len() < expected {
                None
            } else {
                for i in 0..len as usize {
                    let bit_offset = i as u64 * width as u64;
                    let value = read_u64_be_bits(&bytes[current..], bit_offset, width)?;
                    current_values.push(value);
                }
                Some(expected)
            }
        }
        Header::Delta { width, len } => {
            let (base, read_len) = i64::decode_var(&bytes[current..])?;
            current += read_len;
            let (delta, read_len) = i64::decode_var(&bytes[current..])?;
            current += read_len;

            let expected = current + bits_to_bytes(width as u64 * (len as u64 - 2));
            if bytes.len() < expected {
                None
            } else {
                // TODO: handle signed integer types better somewhere around here.
                let signum = delta.signum();
                let base = if signed {
                    base
                } else if base < 0 {
                    (-base * 2) - 1
                } else {
                    base * 2
                };

                current_values.push(base as u64);

                let mut last_value = (base + delta) as u64;
                current_values.push(last_value);

                if width == 0 {
                    for _ in 0..(len as usize) - 2 {
                        last_value = (last_value as i64 + delta) as u64;
                        current_values.push(last_value);
                    }
                } else {
                    for i in 0..(len as usize) - 2 {
                        let bit_offset = i as u64 * width as u64;
                        let value = read_u64_be_bits(&bytes[current..], bit_offset, width)?;

                        last_value = (last_value as i64 + signum * (value as i64)) as u64;
                        current_values.push(last_value);
                    }
                }

                Some(expected)
            }
        }
        Header::PatchedBase {
            width,
            len,
            base_width,
            patch_width,
            patch_gap_width,
            patch_list_len,
        } => {
            let expected = current
                + base_width as usize
                + bits_to_bytes(width as u64 * len as u64)
                + bits_to_bytes(
                    patch_list_len as u64
                        * closest_fixed_bits(patch_gap_width + patch_width) as u64,
                );

            if bytes.len() < expected {
                None
            } else {
                // TODO: handle signed integer types somewhere around here.
                let base = read_u64_be_bytes(&bytes[current..], base_width)?;

                current += base_width as usize;

                let mut data_values = Vec::with_capacity(len as usize);

                for i in 0..len as usize {
                    let bit_offset = i as u64 * width as u64;
                    let value = read_u64_be_bits(&bytes[current..], bit_offset, width)?;
                    data_values.push(value + base);
                }

                current += bits_to_bytes(width as u64 * len as u64);

                let mut patch_pos = 0;
                let patch_list_item_len = closest_fixed_bits(patch_gap_width + patch_width);

                for i in 0..patch_list_len as usize {
                    let bit_offset = i as u64 * patch_list_item_len as u64;
                    let patch_gap = read_u64_be_bits(
                        &bytes[current..],
                        bit_offset + (patch_list_item_len - patch_width - patch_gap_width) as u64,
                        patch_gap_width,
                    )?;
                    let patch_value = read_u64_be_bits(
                        &bytes[current..],
                        bit_offset + (patch_list_item_len - patch_width) as u64,
                        patch_width,
                    )?;

                    patch_pos += patch_gap as usize;
                    data_values[patch_pos] += patch_value << width;
                }

                current_values.extend(data_values);
                Some(expected)
            }
        }
    }
}

fn parse_header(bytes: &[u8]) -> Option<(Header, usize)> {
    if bytes.is_empty() {
        None
    } else {
        let b0 = bytes[0];
        let tag = b0 >> 6 & 0b0000_0011;

        if tag == 0 {
            let width = (b0 >> 3 & 0b0000_0111) + 1;
            let repeat_count = (b0 & 0b0000_0111) + 3;

            Some((
                Header::ShortRepeat {
                    width,
                    repeat_count,
                },
                1,
            ))
        } else if tag > 3 {
            None
        } else {
            let width = five_bit_width((b0 >> 1) & 0b0001_1111, tag == 3);
            let len = (((b0 & 0b0000_0001) as u16) << 8) + bytes[1] as u16 + 1;

            if tag == 1 {
                Some((Header::Direct { width, len }, 2))
            } else if tag == 3 {
                Some((Header::Delta { width, len }, 2))
            } else if tag == 2 {
                let b2 = bytes[2];
                let base_width = (b2 >> 5 & 0b0000_0111) + 1;
                let patch_width = five_bit_width(b2 & 0b0001_1111, false);

                let b3 = bytes[3];
                let patch_gap_width = (b3 >> 5 & 0b0000_0111) + 1;
                let patch_list_len = b3 & 0b0001_1111;

                Some((
                    Header::PatchedBase {
                        width,
                        len,
                        base_width,
                        patch_width,
                        patch_gap_width,
                        patch_list_len,
                    },
                    4,
                ))
            } else {
                None
            }
        }
    }
}

#[derive(Debug)]
enum Header {
    ShortRepeat {
        width: u8,
        repeat_count: u8,
    },
    Direct {
        width: u8,
        len: u16,
    },
    PatchedBase {
        width: u8,
        len: u16,
        base_width: u8,
        patch_width: u8,
        patch_gap_width: u8,
        patch_list_len: u8,
    },
    Delta {
        width: u8,
        len: u16,
    },
}

impl Header {
    fn value_count(&self) -> usize {
        match self {
            Header::ShortRepeat { repeat_count, .. } => *repeat_count as usize,
            Header::Direct { len, .. } => *len as usize,
            Header::PatchedBase { len, .. } => *len as usize,
            Header::Delta { len, .. } => *len as usize,
        }
    }
}

fn read_u64_be_bytes(bytes: &[u8], byte_width: u8) -> Option<u64> {
    if byte_width > 8 || bytes.len() < byte_width as usize {
        None
    } else {
        let mut value: u64 = 0;
        for b in bytes.iter().take(byte_width as usize) {
            value *= 256;
            value += *b as u64;
        }
        Some(value)
    }
}

fn read_u64_be_bits(bytes: &[u8], bit_offset: u64, bit_width: u8) -> Option<u64> {
    let bits_needed = (bit_offset + bit_width as u64) as usize;
    let bits_leftover = bits_needed % 8;
    let bytes_needed = (bits_needed / 8) + if bits_leftover == 0 { 0 } else { 1 };

    if bit_width > 64 || bytes.len() < bytes_needed {
        None
    } else {
        let current_byte = (bit_offset / 8) as usize;
        let current_bit = bit_offset % 8;
        let mut value = (bytes[current_byte] & (255 >> current_bit)) as u64;

        for i in 1..(bytes_needed - current_byte) {
            value *= 256;
            value += bytes[current_byte + i] as u64;
        }

        if bits_leftover != 0 {
            value >>= 8 - bits_leftover;
        }

        Some(value)
    }
}

fn bits_to_bytes(bit_count: u64) -> usize {
    ((bit_count / 8) + if bit_count % 8 == 0 { 0 } else { 1 }) as usize
}

const FIVE_BIT_ENCODING: [u8; 32] = [
    0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 28,
    30, 32, 40, 48, 56, 64,
];

fn five_bit_width(byte: u8, is_delta: bool) -> u8 {
    if !is_delta && byte == 0 {
        1
    } else {
        FIVE_BIT_ENCODING[byte as usize]
    }
}

fn closest_fixed_bits(bits: u8) -> u8 {
    if bits == 0 {
        1
    } else if bits <= 24 {
        bits
    } else if bits <= 26 {
        26
    } else if bits <= 28 {
        28
    } else if bits <= 30 {
        30
    } else if bits <= 32 {
        32
    } else if bits <= 40 {
        40
    } else if bits <= 48 {
        48
    } else if bits <= 56 {
        56
    } else {
        64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHORT_REPEAT_INPUT: [u8; 3] = [0x0a, 0x27, 0x10];
    const DIRECT_INPUT: [u8; 10] = [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef];
    const PATCHED_BASE_INPUT: [u8; 28] = [
        0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50,
        0x5a, 0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8,
    ];
    const DELTA_INPUT: [u8; 8] = [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46];

    const SHORT_REPEAT_OUTPUT: [u64; 5] = [10000; 5];
    const DIRECT_OUTPUT: [u64; 4] = [23713, 43806, 57005, 48879];
    const PATCHED_BASE_OUTPUT: [u64; 20] = [
        2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130,
        2140, 2150, 2160, 2170, 2180, 2190,
    ];
    const DELTA_OUTPUT: [u64; 10] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29];

    #[test]
    fn append_next_u64s_simple_short_repeat() {
        let input = SHORT_REPEAT_INPUT;
        let expected = SHORT_REPEAT_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, true).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn append_next_u64s_simple_direct() {
        let input = DIRECT_INPUT;
        let expected = DIRECT_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, true).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn append_next_u64s_simple_patched_base() {
        let input = PATCHED_BASE_INPUT;
        let expected = PATCHED_BASE_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, true).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn append_next_u64s_simple_delta() {
        let input = DELTA_INPUT;
        let expected = DELTA_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, false).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn concatenation() {
        let mut input = vec![];
        let mut expected = vec![];

        input.extend(SHORT_REPEAT_INPUT);
        input.extend(DIRECT_INPUT);
        input.extend(PATCHED_BASE_INPUT);
        input.extend(DELTA_INPUT);
        input.extend(SHORT_REPEAT_INPUT);
        input.extend(DIRECT_INPUT);
        input.extend(PATCHED_BASE_INPUT);
        input.extend(DELTA_INPUT);

        expected.extend(SHORT_REPEAT_OUTPUT);
        expected.extend(DIRECT_OUTPUT);
        expected.extend(PATCHED_BASE_OUTPUT);
        expected.extend(DELTA_OUTPUT);
        expected.extend(SHORT_REPEAT_OUTPUT);
        expected.extend(DIRECT_OUTPUT);
        expected.extend(PATCHED_BASE_OUTPUT);
        expected.extend(DELTA_OUTPUT);

        let result = decode_u64s(&mut input, None, false).unwrap();

        assert_eq!(result, expected);
    }
}
