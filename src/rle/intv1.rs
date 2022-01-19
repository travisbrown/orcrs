use integer_encoding::VarInt;

const DEFAULT_CAPACITY: usize = 1024;
const MIN_REPEAT_LEN: u8 = 3;

// TODO: Actually handle signed types properly.
pub fn decode_u64s(bytes: &[u8], expected_len: Option<usize>, signed: bool) -> Option<Vec<u64>> {
    let mut values = Vec::with_capacity(expected_len.unwrap_or(DEFAULT_CAPACITY));
    let mut current = bytes;

    while !current.is_empty() {
        let read_len = append_next_u64s(current, &mut values, signed)?;
        current = &current[read_len..];
    }

    Some(values)
}

fn append_next_u64s(bytes: &[u8], current_values: &mut Vec<u64>, _signed: bool) -> Option<usize> {
    bytes.get(0).and_then(|first| {
        if *first < 128 {
            let len = first + MIN_REPEAT_LEN;
            bytes.get(1).and_then(|second| {
                let delta = *second as i8;
                let (mut last_value, read_len) = u64::decode_var(&bytes[2..])?;

                for _ in 0..len {
                    current_values.push(last_value);
                    last_value = (last_value as i64 + delta as i64) as u64;
                }

                Some(read_len + 2)
            })
        } else {
            let len = first.wrapping_neg();

            let mut current = 1;
            for _ in 0..len {
                let (value, read_len) = u64::decode_var(&bytes[current..])?;
                current_values.push(value);
                current += read_len;
            }

            Some(current)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RUN_SAME_INPUT: [u8; 3] = [0x61, 0x00, 0x07];
    const RUN_DELTA_INPUT: [u8; 3] = [0x61, 0xff, 0x64];
    const LITERAL_INPUT: [u8; 6] = [0xfb, 0x02, 0x03, 0x06, 0x07, 0xb];

    const RUN_SAME_OUTPUT: [u64; 100] = [7; 100];
    const RUN_DELTA_OUTPUT: [u64; 100] = [
        100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 79,
        78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56,
        55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33,
        32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10,
        9, 8, 7, 6, 5, 4, 3, 2, 1,
    ];
    const LITERAL_OUTPUT: [u64; 5] = [2, 3, 6, 7, 11];

    #[test]
    fn append_next_u64s_simple_run_same() {
        let input = RUN_SAME_INPUT;
        let expected = RUN_SAME_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, true).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn append_next_u64s_simple_run_delta() {
        let input = RUN_DELTA_INPUT;
        let expected = RUN_DELTA_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, true).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn append_next_u64s_simple_literal() {
        let input = LITERAL_INPUT;
        let expected = LITERAL_OUTPUT.to_vec();

        let mut result = vec![];
        append_next_u64s(&input, &mut result, true).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn concatenation() {
        let mut input = vec![];
        let mut expected = vec![];

        input.extend(RUN_SAME_INPUT);
        input.extend(RUN_DELTA_INPUT);
        input.extend(LITERAL_INPUT);
        input.extend(RUN_SAME_INPUT);
        input.extend(RUN_DELTA_INPUT);
        input.extend(LITERAL_INPUT);
        input.extend(RUN_SAME_INPUT);
        input.extend(RUN_DELTA_INPUT);
        input.extend(LITERAL_INPUT);

        expected.extend(RUN_SAME_OUTPUT);
        expected.extend(RUN_DELTA_OUTPUT);
        expected.extend(LITERAL_OUTPUT);
        expected.extend(RUN_SAME_OUTPUT);
        expected.extend(RUN_DELTA_OUTPUT);
        expected.extend(LITERAL_OUTPUT);
        expected.extend(RUN_SAME_OUTPUT);
        expected.extend(RUN_DELTA_OUTPUT);
        expected.extend(LITERAL_OUTPUT);

        let result = decode_u64s(&mut input, None, false).unwrap();

        assert_eq!(result, expected);
    }
}
