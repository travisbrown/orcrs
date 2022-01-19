use crate::value::Value;
use bit_vec::BitVec;
use std::io::{Error, Write};

const PRESENT_VALUE_CAPACITY: usize = 512;

pub enum Column {
    Utf8Direct {
        data: Vec<u8>,
        indices: Vec<(i64, u64)>,
    },
    Utf8Dictionary {
        data: Vec<i64>,
        dictionary: Vec<u8>,
        indices: Vec<(u64, u64)>,
    },
    Bool {
        row_count: u64,
        values: BitVec,
        nulls: Option<BitVec>,
    },
    U64 {
        values: Vec<u64>,
        nulls: Option<BitVec>,
    },
}

impl Column {
    pub fn get(&self, row: usize) -> Option<Value<'_>> {
        match self {
            Column::Bool {
                row_count,
                values,
                nulls,
            } => {
                if (row as u64) < *row_count {
                    if let Some(nulls) = nulls {
                        if nulls[row] {
                            return Some(Value::Null);
                        }
                    };

                    Some(Value::Bool(values[row]))
                } else {
                    None
                }
            }
            Column::U64 { values, nulls } => {
                if row < values.len() {
                    if let Some(nulls) = nulls {
                        if nulls[row] {
                            return Some(Value::Null);
                        }
                    };

                    Some(Value::U64(values[row]))
                } else {
                    None
                }
            }
            Column::Utf8Dictionary {
                data,
                dictionary,
                indices,
            } => {
                if data[row] == -1 {
                    Some(Value::Null)
                } else {
                    let (start, len) = indices[data[row] as usize];
                    Some(Value::Utf8(
                        // TODO: Don't hard crash here.
                        std::str::from_utf8(&dictionary[start as usize..(start + len) as usize])
                            .unwrap(),
                    ))
                }
            }
            Column::Utf8Direct { data, indices } => {
                let (start, len) = indices[row];

                if start == -1 {
                    Some(Value::Null)
                } else {
                    Some(Value::Utf8(
                        // TODO: Don't hard crash here.
                        std::str::from_utf8(&data[start as usize..(start as usize + len as usize)])
                            .unwrap(),
                    ))
                }
            }
        }
    }

    pub(crate) fn make_u64_column(values: Vec<u64>, null_runs: &[u64]) -> Column {
        if null_runs.is_empty() {
            Column::U64 {
                values,
                nulls: None,
            }
        } else {
            let new_len = values.len() + null_runs.iter().sum::<u64>() as usize;
            let mut new_values = Vec::with_capacity(new_len);
            let mut nulls = BitVec::with_capacity(new_len);

            for (current_present_index, null_run) in null_runs.iter().enumerate() {
                for _ in 0..*null_run {
                    new_values.push(0);
                    nulls.push(true);
                }

                if let Some(value) = values.get(current_present_index) {
                    new_values.push(*value);
                    nulls.push(false);
                }
            }

            Column::U64 {
                values: new_values,
                nulls: Some(nulls),
            }
        }
    }

    pub(crate) fn make_utf8_dictionary_column(
        null_runs: Option<Vec<u64>>,
        data: Vec<u64>,
        dictionary_bytes: Vec<u8>,
        lengths: Vec<u64>,
    ) -> Column {
        let new_data = if let Some(null_runs) = null_runs {
            let new_len = data.len() + null_runs.iter().sum::<u64>() as usize;
            let mut new_data: Vec<i64> = Vec::with_capacity(new_len);

            for (current_present_index, null_run) in null_runs.iter().enumerate() {
                for _ in 0..*null_run {
                    new_data.push(-1);
                }

                if let Some(value) = data.get(current_present_index) {
                    new_data.push(*value as i64);
                }
            }

            new_data
        } else {
            data.iter().map(|v| *v as i64).collect()
        };

        let mut indices = Vec::with_capacity(lengths.len());
        let mut total_inc = 0;

        for length in lengths {
            indices.push((total_inc, length));
            total_inc += length;
        }

        Column::Utf8Dictionary {
            data: new_data,
            dictionary: dictionary_bytes,
            indices,
        }
    }

    pub(crate) fn make_utf8_direct_column(
        null_runs: Option<Vec<u64>>,
        data_bytes: Vec<u8>,
        lengths: Vec<u64>,
    ) -> Column {
        let new_lengths = if let Some(null_runs) = null_runs {
            let new_len = lengths.len() + null_runs.iter().sum::<u64>() as usize;
            let mut new_lengths: Vec<i64> = Vec::with_capacity(new_len);

            for (current_present_index, null_run) in null_runs.iter().enumerate() {
                for _ in 0..*null_run {
                    new_lengths.push(-1);
                }

                if let Some(value) = lengths.get(current_present_index) {
                    new_lengths.push(*value as i64);
                }
            }

            new_lengths
        } else {
            lengths.iter().map(|v| *v as i64).collect()
        };

        let mut indices = Vec::with_capacity(new_lengths.len());
        let mut total_inc = 0;

        for length in new_lengths {
            if length == -1 {
                indices.push((-1, 0));
            } else {
                indices.push((total_inc, length as u64));
                total_inc += length;
            }
        }

        Column::Utf8Direct {
            data: data_bytes,
            indices,
        }
    }
}

pub struct BoolWriter {
    row_count: u64,
    present_info: PresentInfo,
    values: BitVec,
    nulls: Option<BitVec>,
    current_index: usize,
    current_present_index: usize,
}

impl BoolWriter {
    pub fn new(row_count: u64, present_info: PresentInfo) -> BoolWriter {
        let nulls = match present_info {
            PresentInfo::All => None,
            PresentInfo::NullRuns(_) => Some(BitVec::with_capacity(row_count as usize)),
        };

        BoolWriter {
            row_count,
            present_info,
            values: BitVec::with_capacity(row_count as usize),
            nulls,
            current_index: 0,
            current_present_index: 0,
        }
    }

    pub fn finish(mut self) -> Column {
        if let Some(ref mut nulls) = self.nulls {
            let null_run = self.present_info.null_run(self.current_present_index);

            for _ in 0..null_run {
                nulls.push(true);
                self.current_index += 1;
            }
        };

        Column::Bool {
            row_count: self.row_count,
            values: self.values,
            nulls: self.nulls,
        }
    }
}

impl Write for BoolWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        for b in buf {
            for i in 0..8 {
                if let Some(ref mut nulls) = self.nulls {
                    let null_run = self.present_info.null_run(self.current_present_index);

                    for _ in 0..null_run {
                        nulls.push(true);
                        self.values.push(false);
                        self.current_index += 1;
                    }

                    nulls.push(false);
                };

                let value = b & (1 << (7 - i)) != 0;
                self.values.push(value);
                self.current_index += 1;
                self.current_present_index += 1;
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

pub enum PresentInfo {
    All,
    NullRuns(Vec<u64>),
}

impl PresentInfo {
    pub fn new(null_runs: Option<Vec<u64>>) -> PresentInfo {
        null_runs
            .map(PresentInfo::NullRuns)
            .unwrap_or(PresentInfo::All)
    }

    pub fn null_run(&self, index: usize) -> u64 {
        match self {
            PresentInfo::All => 0,
            PresentInfo::NullRuns(values) => *values.get(index).unwrap_or(&0),
        }
    }
}

pub struct PresentInfoWriter {
    row_count: u64,
    null_runs: Vec<u64>,
    current_total: u64,
    current_null_run_len: u64,
}

impl PresentInfoWriter {
    pub fn new(row_count: u64) -> Self {
        Self {
            row_count,
            null_runs: Vec::with_capacity(PRESENT_VALUE_CAPACITY),
            current_total: 0,
            current_null_run_len: 0,
        }
    }

    pub fn into_inner(mut self) -> Vec<u64> {
        self.null_runs.push(self.row_count - self.current_total);
        self.null_runs
    }
}

impl Write for PresentInfoWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        for b in buf {
            for i in 0..8 {
                if b & (1 << (7 - i)) == 0 {
                    self.current_null_run_len += 1;
                } else {
                    self.null_runs.push(self.current_null_run_len);
                    self.current_total += self.current_null_run_len + 1;
                    self.current_null_run_len = 0;
                }
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
