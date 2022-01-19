use clap::Parser;
use orcrs::{parser::OrcFile, value::Value};
use simplelog::LevelFilter;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("Parser error")]
    Parser(#[from] orcrs::parser::Error),
    #[error("CSV writing error")]
    Csv(#[from] csv::Error),
    #[error("Missing value")]
    MissingValue { stripe: u64, row: u64, column: u64 },
}

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let _ = init_logging(opts.verbose);

    match opts.command {
        Command::Export {
            format: _,
            columns,
            header,
            null: null_string_value,
            path,
        } => {
            let mut writer = csv::Writer::from_writer(std::io::stdout());
            let mut orc_file = OrcFile::open(&path)?;
            let column_indices = columns.and_then(|value| parse_column_indices(&value));

            if header {
                let footer = orc_file.get_footer();

                if let Some(field_names) = footer.get_types().get(0).and_then(|struct_type| {
                    let column_indices = match column_indices {
                        Some(ref value) => value.clone(),
                        None => (0..struct_type.get_fieldNames().len() as u32).collect(),
                    };
                    column_indices
                        .iter()
                        .map(|i| struct_type.get_fieldNames().get(*i as usize))
                        .collect::<Option<Vec<_>>>()
                }) {
                    writer.write_record(field_names)?;
                } else {
                    log::warn!("A header was requested but field names could not be found.")
                }
            }

            for (stripe_index, stripe_info) in orc_file.get_stripe_info()?.iter().enumerate() {
                let column_indices = match column_indices {
                    Some(ref value) => value.clone(),
                    None => (0..stripe_info.get_column_count() as u32).collect(),
                };
                let columns = column_indices
                    .iter()
                    .map(|i| orc_file.read_column(stripe_info, *i))
                    .collect::<Result<Vec<_>, _>>()?;

                for row_index in 0..stripe_info.get_row_count() as usize {
                    let record = columns
                        .iter()
                        .zip(&column_indices)
                        .map(|(column, column_index)| {
                            match column.get(row_index).ok_or(Error::MissingValue {
                                stripe: stripe_index as u64,
                                row: row_index as u64,
                                column: *column_index as u64,
                            })? {
                                Value::Null => Ok(null_string_value.clone()),
                                Value::Bool(value) => Ok(value.to_string()),
                                Value::U64(value) => Ok(value.to_string()),
                                Value::Utf8(value) => Ok(value.to_string()),
                            }
                        })
                        .collect::<Result<Vec<_>, Error>>()?;

                    writer.write_record(record)?;
                }
            }

            writer.flush()?;
        }
        Command::Info { path } => {
            let mut orc_file = OrcFile::open(&path)?;
            let footer = orc_file.get_footer();
            println!("Footer: {:?}\n================", footer);

            for (i, (stripe_footer, stripe_info)) in orc_file
                .get_stripe_footers()?
                .iter()
                .zip(orc_file.get_stripe_info()?)
                .enumerate()
            {
                println!("Stripe {} footer: {:?}\n----------------", i, stripe_footer);
                println!("Stripe {} info: {:?}\n================", i, stripe_info);
            }
        }
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "orcrs", about, version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

const EXPORT_FORMATS: &[&str] = &["csv"];

#[derive(Parser)]
enum Command {
    /// Export the contents of the ORC file
    Export {
        /// Export format
        #[clap(short, long, default_value = "csv", possible_values(EXPORT_FORMATS))]
        format: String,
        /// Column indices (comma-separated list of numbers)
        #[clap(short, long)]
        columns: Option<String>,
        /// Include header
        #[clap(short, long)]
        header: bool,
        /// String to use for null values
        #[clap(long, default_value = "")]
        null: String,
        /// ORC file
        path: String,
    },
    /// Dump raw info about the ORC file
    Info {
        /// ORC file
        path: String,
    },
}

fn select_log_level_filter(verbosity: i32) -> LevelFilter {
    match verbosity {
        0 => LevelFilter::Off,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    }
}

fn init_logging(verbosity: i32) -> Result<(), log::SetLoggerError> {
    simplelog::TermLogger::init(
        select_log_level_filter(verbosity),
        simplelog::Config::default(),
        simplelog::TerminalMode::Stderr,
        simplelog::ColorChoice::Auto,
    )
}

fn parse_column_indices(input: &str) -> Option<Vec<u32>> {
    match input
        .split(',')
        .map(|value| value.trim().parse::<u32>())
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(values) => Some(values),
        Err(_) => {
            log::warn!("Cannot parse columns argument; using all columns.");
            None
        }
    }
}
