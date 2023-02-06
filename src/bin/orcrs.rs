use clap::{ArgAction, Parser};
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
            let field_names = orc_file.get_field_names();

            let column_indices = match columns.and_then(|value| parse_column_indices(&value)) {
                Some(ref value) => value.clone(),
                None => (0..field_names.len()).collect(),
            };

            if header {
                if let Some(field_names) = column_indices
                    .iter()
                    .map(|i| field_names.get(*i))
                    .collect::<Option<Vec<_>>>()
                {
                    writer.write_record(field_names)?;
                } else {
                    log::warn!("A header was requested but field names could not be found.")
                }
            }

            for record in orc_file.map_rows(&column_indices, |values| {
                values
                    .iter()
                    .map(|value| match value {
                        Value::Null => Ok(null_string_value.clone()),
                        Value::Bool(value) => Ok(value.to_string()),
                        Value::U64(value) => Ok(value.to_string()),
                        Value::Utf8(value) => Ok(escape(value)),
                    })
                    .collect::<Result<Vec<_>, Error>>()
            })? {
                let record = record?;
                writer.write_record(record)?;
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
        Command::Validate { path } => match OrcFile::open(&path) {
            Ok(_) => {}
            Err(error) => {
                log::error!("Error in {}: {:?}", path, error);
                std::process::exit(1);
            }
        },
    }

    Ok(())
}

#[derive(Parser)]
#[clap(name = "orcrs", about, version, author)]
struct Opts {
    /// Level of verbosity
    #[clap(short, long, global = true, action = ArgAction::Count)]
    verbose: i32,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    /// Export the contents of the ORC file
    Export {
        /// Export format
        #[clap(short, long, default_value = "csv", value_parser(["csv"]))]
        format: String,
        /// Column indices (comma-separated list of numbers)
        #[clap(short, long)]
        columns: Option<String>,
        /// Include header
        #[clap(long)]
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
    /// Validate the ORC file footer
    Validate {
        /// ORC file
        path: String,
    },
}

fn escape(input: &str) -> String {
    input.replace('\n', "\\n")
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

fn parse_column_indices(input: &str) -> Option<Vec<usize>> {
    match input
        .split(',')
        .map(|value| value.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(values) => Some(values),
        Err(_) => {
            log::warn!("Cannot parse columns argument; using all columns.");
            None
        }
    }
}
