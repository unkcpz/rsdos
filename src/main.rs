mod add_files;
mod config;
mod db;
mod init;
mod status;
mod utils;

use clap::{Parser, Subcommand};
use std::{env, fmt::Debug, path::PathBuf};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the repository where container locate, <cwd> if not specified
    #[arg(short, long, value_name = "FOLDER")]
    path: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize container folder to store objects
    Init {
        /// Pack size (in GiB)
        #[arg(short, long, default_value_t = 4, value_name = "PACK_SIZE")]
        pack_size: u64,
    },

    /// Get the status of container
    Status,

    /// Add files to container
    AddFiles {
        /// One or more paths to files to add
        #[arg(required = true, value_name = "FILE(s)")]
        paths: Vec<PathBuf>,
    },

    /// Optimize the storage
    Optimize {
        /// Disable compress object
        #[arg(long, default_value_t = false)]
        no_compress: bool,

        /// Disable vacuum the databass
        #[arg(long, default_value_t = true)]
        no_vacuum: bool,
        // TODO: no interactive, do without ask
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // If container path provided, using it
    // otherwise assume the `container` folder of cwd
    let cnt_path = args.path.unwrap_or(env::current_dir()?.join("container"));

    match args.cmd {
        Commands::Init { pack_size } => {
            crate::init::init(&cnt_path, pack_size)?;
        }
        Commands::Status => {
            crate::status::status(&cnt_path)?;
        }
        Commands::AddFiles { paths } => {
            for path in paths {
                if !path.is_file() {
                    eprintln!("{} is not a file, skipped", path.display());
                    continue;
                }

                crate::add_files::add_file(&path, &cnt_path)?;
            }
        }
        Commands::Optimize {
            no_compress,
            no_vacuum,
        } => {
            dbg!(no_compress, no_vacuum);
        }
        _ => todo!(), // validate/backup subcommands
    };

    Ok(())
}
