use anyhow::Context;
use clap::{Parser, Subcommand};
use human_bytes::human_bytes;
use std::{
    env,
    fmt::Debug,
    path::PathBuf,
};

use std::io::{self, Write};

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

    CatFile {
        #[arg(required = true)]
        object_hash: String,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // If container path provided, using it
    // otherwise assume the `container` folder of cwd
    let cnt_path = args.path.unwrap_or(env::current_dir()?.join("container"));

    match args.cmd {
        Commands::Init { pack_size } => {
            disk_objectstore::init(&cnt_path, pack_size)?;
        }
        Commands::Status => {
            let info = disk_objectstore::stat(&cnt_path)
                .with_context(|| "unable to get container stat")?;
            // print status to stdout
            let state = String::new()
                        // container info
                        + "[container]\n"
                        + &format!("Location = {}\n", info.location)
                        + &format!("Id = {}\n", info.id)
                        + &format!("ZipAlgo = {}\n", info.compression_algorithm)
                        // count
                        + "\n[container.count]\n"
                        + &format!("Loose = {}\n", info.count.loose)
                        + &format!("Packes = {}\n", info.count.packs)
                        + &format!("Pack Files = {}\n", info.count.packs_file)
                        // size
                        + "\n[container.size]\n"
                        + &format!("Loose = {}\n", human_bytes(info.size.loose as f64))
                        + &format!("Packs = {}\n", human_bytes(info.size.packs as f64))
                        + &format!("Packs Files = {}\n", human_bytes(info.size.packs_file as f64))
                        + &format!("Packs DB = {}\n", human_bytes(info.size.packs_db as f64));

            io::stdout().write_all(state.as_bytes())?;
        }
        Commands::AddFiles { paths } => {
            for path in paths {
                if !path.is_file() {
                    eprintln!("Error: {} is not a file, skipped", path.display());
                    continue;
                }

                disk_objectstore::add_file(&path, &cnt_path)?;
            }
        }
        Commands::Optimize {
            no_compress,
            no_vacuum,
        } => {
            dbg!(no_compress, no_vacuum);
        }
        Commands::CatFile { object_hash } => {
            let mut obj = disk_objectstore::Object::from_hash(&object_hash, &cnt_path)?;
            let n = std::io::copy(&mut obj.reader, &mut std::io::stdout())
                .with_context(|| "write object to stdout")?;

            anyhow::ensure!(
                n == obj.expected_size,
                "file was not the expecwed size, expected: {}, got: {}",
                obj.expected_size,
                n
            );
        }
        _ => todo!(), // validate/backup subcommands
    };

    Ok(())
}
