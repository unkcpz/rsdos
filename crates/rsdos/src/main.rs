use anyhow::Context;
use clap::{Parser, Subcommand};
use human_bytes::human_bytes;
use rsdos::cli::StoreType;
use rsdos::io::ReaderMaker;
use rsdos::{config::Config, utils::create_dir, Container};
use std::{env, fmt::Debug, path::PathBuf};

use std::io::{self, BufReader, Write};

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
        
        /// Compression algorithm none for not compressing data or 
        /// (format: <zalgo>:<level>, such as: zlib:+1 or zstd:-2)
        #[arg(short, long, default_value = "zstd:+1", value_name = "COMPRESSION")]
        compression: String,
    },

    /// Get the status of container
    Status,

    /// Add files to container
    AddFiles {
        /// One or more paths to files to add
        #[arg(required = true, value_name = "FILE(s)")]
        paths: Vec<PathBuf>,

        /// Target store type, `loose`/`packs` to add to loose/packs.
        /// Use `auto` (default) if you don't know.
        #[arg(short, long, default_value = "auto", value_name = "DEST")]
        dest: String,
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
        Commands::Init { pack_size, compression } => {
            // if target not exist create folder
            if !cnt_path.exists() {
                create_dir(&cnt_path)?;
            }

            let config = Config::new(pack_size, &compression);
            let cnt = Container::new(&cnt_path);
            cnt.initialize(&config).with_context(|| {
                format!("unable to initialize container at {}", cnt.path.display())
            })?;
        }
        #[allow(clippy::cast_precision_loss)]
        Commands::Status => {
            let cnt = Container::new(&cnt_path);
            let cnt = match cnt.valid() {
                Ok(cnt) => cnt,
                Err(e) => anyhow::bail!(e),
            };

            let info = rsdos::stat(cnt).with_context(|| "unable to get container stat")?;
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
        #[allow(clippy::cast_precision_loss)]
        Commands::AddFiles { paths, dest } => {
            let cnt = Container::new(&cnt_path);
            let cnt = match cnt.valid() {
                Ok(cnt) => cnt,
                Err(e) => anyhow::bail!(e),
            };

            for path in paths {
                if !path.is_file() {
                    eprintln!("Error: {} is not a file, skipped", path.display());
                    continue;
                }

                let dest = match dest.as_str() {
                    "auto" => StoreType::Auto,
                    "loose" => StoreType::Loose,
                    "packs" => StoreType::Packs,
                    _ => {
                        eprintln!("unknown dest '{dest}', expect 'auto', 'loose' or 'packs'");
                        std::process::exit(1);
                    }
                };
                let (hash_hex, filename, expected_size) = rsdos::add_file(&path, cnt, &dest)?;
                println!(
                    "{} - {}: {}",
                    hash_hex,
                    filename,
                    human_bytes(expected_size as f64)
                );
            }
        }
        Commands::Optimize {
            no_compress,
            no_vacuum,
        } => {
            dbg!(no_compress, no_vacuum);
        }
        Commands::CatFile { object_hash } => {
            let cnt = rsdos::Container::new(&cnt_path);
            let obj = rsdos::io_loose::extract(&object_hash, &cnt)?;
            match obj {
                Some(obj) => {
                    let mut buf_rdr = BufReader::new(obj.make_reader()?); 
                    let n = std::io::copy(&mut buf_rdr, &mut std::io::stdout())
                        .with_context(|| "write object to stdout")?;

                    anyhow::ensure!(
                        n == obj.expected_size,
                        "file has wrong size, expected: {}, got: {}, usually caused by data corruption",
                        obj.expected_size,
                        n
                    );
                }
                _ => {
                    eprintln!("object {object_hash} not found");
                }
            }
        } // TODO: validate/backup subcommands
    };

    Ok(())
}
