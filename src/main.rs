use std::{env, fs::{self, File}, io::Write, path::{Path, PathBuf}};
use serde::{Serialize, Deserialize};

use clap::{Parser, Subcommand};
use serde_json::to_string_pretty;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Could not obtain the current directory")]
    CurrentDir(#[from] std::io::Error),
    #[error("Could not open data at '{}'", .path.display())]
    IoOpen { source: std::io::Error, path: PathBuf },
    #[error("Could not write data at '{}'", .path.display())]
    IoWrite { source: std::io::Error, path: PathBuf },
    #[error("Refusing to initialize the existing '{}' directory", .path.display())]
    DirectoryExists { path: PathBuf },
    #[error("Refusing to initialize the non-empty directory as '{}'", .path.display())]
    DirectoryNotEmpty { path: PathBuf },
    #[error("Could not create directory at '{}'", .path.display())]
    CreateDirectory { source: std::io::Error, path: PathBuf },
}

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

const CONTAINER_VERSION: u32 = 1;
const LOOSE_PREFIX_LEN: u32 = 2;
const PACK_SIZE_TARGET: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize container folder to store objects
    Init,

    /// Get the status of container
    Status,

    /// Add files to container
    AddFiles {
        /// One or more paths to files to add
        #[arg(required = true, value_name = "FILE(s)")]
        paths: Vec<String>, 
    },

    /// Optimize the storage
    Optimize {
        /// Disable compress object
        #[arg(long, default_value_t=false)]
        no_compress: bool,

        /// Disable vacuum the databass
        #[arg(long, default_value_t=true)]
        no_vacuum: bool,

        // TODO: no interactive, do without ask
    },
}

fn create_dir(p: &Path) -> Result<(), Error> {
    fs::create_dir_all(p).map_err(|e| Error::CreateDirectory {
        source: e,
        path: p.to_owned(),
    })
}

#[derive(Serialize, Deserialize)]
struct Config {
    container_id: Uuid,
    container_version: u32,
    loose_prefix_len: u32,
    pack_size_target: u64,  // bytes 
    hash_type: String,
    compression_algorithm: String,
}

struct NewDir<'a>(&'a PathBuf);

impl<'a> NewDir<'a> {
    fn at(self, component: &str) -> Result<Self, Error> {
        let mut sub_folder = self.0.clone();
        sub_folder.push(component);
        create_dir(&sub_folder)?;
        Ok(self)
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // If container path provided, using it
    // otherwise assume the `container` folder of cwd
    let cnt_path = match args.path {
        Some(path) => path,
        _ => {
            let mut p = env::current_dir()?;
            p.push("container");
            p
        },
    };

    match args.cmd {
        Commands::Init => {
            // If already init (simply check as none empty), don't do it again
            let number_entries_in_cnt = fs::read_dir(&cnt_path)
                .or_else(|err| {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        fs::create_dir(&cnt_path).and_then(|_| fs::read_dir(&cnt_path))
                    } else {
                        Err(err)
                    }
                })
                .map_err(|err| Error::IoOpen {
                    source: err,
                    path: cnt_path.clone(),
                })?
                .count();
            if number_entries_in_cnt != 0 {
                Err(Error::DirectoryNotEmpty { path: cnt_path.clone() })?;
            }

            create_dir(&cnt_path)?;

            // XXX: check compress/decompress algo can be load?? (dos concept) useless for rust?
            
            // generate container_id to write into config (XXX: (dos concept) for unique but seems useless) 
            let id = uuid::Uuid::new_v4();
        
            // create config and serialize to json file
            let config = Config {
                container_id: id,
                container_version: CONTAINER_VERSION,
                loose_prefix_len: LOOSE_PREFIX_LEN, 
                pack_size_target: PACK_SIZE_TARGET,
                hash_type: "sha256".to_string(),
                compression_algorithm: "zlib+1".to_string(),
            };
            let json_string = to_string_pretty(&config)?;
            let mut config_path = cnt_path.clone();
            config_path.push("config.json");
            let mut config_file = File::create(config_path)?;
            config_file.write_all(json_string.as_bytes())?;
            
            // Create loose/pack/duplicates/sandbox folders
            let _ = NewDir(&cnt_path).at("loose");
            let _ = NewDir(&cnt_path).at("pack");
            let _ = NewDir(&cnt_path).at("duplicates");
            let _ = NewDir(&cnt_path).at("sandbox");

            // Create Sqlite DB for pack->idx mapping
        },
        Commands::Status => {
            println!("Check status of container");
        }
        Commands::AddFiles { paths } => {
            dbg!(paths);
        },
        Commands::Optimize { no_compress, no_vacuum } => {
            dbg!(no_compress, no_vacuum);
        }
    };

    Ok(())
}

