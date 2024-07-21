mod init;

use std::{env, path::PathBuf};

use clap::{Parser, Subcommand};

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
        Commands::Init => {
            crate::init::init(&cnt_path)?;      
        }
        Commands::Status => {
            println!("Check status of container");
        }
        Commands::AddFiles { paths } => {
            dbg!(paths);
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
