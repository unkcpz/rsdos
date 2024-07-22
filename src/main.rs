mod config;
mod db;
mod init;
mod status;
mod utils;

use crate::utils::Dir;
use std::{
    env,
    fmt::Debug,
    fs,
    hash::Hash,
    io::{self, BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
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

/// Copy by chunk and return the size of content that copied
fn copy_by_chunk<R: Read + Debug, W: Write>(
    reader: &mut R,
    writer: &mut W,
    chunk_size: usize,
) -> anyhow::Result<usize> {
    let mut buf = Vec::with_capacity(chunk_size);
    buf.resize(chunk_size, b' ');
    let mut total_bytes_copied = 0;

    loop {
        let bytes_read = reader.read(&mut buf[..]).with_context(|| "read to buf")?;
        // EOF if bytes_read == 0, then stop and flush
        if bytes_read == 0 {
            break;
        }
        total_bytes_copied += bytes_read;
        writer
            .write_all(&buf[..bytes_read])
            .with_context(|| "write to writer")?;
    }

    writer.flush().with_context(|| "flush to buff writer")?;
    Ok(total_bytes_copied)
}

#[derive(Default)]
struct HashWriter<W, H> {
    writer: W,
    hasher: H,
}

impl<W, H> HashWriter<W, H>
where
    W: Write,
    H: Digest,
{
    fn new(writer: W, hasher: H) -> Self {
        Self { writer, hasher }
    }
}


impl<W, H> Write for HashWriter<W, H>
where
    W: Write,
    H: Digest,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.hasher.update(buf);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn add_file(file: &PathBuf, cnt_path: &PathBuf) -> anyhow::Result<()> {
    // stream file to loose object store
    // TODO: let object = Object::blob_from_file(file)
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    // Race here if file changes in between stat and open, the source may changed
    // in the end of add check the size from stat and copied is identical.
    let file = fs::File::open(file).with_context(|| format!("open {} for read", file.display()))?;
    let mut source = BufReader::new(file);

    let chunk_size = 524_288; // 512 MiB TODO: make it configurable??

    // <cnt_path>/sandbox/<uuid> as dst
    let dst = format!("{}.tmp", uuid::Uuid::new_v4());
    let dst = Dir(cnt_path).at_path("sandbox").join(dst);
    let writer =
        fs::File::create(&dst).with_context(|| format!("open {} for write", dst.display()))?;
    let writer = BufWriter::new(writer);

    let hasher = Sha256::new();
    let mut hwriter = HashWriter::new(writer, hasher);

    // write to object and store it in {hash:..2}/{hash:2..} file
    // first write to tmp and get the hash, than move it to the location.
    // TODO: benchmark me (on large amount of data) whether do direct copy if it is a small file < 4M??
    let bytes_copied = copy_by_chunk(&mut source, &mut hwriter, chunk_size)?;
    let hash = hwriter.hasher.finalize();
    let hash_hex = hex::encode(hash);

    anyhow::ensure!(
        bytes_copied == expected_size as usize,
        format!(
            "bytes copied: {}, bytes source: {}",
            bytes_copied, expected_size
        )
    );


    let loose = Dir(cnt_path).at_path("loose");
    fs::create_dir_all(loose.join(format!("{}/", &hash_hex[..2])))?;
    let loose_dst = loose.join(format!("{}/{}", &hash_hex[..2], &hash_hex[2..]));
    fs::rename(&dst, &loose_dst)
        .with_context(|| format!("move from {} to {}", dst.display(), loose_dst.display()))?;

    Ok(())
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

                add_file(&path, &cnt_path)?;
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
