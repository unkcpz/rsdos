use anyhow::Context;
use std::ffi::OsString;
use std::{fs, io::BufReader, path::PathBuf};

use crate::container::{
    traverse_loose, traverse_packs, Container, ContainerInfo, CountInfo, SizeInfo,
};
use crate::io::ReaderMaker;
use crate::io_loose::insert as loose_insert;
use crate::io_packs::insert as packs_insert;
use crate::Error;

use crate::config::Config;
use crate::db::{self};

use crate::container::Compression;
use crate::utils::create_dir;
use clap::{Parser, Subcommand};
use human_bytes::human_bytes;
use std::str::FromStr;
use std::{env, fmt::Debug};

use std::io::{self, Write};

pub const DEFAULT_COMPRESSION_ALGORITHM: &str = "zlib:+1";

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
enum OptimizeCommands {
    /// pack objects from loose to packed store
    Pack {
        /// Disable compress object, default compression algorithm will be used.
        #[arg(long, default_value_t = false)]
        no_compress: bool,

        /// Disable clean up after pack, clean up will delete duplicate objects in loose and vacuum
        /// the DB.
        #[arg(long, default_value_t = false)]
        no_clean: bool,
    },

    /// repack objects in packs
    Repack {
        /// Compression algorithm for repack
        #[arg(short, long, default_value = DEFAULT_COMPRESSION_ALGORITHM, value_name = "COMPRESSION")]
        compression: String,
    },
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
        #[arg(short, long, default_value = "auto", value_name = "TO")]
        to: String,
    },

    /// Optimize the storage
    Optimize {
        #[command(subcommand)]
        cmd: OptimizeCommands,
    },

    CatFile {
        #[arg(required = true)]
        id: String,

        /// Target store type, `loose`/`packs` to add to loose/packs.
        /// Use `auto` (default) if you don't know.
        #[arg(short, long, default_value = "auto", value_name = "FROM")]
        from: String,
    },
}

fn extract(
    id: &str,
    cnt: &Container,
    st: &StoreType,
    mut to: impl Write,
) -> anyhow::Result<Option<u64>> {
    let n = match st {
        StoreType::Loose => _extract_l(id, cnt, to)?,
        StoreType::Packs => _extract_p(id, cnt, to)?,
        StoreType::Auto => {
            // first lookup in loose, if not found lookup in packed
            _extract_l(id, cnt, &mut to)?.or_else(|| _extract_p(id, cnt, &mut to).ok()?)
        }
    };
    Ok(n)
}

fn _extract_l(id: &str, cnt: &Container, mut to: impl Write) -> anyhow::Result<Option<u64>> {
    let obj = crate::io_loose::extract(id, cnt)?;
    if let Some(obj) = obj {
        let rdr = obj.make_reader()?;
        let mut buf_rdr = BufReader::new(rdr);
        let n = std::io::copy(&mut buf_rdr, &mut to).with_context(|| "write object to stdout")?;

        // TODO: (v2) checksum
        anyhow::ensure!(
            n == obj.expected_size,
            "object has wrong size, expected: {}, got: {}, usually caused by data corruption",
            obj.expected_size,
            n
        );
        Ok(Some(n))
    } else {
        Ok(None)
    }
}

fn _extract_p(id: &str, cnt: &Container, mut to: impl Write) -> anyhow::Result<Option<u64>> {
    let obj = crate::io_packs::extract(id, cnt)?;
    if let Some(obj) = obj {
        let rdr = obj.make_reader()?;
        let mut buf_rdr = BufReader::new(rdr);
        let n = std::io::copy(&mut buf_rdr, &mut to).with_context(|| "write object to stdout")?;
        // TODO: (v2) checksum
        anyhow::ensure!(
            n == obj.raw_size,
            "object has wrong size, expected: {}, got: {}, usually caused by data corruption",
            obj.raw_size,
            n
        );
        Ok(Some(n))
    } else {
        Ok(None)
    }
}

pub enum StoreType {
    Auto,
    Loose,
    Packs,
}

pub fn add_file(
    file: &PathBuf,
    cnt: &Container,
    to: &StoreType,
) -> anyhow::Result<(String, String, u64)> {
    // Race here if file changes in between stat and push, the source may changed
    // in the end of add check, the size from stat and copied should be identical.
    // that is why we do streamed size check in the end.
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    let (bytes_streamd, hash_hex) = match to {
        StoreType::Loose | StoreType::Auto => loose_insert(file.clone(), cnt)?,
        StoreType::Packs => packs_insert(file.clone(), cnt)?,
    };

    anyhow::ensure!(
        bytes_streamd == expected_size,
        format!(
            "bytes streamed: {}, bytes source: {}",
            bytes_streamd, expected_size
        )
    );

    Ok((hash_hex, file.display().to_string(), expected_size))
}

pub fn stat(cnt: &Container) -> anyhow::Result<ContainerInfo> {
    cnt.valid()?;

    // Read config.json
    let config_path = cnt.config_file();
    let config = fs::File::open(&config_path).map_err(|err| Error::ConfigFileError {
        source: err,
        path: config_path.clone(),
    })?;
    let reader = BufReader::new(config);

    // read config
    let config: Config = serde_json::from_reader(reader).map_err(|err| Error::ConfigFileError {
        source: err.into(),
        path: config_path.clone(),
    })?;

    // traverse loose and compute number of objects and total size
    let iter_loose = traverse_loose(cnt).with_context(|| "traverse loose by iter")?;
    let (loose_files_count, loose_files_size) =
        iter_loose
            .into_iter()
            .fold((0, 0), |(count, size), path| match fs::metadata(path) {
                Ok(stat) => (count + 1, size + stat.len()),
                Err(_) => (count, size),
            });

    // packs info from db
    let packs_db = cnt.packs_db();
    let packs_db_size = fs::metadata(&packs_db)?.len();
    let (packs_count, packs_size) = db::stats(&packs_db)?;

    // traverse packs and compute
    let iter_packs = traverse_packs(cnt).with_context(|| "traverse packs by iter")?;
    let (packs_file_count, packs_file_size) =
        iter_packs
            .into_iter()
            .fold((0, 0), |(count, size), path| match fs::metadata(path) {
                Ok(stat) => (count + 1, size + stat.len()),
                Err(_) => (count, size),
            });

    Ok(ContainerInfo {
        location: cnt.path.display().to_string(),
        id: config.container_id.to_string(),
        compression_algorithm: config.compression_algorithm,
        count: CountInfo {
            // number of loose objs
            loose: loose_files_count,
            // number of pack objs
            // FIXME: rename -> pack
            packs: packs_count,
            // number of pack files
            packs_file: packs_file_count,
        },
        size: SizeInfo {
            // total size of all loose objs
            loose: loose_files_size,
            // total size of all pack objs
            packs: packs_size,
            // total size of all pack files
            packs_file: packs_file_size,
            // size of pack index db file
            packs_db: packs_db_size,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write};
    use tempfile::NamedTempFile;

    use crate::{
        io_packs,
        test_utils::{new_container, PACK_TARGET_SIZE},
    };

    use super::*;

    #[test]
    fn cli_add_ten_diff_objs_to_loose() {
        let (_tmp_dir, cnt) = new_container(PACK_TARGET_SIZE, "none");

        // add 10 different files to loose
        for i in 0..10 {
            // Note: security view the test is short term so safe to use NamedTempFile.
            let mut tf = NamedTempFile::new().unwrap();
            write!(tf, "test {i}").unwrap();

            let fp = tf.into_temp_path();
            add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose).expect("unable to add file {i}");
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.loose, 10);
    }

    /// Default lifecycle:
    /// Create 10 same loose objects
    /// regression check: get the obj content by hash and compute hash is the same
    #[test]
    fn cli_add_ten_same_objs_to_loose() {
        let (_tmp_dir, cnt) = new_container(PACK_TARGET_SIZE, "none");

        // add 10 different files to loose
        for _i in 0..10 {
            // Note: security view the test is short term so safe to use NamedTempFile.
            let mut tf = NamedTempFile::new().unwrap();
            write!(tf, "test x").unwrap();

            let fp = tf.into_temp_path();
            let _ = add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose)
                .expect("unable to add file {i}");
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.loose, 1);
    }

    #[test]
    fn cli_add_ten_diff_objs_to_packs() -> anyhow::Result<()> {
        let (_tmp_dir, cnt) = new_container(PACK_TARGET_SIZE, "none");

        let orig_objs: HashMap<String, String> = (0..10)
            .map(|i| {
                let content = format!("test {i}");
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "test {i}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        //
        for (hash_hex, expected_content) in orig_objs {
            // find content from packs file
            let obj = io_packs::extract(&hash_hex, &cnt)?.unwrap();
            assert_eq!(
                String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
                expected_content
            );
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 10);

        Ok(())
    }

    #[test]
    fn cli_add_ten_same_objs_to_packs() -> anyhow::Result<()> {
        let (_tmp_dir, cnt) = new_container(PACK_TARGET_SIZE, "none");

        // insert 10 identical object to packs
        let orig_objs: HashMap<String, String> = (0..10)
            .map(|_| {
                let content = "test".to_string();
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "{content}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        //
        for (hash_hex, expected_content) in orig_objs {
            // find content from packs file
            let obj = io_packs::extract(&hash_hex, &cnt)?.unwrap();
            assert_eq!(
                String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
                expected_content
            );
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 1);

        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
pub fn run_cli(args: &[OsString]) -> anyhow::Result<()> {
    let args = Args::parse_from(args);
    // If container path provided, using it
    // otherwise assume the `container` folder of cwd
    let cnt_path = args.path.unwrap_or(env::current_dir()?.join("container"));

    match args.cmd {
        Commands::Init {
            pack_size,
            compression,
        } => {
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

            let info = crate::stat(cnt).with_context(|| "unable to get container stat")?;
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
        Commands::AddFiles { paths, to } => {
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

                let to = match to.as_str() {
                    "auto" => StoreType::Auto,
                    "loose" => StoreType::Loose,
                    "packs" => StoreType::Packs,
                    _ => {
                        eprintln!("unknown store '{to}', expect 'auto', 'loose' or 'packs'");
                        std::process::exit(1);
                    }
                };
                let (hash_hex, filename, expected_size) = add_file(&path, cnt, &to)?;
                println!(
                    "{} - {}: {}",
                    hash_hex,
                    filename,
                    human_bytes(expected_size as f64)
                );
            }
        }
        Commands::Optimize { cmd } => {
            match cmd {
                OptimizeCommands::Pack {
                    no_compress,
                    no_clean,
                } => {
                    let cnt = Container::new(&cnt_path);
                    let cnt = match cnt.valid() {
                        Ok(cnt) => cnt,
                        Err(e) => anyhow::bail!(e),
                    };
                    // get
                    let compression = if no_compress {
                        Compression::from_str("none")?
                    } else {
                        Compression::from_str(DEFAULT_COMPRESSION_ALGORITHM)?
                    };

                    crate::maintain::_pack_loose_internal(cnt, &compression).unwrap_or_else(
                        |err| {
                            eprintln!("failed on pack loose {err}");
                            std::process::exit(1);
                        },
                    );

                    // TODO: clean loose that already packed
                    if !no_clean {
                        todo!()
                    }
                }
                OptimizeCommands::Repack { compression } => {
                    todo!()
                }
            }
        }
        Commands::CatFile { id, from } => {
            let cnt = crate::Container::new(&cnt_path);
            let from = match from.as_str() {
                "auto" => StoreType::Auto,
                "loose" => StoreType::Loose,
                "packs" => StoreType::Packs,
                _ => {
                    eprintln!("unknown store '{from}', expect 'auto', 'loose' or 'packs'");
                    std::process::exit(1);
                }
            };
            let mut to = std::io::stdout();
            let n = extract(&id, &cnt, &from, &mut to)?;

            if n.is_none() {
                eprintln!("object {id} not found");
                std::process::exit(1)
            }
        } // TODO: validate/backup subcommands
    };

    Ok(())
}
