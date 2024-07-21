use human_bytes::human_bytes;
use indicatif::{ProgressBar, ProgressIterator};
use std::path::PathBuf;

use crate::config::{Config, CONFIG_FILE};
use crate::db::{self, PACKS_DB};
use crate::utils::{Dir, Error};

use std::io::Write;
use std::{fs, io, result};

pub fn status(cnt_path: &PathBuf) -> anyhow::Result<()> {
    // Check cnt_path is exist
    if !cnt_path.is_dir() {
        Err(Error::ObtainContainerDir {
            path: cnt_path.clone(),
        })?;
    }

    // Read config.json
    let config_path = Dir(cnt_path).at_path(CONFIG_FILE);
    let config = fs::File::open(&config_path).map_err(|err| Error::ConfigFileRead {
        source: err,
        path: config_path.clone(),
    })?;
    let reader = io::BufReader::new(config);

    // read config
    let config: Config = serde_json::from_reader(reader).map_err(|err| Error::ConfigFileRead {
        source: err.into(),
        path: config_path.clone(),
    })?;

    // traverse loose
    let loose = Dir(cnt_path).at_path("loose");
    let (loose_files_count, loose_files_size) = loose
        .read_dir()?
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .flat_map(|path| {
            path.read_dir()
                .unwrap_or_else(|_| panic!("unable to read {}", path.to_string_lossy()))
        })
        .progress_with(ProgressBar::new_spinner())
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .fold((0, 0), |(count, size), path| match fs::metadata(path) {
            Ok(stat) => (count + 1, size + stat.len()),
            Err(_) => (count, size),
        });

    // traverse packs
    let packs = Dir(cnt_path).at_path("packs");
    let packs_db_size = fs::metadata(&packs)?.len();
    let (packed_count, packed_size) = db::stats(&Dir(cnt_path).at_path(PACKS_DB))?;

    let (pack_files_count, packed_files_size) = packs
        .read_dir()?
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .fold((0, 0), |(count, size), path| match fs::metadata(path) {
            Ok(stat) => (count + 1, size + stat.len()),
            Err(_) => (count, size),
        });
    
    // print status to stdout
    let state = String::new()
                // container info
                + "[container]\n"
                + &format!("Path = {}\n", cnt_path.display())
                + &format!("Id = {}\n", config.container_id)
                + &format!("ZipAlgo = {}\n", config.compression_algorithm)
                // count
                + "\n[container.count]\n"
                + &format!("Loose = {loose_files_count}\n")
                + &format!("Packed = {packed_count}\n")
                + &format!("Pack Files = {pack_files_count}\n")
                // size
                + "\n[container.size]\n"
                + &format!("Loose = {}\n", human_bytes(loose_files_size as f64))
                + &format!("Packed = {}\n", human_bytes(packed_size as f64))
                + &format!("Packed Files = {}\n", human_bytes(packed_files_size as f64))
                + &format!("Packs DB = {}\n", human_bytes(packs_db_size as f64));
    io::stdout().write_all(state.as_bytes())?;

    Ok(())
}
