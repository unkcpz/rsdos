use anyhow::Context;
use core::panic;
use indicatif::{ProgressBar, ProgressIterator};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io, result};

use crate::config::{Config, CONFIG_FILE};
use crate::db::{self, PACKS_DB};
use crate::utils::{Dir, Error};

#[derive(Debug)]
pub struct ContainerInfo {
    pub location: String,
    pub id: String,
    pub compression_algorithm: String,
    pub count: CountInfo,
    pub size: SizeInfo,
}

#[derive(Debug)]
pub struct CountInfo {
    pub loose: u64,
    pub packs: u64,
    pub packs_file: u64,
}

#[derive(Debug)]
pub struct SizeInfo {
    pub loose: u64,
    pub packs: u64,
    pub packs_file: u64,
    pub packs_db: u64,
}

pub fn traverse_loose(cnt_path: &PathBuf) -> anyhow::Result<impl Iterator<Item = PathBuf>> {
    let spinnner = ProgressBar::new_spinner().with_message("Auditing container stat ...");
    spinnner.enable_steady_tick(Duration::from_millis(500));

    let loose = Dir(cnt_path).at_path("loose");
    Ok(loose
        .read_dir()?
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        // .filter(|path| path.is_dir()) // NOTE: this slow down the bin by ~10 % of system time.
        .flat_map(|path| {
            path.read_dir()
                .unwrap_or_else(|_| panic!("unable to read {}", path.display()))
        })
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        // .filter(|path| path.is_file())
        .progress_with(spinnner))
}

pub fn traverse_packs(cnt_path: &PathBuf) -> anyhow::Result<impl Iterator<Item = PathBuf>> {
    let spinnner = ProgressBar::new_spinner().with_message("Auditing container stat ...");
    spinnner.enable_steady_tick(Duration::from_millis(500));

    let packs = Dir(cnt_path).at_path("packs");
    Ok(packs
        .read_dir()
        .with_context(|| format!("not able to read dir {}", packs.display()))?
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        // .filter(|path| path.is_file())
        .progress_with(spinnner))
}

pub fn stat(cnt_path: &PathBuf) -> anyhow::Result<ContainerInfo> {
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

    // traverse loose and compute number of objects and total size
    let iter_loose = traverse_loose(cnt_path).with_context(|| "traverse loose by iter")?;
    let (loose_files_count, loose_files_size) =
        iter_loose
            .into_iter()
            .fold((0, 0), |(count, size), path| match fs::metadata(path) {
                Ok(stat) => (count + 1, size + stat.len()),
                Err(_) => (count, size),
            });

    // packs info from db
    let packs_db = Dir(cnt_path).at_path(PACKS_DB);
    let packs_db_size = fs::metadata(&packs_db)?.len();
    let (packs_count, packs_size) = db::stats(&packs_db)?;

    // traverse packs and compute
    let iter_packs = traverse_packs(cnt_path).with_context(|| "traverse packs by iter")?;
    let (packs_file_count, packs_file_size) = iter_packs
        .into_iter()
        .fold((0, 0), |(count, size), path| match fs::metadata(path) {
            Ok(stat) => (count + 1, size + stat.len()),
            Err(_) => (count, size),
        });

    Ok(ContainerInfo {
        location: cnt_path.display().to_string(),
        id: config.container_id.to_string(),
        compression_algorithm: config.compression_algorithm,
        count: CountInfo {
            loose: loose_files_count,
            packs: packs_count,
            packs_file: packs_file_count,
        },
        size: SizeInfo {
            loose: loose_files_size,
            packs: packs_size,
            packs_file: packs_file_size,
            packs_db: packs_db_size,
        },
    })
}
