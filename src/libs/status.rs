use anyhow::Context;
use indicatif::{ProgressBar, ProgressIterator};
use std::{fs, io, result};
use std::path::PathBuf;
use std::time::Duration;

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

    // traverse loose
    let loose = Dir(cnt_path).at_path("loose");
    let spinnner = ProgressBar::new_spinner().with_message("Auditing container stat ...");
    spinnner.enable_steady_tick(Duration::from_millis(500));
    let (loose_files_count, loose_files_size) = loose
        .read_dir()?
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        // .filter(|path| path.is_dir()) // NOTE: this slow down the bin by ~10 % of system time.
        .flat_map(|path| {
            path.read_dir()
                .unwrap_or_else(|_| panic!("unable to read {}", path.to_string_lossy()))
        })
        .progress_with(spinnner)
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        // .filter(|path| path.is_file())
        .fold((0, 0), |(count, size), path| match fs::metadata(path) {
            Ok(stat) => (count + 1, size + stat.len()),
            Err(_) => (count, size),
        });

    // traverse packs
    let packs_db = Dir(cnt_path).at_path(PACKS_DB);
    let packs_db_size = fs::metadata(&packs_db)?.len();
    let (packs_count, packs_size) = db::stats(&packs_db)?;

    let packs = Dir(cnt_path).at_path("packs");
    let (packs_file_count, packs_file_size) = packs
        .read_dir()
        .with_context(|| format!("not able to read dir {}", packs.display()))?
        .filter_map(result::Result::ok)
        .map(|entry| entry.path())
        // .filter(|path| path.is_file())
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
