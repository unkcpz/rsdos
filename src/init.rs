use crate::config::{Config, CONFIG_FILE};
use crate::db::{create, PACKS_DB};
use crate::utils::{create_dir, Dir, Error};
use anyhow::Context;
use serde_json::to_string_pretty;

use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
};

const CONTAINER_VERSION: u32 = 1;
const LOOSE_PREFIX_LEN: u32 = 2;

pub fn init(cnt_path: &PathBuf, pack_size: u64) -> anyhow::Result<()> {
    let number_entries_in_cnt = fs::read_dir(cnt_path)
        .or_else(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                fs::create_dir(cnt_path).and_then(|()| fs::read_dir(cnt_path))
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
        Err(Error::DirectoryNotEmpty {
            path: cnt_path.clone(),
        })?;
    }

    create_dir(cnt_path)?;

    // generate container_id to write into config (XXX: (dos concept) for unique but seems useless)
    let id = uuid::Uuid::new_v4();

    // create config and serialize to json file
    // TODO: all these should able to be set from passed input parameters
    let config = Config {
        container_id: id,
        container_version: CONTAINER_VERSION,
        loose_prefix_len: LOOSE_PREFIX_LEN,
        pack_size_target: pack_size * 1024 * 1024, // GiB -> bytes
        hash_type: "sha256".to_string(),
        compression_algorithm: "zlib+1".to_string(),
    };
    let json_string = to_string_pretty(&config)?;
    let config = Dir(cnt_path).at_path(CONFIG_FILE);
    File::create(config.clone())?
        .write_all(json_string.as_bytes())
        .map_err(|err| Error::IoWrite {
            source: err,
            path: config.clone(),
        })?;

    // Create loose/pack/duplicates/sandbox folders
    Dir(cnt_path).new_folder("loose")?;
    Dir(cnt_path).new_folder("pack")?;
    Dir(cnt_path).new_folder("duplicates")?;
    Dir(cnt_path).new_folder("sandbox")?;

    // Create Sqlite DB for pack->idx mapping
    let db = Dir(cnt_path).at_path(PACKS_DB);

    create(&db).with_context(|| format!("create db at {}", db.to_string_lossy()))?;

    Ok(())
}
