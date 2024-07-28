use anyhow::Context;
use serde_json::to_string_pretty;

use crate::utils;
use crate::{config::Config, db, utils::Dir};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub struct Container {
    pub path: PathBuf,
}

const PACKS_DB: &str = "packs.idx";
const CONFIG_FILE: &str = "config.json";
const LOOSE: &str = "loose";
const PACKS: &str = "packs";
const DUPLICATES: &str = "duplicates";
const SANDBOX: &str = "sandbox";

#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Refusing to initialize in non-empty directory as '{}'", .path.display())]
    DirectoryNotEmpty { path: PathBuf },
    #[error("Could not obtain the container directory at {}", .path.display())]
    ObtainContainerDir { path: PathBuf },
    #[error("Could not read the container config file at {}", .path.display())]
    ConfigFileRead {
        source: std::io::Error,
        path: PathBuf,
    },
}

impl Container {
    pub fn new<P: AsRef<Path>>(path: P) -> Container {
        Container {
            path: path.as_ref().to_owned(),
        }
    }

    pub fn initialize(&self, config: &Config) -> anyhow::Result<()> {
        if Dir(&self.path).is_empty()? {
            let json_string = to_string_pretty(&config)?;
            let config = self.path.join(CONFIG_FILE);
            fs::File::create(config.clone())?
                .write_all(json_string.as_bytes())
                .map_err(|err| utils::Error::IoWrite {
                    source: err,
                    path: config.clone(),
                })?;

            // Create loose/pack/duplicates/sandbox folders
            Dir(&self.path).new_folder(LOOSE)?;
            Dir(&self.path).new_folder(PACKS)?;
            Dir(&self.path).new_folder(DUPLICATES)?;
            Dir(&self.path).new_folder(SANDBOX)?;

            // Create Sqlite DB for pack->idx mapping
            let db = self.path.join(PACKS_DB);

            db::create(&db).with_context(|| format!("create db at {}", db.display()))?;
        } else {
            // is not empty, check if it is properly initialized
            let cnt = self.validate()?;
            anyhow::bail!("{} already initialized", cnt.path.display())
        }

        Ok(())
    }

    /// validate if it is a valid container (means properly initialized from empty dir), return itself if valid.
    pub fn validate(&self) -> anyhow::Result<&Self> {
        if !self.path.exists() {
            anyhow::bail!("{} not exist, initialize first", self.path.display());
        }

        // if !self.path.is_dir() {
        //     anyhow::bail!("{} is not a directory", self.path.display());
        // }

        if Dir(&self.path).is_empty()? {
            anyhow::bail!("{} is empty, initialize first", self.path.display());
        }

        for entry in self.path.read_dir()? {
            let path = entry?.path();
            if let Some(filename) = path.file_name() {
                match filename.to_string_lossy().as_ref() {
                    LOOSE | PACKS | DUPLICATES | SANDBOX => {
                        if !path.is_dir() {
                            anyhow::bail!("{} is not a directory", path.display())
                        }
                    }
                    CONFIG_FILE | PACKS_DB => {
                        if !path.is_file() {
                            anyhow::bail!("{} is not a file", path.display())
                        }
                    }
                    // _ => unreachable!("unknow path {}", filename.to_string_lossy()),
                    _ => Err(Error::DirectoryNotEmpty { path })?,
                }
            }
        }

        Ok(self)
    }

    pub fn loose(&self) -> anyhow::Result<PathBuf> {
        let path = Dir(&self.path).at_path(LOOSE);
        if !path.exists() {
            anyhow::bail!("{} not exist", path.display());
        }

        // NOTE: profiling shows this check calls fs and loose check is called in every from_hash
        // call.
        if !path.is_dir() {
            anyhow::bail!("{} is not a directory", path.display());
        }

        Ok(path)
    }

    pub fn sandbox(&self) -> anyhow::Result<PathBuf> {
        let path = Dir(&self.path).at_path(SANDBOX);
        if !path.exists() {
            anyhow::bail!("{} not exist", path.display());
        }

        if !path.is_dir() {
            anyhow::bail!("{} is not a directory", path.display());
        }

        Ok(path)
    }

    pub fn packs(&self) -> anyhow::Result<PathBuf> {
        let path = Dir(&self.path).at_path(PACKS);
        if !path.exists() {
            anyhow::bail!("{} not exist", path.display());
        }

        if !path.is_dir() {
            anyhow::bail!("{} is not a directory", path.display());
        }

        Ok(path)
    }

    pub fn packs_db(&self) -> anyhow::Result<PathBuf> {
        let path = Dir(&self.path).at_path(PACKS_DB);
        if !path.exists() {
            anyhow::bail!("{} not exist", path.display());
        }

        if !path.is_file() {
            anyhow::bail!("{} is not a file", path.display());
        }

        Ok(path)
    }

    pub fn config_file(&self) -> anyhow::Result<PathBuf> {
        let path = Dir(&self.path).at_path(CONFIG_FILE);
        if !path.exists() {
            anyhow::bail!("{} not exist", path.display());
        }

        if !path.is_file() {
            anyhow::bail!("{} is not a file", path.display());
        }

        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::config;

    use super::*;

    #[test]
    fn default_init() {
        let config = config::Config::new(4);
        let tmp = tempdir().unwrap();
        let cnt = Container::new(&tmp);
        assert!(Dir(&cnt.path).is_empty().unwrap());

        cnt.initialize(&config).unwrap();
        let cnt = cnt.validate().unwrap();

        assert!(!Dir(&cnt.path).is_empty().unwrap());
    }

    #[test]
    fn init_in_non_empty_folder() {
        let config = config::Config::new(4);
        let tmp = tempdir().unwrap();
        let cnt = Container::new(&tmp);
        let _ = fs::File::create(cnt.path.join("unexpected"));

        let err = cnt.initialize(&config).unwrap_err();
        assert!(err
            .to_string()
            .starts_with("Refusing to initialize in non-empty directory"));
    }
}
