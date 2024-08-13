use anyhow::Context;
use serde_json::to_string_pretty;

use crate::Error;
use crate::{config::Config, db, utils::Dir};
use core::panic;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct Container {
    pub path: PathBuf,
}

pub const PACKS_DB: &str = "packs.idx";
const CONFIG_FILE: &str = "config.json";
const LOOSE: &str = "loose";
const PACKS: &str = "packs";
const DUPLICATES: &str = "duplicates";
const SANDBOX: &str = "sandbox";

impl Container {
    pub fn new<P: AsRef<Path>>(path: P) -> Container {
        Container {
            path: path.as_ref().to_owned(),
        }
    }

    /// This will remove everything in the container folder. Use carefully!
    ///
    /// # Panics
    ///
    /// If the `remove_dir_all` or `create_dir_all` failed it will panic.
    pub fn reset(&self) {
        fs::remove_dir_all(&self.path)
            .unwrap_or_else(|err| panic!("not able to purge {}: {}", self.path.display(), err));
        fs::create_dir_all(&self.path).unwrap_or_else(|err| {
            panic!(
                "not able to create after purge {}: {}",
                self.path.display(),
                err
            )
        });
    }

    pub fn initialize(&self, config: &Config) -> anyhow::Result<&Self> {
        if Dir(&self.path).is_empty()? {
            let json_string = to_string_pretty(&config)?;
            let config = self.path.join(CONFIG_FILE);
            fs::File::create(config.clone())?
                .write_all(json_string.as_bytes())
                .map_err(|err| Error::IoWrite {
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

        Ok(self)
    }

    pub fn config(&self) -> Result<Config, Error> {
        let config_path = self.config_file()?;
        let config = fs::read_to_string(&config_path)?;
        let config = serde_json::from_str(&config)
            .map_err(|_| Error::ConfigFileError { path: config_path })?;

        Ok(config)
    }

    /// The method validate if it is a valid container (means properly initialized from empty dir), return itself if valid.
    /// This function is supposed to be called before heavy operation such as repack and
    /// ``extract_many`` to avoid the container folder is malfunctional. This can also be called at
    /// very begining of every CLI commands to make sure that operation are ready to proceed.
    /// On the contrary, this should not be called for dense small operations (e.g. inside
    /// ``insert_many`` or ``extract_many``) just for a tiny performance save (which matters).
    pub fn validate(&self) -> Result<&Self, Error> {
        if !self.path.exists() || Dir(&self.path).is_empty()? {
            return Err(Error::Uninitialized {
                path: self.path.clone(),
            });
        }

        if !self.path.is_dir() {
            return Err(Error::UnableObtainDir {
                path: self.path.clone(),
            });
        }

        for entry in self.path.read_dir()? {
            let path = entry?.path();
            if let Some(filename) = path.file_name() {
                match filename.to_string_lossy().as_ref() {
                    LOOSE | PACKS | DUPLICATES | SANDBOX => {
                        if !path.is_dir() {
                            return Err(Error::StoreComponentError {
                                path: self.path.clone(),
                                cause: "not a dir".to_string(),
                            });
                        }
                    }
                    CONFIG_FILE | PACKS_DB => {
                        if !path.is_file() {
                            return Err(Error::StoreComponentError {
                                path: self.path.clone(),
                                cause: "not a file".to_string(),
                            });
                        }
                    }
                    // _ => unreachable!("unknow path {}", filename.to_string_lossy()),
                    _ => Err(Error::DirectoryNotEmpty { path })?,
                }
            }
        }

        Ok(self)
    }

    pub fn loose(&self) -> Result<PathBuf, Error> {
        let path = Dir(&self.path).at_path(LOOSE);
        if !path.exists() || !path.is_dir() {
            return Err(Error::StoreComponentError {
                path: self.path.clone(),
                cause: "should be a dir".to_string(),
            });
        }

        Ok(path)
    }

    pub fn sandbox(&self) -> Result<PathBuf, Error> {
        let path = Dir(&self.path).at_path(SANDBOX);
        if !path.exists() || !path.is_dir() {
            return Err(Error::StoreComponentError {
                path: self.path.clone(),
                cause: "should be a dir".to_string(),
            });
        }

        Ok(path)
    }

    pub fn packs(&self) -> Result<PathBuf, Error> {
        let path = Dir(&self.path).at_path(PACKS);
        if !path.exists() || !path.is_dir() {
            return Err(Error::StoreComponentError {
                path: self.path.clone(),
                cause: "should be a dir".to_string(),
            });
        }

        Ok(path)
    }

    pub fn packs_db(&self) -> Result<PathBuf, Error> {
        let path = Dir(&self.path).at_path(PACKS_DB);
        if !path.exists() || !path.is_file() {
            return Err(Error::StoreComponentError {
                path: self.path.clone(),
                cause: "should be a file".to_string(),
            });
        }

        Ok(path)
    }

    pub fn config_file(&self) -> Result<PathBuf, Error> {
        let path = Dir(&self.path).at_path(CONFIG_FILE);
        if !path.exists() || !path.is_file() {
            return Err(Error::StoreComponentError {
                path: self.path.clone(),
                cause: "should be a file".to_string(),
            });
        }

        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::test_utils::{gen_tmp_container, PACK_TARGET_SIZE};

    use super::*;

    #[test]
    fn default_init() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE).lock().unwrap();

        assert!(!Dir(&cnt.path).is_empty().unwrap());
    }

    #[test]
    fn init_in_inited_folder() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE).lock().unwrap();

        let err = cnt.initialize(&Config::new(4 * 1024 * 1024)).unwrap_err();
        assert!(
            err.to_string().contains("already initialized"),
            "got err: {err}"
        );
    }

    #[test]
    fn init_in_non_empty_folder() {
        let tmp = tempdir().unwrap();
        let cnt = Container::new(&tmp);
        let _ = fs::File::create(cnt.path.join("unexpected"));

        let err = cnt.initialize(&Config::new(4 * 1024 * 1024)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Refusing to initialize in non-empty directory"),
            "got err: {err}"
        );
    }
}
