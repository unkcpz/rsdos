use anyhow::Context;
use serde_json::to_string_pretty;

use crate::Error;
use crate::{config::Config, db, utils::Dir};
use core::panic;
use std::str::FromStr;
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

#[derive(Debug, PartialEq)]
pub enum Compression {
    Zlib(u32),
    Zstd(i32),
    Uncompressed,
}

#[allow(clippy::cast_sign_loss)]
impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim() == "none" {
            return Ok(Compression::Uncompressed);
        }

        // NOTE: for backwark compatibility with legacy dos
        if s.trim() == "zlib+1" {
            return Ok(Compression::Zlib(1));
        }

        let vs = s.split(':').collect::<Vec<_>>();
        if vs.len() != 2 {
            return Err(Error::ParseCompressionError { s: s.to_string() });
        }

        let (algo, level) = (
            vs[0].trim(),
            vs[1].trim()
                .parse::<i32>()
                .map_err(|_| Error::ParseCompressionError { s: s.to_string() })?,
        );
        match algo {
            "zlib" => Ok(Compression::Zlib(level as u32)),
            "zstd" => Ok(Compression::Zstd(level)), // NOTE: should not exposed before v2
            _ => Err(Error::ParseCompressionError { s: s.to_string() }),
        }
    }
}

impl Container {
    pub fn new<P: AsRef<Path>>(path: P) -> Container {
        Container {
            path: path.as_ref().to_owned(),
        }
    }

    pub fn compression(&self) -> Result<Compression, Error> {
        let algo = self.config()?.compression_algorithm;
        Compression::from_str(&algo)
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
            let cnt = self.valid()?;
            anyhow::bail!("{} already initialized", cnt.path.display())
        }

        Ok(self)
    }

    pub fn config(&self) -> Result<Config, Error> {
        let config_path = self.config_file();
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
    pub fn valid(&self) -> Result<&Self, Error> {
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
                let filename = filename.to_string_lossy();
                match filename.as_ref() {
                    LOOSE | PACKS | DUPLICATES | SANDBOX => {
                        if !path.is_dir() {
                            return Err(Error::StoreComponentError {
                                path: self.path.clone(),
                                cause: "not a dir".to_string(),
                            });
                        }
                    }
                    CONFIG_FILE => {
                        if !path.is_file() {
                            return Err(Error::StoreComponentError {
                                path: self.path.clone(),
                                cause: "not a file".to_string(),
                            });
                        }
                    }
                    _ if filename.contains(PACKS_DB) => {
                        if !path.is_file() {
                            return Err(Error::StoreComponentError {
                                path: self.path.clone(),
                                cause: "not a file".to_string(),
                            });
                        }
                    }
                    _ => Err(Error::DirectoryNotEmpty { path })?,
                }
            }
        }

        Ok(self)
    }

    #[must_use]
    pub fn loose(&self) -> PathBuf {
        Dir(&self.path).at_path(LOOSE)
    }

    #[must_use]
    pub fn sandbox(&self) -> PathBuf {
        Dir(&self.path).at_path(SANDBOX)
    }

    #[must_use]
    pub fn packs(&self) -> PathBuf {
        Dir(&self.path).at_path(PACKS)
    }

    #[must_use]
    pub fn packs_db(&self) -> PathBuf {
        Dir(&self.path).at_path(PACKS_DB)
    }

    #[must_use]
    pub fn config_file(&self) -> PathBuf {
        Dir(&self.path).at_path(CONFIG_FILE)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::test_utils::{gen_tmp_container, PACK_TARGET_SIZE};

    use super::*;

    #[test]
    fn default_init() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        assert!(!Dir(&cnt.path).is_empty().unwrap());
    }

    #[test]
    fn init_in_inited_folder() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        let err = cnt.initialize(&Config::new(4 * 1024 * 1024, "none")).unwrap_err();
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

        let err = cnt.initialize(&Config::new(4 * 1024 * 1024, "none")).unwrap_err();
        assert!(
            err.to_string()
                .contains("Refusing to initialize in non-empty directory"),
            "got err: {err}"
        );
    }

    #[test]
    fn parse_compression() {
        assert_eq!(
            Compression::from_str("none").unwrap(),
            Compression::Uncompressed
        );
        assert_eq!(
            Compression::from_str("zlib:+1").unwrap(),
            Compression::Zlib(1)
        );
        assert_eq!(
            Compression::from_str("zstd:-7").unwrap(),
            Compression::Zstd(-7)
        );

        // white spaces are trimed
        assert_eq!(
            Compression::from_str("none ").unwrap(),
            Compression::Uncompressed
        );
        assert_eq!(
            Compression::from_str("zlib :+1 ").unwrap(),
            Compression::Zlib(1)
        );
        assert_eq!(
            Compression::from_str("zstd:-7 ").unwrap(),
            Compression::Zstd(-7)
        );

        // zlib+1
        assert_eq!(
            Compression::from_str("zlib+1").unwrap(),
            Compression::Zlib(1)
        );

        // unable to parse
        assert!(Compression::from_str("zzzz").is_err());
    }

}
