use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("Could not communicate to the container")]
    ContainerError(#[from] crate::container::Error),
    #[error("Could not proceed io operation")]
    Io(#[from] std::io::Error),
    #[error("Could not open data at '{}'", .path.display())]
    IoOpen {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("Could not write data at '{}'", .path.display())]
    IoWrite {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("Could not create directory at '{}'", .path.display())]
    CreateDirectory {
        source: std::io::Error,
        path: PathBuf,
    },
}

pub fn create_dir(p: &Path) -> Result<(), Error> {
    fs::create_dir_all(p).map_err(|e| Error::CreateDirectory {
        source: e,
        path: p.to_owned(),
    })
}

pub struct Dir<'a>(pub &'a PathBuf);

impl<'a> Dir<'a> {
    pub fn new_folder(self, component: &str) -> Result<(), Error> {
        let mut sub_folder = self.0.clone();
        sub_folder.push(component);
        create_dir(&sub_folder)?;
        Ok(())
    }

    #[must_use]
    pub fn at_path(self, component: &str) -> PathBuf {
        let mut file = self.0.clone();
        file.push(component);
        file
    }

    pub fn is_empty(self) -> Result<bool, std::io::Error> {
        let mut entries = fs::read_dir(self.0.clone())?;
        Ok(entries.next().is_none())
    }
}
