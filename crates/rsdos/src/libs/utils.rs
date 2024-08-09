use std::fs;
use std::path::{Path, PathBuf};

use crate::Error;


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
