use std::io::{BufReader, BufRead};
use std::fs;
use std::path::PathBuf;

use anyhow::Context;

use crate::utils::Dir;

pub struct Object<R> {
    pub reader: R,
    pub expected_size: u64,
}

impl Object<()> {
    pub fn from_hash(obj_hash: &str, cnt_path: &PathBuf) -> anyhow::Result<Object<impl BufRead>> {
        let obj = Dir(cnt_path).at_path(&format!("loose/{}/{}", &obj_hash[..2], &obj_hash[2..]));
        let f = fs::File::open(&obj).with_context(|| format!("cannot open {}", obj.display()))?;
        let expected_size = f.metadata()?.len();
        let z = BufReader::new(f);
        Ok(Object {
            reader: z,
            expected_size,
        })
    }
}

