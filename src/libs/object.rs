use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::Context;

use crate::utils::Dir;

pub struct Object<R> {
    pub reader: R,
    pub expected_size: u64,
}

impl Object<()> {
    pub fn from_hash(
        obj_hash: &str,
        cnt_path: &PathBuf,
    ) -> anyhow::Result<Option<Object<impl BufRead>>> {
        let obj = Dir(cnt_path).at_path(&format!("loose/{}/{}", &obj_hash[..2], &obj_hash[2..]));
        if obj.exists() {
            let f =
                fs::File::open(&obj).with_context(|| format!("cannot open {}", obj.display()))?;
            let expected_size = f.metadata()?.len();
            let z = BufReader::new(f);

            let obj = Object {
                reader: z,
                expected_size,
            };
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }
}
