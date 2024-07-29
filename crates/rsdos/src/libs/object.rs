use std::{fs, usize};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

use crate::add_file::StoreType;
use crate::{db, Container};
use anyhow::Context;
use rusqlite::Connection;

pub struct Object<R> {
    pub reader: R,
    pub expected_size: usize,
}

impl Object<()> {
    pub fn from_hash(
        obj_hash: &str,
        cnt: &Container,
        store_type: &StoreType,
    ) -> anyhow::Result<Option<Object<impl BufRead>>> {
        let obj = match store_type {
            StoreType::Loose => {
                let obj = cnt
                    .loose()?
                    .join(format!("{}/{}", &obj_hash[..2], &obj_hash[2..]));
                if obj.exists() {
                    let f = fs::File::open(&obj)
                        .with_context(|| format!("cannot open {}", obj.display()))?;
                    let expected_size = f.metadata()?.len();
                    let z = BufReader::new(f);

                    let obj = Object {
                        reader: z.take(expected_size),
                        expected_size: expected_size as usize,
                    };
                    Some(obj)
                } else {
                    None
                }
            }
            StoreType::Packs => {
                let conn = Connection::open(cnt.packs_db()?)?;
                if let Some(pack_entry) = db::select(&conn, obj_hash)? {
                    let pack_id = pack_entry.pack_id;
                    let expected_size = pack_entry.size;
                    let mut pack = fs::OpenOptions::new()
                        .read(true)
                        .open(cnt.packs()?.join(format!("{pack_id}")))?;
                    pack.seek(SeekFrom::Start(pack_entry.offset))?;

                    // open a buffer as reader
                    let z = BufReader::new(pack);
                    let obj = Object {
                        reader: z.take(expected_size),
                        expected_size: expected_size as usize,
                    };
                    Some(obj)
                } else {
                    None
                }
            }
        };

        Ok(obj)
    }
}
