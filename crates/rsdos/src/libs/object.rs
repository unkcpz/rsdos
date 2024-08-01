use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Take};
use std::{fs, usize};

use crate::add_file::StoreType;
use crate::{db, Container};
use anyhow::Context;
use rusqlite::{params_from_iter, Connection};

pub struct Object<R> {
    pub reader: R,
    pub expected_size: usize,
    pub hashkey: String,
}

impl Object<()> {
    pub fn from_hash(
        obj_hash: &str,
        cnt: &Container,
        store_type: &StoreType,
    ) -> anyhow::Result<Option<Object<Take<BufReader<File>>>>> {
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
                        hashkey: obj_hash.to_string(),
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
                        hashkey: obj_hash.to_string(),
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

pub fn stream_from_packs_multi(
    cnt: &Container,
    hashkeys: Vec<String>,
) -> anyhow::Result<Vec<Object<Take<BufReader<File>>>>> {
    // TODO: make chunk size configuable
    let MAX_CHUNK_ITERATE_LENGTH = 9500;
    let IN_SQL_MAX_LENGTH = 950;

    let conn = Connection::open(cnt.packs_db()?)?;
    let mut objs: Vec<_> = Vec::with_capacity(hashkeys.len());
    for chunk in hashkeys.chunks(IN_SQL_MAX_LENGTH) {
        let placeholders: Vec<&str> = (0..chunk.len()).map(|_| "?").collect();
        // XXX: overhead that create a sql every time. but in one chunk should be fine.
        let mut stmt = conn.prepare_cached(&format!("SELECT hashkey, compressed, size, offset, length, pack_id FROM db_object WHERE hashkey IN ({})", placeholders.join(",")))?;
        let rows = stmt.query_map(params_from_iter(chunk), |row| {
            let hashkey: String = row.get(0)?;
            let compressed: bool = row.get(1)?;
            let size: u64 = row.get(2)?;
            let offset: u64 = row.get(3)?;
            let length: u64 = row.get(4)?;
            let pack_id: u64 = row.get(5)?;

            Ok((hashkey, compressed, size, offset, length, pack_id))
        })?;

        // let mut rows: Vec<_> = rows.into_iter().map(|row| row.unwrap()).collect();
        // rows.sort_by_key(|k| k.3);
        //
        for row in rows {
            let (hashkey, _, size, offset, _, pack_id) = row?;
            // XXX: overhead may come from too many file openning, which only need once for a
            // packid.
            let mut pack = fs::OpenOptions::new()
                .read(true)
                .open(cnt.packs()?.join(format!("{pack_id}")))?;
            pack.seek(SeekFrom::Start(offset))?;

            // open a buffer as reader
            let z = BufReader::new(pack);
            let obj = Object {
                reader: z.take(size),
                expected_size: size as usize,
                hashkey, 
            };
            objs.push(obj);
        }
    }
    Ok(objs)
}
