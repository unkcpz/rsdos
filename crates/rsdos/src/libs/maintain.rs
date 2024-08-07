use std::path::{Path, PathBuf};

use anyhow::{self, Context};
use rusqlite::Connection;

use crate::{io_packs::multi_push_to_packs, status::traverse_loose, Container};

fn extract_hash(loose_obj: &Path) -> String {
    // use a bunch of unwrap, which should be save since operating under loose folder
    let parent = loose_obj.parent().unwrap().file_name().unwrap();
    let filename = loose_obj.file_name().unwrap();
    format!("{}{}", parent.to_str().unwrap(), filename.to_str().unwrap())
}

// XXX: flag to set if do the validate, if no, use reguler writer not hash writer.
pub fn pack_loose(cnt: &Container) -> anyhow::Result<()> {
    let mut loose_objs: Vec<PathBuf> = traverse_loose(cnt)
        .with_context(|| "traverse loose by iter")?
        .collect();

    // if objs in packs, remove it from Vec
    let db = sled::open(cnt.packs_db()?)?;
    // let mut stmt = conn.prepare("SELECT hashkey FROM db_object")?;
    // let rows: Vec<_> = stmt
    //     .query([])?
    //     .mapped(|row| row.get::<_, String>(0))
    //     .map(|r| r.unwrap()) // TODO: decide to discard error finding or panic
    //     .collect();
    let rows: Vec<_> = db
        .iter()
        .map(|kv| {
            let (key, _) = kv.unwrap();
            String::from_utf8(key.to_vec()).unwrap()
        })
        .collect();

    loose_objs.retain(|obj| {
        let hash = extract_hash(obj);
        !rows.contains(&hash)
    });
    let expected_hashkeys: Vec<_> = loose_objs.iter().map(|obj| extract_hash(obj)).collect();

    let nbytes_hashkeys = multi_push_to_packs(loose_objs, cnt, &db)?;
    let got_hashkeys: Vec<_> = nbytes_hashkeys
        .iter()
        .map(|(_, hashkey)| hashkey.clone())
        .collect();

    // XXX: the goal is unclear in legacy dos, there are following reasons that can cause the hash
    // mismatched:
    // 1. content change for loose object (this should be checked independently for loose)
    // 2. loose -> pack is not proceed correctly. (this better to be checkd by cheap checksum)
    for (h1, h2) in got_hashkeys.iter().zip(expected_hashkeys.iter()) {
        anyhow::ensure!(*h1 == *h2, format!("{} != {}", h1, h2));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{pull_from_packs, push_to_loose, stat, test_utils::gen_tmp_container};
    use std::{collections::HashMap, io::Read};

    #[test]
    fn pack_loose_default() {
        let cnt = gen_tmp_container(1024).lock().unwrap();
        let n = 200;

        // add 10 obj to loose
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..n {
            let content = format!("test {i:03}"); // 8 bytes each
            let buf = content.clone().into_bytes();
            let (_, hash) = push_to_loose(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.loose, n);

        pack_loose(&cnt).unwrap();

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs, n);
        assert_eq!(info.count.packs_file, n * 8 / 1024 + 1);

        // read from packs
        for (hash, content) in hash_content_map {
            let obj = pull_from_packs(&hash, &cnt).unwrap();
            let mut sbuf = String::new();
            obj.unwrap().reader.read_to_string(&mut sbuf).unwrap();
            assert_eq!(sbuf, content);
        }
    }
}
