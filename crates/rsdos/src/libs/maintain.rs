use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::{container::Compression, io_packs, status::traverse_loose, Container, Error};

fn extract_hash(loose_obj: &Path) -> String {
    // use a bunch of unwrap, which should be save since operating under loose folder
    let parent = loose_obj.parent().unwrap().file_name().unwrap();
    let filename = loose_obj.file_name().unwrap();
    format!("{}{}", parent.to_str().unwrap(), filename.to_str().unwrap())
}

pub fn pack_loose(cnt: &Container) -> Result<(), Error> {
    let compression = cnt.compression()?;
    _pack_loose_internal(cnt, &compression)
}

// XXX: flag to set if do the validate, if no, use reguler writer not hash writer.
pub fn _pack_loose_internal(cnt: &Container, compression: &Compression) -> Result<(), Error> {
    cnt.valid()?;

    let mut loose_objs: Vec<PathBuf> = traverse_loose(cnt)?.collect();

    // if objs in packs, remove it from Vec
    let conn = Connection::open(cnt.packs_db())?;
    let mut stmt = conn.prepare("SELECT hashkey FROM db_object")?;
    let rows: Vec<_> = stmt
        .query([])?
        .mapped(|row| row.get::<_, String>(0))
        .filter_map(std::result::Result::ok)
        .collect();

    loose_objs.retain(|obj| {
        let hash = extract_hash(obj);
        !rows.contains(&hash)
    });
    let expected_hashkeys: Vec<_> = loose_objs.iter().map(|obj| extract_hash(obj)).collect();

    let nbytes_hashkeys = io_packs::_insert_many_internal(loose_objs, cnt, compression)?;
    let got_hashkeys: Vec<_> = nbytes_hashkeys
        .into_iter()
        .map(|(_, hashkey)| hashkey.clone())
        .collect();

    // XXX: the goal is unclear in legacy dos, there are following reasons that can cause the hash
    // mismatched:
    // 1. content change for loose object (this should be checked independently for loose)
    // 2. loose -> pack is not proceed correctly. (this better to be checkd by cheap checksum)
    for (h1, h2) in got_hashkeys.iter().zip(expected_hashkeys.iter()) {
        if *h1 != *h2 {
            return Err(Error::IntegrityError {
                got: h1.to_string(),
                expected: h2.to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::io_loose::insert as loose_insert;
    use crate::io_packs::extract as packs_extract;
    use crate::{stat, test_utils::gen_tmp_container};
    use std::collections::HashMap;

    #[test]
    fn pack_loose_default() {
        let cnt = gen_tmp_container(1024, "none").lock().unwrap();
        let n = 200;

        // add 10 obj to loose
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..n {
            let content = format!("test {i:03}"); // 8 bytes each
            let buf = content.clone().into_bytes();
            let (_, hash) = loose_insert(buf, &cnt).unwrap();
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
            let obj = packs_extract(&hash, &cnt).unwrap().unwrap();
            assert_eq!(String::from_utf8(obj.to_bytes().unwrap()).unwrap(), content);
        }
    }

    #[test]
    fn pack_loose_default_compress() {
        let cnt = gen_tmp_container(1024, "zlib:+1").lock().unwrap();

        // add 10 obj to loose
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}").repeat(i); // 8 bytes each
            let buf = content.clone().into_bytes();
            let (_, hash) = loose_insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.loose, 100);

        pack_loose(&cnt).unwrap();

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs, 100);

        // read from packs
        for (hash, content) in hash_content_map {
            let obj = packs_extract(&hash, &cnt).unwrap().unwrap();
            assert_eq!(String::from_utf8(obj.to_bytes().unwrap()).unwrap(), content);
        }
    }
}
