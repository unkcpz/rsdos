use sled::IVec;

use crate::container::{traverse_loose, Compression, Container};
use crate::{io_packs, Error};

pub fn pack_loose(cnt: &Container, db: &sled::Db) -> Result<(), Error> {
    let compression = cnt.compression()?;
    _pack_loose_internal(cnt, db, &compression)
}

// XXX: flag to set if do the validate, if no, use reguler writer not hash writer.
pub fn _pack_loose_internal(cnt: &Container, db: &sled::Db, compression: &Compression) -> Result<(), Error> {
    cnt.valid()?;
    let loose_objs = traverse_loose(cnt)?;

    // Iterate over all key-value pairs and collect keys
    let keys = db
        .iter()
        .keys()
        .filter_map(Result::ok)
        .collect::<Vec<_>>();

    let sources = loose_objs.filter(|obj| {
        let hash = obj.parent().and_then(|p| p.file_name()).and_then(|parent| {
            obj.file_name().map(|filename| {
                format!("{}{}", parent.to_str().unwrap(), filename.to_str().unwrap())
            })
        });
        hash.map_or(false, |h| !keys.contains(&IVec::from(h.into_bytes())))
    });

    // race may happened during packing, I pass path as iterator which can be modified or doesn't
    // catch newly added objects to loose folder.
    io_packs::_insert_many_internal(sources, cnt, &db, compression)?;

    // XXX: the goal is unclear in legacy dos, there are following reasons that can cause the hash
    // mismatched:
    // 1. content change for loose object (this should be checked independently for loose)
    // 2. loose -> pack is not proceed correctly. (this better to be checkd by cheap checksum)
    // let got_hashkeys: Vec<_> = nbytes_hashkeys
    //     .into_iter()
    //     .map(|(_, hashkey)| hashkey.clone())
    //     .collect();
    // for (h1, h2) in got_hashkeys.iter().zip(expected_hashkeys.iter()) {
    //     if *h1 != *h2 {
    //         return Err(Error::IntegrityError {
    //             got: h1.to_string(),
    //             expected: h2.to_string(),
    //         });
    //     }
    // }

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
        let db = sled::open(cnt.packs_db()).unwrap();
        let n = 200;

        // add 10 obj to loose
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..n {
            let content = format!("test {i:03}"); // 8 bytes each
            let buf = content.clone().into_bytes();
            let (_, hash) = loose_insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt, &db).unwrap();
        assert_eq!(info.count.loose, n);

        pack_loose(&cnt, &db).unwrap();

        let info = stat(&cnt, &db).unwrap();
        assert_eq!(info.count.packs, n);
        assert_eq!(info.count.packs_file, n * 8 / 1024 + 1);

        // read from packs
        for (hash, content) in hash_content_map {
            let obj = packs_extract(&hash, &cnt, &db).unwrap().unwrap();
            assert_eq!(String::from_utf8(obj.to_bytes().unwrap()).unwrap(), content);
        }
    }

    #[test]
    fn pack_loose_default_compress() {
        let cnt = gen_tmp_container(1024, "zlib:+1").lock().unwrap();
        let db = sled::open(cnt.packs_db()).unwrap();

        // add 10 obj to loose
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}").repeat(i); // 8 bytes each
            let buf = content.clone().into_bytes();
            let (_, hash) = loose_insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt, &db).unwrap();
        assert_eq!(info.count.loose, 100);

        pack_loose(&cnt, &db).unwrap();

        let info = stat(&cnt, &db).unwrap();
        assert_eq!(info.count.packs, 100);

        // read from packs
        for (hash, content) in hash_content_map {
            let obj = packs_extract(&hash, &cnt, &db).unwrap().unwrap();
            assert_eq!(String::from_utf8(obj.to_bytes().unwrap()).unwrap(), content);
        }
    }
}
