use anyhow::{self, Context};

use crate::{io_packs::multi_push_to_packs, status::traverse_loose, Container};

// XXX: flag to set if do the validate, if no, use reguler writer not hash writer.
pub fn pack_loose(cnt: &Container) -> anyhow::Result<()> {
    let loose_objs = traverse_loose(cnt)
        .with_context(|| "traverse loose by iter")?
        .collect();
    multi_push_to_packs(loose_objs, cnt)?;

    Ok(())

    // XXX: the goal is unclear in legacy dos, there are following reasons that can cause the hash
    // mismatched:
    // 1. content change for loose object (this should be checked independently for loose)
    // 2. loose -> pack is not proceed correctly. (this better to be checkd by cheap checksum)
    // for (h1, h2) in hashkeys.iter().zip(got_hashkeys.iter()) {
    //     anyhow::ensure!(h1 == h2, format!("{} != {}", h1, h2));
    // }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Read};

    use bytes::Buf;

    use crate::{pull_from_packs, push_to_loose, stat, test_utils::gen_tmp_container};

    use super::*;

    #[test]
    fn pack_loose_default() {
        let cnt = gen_tmp_container(1024).lock().unwrap();
        let n = 200;

        // add 10 obj to loose
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..n {
            let content = format!("test {i:03}"); // 8 bytes each
            let buf = content.clone().into_bytes();
            let (_, hash) = push_to_loose(&mut buf.reader(), &cnt).unwrap();
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
