use std::{fs, io::{self, BufReader, Seek}, path::PathBuf};

use anyhow::{self, Context};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};

use crate::{io::{copy_by_chunk, HashWriter}, io_packs::multi_push_to_packs, pull_from_loose, status::traverse_loose, utils::Dir, Container};

fn find_current_pack_id(packs: &PathBuf, pack_size_target: u64) -> anyhow::Result<u64> {
    // make sure there is a pack if not create 0
    if Dir(packs).is_empty()? {
        fs::File::create(packs.join("0"))?;
    }
    let mut current_pack_id = 0;
    for entry in packs.read_dir()? {
        let path = entry?.path();
        if let Some(filename) = path.file_name() {
            let n = filename.to_string_lossy();
            let n = n.parse()?;
            current_pack_id = std::cmp::max(current_pack_id, n);
        }
    }

    // check if the current pack exceed pack target size
    let p = Dir(packs).at_path(&format!("{current_pack_id}"));
    let fpack = fs::OpenOptions::new().read(true).open(p)?;
    if fpack.metadata()?.len() >= pack_size_target {
        current_pack_id += 1;
        let p = Dir(packs).at_path(&format!("{current_pack_id}"));
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&p)
            .with_context(|| format!("create {}", &p.display()))?;
    }

    Ok(current_pack_id)
}

// XXX: flag to set if do the validate, if no, use reguler writer not hash writer.
pub fn pack_loose(cnt: &Container) -> anyhow::Result<()> {
    let iter_loose = traverse_loose(cnt).with_context(|| "traverse loose by iter")?;
    // let hashkeys: Vec<String> = iter_loose
    //     .map(|p| {
    //         let parent = p
    //             .parent()
    //             .unwrap_or_else(|| panic!("{} has no parent", p.display()));
    //         let parent = parent.file_name().unwrap().to_str().unwrap();
    //         let filename = p.file_name().unwrap().to_str().unwrap();
    //         let hashkey = format!("{parent}{filename}");
    //         hashkey
    //     })
    //     .collect();

    // let mut results = Vec::new();
    let mut conn = Connection::open(cnt.packs_db()?)?;
    let packs = cnt.packs()?;
    let pack_size_target = cnt.config()?.pack_size_target;

    // cwp: current working pack
    let mut cwp_id = find_current_pack_id(&cnt.packs()?, pack_size_target)?;
    let cwp = cnt.packs()?.join(format!("{cwp_id}"));
    let mut cwp = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(cwp)?;
    let mut offset = cwp.seek(io::SeekFrom::End(0))?;

    let tx = conn.transaction()?;

    {
        let mut stmt = tx.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;

        let mut hasher = Sha256::new();
        // for stream in sources {
        for path in iter_loose {
            let f = fs::OpenOptions::new().read(true).open(path)?;
            let mut stream = BufReader::new(f);
            // check offset (which is in the end of file when writing) is exceed limit
            // if so create new file with +1 incremental as cwp, reset offset to 0 and continue
            if offset >= pack_size_target {
                cwp_id += 1;
                offset = 0;
                let p = Dir(&packs).at_path(&format!("{cwp_id}"));
                cwp = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(p)?;
            }

            let mut hwriter = HashWriter::new(&mut cwp, &mut hasher);

            // NOTE: Using small chunk_size can be fast in terms of benchmark.
            // Ideally should accept a hint for buffer size (loose -> packs)
            // 64 MiB from legacy dos  TODO: make it configurable??
            let chunk_size = 65_536;
            let bytes_copied = copy_by_chunk(&mut stream, &mut hwriter, chunk_size)?;

            let hash = hasher.finalize_reset();
            let hash_hex = hex::encode(hash);

            stmt.execute(params![
                &hash_hex,
                false,
                bytes_copied as u64,
                offset,
                bytes_copied as u64,
                cwp_id,
            ])
            .with_context(|| "insert to db")?;
            offset += bytes_copied as u64;

            // results.push((bytes_copied as u64, hash_hex));
        }
    }
    tx.commit()?;

    // Ok(results)
    // for path in iter_loose {
    //     // let obj = pull_from_loose(hash, cnt)?.unwrap();
    //     // let reader = obj.reader;
    //     // let f = fs::File::open(&path).with_context(|| format!("cannot open {}", path.display()))?;
    //     let f = fs::OpenOptions::new().read(true).open(path)?;
    //     let reader = BufReader::new(f);
    //     sources.push(reader);
    // }
    //
    // let sources: Vec<_> = sources.iter_mut().collect();
    // let bindings = multi_push_to_packs(sources, cnt)?;
    // let got_hashkeys: Vec<String> = bindings
    //     .iter()
    //     .map(|(_, hashkey)| hashkey.to_string())
    //     .collect();

    // XXX: the goal is unclear in legacy dos, there are following reasons that can cause the hash
    // mismatched:
    // 1. content change for loose object (this should be checked independently for loose)
    // 2. loose -> pack is not proceed correctly. (this better to be checkd by cheap checksum)
    // for (h1, h2) in hashkeys.iter().zip(got_hashkeys.iter()) {
    //     anyhow::ensure!(h1 == h2, format!("{} != {}", h1, h2));
    // }

    Ok(())
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
