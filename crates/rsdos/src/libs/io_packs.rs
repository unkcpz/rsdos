use anyhow::Context;
use rusqlite::{params, params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::io::{self, BufReader, Cursor, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs, usize};

use crate::io::{copy_by_chunk, HashWriter, ReaderMaker};
use crate::{db, Container, Object};

use crate::utils::Dir;

// XXX: how to combine this with using multi_pull_from_packs???
pub fn pull_from_packs(
    hashkey: &str,
    cnt: &Container,
) -> anyhow::Result<Option<Object<impl Read>>> {
    let conn = Connection::open(cnt.packs_db()?)?;
    if let Some(pack_entry) = db::select(&conn, hashkey)? {
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
            hashkey: hashkey.to_string(),
        };
        Ok(Some(obj))
    } else {
        Ok(None)
    }
}

// XXX: switch argument order, hashkeys and then cnt
pub fn multi_pull_from_packs(
    cnt: &Container,
    hashkeys: &[String],
) -> anyhow::Result<Vec<Option<Object<impl Read>>>> {
    // TODO: make chunk size configuable
    let _max_chunk_iterate_length = 9500;
    let in_sql_max_length = 950;

    let mut conn = Connection::open(cnt.packs_db()?)?;
    let tx = conn.transaction()?;
    let mut objs: Vec<_> = Vec::with_capacity(hashkeys.len());
    for chunk in hashkeys.chunks(in_sql_max_length) {
        let placeholders: Vec<&str> = (0..chunk.len()).map(|_| "?").collect();
        let mut stmt = tx.prepare_cached(&format!("SELECT hashkey, compressed, size, offset, length, pack_id FROM db_object WHERE hashkey IN ({})", placeholders.join(",")))?;
        let rows = stmt.query_map(params_from_iter(chunk), |row| {
            let hashkey: String = row.get(0)?;
            let compressed: bool = row.get(1)?;
            let size: u64 = row.get(2)?;
            let offset: u64 = row.get(3)?;
            let length: u64 = row.get(4)?;
            let pack_id: u64 = row.get(5)?;

            Ok((hashkey, compressed, size, offset, length, pack_id))
        })?;

        // collect and sort by offset
        let rows: Vec<_> = rows
            .into_iter()
            .map(|row| match row {
                Ok(r) => Some(r),
                Err(_) => None,
            })
            .collect();
        // rows.sort_by_key(|k| k.3);

        for row in rows {
            if let Some(row) = row {
                let (hashkey, _, _, offset, length, pack_id) = row;
                let buf_size = usize::try_from(length)?;

                // XXX: sort pack_id and then offset to read in sequence
                let pack = fs::OpenOptions::new()
                    .read(true)
                    .open(cnt.packs()?.join(format!("{pack_id}")))?;
                let mut buf = vec![0u8; buf_size];
                pack.read_exact_at(&mut buf, offset)?;
                let obj = Object {
                    reader: Cursor::new(buf),
                    expected_size: buf_size,
                    hashkey,
                };
                objs.push(Some(obj));
            } else {
                objs.push(None);
            }
        }
    }
    tx.commit()?;
    Ok(objs)
}

pub fn push_to_packs(source: impl ReaderMaker, cnt: &Container) -> anyhow::Result<(u64, String)> {
    let (bytes_copied, hash_hex) = multi_push_to_packs(vec![source], cnt)?
        .first()
        .map(|(n, hash)| (*n, hash.clone()))
        .expect("can't find 1st source");

    Ok((bytes_copied, hash_hex))
}

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

pub fn multi_push_to_packs<I>(sources: I, cnt: &Container) -> anyhow::Result<Vec<(u64, String)>>
where
    I: IntoIterator,
    I::Item: ReaderMaker,
{
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
    let mut hasher = Sha256::new();

    let mut nbytes_hash = Vec::new();
    let mut sources = sources.into_iter().peekable();

    // outer loop control the increment of pack id
    loop {
        let tx = conn.transaction()?;
        if offset >= pack_size_target {
            // reset
            cwp_id += 1;
            offset = 0;
            let p = Dir(&packs).at_path(&format!("{cwp_id}"));
            cwp = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(p)?;
        }

        if sources.peek().is_none() {
            break;
        }

        for rmaker in sources.by_ref() {
            // NOTE: Using small chunk_size can be fast in terms of benchmark.
            // Ideally should accept a hint for buffer size (loose -> packs)
            // 64 MiB from legacy dos  TODO: make it configurable??
            let chunk_size = 65_536;

            // XXX: for if need to do the valitation for the hash, the idea is to having an object
            // encapsulate the pre-computed hash. For Readers that has no pre-compute hash it return
            // None. The method is from ReaderMaker and calling rmaker.expected_hash(). If the hash
            // already exist and do not need to run validation, the writer can be normal writer without
            // hash.
            //
            // XXX: for the compression, it is a flag of writer to tell which compression algorithm to
            // use.
            let mut hwriter = HashWriter::new(&mut cwp, &mut hasher);
            let mut stream = rmaker.make_reader();
            let bytes_copied = copy_by_chunk(&mut stream, &mut hwriter, chunk_size)?;

            let hash = hasher.finalize_reset();
            let hash_hex = hex::encode(hash);

            let mut stmt = tx.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
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

            nbytes_hash.push((bytes_copied as u64, hash_hex));

            if offset >= pack_size_target {
                break;
            }
        }

        tx.commit()?;
    }

    Ok(nbytes_hash)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write};

    use crate::{
        io::ByteString,
        stat,
        test_utils::{gen_tmp_container, PACK_TARGET_SIZE},
    };

    use super::*;

    #[test]
    fn io_packs_push_to_0_when_empty() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE).lock().unwrap();

        let bstr: ByteString = b"test 0".to_vec();
        let (_, hash) = push_to_packs(bstr, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, 1);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new()
            .read(true)
            .open(cnt.packs().unwrap().join("0"))
            .unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");

        // also check pack DB point to correct location to extract content
        let obj = pull_from_packs(&hash, &cnt).unwrap();
        let mut sbuf = String::new();
        obj.unwrap().reader.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");

        // subsquent add will still goes to pack 0 (since pack_target_size is 4 GiB)
        let bstr: ByteString = b"test 1".to_vec();
        let (_, hash) = push_to_packs(bstr, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, 2);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new()
            .read(true)
            .open(cnt.packs().unwrap().join("0"))
            .unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0test 1");

        let obj = pull_from_packs(&hash, &cnt).unwrap();
        let mut sbuf = String::new();
        obj.unwrap().reader.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 1");
    }

    #[test]
    fn io_packs_push_to_1_when_1_exist() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE).lock().unwrap();

        // create fack placeholder empty pack 0 and pack 1
        // it is expected that content will be added to pack1
        let packs = cnt.packs().unwrap();
        fs::File::create(packs.join("0")).unwrap();
        fs::File::create(packs.join("1")).unwrap();

        let bstr: ByteString = b"test 0".to_vec();
        let (_, hash) = push_to_packs(bstr, &cnt).unwrap();

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 2);
        assert_eq!(info.count.packs, 1);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new()
            .read(true)
            .open(cnt.packs().unwrap().join("1"))
            .unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");

        let obj = pull_from_packs(&hash, &cnt).unwrap();
        let mut sbuf = String::new();
        obj.unwrap().reader.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");
    }

    #[test]
    fn io_packs_push_to_2_when_1_reach_limit() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE).lock().unwrap();
        let pack_target_size = cnt.config().unwrap().pack_size_target;

        // snuck limit size of bytes into pack 1 and new bytes will go to pack 2
        let packs = cnt.packs().unwrap();
        fs::File::create(packs.join("0")).unwrap();
        let mut p1 = fs::File::create(packs.join("1")).unwrap();
        let bytes_holder = vec![0u8; pack_target_size as usize];
        p1.write_all(&bytes_holder).unwrap();

        // more bytes
        // let mut buf = b"test 0".reader();
        // let (_, hash) = push_to_packs(&mut buf, &cnt).unwrap();
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}");
            let buf = content.clone().into_bytes();
            let (_, hash) = push_to_packs(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 3);
        assert_eq!(info.count.packs, 100);

        for (hash, content) in hash_content_map {
            let obj = pull_from_packs(&hash, &cnt).unwrap();
            let mut sbuf = String::new();
            obj.unwrap().reader.read_to_string(&mut sbuf).unwrap();
            assert_eq!(sbuf, content);
        }
    }

    #[test]
    fn io_packs_pull_from_any_single() {
        let cnt = gen_tmp_container(64).lock().unwrap();

        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}");
            let buf = content.clone().into_bytes();
            let (_, hash) = push_to_packs(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 10);
        assert_eq!(info.count.packs, 100);

        for (hash, content) in hash_content_map {
            let obj = pull_from_packs(&hash, &cnt).unwrap();
            let mut sbuf = String::new();
            obj.unwrap().reader.read_to_string(&mut sbuf).unwrap();
            assert_eq!(sbuf, content);
        }
    }

    #[test]
    fn io_packs_pull_from_multi() {
        let cnt = gen_tmp_container(64).lock().unwrap();

        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}");
            let buf = content.clone().into_bytes();
            let (_, hash) = push_to_packs(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 10);
        assert_eq!(info.count.packs, 100);

        let hashkeys = hash_content_map.keys().collect::<Vec<_>>();
        let hashkeys: Vec<_> = hashkeys.iter().map(|&s| s.to_owned()).collect();
        let objs = multi_pull_from_packs(&cnt, &hashkeys).unwrap();
        for mut obj in objs.into_iter().flatten() {
            let mut sbuf = String::new();
            let content = hash_content_map.get(&obj.hashkey).unwrap();
            obj.reader.read_to_string(&mut sbuf).unwrap();
            assert_eq!(sbuf, content.clone());
            sbuf.clear();
        }
    }
}
