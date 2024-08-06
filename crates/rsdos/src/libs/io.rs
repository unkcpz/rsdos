use std::io::{self, Write};
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::{fs, usize};

use crate::{db, Container};
use anyhow::Context;
use rusqlite::{params, params_from_iter, Connection};

use crate::{db::PackEntry, utils::Dir};
use sha2::{Digest, Sha256};

pub struct Object<R> {
    pub reader: R,
    pub expected_size: usize,
    pub hashkey: String,
}

pub struct HashWriter<'a, W, H> {
    pub writer: W,
    pub hasher: &'a mut H,
}

impl<'a, W, H> HashWriter<'a, W, H>
where
    W: Write,
    H: Digest,
{
    pub fn new(writer: W, hasher: &'a mut H) -> Self {
        Self { writer, hasher }
    }
}

impl<'a, W, H> Write for HashWriter<'a, W, H>
where
    W: Write,
    H: Digest,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.hasher.update(buf);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Copy by chunk (``chunk_size`` in unit bytes) and return the size of content that copied
pub fn copy_by_chunk<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    chunk_size: usize,
) -> anyhow::Result<usize> {
    let mut buf = vec![0u8; chunk_size];
    let mut total_bytes_copied = 0;

    loop {
        let bytes_read = reader.read(&mut buf[..]).with_context(|| "read to buf")?;
        // EOF if bytes_read == 0, then stop and flush
        if bytes_read == 0 {
            break;
        }
        total_bytes_copied += bytes_read;
        writer
            .write_all(&buf[..bytes_read])
            .with_context(|| "write to writer")?;
    }

    writer.flush().with_context(|| "flush to buff writer")?;
    Ok(total_bytes_copied)
}

pub fn pull_from_loose(
    obj_hash: &str,
    cnt: &Container,
) -> anyhow::Result<Option<Object<impl BufRead>>> {
    let obj = cnt
        .loose()?
        .join(format!("{}/{}", &obj_hash[..2], &obj_hash[2..]));
    if obj.exists() {
        let f = fs::File::open(&obj).with_context(|| format!("cannot open {}", obj.display()))?;
        let expected_size = f.metadata()?.len();
        let z = BufReader::new(f);

        let obj = Object {
            reader: z.take(expected_size),
            expected_size: expected_size as usize,
            hashkey: obj_hash.to_string(),
        };
        Ok(Some(obj))
    } else {
        Ok(None)
    }
}

pub fn pull_from_packs(
    obj_hash: &str,
    cnt: &Container,
) -> anyhow::Result<Option<Object<impl BufRead>>> {
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
        Ok(Some(obj))
    } else {
        Ok(None)
    }
}

pub fn multi_pull_from_packs(
    cnt: &Container,
    hashkeys: &[String],
) -> anyhow::Result<Vec<Object<impl Read>>> {
    // TODO: make chunk size configuable
    let MAX_CHUNK_ITERATE_LENGTH = 9500;
    let IN_SQL_MAX_LENGTH = 950;

    let mut conn = Connection::open(cnt.packs_db()?)?;
    let tx = conn.transaction()?;
    let mut objs: Vec<_> = Vec::with_capacity(hashkeys.len());
    for chunk in hashkeys.chunks(IN_SQL_MAX_LENGTH) {
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
        let mut rows: Vec<_> = rows.into_iter().map(|row| row.unwrap()).collect();
        rows.sort_by_key(|k| k.3);

        // XXX: find correct pack_id
        let pack = fs::OpenOptions::new()
            .read(true)
            .open(cnt.packs()?.join("0"))?;

        for row in rows {
            let (hashkey, _, _, offset, length, _pack_id) = row;
            let buf_size = usize::try_from(length)?;

            let mut buf = vec![0u8; buf_size];
            pack.read_exact_at(&mut buf, offset)?;
            let obj = Object {
                reader: Cursor::new(buf),
                expected_size: buf_size,
                hashkey,
            };
            objs.push(obj);
        }
    }
    tx.commit()?;
    Ok(objs)
}

pub fn push_to_loose<R>(source: &mut R, cnt: &Container) -> anyhow::Result<(u64, String)>
where
    R: Read,
{
    let chunk_size = 524_288; // 512 MiB TODO: make it configurable??

    // <cnt_path>/sandbox/<uuid> as dst
    let dst = format!("{}.tmp", uuid::Uuid::new_v4());
    let dst = cnt.sandbox()?.join(dst);
    let writer =
        fs::File::create(&dst).with_context(|| format!("open {} for write", dst.display()))?;
    let mut writer = BufWriter::new(writer); // XXX: ??? is this convert necessary??

    // TODO: hasher can be passed as ref and using reset to avoid re-alloc in heap
    let mut hasher = Sha256::new();
    let mut hwriter = HashWriter::new(&mut writer, &mut hasher);

    // write to object and store it in {hash:..2}/{hash:2..} file
    // first write to tmp and get the hash, than move it to the location.
    //
    // Note: using chunk copy is a slightly slow than direct copy but since I don't know the size,
    // have to do the pre-allocate with specific chunk size.
    let bytes_copied = copy_by_chunk(source, &mut hwriter, chunk_size)?;
    // let bytes_copied = std::io::copy(source, &mut hwriter)?;
    let hash = hasher.finalize();
    let hash_hex = hex::encode(hash);

    let loose = cnt.loose()?;
    fs::create_dir_all(loose.join(format!("{}/", &hash_hex[..2])))?;
    let loose_dst = loose.join(format!("{}/{}", &hash_hex[..2], &hash_hex[2..]));

    // avoid move if duplicate exist to reduce overhead
    if !loose_dst.exists() {
        fs::rename(&dst, &loose_dst)
            .with_context(|| format!("move from {} to {}", dst.display(), loose_dst.display()))?;
    }

    Ok((bytes_copied as u64, hash_hex))
}

fn _stream_to_packs<R>(
    source: &mut R,
    fpack: &mut fs::File,
    conn: &Connection,
    offset: u64,
    current_pack_id: u64,
) -> anyhow::Result<(u64, String)>
where
    R: Read,
{
    let mut hasher = Sha256::new();
    let mut hwriter = HashWriter::new(fpack, &mut hasher);

    // 64 MiB TODO: make it configurable??
    let chunk_size = 65_536;
    let bytes_copied = copy_by_chunk(source, &mut hwriter, chunk_size)?;

    let hash = hasher.finalize();
    let hash_hex = hex::encode(hash);

    // entry record to DB
    let packin = PackEntry {
        hashkey: hash_hex.clone(),
        compressed: false,
        size: bytes_copied as u64,
        offset,
        length: bytes_copied as u64, // redundent as size
        pack_id: current_pack_id,
    };

    db::insert_packin(conn, &packin)?;

    Ok((bytes_copied as u64, hash_hex))
}

pub fn push_to_packs<R>(source: &mut R, cnt: &Container) -> anyhow::Result<(u64, String)>
where
    R: Read,
{
    // write to <cnt_path>/packs/<u32>
    let packs = cnt.packs()?;

    // Get the current addable pack
    // Create pack_id = 0 if not yet packs exists.
    let mut current_pack_id: u64 = 0;
    if !Dir(&packs).is_empty()? {
        for entry in packs.read_dir()? {
            let path = entry?.path();
            if let Some(filename) = path.file_name() {
                let n = filename.to_string_lossy();
                let n = n.parse().with_context(|| format!("parse {n} to u64"))?;
                current_pack_id = std::cmp::max(current_pack_id, n);
            }
        }
    }

    // If size of current pack exceed the single pack limit, create next pack
    let p = Dir(&packs).at_path(&format!("{current_pack_id}"));
    let mut fpack = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(p)
        .with_context(|| format!("open packs/{current_pack_id}"))?;

    // Use new pack if size of the current pack reach or exceed the threshold limit
    let offset = if fpack.metadata()?.len() >= cnt.config()?.pack_size_target {
        current_pack_id += 1;
        0
    } else {
        fpack.seek(io::SeekFrom::End(0))?
    };

    let mut fpack = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(Dir(&packs).at_path(&format!("{current_pack_id}")))?;

    let conn = Connection::open(cnt.packs_db()?)?;

    let (bytes_copied, hash_hex) =
        _stream_to_packs::<R>(source, &mut fpack, &conn, offset, current_pack_id)?;

    Ok((bytes_copied, hash_hex))
}

fn find_current_pack_id(packs: &PathBuf, pack_size_target: u64) -> anyhow::Result<u64> {
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
        fs::OpenOptions::new().create(true).truncate(true).open(p)?;
    }

    Ok(current_pack_id)
}

pub fn multi_push_to_packs<R>(sources: Vec<&mut R>, cnt: &Container) -> anyhow::Result<Vec<String>>
where
    R: Read,
{
    let mut results = Vec::new();
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
        for stream in sources {
            // check offset (which is in the end of file when writing) is exceed limit
            // if so create new file with +1 incremental as cwp, reset offset to 0 and continue
            if offset >= pack_size_target {
                cwp_id += 1;
                offset = 0;
                let p = Dir(&packs).at_path(&format!("{cwp_id}"));
                fs::OpenOptions::new().create(true).truncate(true).open(p)?;
                continue;
            }

            let mut hwriter = HashWriter::new(&mut cwp, &mut hasher);

            // NOTE: Using small chunk_size can be fast in terms of benchmark.
            // Ideally should accept a hint for buffer size (loose -> packs)
            // 64 MiB from legacy dos  TODO: make it configurable??
            let chunk_size = 65_536;
            let bytes_copied = copy_by_chunk(stream, &mut hwriter, chunk_size)?;

            let hash = hasher.finalize_reset();
            let hash_hex = hex::encode(hash);

            stmt.execute(params![
                &hash_hex,
                false,
                bytes_copied as u64,
                offset,
                bytes_copied as u64,
            ])
            .with_context(|| "insert to db")?;
            offset += bytes_copied as u64;

            results.push(hash_hex);
        }
    }
    tx.commit()?;

    Ok(results)
}

#[cfg(test)]
mod tests {
    use crate::{stat, test_utils::gen_tmp_container};
    use bytes::Buf;

    use super::*;

    #[test]
    fn push_to_pack_0_when_empty() {
        let cnt = gen_tmp_container().lock().unwrap();

        let mut buf = b"test 0".reader();
        push_to_packs(&mut buf, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, 1);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new().read(true).open(cnt.packs().unwrap().join("0")).unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");

        // subsquent add will still goes to pack 0 (since pack_target_size is 4 GiB)
        let mut buf = b"test 1".reader();
        push_to_packs(&mut buf, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, 2);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new().read(true).open(cnt.packs().unwrap().join("0")).unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0test 1");
    }

    #[test]
    fn push_to_pack_1_when_1_exist() {
        let cnt = gen_tmp_container().lock().unwrap();

        // create fack placeholder empty pack 0 and pack 1
        // it is expected that content will be added to pack1
        let packs = cnt.packs().unwrap();
        fs::File::create(packs.join("0")).unwrap();
        fs::File::create(packs.join("1")).unwrap();

        let mut buf = b"test 0".reader();
        push_to_packs(&mut buf, &cnt).unwrap();

        // check packs has 2 packs 
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 2);
        assert_eq!(info.count.packs, 1);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new().read(true).open(cnt.packs().unwrap().join("1")).unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");
    }

    #[test]
    fn push_to_pack_2_when_1_reach_limit() {
        let cnt = gen_tmp_container().lock().unwrap();
        let pack_target_size = cnt.config().unwrap().pack_size_target;

        // snuck limit size of bytes into pack 1 and new bytes will go to pack 2
        let packs = cnt.packs().unwrap();
        fs::File::create(packs.join("0")).unwrap();
        let mut p1 = fs::File::create(packs.join("1")).unwrap();
        let mut bytes_holder = vec![0u8; pack_target_size as usize];
        p1.write_all(&bytes_holder).unwrap();

        // more bytes
        let mut buf = b"test 0".reader();
        push_to_packs(&mut buf, &cnt).unwrap();

        // check packs has 2 packs 
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 3);
        assert_eq!(info.count.packs, 1);

        let mut sbuf = String::new();
        let mut f0pack = fs::OpenOptions::new().read(true).open(cnt.packs().unwrap().join("2")).unwrap();
        f0pack.read_to_string(&mut sbuf).unwrap();
        assert_eq!(sbuf, "test 0");
        
    }
}
