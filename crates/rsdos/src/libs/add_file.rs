use crate::{
    db::{self, PackEntry},
    utils::Dir,
    Container,
};
use anyhow::Context;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::{
    fs::{self},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

#[derive(Default)]
pub struct HashWriter<W, H> {
    pub writer: W,
    pub hasher: H,
}

impl<W, H> HashWriter<W, H>
where
    W: Write,
    H: Digest,
{
    pub fn new(writer: W, hasher: H) -> Self {
        Self { writer, hasher }
    }
}

impl<W, H> Write for HashWriter<W, H>
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
    let mut buf = vec![b' '; chunk_size];
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

pub enum StoreType {
    Loose,
    Packs,
}

pub fn add_file(
    file: &PathBuf,
    cnt: &Container,
    target: &StoreType,
) -> anyhow::Result<(String, String, u64)> {
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    // Race here if file changes in between stat and open, the source may changed
    // in the end of add check the size from stat and copied is identical.
    let source =
        fs::File::open(file).with_context(|| format!("open {} for read", file.display()))?;
    let mut source = BufReader::new(source);

    let (bytes_streamd, hash_hex) = match target {
        StoreType::Loose => stream_to_loose(&mut source, cnt)?,
        StoreType::Packs => stream_to_packs(&mut source, cnt)?,
    };

    anyhow::ensure!(
        bytes_streamd == expected_size,
        format!(
            "bytes streamed: {}, bytes source: {}",
            bytes_streamd, expected_size
        )
    );

    Ok((hash_hex, file.display().to_string(), expected_size))
}

pub fn stream_to_loose<R>(source: &mut R, cnt: &Container) -> anyhow::Result<(u64, String)>
where
    R: Read,
{
    // stream file to loose object store
    // TODO: let object = Object::blob_from_file(file)

    let chunk_size = 524_288; // 512 MiB TODO: make it configurable??

    // <cnt_path>/sandbox/<uuid> as dst
    let dst = format!("{}.tmp", uuid::Uuid::new_v4());
    let dst = cnt.sandbox()?.join(dst);
    let writer =
        fs::File::create(&dst).with_context(|| format!("open {} for write", dst.display()))?;
    let writer = BufWriter::new(writer); // XXX: ??? is this convert necessary??

    // TODO: hasher can be passed as ref and using reset to avoid re-alloc in heap
    let hasher = Sha256::new();
    let mut hwriter = HashWriter::new(writer, hasher);

    // write to object and store it in {hash:..2}/{hash:2..} file
    // first write to tmp and get the hash, than move it to the location.
    //
    // Note: using chunk copy is a slightly slow than direct copy but since I don't know the size,
    // have to do the pre-allocate with specific chunk size.
    let bytes_copied = copy_by_chunk(source, &mut hwriter, chunk_size)?;
    // let bytes_copied = std::io::copy(source, &mut hwriter)?;
    let hash = hwriter.hasher.finalize();
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

pub fn stream_to_packs<R>(source: &mut R, cnt: &Container) -> anyhow::Result<(u64, String)>
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

pub fn stream_to_packs_multi<R: Read>(
    sources: Vec<&mut R>,
    cnt: &Container,
) -> anyhow::Result<Vec<String>> {
    let mut results = Vec::new();
    let packs = cnt.packs()?;
    let mut conn = Connection::open(cnt.packs_db()?)?;

    let mut current_pack_id: u64 = 0;
    if !Dir(&packs).is_empty()? {
        for entry in packs.read_dir()? {
            let path = entry?.path();
            if let Some(filename) = path.file_name() {
                let n = filename.to_string_lossy();
                let n = n.parse()?;
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
        .open(p)?;

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

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;

        for stream in sources {
            // results.push(self.stream_to_packs(file)?);
            // let mut file_like = PyFileLikeObject::with_requirements(stream, true, false, false, false)?;

            // TODO: need to check if new pack file needed. Create one if needed based on the
            // growth of size.
            // let (_, hash_hex) =
            //     _stream_to_packs(stream, &mut fpack, &conn, offset, current_pack_id)?;
            let hasher = Sha256::new();
            let mut hwriter = HashWriter::new(&fpack, hasher);

            // 512 MiB TODO: make it configurable??
            let chunk_size = 524_288;
            let bytes_copied = copy_by_chunk(stream, &mut hwriter, chunk_size)?;

            let hash = hwriter.hasher.finalize();
            let hash_hex = hex::encode(hash);

            // entry record to DB
            // let packin = PackEntry {
            //     hashkey: hash_hex.clone(),
            //     compressed: false,
            //     size: bytes_copied as u64,
            //     offset,
            //     length: bytes_copied as u64, // redundent as size
            //     pack_id: current_pack_id,
            // };
            //
            stmt.execute(params![
                &hash_hex,
                false,
                bytes_copied as u64,
                offset,
                bytes_copied as u64,
                current_pack_id
            ])
            .unwrap();
            // .with_context(|| "insert to db")?;

            results.push(hash_hex);
        }
    }
    tx.commit()?;

    Ok(results)
}

pub fn _stream_to_packs<R>(
    source: &mut R,
    fpack: &mut fs::File,
    conn: &Connection,
    offset: u64,
    current_pack_id: u64,
) -> anyhow::Result<(u64, String)>
where
    R: Read,
{
    let hasher = Sha256::new();
    let mut hwriter = HashWriter::new(fpack, hasher);

    // 512 MiB TODO: make it configurable??
    let chunk_size = 524_288;
    let bytes_copied = copy_by_chunk(source, &mut hwriter, chunk_size)?;

    let hash = hwriter.hasher.finalize();
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

    // // record with overhead of creating a packin in heap NOTE: this does not help too much
    // db::insert(conn, &hash_hex, false, bytes_copied as u64, offset, bytes_copied as u64, current_pack_id)?;
    //
    Ok((bytes_copied as u64, hash_hex))
}
