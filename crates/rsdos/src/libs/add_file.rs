use crate::Container;
use anyhow::Context;
use human_bytes::human_bytes;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

#[derive(Default)]
struct HashWriter<W, H> {
    writer: W,
    hasher: H,
}

impl<W, H> HashWriter<W, H>
where
    W: Write,
    H: Digest,
{
    fn new(writer: W, hasher: H) -> Self {
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
fn copy_by_chunk<R: Read, W: Write>(
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

pub fn add_file(file: &PathBuf, cnt: &Container) -> anyhow::Result<String> {
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    // Race here if file changes in between stat and open, the source may changed
    // in the end of add check the size from stat and copied is identical.
    let source =
        fs::File::open(file).with_context(|| format!("open {} for read", file.display()))?;
    let mut source = BufReader::new(source);

    let (bytes_streamd, hash_hex) = stream_to_loose(&mut source, cnt)?;

    anyhow::ensure!(
        bytes_streamd == expected_size,
        format!(
            "bytes streamed: {}, bytes source: {}",
            bytes_streamd, expected_size
        )
    );

    println!(
        "{} - {}: {}",
        hash_hex,
        file.display(),
        human_bytes(expected_size as f64)
    );

    Ok(hash_hex)
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
    let writer = BufWriter::new(writer);

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
