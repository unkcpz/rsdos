use crate::utils::Dir;
use anyhow::Context;
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

/// Copy by chunk and return the size of content that copied
fn copy_by_chunk<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    chunk_size: usize,
) -> anyhow::Result<usize> {
    let mut buf = Vec::with_capacity(chunk_size);
    buf.resize(chunk_size, b' ');
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

pub fn add_file(file: &PathBuf, cnt_path: &PathBuf) -> anyhow::Result<()> {
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    // Race here if file changes in between stat and open, the source may changed
    // in the end of add check the size from stat and copied is identical.
    let file = fs::File::open(file).with_context(|| format!("open {} for read", file.display()))?;
    let mut source = BufReader::new(file);

    let bytes_streamd = stream_to_loose(&mut source, cnt_path)?;

    anyhow::ensure!(
        bytes_streamd == expected_size,
        format!(
            "bytes streamed: {}, bytes source: {}",
            bytes_streamd, expected_size
        )
    );

    Ok(())
}

pub fn stream_to_loose<R>(source: &mut R, cnt_path: &PathBuf) -> anyhow::Result<u64> where R: Read {
    // stream file to loose object store
    // TODO: let object = Object::blob_from_file(file)

    let chunk_size = 524_288; // 512 MiB TODO: make it configurable??

    // <cnt_path>/sandbox/<uuid> as dst
    let dst = format!("{}.tmp", uuid::Uuid::new_v4());
    let dst = Dir(cnt_path).at_path("sandbox").join(dst);
    let writer =
        fs::File::create(&dst).with_context(|| format!("open {} for write", dst.display()))?;
    let writer = BufWriter::new(writer);

    let hasher = Sha256::new();
    let mut hwriter = HashWriter::new(writer, hasher);

    // write to object and store it in {hash:..2}/{hash:2..} file
    // first write to tmp and get the hash, than move it to the location.
    // TODO: benchmark me (on large amount of data) whether do direct copy if it is a small file < 4M??
    let bytes_copied = copy_by_chunk(source, &mut hwriter, chunk_size)?;
    let hash = hwriter.hasher.finalize();
    let hash_hex = hex::encode(hash);


    let loose = Dir(cnt_path).at_path("loose");
    fs::create_dir_all(loose.join(format!("{}/", &hash_hex[..2])))?;
    let loose_dst = loose.join(format!("{}/{}", &hash_hex[..2], &hash_hex[2..]));
    fs::rename(&dst, &loose_dst)
        .with_context(|| format!("move from {} to {}", dst.display(), loose_dst.display()))?;

    Ok(bytes_copied as u64)
}
