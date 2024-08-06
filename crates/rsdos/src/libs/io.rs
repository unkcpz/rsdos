use anyhow::Context;
use sha2::Digest;
use std::io::{self, Read, Write};
use std::usize;

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
