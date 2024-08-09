use anyhow::Context;
use bytes::Buf;
use sha2::Digest;
use std::io::{self, BufReader, Read, Write};
use std::path::PathBuf;
use std::{fs, usize};


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
pub fn copy_by_chunk<R, W>(
    reader: &mut R,
    writer: &mut W,
    chunk_size: usize,
) -> anyhow::Result<usize>
where
    R: Read,
    W: Write,
{
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

pub trait ReaderMaker {
    fn make_reader(&self) -> impl Read;
}

impl ReaderMaker for PathBuf {
    fn make_reader(&self) -> impl Read {
        let f = fs::OpenOptions::new()
            .read(true)
            .open(self)
            .unwrap_or_else(|_| panic!("open {}", self.display()));
        f
    }
}

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

impl ReaderMaker for ByteString {
    fn make_reader(&self) -> impl Read {
        self.reader()
    }
}

pub struct Object<R> {
    pub reader: R,
    pub expected_size: usize,
    pub hashkey: String,
}

