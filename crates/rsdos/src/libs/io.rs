use crate::Error;
use bytes::Buf;
use sha2::Digest;
use std::io::{self, Read, Write};
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
        // hasher compute hash from original data pass to buf
        self.hasher.update(buf);

        let n = self.writer.write(buf)?;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Copy by chunk (``chunk_size`` in unit bytes) and return a tuple of total bytes read from reader
/// and total bytes write to writer.
pub fn copy_by_chunk<R, W>(
    reader: &mut R,
    writer: &mut W,
    chunk_size: usize,
) -> Result<usize, std::io::Error>
where
    R: Read,
    W: Write,
{
    let mut buf = vec![0u8; chunk_size];
    let mut total_bytes_read = 0;

    loop {
        let bytes_read = reader.read(&mut buf[..])?;
        // EOF if bytes_read == 0, then stop and flush
        if bytes_read == 0 {
            break;
        }
        total_bytes_read += bytes_read;

        writer.write_all(&buf[..bytes_read])?;
    }

    writer.flush()?;
    Ok(total_bytes_read)
}

pub trait ReaderMaker {
    fn make_reader(&self) -> Result<impl Read, Error>;
}

impl ReaderMaker for PathBuf {
    fn make_reader(&self) -> Result<impl Read, Error> {
        let f = fs::OpenOptions::new()
            .read(true)
            .open(self)
            .unwrap_or_else(|_| panic!("open {}", self.display()));
        Ok(f)
    }
}

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

impl ReaderMaker for ByteString {
    fn make_reader(&self) -> Result<impl Read, Error> {
        Ok(self.reader())
    }
}

