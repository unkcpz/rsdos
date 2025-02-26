use crate::Error;
use bytes::Buf;
use ring::digest::{Algorithm, Context, Digest};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub trait Finishable: Write {
    fn finish(self) -> io::Result<()>;
}

// Implement `Finishable` for any type that implements `Write` without a specific finish behavior.
impl<W: Write> Finishable for W {
    fn finish(mut self) -> io::Result<()> {
        self.flush()
    }
}

pub struct HashWriter<W>
where
    W: Finishable,
{
    pub writer: W,
    pub ctx: Context,
}

impl<W> HashWriter<W>
where
    W: Finishable + Write,
{
    pub fn new(writer: W, algorithm: &'static Algorithm) -> Self {
        let ctx = Context::new(algorithm);
        Self { writer, ctx }
    }

    pub fn finish(mut self) -> Digest {
        let _ = self.writer.flush();
        let _ = self.writer.finish();
        self.ctx.clone().finish()
    }
}

impl<W> Write for HashWriter<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Write first and then update the digest with exact n bytes.
        // The order matters since for zlib encoder it tries to use all buffer
        // see https://github.com/rust-lang/flate2-rs/discussions/447
        let n = self.writer.write(buf)?;
        self.ctx.update(&buf[..n]);
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
) -> Result<u64, std::io::Error>
where
    R: Read,
    W: Write,
{
    let mut buf = vec![0u8; chunk_size];
    let mut total_bytes_read = 0;

    // NOTE: using fill_buf/consume (low level call close to unsafe code) could gainning ~2% speed up
    // but need to sophisticately control the buf not worth to. The code in this function is clear
    // to understand and quite efficient.
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
    Ok(total_bytes_read as u64)
}

#[derive(Debug, PartialEq)]
pub enum MaybeContentFormat {
    MaybeLargeText,
    SmallContent,
    MaybeBinary,
    ZFile([u8; 4]),
}

pub trait ReaderMaker {
    fn make_reader(&self) -> Result<impl Read, Error>;

    fn maybe_content_format(&self) -> Result<MaybeContentFormat, Error> {
        Ok(MaybeContentFormat::MaybeLargeText)
    }
}

impl ReaderMaker for PathBuf {
    fn make_reader(&self) -> Result<impl Read, Error> {
        let f = fs::OpenOptions::new().read(true).open(self)?;
        Ok(f)
    }

    /// The method on ``PathBuf`` will estimate whether it is worth to compress.
    /// Here is the decision making flow:
    /// - If it is a file (`SmallContent`) < 850 bytes don't compress. For why 850 bytes see: https://developer.att.com/video-optimizer/docs/best-practices/text-file-compression
    /// - Read 2 header bytes if it is a zilb or a zstd(which is 4 bytes in header) (`ZFile([u8; 4])`), don't compress. (this will be override if recompress was on and different compression algorithm is assigned.)
    /// - Read 512 bytes and check if it is a binary (`MaybeBinary`) (by checking null bytes which is a heuristic for it is a binary data)
    /// - none of above is true, regard it as "worth to compress!" (`MabyLargeText`)
    ///
    /// This avoid to run actuall compress which bring overhead.
    /// XXX: rename to maybe_text_format, content is a bit vague
    fn maybe_content_format(&self) -> Result<MaybeContentFormat, Error> {
        let mut f = fs::OpenOptions::new().read(true).open(self)?;
        if f.metadata().unwrap().len() <= 850 {
            return Ok(MaybeContentFormat::SmallContent);
        }

        // if it is zlib/zstd
        let mut buf = [0u8; 4];
        f.read_exact(&mut buf)?;

        if buf[0] == 0x78 || buf == [0x28, 0xB5, 0x2F, 0xFD] {
            return Ok(MaybeContentFormat::ZFile(buf));
        }

        let mut buf = [0u8; 512];
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut buf)?;

        // if find any null bytes then it is maybe binary
        if buf.contains(&0x00) {
            return Ok(MaybeContentFormat::MaybeBinary);
        }

        Ok(MaybeContentFormat::MaybeLargeText)
    }
}

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

impl ReaderMaker for ByteString {
    fn make_reader(&self) -> Result<impl Read, Error> {
        Ok(self.reader())
    }
}

#[cfg(test)]

mod tests {
    use core::panic;

    use super::*;
    use flate2::{write::ZlibEncoder, Compression};
    use rand;

    #[test]
    fn io_maybe_content_format_guess() {
        // small text
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(&mut f, "a small text").unwrap();

        let p = f.path().to_path_buf();
        assert_eq!(
            p.maybe_content_format().unwrap(),
            MaybeContentFormat::SmallContent
        );

        f.close().unwrap();

        // small binary text is small
        let mut f = tempfile::NamedTempFile::new().unwrap();
        let rng_bytes = (0..850).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
        f.write_all(&rng_bytes).unwrap();

        let p = f.path().to_path_buf();
        assert_eq!(
            p.maybe_content_format().unwrap(),
            MaybeContentFormat::SmallContent
        );

        f.close().unwrap();

        // large binary text
        let mut f = tempfile::NamedTempFile::new().unwrap();
        let mut rng_bytes = (0..1000).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
        rng_bytes.insert(100, 0x00); // we use this condition so add it for sure test
        f.write_all(&rng_bytes).unwrap();
        f.flush().unwrap();

        let p = f.path().to_path_buf();
        assert_eq!(
            p.maybe_content_format().unwrap(),
            MaybeContentFormat::MaybeBinary
        );

        f.close().unwrap();

        // large text
        let mut f = tempfile::NamedTempFile::new().unwrap();
        let s = "📝 Repeat me 200 times".repeat(200);
        write!(&mut f, "{s}").unwrap();

        let p = f.path().to_path_buf();
        assert_eq!(
            p.maybe_content_format().unwrap(),
            MaybeContentFormat::MaybeLargeText
        );

        f.close().unwrap();
    }

    #[test]
    fn io_maybe_content_format_guess_zfile() {
        // large zlib file
        let mut f = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = ZlibEncoder::new(&f, Compression::default());

        let rng_string: String = (0..1000).map(|_| rand::random::<char>()).collect();
        encoder.write_all(rng_string.as_bytes()).unwrap();
        encoder.finish().unwrap();
        f.flush().unwrap();

        let p = f.path().to_path_buf();
        assert!(std::fs::metadata(f.path()).unwrap().len() > 850);

        match p.maybe_content_format().unwrap() {
            MaybeContentFormat::ZFile([b0, _, _, _]) => assert_eq!(b0, 0x78),
            _ => panic!("should be a ZFile compressed with zlib"),
        };

        f.close().unwrap();

        // large zstd file
        let mut f = tempfile::NamedTempFile::new().unwrap();

        let rng_string: String = (0..1000).map(|_| rand::random::<char>()).collect();
        zstd::stream::copy_encode(rng_string.as_bytes(), &mut f, 0).unwrap();
        f.flush().unwrap();

        let p = f.path().to_path_buf();

        assert!(std::fs::metadata(f.path()).unwrap().len() > 850);

        match p.maybe_content_format().unwrap() {
            MaybeContentFormat::ZFile(b) => assert_eq!(b, [0x28, 0xB5, 0x2F, 0xFD]),
            _ => panic!("should be a ZFile compressed with zlib"),
        };

        f.close().unwrap();
    }
}
