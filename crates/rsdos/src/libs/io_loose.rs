use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::io::{copy_by_chunk, ByteString, HashWriter, ReaderMaker};
use crate::Container;
use crate::Error;

pub struct LObject {
    pub id: String,
    pub loc: PathBuf,
    pub expected_size: u64, // used for validate write operation
}

impl LObject {
    fn new<P: AsRef<Path>>(id: &str, loc: P, expected_size: u64) -> Self {
        Self {
            id: id.to_string(),
            loc: loc.as_ref().to_path_buf(),
            expected_size,
        }
    }

    #[allow(dead_code)]
    fn to_bytes(&self) -> Result<ByteString, Error> {
        let mut rdr = self.make_reader()?;
        let mut buf = vec![];
        let n = std::io::copy(&mut rdr, &mut buf)?;
        if n == self.expected_size {
            Ok(buf)
        } else {
            Err(Error::UnexpectedCopySize {
                expected: self.expected_size,
                got: n,
            })
        }
    }
}

impl ReaderMaker for LObject {
    fn make_reader(&self) -> Result<impl Read, Error> {
        Ok(fs::OpenOptions::new().read(true).open(&self.loc)?)
    }
}

pub fn pull_from_loose(hashkey: &str, cnt: &Container) -> Result<Option<LObject>, Error> {
    let loc = cnt
        .loose()?
        .join(format!("{}/{}", &hashkey[..2], &hashkey[2..]));
    if loc.exists() {
        let f = fs::File::open(&loc)?;
        let expected_size = f.metadata()?.len();
        let obj = LObject::new(hashkey, loc, expected_size);
        Ok(Some(obj))
    } else {
        Ok(None)
    }
}

pub fn push_to_loose(source: &impl ReaderMaker, cnt: &Container) -> Result<(u64, String), Error> {
    // <cnt_path>/sandbox/<uuid> as dst
    let dst = format!("{}.tmp", uuid::Uuid::new_v4());
    let dst = cnt.sandbox()?.join(dst);
    let mut writer = fs::File::create(&dst)?;

    let mut hasher = Sha256::new();
    let mut hwriter = HashWriter::new(&mut writer, &mut hasher);

    // write to object and store it in {hash:..2}/{hash:2..} file
    // first write to tmp and get the hash, than move it to the location.
    //
    // Note: using chunk copy is a slightly slow than direct copy but since I don't know the size,
    // have to do the pre-allocate with specific chunk size.
    // NOTE: this chunk_size is the upbound of the buf, which in order to control the size of
    // memory usage when coping large file. 512 KiB is way larger then the default buffer size in rust
    // (4KiB). Large buffer may increase change of loosing data.
    let chunk_size = 524_288; // 512 KiB TODO: make it configurable??
                              //
    let mut stream = source.make_reader()?;
    let bytes_copied = copy_by_chunk(&mut stream, &mut hwriter, chunk_size)
        .map_err(|err| Error::ChunkCopyError { source: err })?;
    let hash = hasher.finalize();
    let hash_hex = hex::encode(hash);

    let loose = cnt.loose()?;
    fs::create_dir_all(loose.join(format!("{}/", &hash_hex[..2])))?;
    let loose_dst = loose.join(format!("{}/{}", &hash_hex[..2], &hash_hex[2..]));

    // avoid move if duplicate exist to reduce overhead
    if !loose_dst.exists() {
        fs::rename(&dst, &loose_dst)?;
    }

    Ok((bytes_copied as u64, hash_hex))
}

#[cfg(test)]
mod tests {
    use crate::{
        io::ByteString,
        stat,
        test_utils::{gen_tmp_container, PACK_TARGET_SIZE},
    };

    use super::*;

    #[test]
    fn io_loose_insert_and_extract() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE).lock().unwrap();

        let bstr: ByteString = b"test 0".to_vec();
        let (_, hashkey) = push_to_loose(&bstr, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.loose, 1);

        let obj = pull_from_loose(&hashkey, &cnt).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(obj.to_bytes().unwrap()),
            String::from_utf8(b"test 0".to_vec())
        );
    }
}
