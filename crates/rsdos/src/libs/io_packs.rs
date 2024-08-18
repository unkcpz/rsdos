use flate2;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use rusqlite::{params, params_from_iter, Connection};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Take};
use std::path::{Path, PathBuf};

use crate::container::Compression;
use crate::db::PackEntry;
use crate::io::{copy_by_chunk, ByteString, HashWriter, MaybeContentFormat, ReaderMaker};
use crate::{db, Container};

use crate::utils::Dir;
use crate::Error;

/// ``raw_size`` is the size without compress.
pub struct PObject {
    pub id: String,
    pub loc: PathBuf,
    pub offset: u64,
    pub raw_size: u64, // used for checking data integrity
    pub size: u64,
    pub compressed: bool,
    // pub compress_algo: String, (zstd+22, zlib+3 ...)
    // pub checksum: u64, // CRC32
}

impl PObject {
    fn new<P: AsRef<Path>>(
        id: &str,
        loc: P,
        offset: u64,
        raw_size: u64,
        size: u64,
        compressed: bool,
    ) -> Self {
        Self {
            id: id.to_string(),
            loc: loc.as_ref().to_path_buf(),
            offset,
            raw_size,
            size,
            compressed,
        }
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Result<ByteString, Error> {
        let mut rdr = self.make_reader()?;
        let mut buf = vec![];
        let n = std::io::copy(&mut rdr, &mut buf)?;
        // FIXME: (v2) flasky assert, less valid if read and write size is different after compression
        // At the moment, n is the size from rdr which is the compressed data therefore should
        // equal to self.size. But the purpose of this test is for data integrity, the better way
        // is to using a cheap checksum (CRC32 for error detecting, md5/sha is for crypt) as the
        // content header.
        if n == self.raw_size {
            Ok(buf)
        } else {
            Err(Error::UnexpectedCopySize {
                expected: self.raw_size,
                got: n,
            })
        }
    }
}

enum PReader {
    Uncompressed(Take<File>),
    Zlib(ZlibDecoder<Take<File>>),
}

impl Read for PReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            PReader::Zlib(inner) => inner.read(buf),
            PReader::Uncompressed(inner) => inner.read(buf),
        }
    }
}

impl ReaderMaker for PObject {
    fn make_reader(&self) -> Result<impl Read, crate::Error> {
        let mut f = fs::OpenOptions::new().read(true).open(&self.loc)?;
        f.seek(SeekFrom::Start(self.offset))?;
        // NOTE: V2 should add support to zstd
        if self.compressed {
            let rdr = PReader::Zlib(ZlibDecoder::new(f.take(self.size)));
            Ok(rdr)
        } else {
            let rdr = PReader::Uncompressed(f.take(self.size));
            Ok(rdr)
        }
    }
}

// XXX: how to combine this with using extract_many???
// In principle, single read is more practical than the multiple read,
// should considered other way around to use this extract in extract_many function.
// The reason is that read will first fill the memory so kind of a problem when reading large
// file. For a single read, the reader can be returned (file with offset and size to read), and
// then proceed with write to writer using buffer reader/writer.
pub fn extract(hashkey: &str, cnt: &Container) -> Result<Option<PObject>, Error> {
    cnt.valid()?;
    let conn = Connection::open(cnt.packs_db())?;
    if let Some(pn) = db::select(&conn, hashkey)? {
        let pack_id = pn.pack_id;
        let loc = cnt.packs().join(format!("{pack_id}"));
        let obj = PObject::new(hashkey, loc, pn.offset, pn.raw_size, pn.size, pn.compressed);
        Ok(Some(obj))
    } else {
        Ok(None)
    }
}

fn _chunked<I>(mut iter: I, chunk_size: usize) -> impl Iterator<Item = Vec<I::Item>>
where
    I: Iterator,
{
    std::iter::from_fn(move || {
        let chunk: Vec<_> = iter.by_ref().take(chunk_size).collect();
        if chunk.is_empty() {
            None
        } else {
            Some(chunk)
        }
    })
}

/// ``extract_many`` fetch an interator of ``PObject`` from given hashkeys
///
/// NOTE: the return type declaration is not fully correct, the return iterator should live as long
/// as at most of ``hashkeys`` iterator, but the return type means it live as long as at least of
/// ``hashkeys``. It is subtle because of Rust impl trait problem, and will be changed in the
/// future. See: the talk of [Impl Trait aka Look ma’, no generics! by Jon Gjengset](https://www.youtube.com/watch?v=CWiz_RtA1Hw&t=2230s) for how to correct
/// it. I didn't implement the change because this function in this crate is usually called once
/// therefore less likely hit the edge case that borrow checker will confuse from this.
pub fn extract_many<'a, I>(
    hashkeys: I,
    cnt: &'a Container,
) -> Result<impl Iterator<Item = PObject> + 'a, Error>
where
    I: IntoIterator + 'a,
    I::Item: ToString,
{
    cnt.valid()?;

    // TODO: make chunk size configuable
    let _max_chunk_iterate_length = 9500;
    let in_sql_max_length = 950;

    let conn = Connection::open(cnt.packs_db())?;
    let chunked_iter = _chunked(hashkeys.into_iter(), in_sql_max_length);

    // XXX: why not work??
    // let chunked_iter = std::iter::from_fn(move || {
    //     let chunk: Vec<_> = hashkeys.into_iter().by_ref().take(in_sql_max_length).collect();
    //     if chunk.is_empty() {
    //         None
    //     } else {
    //         Some(chunk)
    //     }
    // });

    // NOTE: I believe when yield is available in rust (https://without.boats/blog/a-four-year-plan/)
    // this can be more straightforward implemented. I was quite struggle with the ownership here
    // and have to use move for both `chunk` and inner iterator.
    let iter_vec = chunked_iter.flat_map(move |chunk| {
        let placeholders: Vec<&str> = (0..chunk.len()).map(|_| "?").collect();
        let mut stmt = conn.prepare_cached(&format!("SELECT hashkey, compressed, size, offset, length, pack_id FROM db_object WHERE hashkey IN ({})", placeholders.join(","))).unwrap();
        let chunk = chunk.into_iter().map(|x| x.to_string());
        let rows = stmt
            .query_map(params_from_iter(chunk), |row| {
                let hashkey: String = row.get(0)?;
                let compressed: bool = row.get(1)?;
                let raw_size: u64 = row.get(2)?;
                let offset: u64 = row.get(3)?;
                let size: u64 = row.get(4)?;
                let pack_id: u64 = row.get(5)?;

                Ok(PackEntry {
                    hashkey,
                    compressed,
                    raw_size,
                    size,
                    offset,
                    pack_id,
                })
            }).expect("query failed");
        let mut rows = rows
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        std::iter::from_fn(move || {
            if let Some(pn) = rows.pop() {
                let pack_id = pn.pack_id;
                // XXX: I should not return Result for cnt.<subfolder>, instead better to valitate
                // the cnt and then just return PathBuf. Then I can get rid of `unwrap` for some
                // places.
                let packs_path = cnt.packs();
                let loc = packs_path.join(format!("{pack_id}"));
                let obj = PObject::new(
                    &pn.hashkey,
                    loc,
                    pn.offset,
                    pn.raw_size,
                    pn.size,
                    pn.compressed,
                );
                Some(obj)
            } else {
                None
            }

        })
    });
    Ok(iter_vec)
}

pub fn insert<T>(source: T, cnt: &Container) -> Result<(u64, String), Error>
where
    T: ReaderMaker,
{
    let (bytes_copied, hash_hex) = insert_many(vec![source], cnt)?
        .first()
        .map(|(n, hash)| (*n, hash.clone()))
        .unwrap();

    Ok((bytes_copied, hash_hex))
}

fn find_current_pack_id(packs: &PathBuf, pack_size_target: u64) -> Result<u64, Error> {
    // make sure there is a pack if not create 0
    if Dir(packs).is_empty()? {
        fs::File::create(packs.join("0"))?;
    }
    let mut current_pack_id = 0;
    for entry in packs.read_dir()? {
        let path = entry?.path();
        if let Some(filename) = path.file_name() {
            let n = filename.to_string_lossy();
            let n = n
                .parse::<u64>()
                .map_err(|err| Error::ParsePackFilenameError {
                    source: err,
                    n: n.to_string(),
                })?;
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
            .map_err(|err| Error::IoOpen {
                source: err,
                path: p,
            })?;
    }

    Ok(current_pack_id)
}

pub fn insert_many<I>(sources: I, cnt: &Container) -> Result<Vec<(u64, String)>, Error>
where
    I: IntoIterator,
    I::Item: ReaderMaker,
{
    let compression = cnt.compression()?;
    _insert_many_internal(sources, cnt, &compression)
}

pub fn _insert_many_internal<I>(
    sources: I,
    cnt: &Container,
    compression: &Compression,
) -> Result<Vec<(u64, String)>, Error>
where
    I: IntoIterator,
    I::Item: ReaderMaker,
{
    cnt.valid()?;

    let mut conn = Connection::open(cnt.packs_db())?;
    let packs = cnt.packs();
    let pack_size_target = cnt.config()?.pack_size_target;

    // cwp: current working pack
    let mut cwp_id = find_current_pack_id(&cnt.packs(), pack_size_target)?;
    let cwp = cnt.packs().join(format!("{cwp_id}"));
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
        // transaction for every pack writing
        let tx = conn.transaction()?;
        if offset >= pack_size_target {
            // reset when move to new pack
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
            // 64 KiB from legacy dos  TODO: make it configurable??
            let chunk_size = 65_536;

            // XXX: for if need to do the valitation for the hash, the idea is to having an object
            // encapsulate the pre-computed hash (better with cheap checksum). For Readers that has no pre-compute hash it return
            // None. The method is from ReaderMaker and calling rmaker.expected_hash(). If the hash
            // already exist and do not need to run validation, the writer can be normal writer without
            // hash.

            let mut stream = rmaker.make_reader()?;
            let mut writer = HashWriter::new(&mut cwp, &mut hasher);

            let (bytes_read, compressed) = match (compression, rmaker.maybe_content_format()) {
                (Compression::Zlib(level), Ok(MaybeContentFormat::MaybeLargeText)) => {
                    // The buff (when calling `copy_by_chunk`) is created from reader (e.g. the original data) so does not matter
                    // HashWriter wraps Compression Writer or v.v.
                    let mut writer =
                        ZlibEncoder::new(&mut writer, flate2::Compression::new(*level));
                    let bytes_copied = copy_by_chunk(&mut stream, &mut writer, chunk_size)?;

                    (bytes_copied, true)
                }
                (Compression::Zstd(_), Ok(MaybeContentFormat::MaybeLargeText)) => {
                    todo!()
                }
                _ => {
                    let mut writer = HashWriter::new(&mut cwp, &mut hasher);
                    let bytes_copied = copy_by_chunk(&mut stream, &mut writer, chunk_size)?;

                    (bytes_copied, false)
                }
            };
            let hash = hasher.finalize_reset();
            let hash_hex = hex::encode(hash);

            // look at the end of cwp compute how many bytes had been written
            let bytes_write = cwp.stream_position()? - offset;

            let mut stmt = tx.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
            stmt.execute(params![
                &hash_hex,
                compressed,
                bytes_read,
                offset,
                bytes_write,
                cwp_id,
            ])
            .map_err(|err| Error::SQLiteInsertError { source: err })?;
            offset += bytes_write;

            // TODO: Should be removed bytes_write with considering using cheap checksum for PObject.
            nbytes_hash.push((bytes_write, hash_hex));

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
    use rstest::*;
    use std::{collections::HashMap, io::Write};

    use crate::{
        io::ByteString,
        stat,
        test_utils::{gen_tmp_container, PACK_TARGET_SIZE},
    };

    use super::*;

    #[test]
    fn io_packs_insert_0_when_empty() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        let bstr: ByteString = b"test 0".to_vec();
        let (_, hash) = insert(bstr, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, 1);

        // also check pack DB point to correct location to extract content
        let obj = extract(&hash, &cnt).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
            "test 0".to_string()
        );

        // subsquent add will still goes to pack 0 (since pack_target_size is 4 GiB)
        let bstr: ByteString = b"test 1".to_vec();
        let (_, hash) = insert(bstr, &cnt).unwrap();

        // check packs has `0` and audit has only one pack
        // check content of 0 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, 2);

        let obj = extract(&hash, &cnt).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
            "test 1".to_string()
        );
    }

    #[test]
    fn io_packs_insert_1_when_1_exist() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        // create fack placeholder empty pack 0 and pack 1
        // it is expected that content will be added to pack1
        let packs = cnt.packs();
        fs::File::create(packs.join("0")).unwrap();
        fs::File::create(packs.join("1")).unwrap();

        let bstr: ByteString = b"test 0".to_vec();
        let (_, hash) = insert(bstr, &cnt).unwrap();

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 2);
        assert_eq!(info.count.packs, 1);

        let obj = extract(&hash, &cnt).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
            "test 0".to_string()
        );
    }

    #[test]
    fn io_packs_insert_2_when_1_reach_limit() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();
        let pack_target_size = cnt.config().unwrap().pack_size_target;

        // snuck limit size of bytes into pack 1 and new bytes will go to pack 2
        let packs = cnt.packs();
        fs::File::create(packs.join("0")).unwrap();
        let mut p1 = fs::File::create(packs.join("1")).unwrap();
        let bytes_holder = vec![0u8; usize::try_from(pack_target_size).unwrap()];
        p1.write_all(&bytes_holder).unwrap();

        // more bytes
        // let mut buf = b"test 0".reader();
        // let (_, hash) = insert(&mut buf, &cnt).unwrap();
        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}");
            let buf = content.clone().into_bytes();
            let (_, hash) = insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 3);
        assert_eq!(info.count.packs, 100);

        for (hash, content) in hash_content_map {
            let obj = extract(&hash, &cnt).unwrap().unwrap();
            assert_eq!(String::from_utf8(obj.to_bytes().unwrap()).unwrap(), content);
        }
    }

    #[test]
    fn io_packs_extract_from_any_single() {
        let cnt = gen_tmp_container(6400, "none").lock().unwrap();
        let n = 100;

        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..n {
            let content = format!("test {i}");
            let buf = content.clone().into_bytes();
            let (_, hash) = insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        // check packs has 2 packs
        // check content of 1 pack is `test 0`
        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 1);
        assert_eq!(info.count.packs, n);

        for (hash, content) in hash_content_map {
            let obj = extract(&hash, &cnt).unwrap().unwrap();
            assert_eq!(String::from_utf8(obj.to_bytes().unwrap()).unwrap(), content);
        }
    }

    #[test]
    fn io_packs_inselt_many() {
        let cnt = gen_tmp_container(64, "zlib:+1").lock().unwrap();

        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}").repeat(i);
            let buf = content.clone().into_bytes();
            let (_, hash) = insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs_file, 34);
        assert_eq!(info.count.packs, 100);
    }

    #[rstest]
    #[case("none")]
    #[case("zlib+1")]
    #[case("zlib:+9")]
    fn io_packs_extract_many(#[case] algo: &str) {
        let cnt = gen_tmp_container(64, algo).lock().unwrap();

        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..100 {
            let content = format!("test {i}").repeat(i);
            let buf = content.clone().into_bytes();
            let (_, hash) = insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs, 100);

        let mut hashkeys = hash_content_map
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        // add two random hashkeys that will not be found therefore will not influence the result
        hashkeys
            .push("68e2056a0496c469727fa5ab041e1778e39137643fd24db94dd7a532db17aaba".to_string());
        hashkeys
            .push("7e76df6ac7d08a837f7212e765edd07333c8159ffa0484bc26394e7ffd898817".to_string());

        let objs = extract_many(&hashkeys, &cnt).unwrap();

        let mut count = 0;
        for obj in objs {
            count += 1;
            let content = hash_content_map.get(&obj.id).unwrap();
            assert_eq!(
                String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
                content.to_owned()
            );
        }
        assert_eq!(count + 2, hashkeys.len());
    }

    #[rstest]
    #[case("none")]
    #[case("zlib+1")]
    #[case("zlib:+9")]
    /// Test if the content size is larger than the copy chunk size (64KiB)
    fn io_packs_extract_many_large_content(#[case] algo: &str) {
        let cnt = gen_tmp_container(64 * 1024 * 1024, algo).lock().unwrap();

        let mut hash_content_map: HashMap<String, String> = HashMap::new();
        for i in 0..10 {
            // create a 800 KiB data
            let content = format!("8bytes{i}").repeat(100 * 1024);
            let buf = content.clone().into_bytes();
            let (_, hash) = insert(buf, &cnt).unwrap();
            hash_content_map.insert(hash, content);
        }

        let info = stat(&cnt).unwrap();
        assert_eq!(info.count.packs, 10);

        let mut hashkeys = hash_content_map
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        // add two random hashkeys that will not be found therefore will not influence the result
        hashkeys
            .push("68e2056a0496c469727fa5ab041e1778e39137643fd24db94dd7a532db17aaba".to_string());
        hashkeys
            .push("7e76df6ac7d08a837f7212e765edd07333c8159ffa0484bc26394e7ffd898817".to_string());

        let objs = extract_many(&hashkeys, &cnt).unwrap();

        let mut count = 0;
        for obj in objs {
            count += 1;
            let content = hash_content_map.get(&obj.id).unwrap();
            assert_eq!(
                String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
                content.to_owned()
            );
        }
        assert_eq!(count + 2, hashkeys.len());
    }
}
