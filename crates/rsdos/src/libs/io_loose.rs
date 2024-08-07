use std::io::{BufRead, BufReader, BufWriter, Read};
use std::{fs, usize};
use anyhow::Context;
use sha2::{Digest, Sha256};

use crate::io::{copy_by_chunk, HashWriter, ReaderMaker};
use crate::{Container, Object};

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

pub fn push_to_loose(source: impl ReaderMaker, cnt: &Container) -> anyhow::Result<(u64, String)>
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
    let mut stream = source.make_reader();
    let bytes_copied = copy_by_chunk(&mut stream, &mut hwriter, chunk_size)?;
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
