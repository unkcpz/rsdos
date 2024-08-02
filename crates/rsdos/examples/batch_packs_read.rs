use rsdos::add_file::StoreType;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env, fs,
    io::{Cursor, Write},
};
use tempfile::NamedTempFile;

fn main() -> anyhow::Result<()> {
    let cnt_path = env::current_dir()?.join("sample");
    fs::create_dir_all(&cnt_path)?;

    let config = rsdos::Config::new(4 * 1024 * 1024);

    let cnt = rsdos::Container::new(cnt_path);

    // // INITIALIZE AND ADD FILES TO LOOSE
    // cnt.initialize(&config)
    //     .expect("fail to initialize container");
    //
    // let mut hashkeys = Vec::with_capacity(1000);
    // let orig_d: HashMap<String, String> = (0..1000)
    //     .map(|i| {
    //         let mut tf = NamedTempFile::new().unwrap();
    //         let content = format!("test {i}");
    //         write!(tf, "{content}").unwrap();
    //
    //         let fp = tf.into_temp_path();
    //         let (hashkey, _, _) = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
    //             .expect("unable to add file {i}");
    //         hashkeys.push(hashkey.clone());
    //         (hashkey, content.to_string())
    //     })
    //     .collect();

    // read by hashkey
    let hashkeys: Vec<String> = (0..1000)
        .map(|i| -> String {
            let content = format!("test {i}");
            let mut hasher = Sha256::new();
            hasher.update(content.as_bytes());
            let hashkey = hasher.finalize();
            hex::encode(hashkey)
        })
        .collect();
    let mut objs = rsdos::io::multi_pull_from_packs(&cnt, &hashkeys)?;
    let mut buf = Vec::new();
    let d: HashMap<String, String> = objs
        .iter_mut()
        .map(|obj| {
            let hashkey = &obj.hashkey;
            buf.clear();
            let mut cursor = Cursor::new(&mut buf);
            std::io::copy(&mut obj.reader, &mut cursor).unwrap();
            (hashkey.to_owned(), String::from_utf8(buf.clone()).unwrap())
        })
        .collect();

    // for (k, v) in orig_d {
    //     assert_eq!(*d.get(&k).unwrap().trim().to_string(), v);
    // }
    //
    // // status audit
    // let _ = rsdos::stat(&cnt).expect("fail to audit container stat");

    Ok(())
}
