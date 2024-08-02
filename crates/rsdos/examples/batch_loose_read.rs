use rsdos::add_file::StoreType;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use std::{collections::HashMap, env, fs, io::{Cursor, Write}};

fn main() -> anyhow::Result<()> {
    // let cnt = tempdir()?;
    // let cnt_path = cnt.into_path();
    let cnt_path = env::current_dir()?.join("sample");
    fs::create_dir_all(&cnt_path)?;

    let config = rsdos::Config::new(4);

    let cnt = rsdos::Container::new(cnt_path);

    // INITIALIZE AND ADD FILES TO LOOSE
    cnt.initialize(&config)
        .expect("fail to initialize container");

    // add 10 different files to loose
    let mut hashkeys = Vec::with_capacity(1000);
    // }
    let orig_d: HashMap<String, String> = (0..1000)
        .map(|i| {
            let mut tf = NamedTempFile::new().unwrap();
            let content = format!("test {i}");
            write!(tf, "{content}").unwrap();

            let fp = tf.into_temp_path();
            let (hashkey, _, _) =
                rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose).expect("unable to add file {i}");
            hashkeys.push(hashkey.clone());
            (hashkey, content.to_string())
        })
        .collect();

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
    let d: HashMap<String, String> = hashkeys
        .iter()
        .map(|hashkey| {
            let content = match rsdos::io::pull_from_loose(hashkey, &cnt).unwrap()
            {
                Some(mut obj) => {
                    let mut buf = Vec::new();
                    let mut cursor = Cursor::new(&mut buf);

                    std::io::copy(&mut obj.reader, &mut cursor).unwrap();
                    buf
                }
                _ => todo!(),
            };
            (hashkey.to_string(), String::from_utf8(content).unwrap())
        })
        .collect();

    for (k, v) in orig_d {
        assert_eq!(*d.get(&k).unwrap().trim().to_string(), v);
    }

    // status audit
    let _ = rsdos::stat(&cnt).expect("fail to audit container stat");

    Ok(())
}
