use bytes::Buf;
use rsdos::push_to_loose;
use std::{collections::HashMap, env, fs};

fn main() -> anyhow::Result<()> {
    let cnt_path = env::current_dir()?.join("sample");
    fs::create_dir_all(&cnt_path)?;

    let config = rsdos::Config::new(4 * 1024 * 1024);

    let cnt = rsdos::Container::new(cnt_path);

    // INITIALIZE AND ADD FILES TO LOOSE
    // cnt.initialize(&config)
    //     .expect("fail to initialize container");
    //
    // let n = 10000;
    // let mut hash_content_map: HashMap<String, String> = HashMap::new();
    // for i in 0..n {
    //     let content = format!("test {i:03}"); // 8 bytes each
    //     let buf = content.clone().into_bytes();
    //     let (_, hash) = push_to_loose(&mut buf.reader(), &cnt).unwrap();
    //     hash_content_map.insert(hash, content);
    // }

    // pack loose
    rsdos::maintain::pack_loose(&cnt)?;

    Ok(())
}
