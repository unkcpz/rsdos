use rsdos::add_file::{stream_to_packs_multi, StoreType};
use sha2::{Digest, Sha256};
use tempfile::{tempdir, NamedTempFile};
use std::{collections::HashMap, env, fs, io::{Cursor, Read, Write}};

fn main() -> anyhow::Result<()> {
    let cnt = tempdir()?;
    let cnt_path = cnt.into_path();
    fs::create_dir_all(&cnt_path)?;

    let config = rsdos::Config::new(4);
    let cnt = rsdos::Container::new(cnt_path);

    // // INITIALIZE AND ADD FILES TO LOOSE
    // cnt.initialize(&config)
    //     .expect("fail to initialize container");
    //
    // // add 10 different files to loose
    // let mut hashkeys = Vec::with_capacity(1000);
    // // }
    // let sources: Vec<&mut [u8]> = (0..1000)
    //     .map(|i| {
    //         unsafe {
    //             let mut content = format!("test {i}");
    //             content.as_bytes_mut()
    //         }
    //     })
    //     .collect();
    //
    // stream_to_packs_multi(sources, &cnt);
    //
    // status audit
    let _ = rsdos::stat(&cnt).expect("fail to audit container stat");

    Ok(())
}
