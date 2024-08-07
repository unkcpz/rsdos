use indicatif::ProgressBar;
use sha2::{Digest, Sha256};
use std::{env, fs};

fn main() -> anyhow::Result<()> {
    let cnt_path = env::current_dir()?.join("sample_packs_read");
    fs::create_dir_all(&cnt_path)?;
    let n = 1000;
    let pack_target_size = 1024;
    let config = rsdos::Config::new(pack_target_size);

    let cnt = rsdos::Container::new(cnt_path);
    let args: Vec<String> = std::env::args().collect();
    let arg = args.get(1).unwrap();

    if args.len() > 1 {
        match &arg[..] {
            "reset" => {
                // INITIALIZE AND ADD FILES TO LOOSE
                cnt.reset();
                cnt.initialize(&config)
                    .expect("fail to initialize container");
                let bar = ProgressBar::new(n);
                let db = sled::open(cnt.packs_db()?)?;

                for i in 0..n {
                    bar.inc(1);
                    let content = format!("test {i}");
                    let bstring = content.as_bytes().to_vec();

                    rsdos::push_to_packs(bstring, &cnt, &db)?;
                }
            }
            "purge" => {
                fs::remove_dir_all(cnt.path)?;
            }
            "bench" => {
                // FN to benchmark
                let db = sled::open(cnt.packs_db()?)?;
                let hashkeys: Vec<String> = (0..n)
                    .map(|i| -> String {
                        let content = format!("test {i}");
                        let mut hasher = Sha256::new();
                        hasher.update(content.as_bytes());
                        let hashkey = hasher.finalize();
                        hex::encode(hashkey)
                    })
                    .collect();
                let _ = rsdos::io_packs::multi_pull_from_packs(&cnt, &hashkeys, &db)?;
            }
            _ => anyhow::bail!("unknown flag `{}`, expect `purge`, `bench` or `reset`", arg),
        }
    }

    Ok(())
}
