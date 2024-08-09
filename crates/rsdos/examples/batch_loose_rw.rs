use indicatif::ProgressBar;
use sha2::{Digest, Sha256};
use std::{env, fs};

fn main() -> anyhow::Result<()> {
    let cnt_path = env::current_dir()?.join("sample_loose_rw");
    fs::create_dir_all(&cnt_path)?;
    let n = 5000;
    let pack_target_size = 4 * 1024;
    let config = rsdos::Config::new(pack_target_size);

    let cnt = rsdos::Container::new(cnt_path);
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 {
        let arg1 = args.get(1).unwrap();

        match &arg1[..] {
            "reset" => {
                // INITIALIZE AND ADD FILES TO LOOSE
                cnt.reset();
                cnt.initialize(&config)
                    .expect("fail to initialize container");
            }
            "purge" => {
                fs::remove_dir_all(cnt.path)?;
            }
            "bench" => {
                let arg2 = args.get(2).unwrap();
                match &arg2[..] {
                    "push" => {
                        let bar = ProgressBar::new(n);

                        for i in 0..n {
                            bar.inc(1);
                            // let content = format!("test {i}");
                            let content = "test".repeat(i as usize);
                            let bstring = content.as_bytes().to_vec();

                            rsdos::io_loose::insert(bstring, &cnt)?;
                        }
                    }
                    "pull" => {
                        // FN to benchmark
                        let mut hasher = Sha256::new();
                        for i in 0..n {
                            let content = "test".repeat(i as usize);
                            hasher.update(content.as_bytes());
                            let hashkey = &hasher.finalize_reset();
                            let hashkey = hex::encode(hashkey);

                            let _ = rsdos::io_loose::extract(&hashkey, &cnt)?;
                        }
                    }
                    _ => anyhow::bail!("unknown flag `{}`, expect `push`, `pull`", arg2),
                }
            }
            _ => anyhow::bail!(
                "unknown flag `{}`, expect `purge`, `bench` or `reset`",
                arg1
            ),
        }
    }

    Ok(())
}
