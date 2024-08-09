use indicatif::ProgressBar;
use std::{env, fs};

fn main() -> anyhow::Result<()> {
    let cnt_path = env::current_dir()?.join("sample_pack_all_loose");
    fs::create_dir_all(&cnt_path)?;
    let n = 5000;
    let pack_target_size = 4 * 1024;
    let config = rsdos::Config::new(pack_target_size);

    let cnt = rsdos::Container::new(cnt_path);
    let args: Vec<String> = std::env::args().collect();
    let arg1 = args.get(1).unwrap();

    if args.len() > 1 {
        match &arg1[..] {
            "reset" => {
                // INITIALIZE AND ADD FILES TO LOOSE
                cnt.reset();
                cnt.initialize(&config)
                    .expect("fail to initialize container");
                let bar = ProgressBar::new(n);

                for i in 0..n {
                    bar.inc(1);
                    // let content = format!("test {i}");
                    let content = "test".repeat(i as usize);
                    let bstring = content.as_bytes().to_vec();

                    rsdos::io_loose::insert(bstring, &cnt)?;
                }
            }
            "purge" => {
                fs::remove_dir_all(cnt.path)?;
            }
            "bench" => {
                rsdos::maintain::pack_loose(&cnt)?;
            }
            _ => anyhow::bail!(
                "unknown flag `{}`, expect `purge`, `bench` or `reset`",
                arg1
            ),
        }
    }

    Ok(())
}
