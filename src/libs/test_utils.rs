use tempfile::{tempdir, TempDir};

use crate::{Config, Container};

pub const PACK_TARGET_SIZE: u64 = 4 * 1024 * 1024; // 4 MiB

pub fn new_container(pack_target_size: u64, compression: &str) -> (TempDir, Container) {
    let tmp_dir = tempdir().expect("Falied to create temp dir");
    let config = Config::new(pack_target_size, compression);

    let cnt = Container::new(tmp_dir.path());
    cnt.initialize(&config)
        .expect("fail to initialize container");

    // return ownership so the tmpdir will clean it self after the scope thus resource not leak
    (tmp_dir, cnt)
}
