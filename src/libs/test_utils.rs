use tempfile::{tempdir, TempDir};

use crate::{Config, Container};

pub const PACK_TARGET_SIZE: u64 = 4 * 1024 * 1024;

pub fn new_container(pack_target_size: u64, compression: &str) -> (TempDir, Container) {
    let tmp_dir = tempdir().expect("Falied to create temp dir");
    let config = Config::new(pack_target_size, compression);

    dbg!(tmp_dir.path());
    let cnt = Container::new(tmp_dir.path());
    cnt.initialize(&config)
        .expect("fail to initialize container");

    // return ownership so the resource not leak
    (tmp_dir, cnt)
}
