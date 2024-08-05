use std::sync::Mutex;

use once_cell::sync::OnceCell;
use tempfile::tempdir;

use crate::{Config, Container};

pub const PACK_TARGET_SIZE: u64 = 4 * 1024 * 1024;

pub fn gen_tmp_container() -> &'static Mutex<Container> {
    static TMP_CONTAINER: OnceCell<Mutex<Container>> = OnceCell::new();
    TMP_CONTAINER.get_or_init(|| {
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        let config = Config::new(PACK_TARGET_SIZE);

        let cnt = Container::new(cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");
        Mutex::new(cnt)
    })
}
