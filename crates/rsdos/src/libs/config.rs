use serde::{Deserialize, Serialize};
use uuid::Uuid;

const CONTAINER_VERSION: u32 = 1;
const LOOSE_PREFIX_LEN: u32 = 2;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub container_id: Uuid,
    pub container_version: u32,
    pub loose_prefix_len: u32,
    pub pack_size_target: u64, // bytes
    pub hash_type: String,
    pub compression_algorithm: String,
}

impl Config {
    #[must_use]
    pub fn new(pack_size_target: u64, compression: &str) -> Self {
        // TODO: use parse-size crate to parse size_target that accept GiB, MiB ...
        Config {
            container_id: uuid::Uuid::new_v4(),
            container_version: CONTAINER_VERSION,
            loose_prefix_len: LOOSE_PREFIX_LEN,
            pack_size_target,
            hash_type: "sha256".to_string(),
            compression_algorithm: compression.to_string(),
        }
    }
}
