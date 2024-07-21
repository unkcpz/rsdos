use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const CONFIG_FILE: &str = "config.json";

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub container_id: Uuid,
    pub container_version: u32,
    pub loose_prefix_len: u32,
    pub pack_size_target: u64, // bytes
    pub hash_type: String,
    pub compression_algorithm: String,
}
