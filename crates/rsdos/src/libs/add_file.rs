use anyhow::Context;
use std::{
    fs::{self},
    path::PathBuf,
};

use crate::Container;
use crate::io_loose::insert as loose_insert;
use crate::io_packs::insert as packs_insert;

pub enum StoreType {
    Loose,
    Packs,
}

pub fn add_file(
    file: &PathBuf,
    cnt: &Container,
    target: &StoreType,
) -> anyhow::Result<(String, String, u64)> {
    // Race here if file changes in between stat and push, the source may changed
    // in the end of add check, the size from stat and copied should be identical.
    // that is why we do streamed size check in the end.
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    let (bytes_streamd, hash_hex) = match target {
        StoreType::Loose => loose_insert(file.clone(), cnt)?,
        StoreType::Packs => packs_insert(file.clone(), cnt)?,
    };

    anyhow::ensure!(
        bytes_streamd == expected_size,
        format!(
            "bytes streamed: {}, bytes source: {}",
            bytes_streamd, expected_size
        )
    );

    Ok((hash_hex, file.display().to_string(), expected_size))
}
