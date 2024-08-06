use crate::{
    io_loose::push_to_loose, io_packs::push_to_packs, Container
};
use anyhow::Context;
use std::{
    fs::{self},
    io::BufReader,
    path::PathBuf,
};

pub enum StoreType {
    Loose,
    Packs,
}

pub fn add_file(
    file: &PathBuf,
    cnt: &Container,
    target: &StoreType,
) -> anyhow::Result<(String, String, u64)> {
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    // Race here if file changes in between stat and open, the source may changed
    // in the end of add check the size from stat and copied is identical.
    let source =
        fs::File::open(file).with_context(|| format!("open {} for read", file.display()))?;
    let mut source = BufReader::new(source);

    let (bytes_streamd, hash_hex) = match target {
        StoreType::Loose => push_to_loose(&mut source, cnt)?,
        StoreType::Packs => push_to_packs(file.clone(), cnt)?,
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
