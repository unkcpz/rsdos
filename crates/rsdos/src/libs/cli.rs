use anyhow::Context;
use std::{fs, io::BufReader, path::PathBuf};

use crate::container::{
    traverse_loose, traverse_packs, Container, ContainerInfo, CountInfo, SizeInfo,
};
use crate::io_loose::insert as loose_insert;
use crate::io_packs::insert as packs_insert;
use crate::Error;

use crate::config::Config;
use crate::db::{self};

pub enum StoreType {
    Auto,
    Loose,
    Packs,
}

pub fn add_file(
    file: &PathBuf,
    cnt: &Container,
    to: &StoreType,
) -> anyhow::Result<(String, String, u64)> {
    // Race here if file changes in between stat and push, the source may changed
    // in the end of add check, the size from stat and copied should be identical.
    // that is why we do streamed size check in the end.
    let stat = fs::metadata(file).with_context(|| format!("stat {}", file.display()))?;
    let expected_size = stat.len();

    let (bytes_streamd, hash_hex) = match to {
        StoreType::Loose | StoreType::Auto => loose_insert(file.clone(), cnt)?,
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

pub fn stat(cnt: &Container) -> anyhow::Result<ContainerInfo> {
    cnt.valid()?;

    // Read config.json
    let config_path = cnt.config_file();
    let config = fs::File::open(&config_path).map_err(|err| Error::ConfigFileError {
        source: err,
        path: config_path.clone(),
    })?;
    let reader = BufReader::new(config);

    // read config
    let config: Config = serde_json::from_reader(reader).map_err(|err| Error::ConfigFileError {
        source: err.into(),
        path: config_path.clone(),
    })?;

    // traverse loose and compute number of objects and total size
    let iter_loose = traverse_loose(cnt).with_context(|| "traverse loose by iter")?;
    let (loose_files_count, loose_files_size) =
        iter_loose
            .into_iter()
            .fold((0, 0), |(count, size), path| match fs::metadata(path) {
                Ok(stat) => (count + 1, size + stat.len()),
                Err(_) => (count, size),
            });

    // packs info from db
    let packs_db = cnt.packs_db();
    let packs_db_size = fs::metadata(&packs_db)?.len();
    let (packs_count, packs_size) = db::stats(&packs_db)?;

    // traverse packs and compute
    let iter_packs = traverse_packs(cnt).with_context(|| "traverse packs by iter")?;
    let (packs_file_count, packs_file_size) =
        iter_packs
            .into_iter()
            .fold((0, 0), |(count, size), path| match fs::metadata(path) {
                Ok(stat) => (count + 1, size + stat.len()),
                Err(_) => (count, size),
            });

    Ok(ContainerInfo {
        location: cnt.path.display().to_string(),
        id: config.container_id.to_string(),
        compression_algorithm: config.compression_algorithm,
        count: CountInfo {
            loose: loose_files_count,
            packs: packs_count,
            packs_file: packs_file_count,
        },
        size: SizeInfo {
            loose: loose_files_size,
            packs: packs_size,
            packs_file: packs_file_size,
            packs_db: packs_db_size,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write};
    use tempfile::NamedTempFile;

    use crate::{
        io_packs,
        test_utils::{gen_tmp_container, PACK_TARGET_SIZE},
    };

    use super::*;

    #[test]
    fn cli_add_ten_diff_objs_to_loose() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        // add 10 different files to loose
        for i in 0..10 {
            // Note: security view the test is short term so safe to use NamedTempFile.
            let mut tf = NamedTempFile::new().unwrap();
            write!(tf, "test {i}").unwrap();

            let fp = tf.into_temp_path();
            add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose).expect("unable to add file {i}");
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.loose, 10);
    }

    /// Default lifecycle:
    /// Create 10 same loose objects
    /// regression check: get the obj content by hash and compute hash is the same
    #[test]
    fn cli_add_ten_same_objs_to_loose() {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        // add 10 different files to loose
        for _i in 0..10 {
            // Note: security view the test is short term so safe to use NamedTempFile.
            let mut tf = NamedTempFile::new().unwrap();
            write!(tf, "test x").unwrap();

            let fp = tf.into_temp_path();
            let _ = add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose)
                .expect("unable to add file {i}");
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.loose, 1);
    }

    #[test]
    fn cli_add_ten_diff_objs_to_packs() -> anyhow::Result<()> {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        let orig_objs: HashMap<String, String> = (0..10)
            .map(|i| {
                let content = format!("test {i}");
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "test {i}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        //
        for (hash_hex, expected_content) in orig_objs {
            // find content from packs file
            let obj = io_packs::extract(&hash_hex, &cnt)?.unwrap();
            assert_eq!(
                String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
                expected_content
            );
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 10);

        Ok(())
    }

    #[test]
    fn cli_add_ten_same_objs_to_packs() -> anyhow::Result<()> {
        let cnt = gen_tmp_container(PACK_TARGET_SIZE, "none").lock().unwrap();

        // insert 10 identical object to packs
        let orig_objs: HashMap<String, String> = (0..10)
            .map(|_| {
                let content = "test".to_string();
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "{content}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        //
        for (hash_hex, expected_content) in orig_objs {
            // find content from packs file
            let obj = io_packs::extract(&hash_hex, &cnt)?.unwrap();
            assert_eq!(
                String::from_utf8(obj.to_bytes().unwrap()).unwrap(),
                expected_content
            );
        }

        // status audit
        let info = stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 1);

        Ok(())
    }
}
