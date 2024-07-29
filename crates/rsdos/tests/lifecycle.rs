#[cfg(test)]
mod tests {
    use rsdos::add_file::StoreType;
    use std::{
        collections::HashMap,
        io::{Read, Write},
    };
    use tempfile::{tempdir, NamedTempFile};

    const PACK_SIZE_TARGET: u64 = 4 * 1024 * 1024;

    #[test]
    fn lifecycle0() {
        // Default lifecycle:
        // Create 10 different loose objects
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        let config = rsdos::Config::new(PACK_SIZE_TARGET);

        let cnt = rsdos::Container::new(cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");

        // add 10 different files to loose
        for i in 0..10 {
            // Note: security view the test is short term so safe to use NamedTempFile.
            let mut tf = NamedTempFile::new().unwrap();
            write!(tf, "test {i}").unwrap();

            let fp = tf.into_temp_path();
            rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose)
                .expect("unable to add file {i}");
        }

        // status audit
        let info = rsdos::stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.loose, 10);
    }

    #[test]
    fn lifecycle1() {
        // Default lifecycle:
        // Create 10 same loose objects
        // regression checke: get the obj content by hash and compute hash is the same
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        let config = rsdos::Config::new(PACK_SIZE_TARGET);

        let cnt = rsdos::Container::new(cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");

        // add 10 different files to loose
        for _i in 0..10 {
            // Note: security view the test is short term so safe to use NamedTempFile.
            let mut tf = NamedTempFile::new().unwrap();
            write!(tf, "test x").unwrap();

            let fp = tf.into_temp_path();
            let _ = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose)
                .expect("unable to add file {i}");
        }

        // status audit
        let info = rsdos::stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.loose, 1);

        //
    }

    #[test]
    fn lifecycle2() {
        // Default lifecycle:
        // Create a loose object
        // regression checke: save, get and check the obj content
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        let config = rsdos::Config::new(PACK_SIZE_TARGET);

        let cnt = rsdos::Container::new(&cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");

        // Note: security view the test is short term so safe to use NamedTempFile.
        let mut tf = NamedTempFile::new().unwrap();
        write!(tf, "test x").unwrap();

        let fp = tf.into_temp_path();
        let (hash_hex, _, _) = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Loose)
            .expect("unable to add file {i}");

        // get obj by hash_hex
        let cnt = rsdos::Container::new(&cnt_path);
        let obj = rsdos::Object::from_hash(&hash_hex, &cnt, &StoreType::Loose)
            .expect("get object from hash");

        let mut content = String::new();
        obj.unwrap().reader.read_to_string(&mut content).unwrap();

        assert_eq!(content, "test x".to_string());
    }

    #[test]
    fn lifecycle3() -> anyhow::Result<()> {
        // Default lifecycle:
        // Create a 0 pack object
        // regression checke: save, get and check the obj content
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        let config = rsdos::Config::new(4);

        let cnt = rsdos::Container::new(cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");

        let orig_objs: HashMap<String, String> = (0..10)
            .map(|i| {
                let content = format!("test {i}");
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "test {i}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        //
        for (hash_hex, expected_content) in orig_objs {
            // find content from packs file
            let mut obj = rsdos::Object::from_hash(&hash_hex, &cnt, &StoreType::Packs)?.unwrap();
            let mut buffer = vec![];
            std::io::copy(&mut obj.reader, &mut buffer)?;
            let content = String::from_utf8(buffer)?;

            assert_eq!(content, expected_content);
        }

        // status audit
        let info = rsdos::stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 10);

        Ok(())
    }

    #[test]
    fn lifecycle4() -> anyhow::Result<()> {
        // Default lifecycle:
        // Have a large pack/0 file that exceed single file limit
        // Add a new file to pack will add to pack/1
        // regression checke: save, get and check the obj content
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        // Create a container with single pack target 1024 bytes
        let size_in_bytes = 1024;
        let config = rsdos::Config::new(size_in_bytes);

        let cnt = rsdos::Container::new(cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");

        let first_string = "0".repeat(usize::try_from(size_in_bytes)?);
        let mut tf = NamedTempFile::new().unwrap();
        write!(tf, "{first_string}").unwrap();
        let fp = tf.into_temp_path();
        let _ = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)?;

        let orig_objs: HashMap<String, String> = (0..10)
            .map(|i| {
                let content = format!("test {i}");
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "{content}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        // let out = fs::read_to_string(cnt.packs()?.join("0"))?;
        //
        for (hash_hex, expected_content) in orig_objs {
            let mut obj = rsdos::Object::from_hash(&hash_hex, &cnt, &StoreType::Packs)?.unwrap();
            let mut buffer = vec![];
            std::io::copy(&mut obj.reader, &mut buffer)?;
            let content = String::from_utf8(buffer)?;

            assert_eq!(content, expected_content);
        }

        // status audit
        let info = rsdos::stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 11);


        Ok(())
    }

    #[test]
    fn lifecycle5() -> anyhow::Result<()> {
        // insert 10 identical object to packs
        let cnt = tempdir().unwrap();
        let cnt_path = cnt.into_path();

        let config = rsdos::Config::new(4);

        let cnt = rsdos::Container::new(cnt_path);
        cnt.initialize(&config)
            .expect("fail to initialize container");

        let orig_objs: HashMap<String, String> = (0..10)
            .map(|_| {
                let content = "test".to_string();
                let mut tf = NamedTempFile::new().unwrap();
                write!(tf, "{content}").unwrap();

                let fp = tf.into_temp_path();
                let (hash_hex, _, _) = rsdos::add_file(&fp.to_path_buf(), &cnt, &StoreType::Packs)
                    .expect("add file to pack failed");

                (hash_hex, content)
            })
            .collect();

        //
        for (hash_hex, expected_content) in orig_objs {
            // find content from packs file
            let mut obj = rsdos::Object::from_hash(&hash_hex, &cnt, &StoreType::Packs)?.unwrap();
            let mut buffer = vec![];
            std::io::copy(&mut obj.reader, &mut buffer)?;
            let content = String::from_utf8(buffer)?;

            assert_eq!(content, expected_content);
        }

        // status audit
        let info = rsdos::stat(&cnt).expect("fail to audit container stat");
        assert_eq!(info.count.packs, 1);

        Ok(())
    }
}
