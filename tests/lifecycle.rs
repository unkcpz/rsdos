use disk_objectstore as dos;
use std::io::{Read, Write};
use tempfile::{tempdir, NamedTempFile};

#[test]
fn lifecycle0() {
    // Default lifecycle:
    // Create 10 different loose objects
    let cnt = tempdir().unwrap();
    let cnt_path = cnt.into_path();

    dos::init(&cnt_path, 4).expect("unable to initialize container");

    // add 10 different files to loose
    for i in 0..10 {
        // Note: security view the test is short term so safe to use NamedTempFile.
        let mut tf = NamedTempFile::new().unwrap();
        writeln!(tf, "test {i}").unwrap();

        let fp = tf.into_temp_path();
        dos::add_file(&fp.to_path_buf(), &cnt_path).expect("unable to add file {i}");
    }

    // status audit
    let info = dos::stat(&cnt_path).expect("fail to audit container stat");
    assert_eq!(info.count.loose, 10);
}

#[test]
fn lifecycle1() {
    // Default lifecycle:
    // Create 10 same loose objects
    // regression checke: get the obj content by hash and compute hash is the same
    let cnt = tempdir().unwrap();
    let cnt_path = cnt.into_path();

    dos::init(&cnt_path, 4).expect("unable to initialize container");

    // add 10 different files to loose
    for _i in 0..10 {
        // Note: security view the test is short term so safe to use NamedTempFile.
        let mut tf = NamedTempFile::new().unwrap();
        writeln!(tf, "test x").unwrap();

        let fp = tf.into_temp_path();
        let _ = dos::add_file(&fp.to_path_buf(), &cnt_path).expect("unable to add file {i}");
    }

    // status audit
    let info = dos::stat(&cnt_path).expect("fail to audit container stat");
    assert_eq!(info.count.loose, 1);

    // 
}

#[test]
fn lifecycle2() {
    // Default lifecycle:
    // Create a loose objects
    // regression checke: save, get and check the obj content
    let cnt = tempdir().unwrap();
    let cnt_path = cnt.into_path();

    dos::init(&cnt_path, 4).expect("unable to initialize container");

    // Note: security view the test is short term so safe to use NamedTempFile.
    let mut tf = NamedTempFile::new().unwrap();
    writeln!(tf, "test x").unwrap();

    let fp = tf.into_temp_path();
    let hash_hex = dos::add_file(&fp.to_path_buf(), &cnt_path).expect("unable to add file {i}");

    // get obj by hash_hex
    let mut obj = dos::Object::from_hash(&hash_hex, &cnt_path).expect("get object from hash");

    let mut content = String::new();
    obj.reader.read_to_string(&mut content).unwrap();

    assert_eq!(content, "test x\n".to_string());
}
