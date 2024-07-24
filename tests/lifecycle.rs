use disk_objectstore as dos;
use std::io::Write;
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
    let cnt = tempdir().unwrap();
    let cnt_path = cnt.into_path();

    dos::init(&cnt_path, 4).expect("unable to initialize container");

    // add 10 different files to loose
    for _i in 0..10 {
        // Note: security view the test is short term so safe to use NamedTempFile.
        let mut tf = NamedTempFile::new().unwrap();
        writeln!(tf, "test x").unwrap();

        let fp = tf.into_temp_path();
        dos::add_file(&fp.to_path_buf(), &cnt_path).expect("unable to add file {i}");
    }

    // status audit
    let info = dos::stat(&cnt_path).expect("fail to audit container stat");
    assert_eq!(info.count.loose, 1);
}
