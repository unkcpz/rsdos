import hashlib

from rsdos import Container


def test_write_single(rs_container):
    """test write 1 object to container in loose form"""
    content = str("test").encode("ascii")
    expected_hashkey = hashlib.sha256(content).hexdigest()

    hashkey = rs_container.add_object(content)

    assert hashkey == expected_hashkey


def test_read_single(rs_container):
    """Add 1 objects to the container in loose form, and test read"""
    content = str(5).encode("ascii")
    hashkey = rs_container.add_object(content)

    # Note that here however the OS will be using the disk caches
    result = rs_container.get_object_content(hashkey)

    assert result == content


def test_write_1000_files(rs_container):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    contents = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [hashlib.sha256(content).hexdigest() for content in contents]

    hashkeys = []
    for content in contents:
        hashkeys.append(rs_container.add_object(content))

    assert len(hashkeys) == len(contents)
    assert expected_hashkeys == hashkeys


def test_count_10000(tmp_path):
    rs_container = Container(tmp_path)
    rs_container.init_container()

    num_files = 10000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    for content in data_content:
        rs_container.add_object(content)

    n_objs = rs_container.count_objects()

    assert n_objs == num_files


def test_get_total_size_10000(rs_container):
    num_files = 10000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    for content in data_content:
        rs_container.add_object(content)

    total_size = rs_container.get_total_size()

    print(total_size)
