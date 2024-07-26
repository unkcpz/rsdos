import pytest
import hashlib

@pytest.mark.benchmark(group="read_single")
def test_loose_read_single_rs(benchmark, rs_container):
    """Add 1 objects to the container in loose form, and benchmark write and read speed."""
    content = str(5).encode("ascii")
    hashkey = rs_container.add_object(content)

    # Note that here however the OS will be using the disk caches
    result = benchmark(rs_container.get_object_content, hashkey)

    assert result == content

@pytest.mark.benchmark(group="read_single")
def test_loose_read_single_py(benchmark, py_container):
    """Add 1 objects to the container in loose form, and benchmark write and read speed."""
    content = str(5).encode("ascii")
    hashkey = py_container.add_object(content)

    # Note that here however the OS will be using the disk caches
    result = benchmark(py_container.get_object_content, hashkey)

    assert result == content

@pytest.mark.benchmark(group="write_single", min_rounds=3)
def test_loose_write_single_rs(rs_container, benchmark):
    """Add 1 objects to the container in packed form, and benchmark write and read speed."""
    content = str('test').encode("ascii")
    expected_hashkey = hashlib.sha256(content).hexdigest()

    def write_loose(rs_container, content):
        return rs_container.add_object(content)

    hashkey = benchmark(write_loose, rs_container, content)

    assert hashkey == expected_hashkey

@pytest.mark.benchmark(group="write_single", min_rounds=3)
def test_loose_write_single_py(py_container, benchmark):
    """Add 1 objects to the container in packed form, and benchmark write and read speed."""
    content = str('test').encode("ascii")
    expected_hashkey = hashlib.sha256(content).hexdigest()

    def write_loose(py_container, content):
        return py_container.add_object(content)

    hashkey = benchmark(write_loose, py_container, content)

    assert hashkey == expected_hashkey

@pytest.mark.benchmark(group="read_1000")
def test_loose_read_rs(benchmark, rs_container):
    """Add 1'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(rs_container.add_object(content))
    expected_results = dict(zip(hashkeys, data_content))

    # Note that here however the OS will be using the disk caches
    results = benchmark(rs_container.get_objects_content, hashkeys)

    assert results == expected_results

@pytest.mark.benchmark(group="read_1000")
def test_loose_read_py(benchmark, py_container):
    """Add 1'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(py_container.add_object(content))
    expected_results = dict(zip(hashkeys, data_content))

    # Note that here however the OS will be using the disk caches
    results = benchmark(py_container.get_objects_content, hashkeys)

    assert results == expected_results

@pytest.mark.benchmark(group="write_1000", min_rounds=3)
def test_loose_write_rs(rs_container, benchmark):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    def write_loose(rs_container, contents):
        retval = []
        for content in contents:
            retval.append(rs_container.add_object(content))
        return retval

    hashkeys = benchmark(write_loose, rs_container, data_content)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys

@pytest.mark.benchmark(group="write_1000", min_rounds=3)
def test_loose_write_py(py_container, benchmark):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    def write_loose(py_container, contents):
        retval = []
        for content in contents:
            retval.append(py_container.add_object(content))
        return retval

    hashkeys = benchmark(write_loose, py_container, data_content)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys

