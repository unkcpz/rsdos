import pytest
import hashlib
import random

@pytest.mark.benchmark(group="read_single")
def test_packs_read_single_rs(rs_container, benchmark):
    """Add 10'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 10000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]
    expected_results_dict = dict(zip(expected_hashkeys, data_content))

    hashkeys = rs_container.add_objects_to_pack(data_content, compress=False)
    random.shuffle(hashkeys)
    # Note that here however the OS will be using the disk caches
    result = benchmark(rs_container.get_object_content, hashkeys[0])

    assert result == expected_results_dict[hashkeys[0]]

@pytest.mark.benchmark(group="read_single")
def test_packs_read_single_py(py_container, benchmark):
    """Add 10'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 10000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]
    expected_results_dict = dict(zip(expected_hashkeys, data_content))

    hashkeys = py_container.add_objects_to_pack(data_content, compress=False)
    random.shuffle(hashkeys)
    # Note that here however the OS will be using the disk caches
    result = benchmark(py_container.get_object_content, hashkeys[0])

    assert result == expected_results_dict[hashkeys[0]]

@pytest.mark.benchmark(group="read_10000")
def test_packs_read_rs(benchmark, rs_container):
    """Add 10'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 10000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]
    expected_results_dict = dict(zip(expected_hashkeys, data_content))

    hashkeys = rs_container.add_objects_to_pack(data_content, compress=False)
    random.shuffle(hashkeys)
    # Note that here however the OS will be using the disk caches
    results = benchmark(rs_container.get_objects_content, hashkeys)

    assert results == expected_results_dict

@pytest.mark.benchmark(group="read_10000")
def test_packs_read_py(benchmark, py_container):
    """Add 10'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 10000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]
    expected_results_dict = dict(zip(expected_hashkeys, data_content))

    hashkeys = py_container.add_objects_to_pack(data_content, compress=False)
    random.shuffle(hashkeys)
    # Note that here however the OS will be using the disk caches
    results = benchmark(py_container.get_objects_content, hashkeys)

    assert results == expected_results_dict

@pytest.mark.benchmark(group="write_1_packs", min_rounds=3)
def test_packs_write_rs_single(rs_container, benchmark):
    """Add 1 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    hashkeys = benchmark(rs_container.add_objects_to_pack, data_content, compress=False)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys
    
@pytest.mark.benchmark(group="write_1_packs", min_rounds=3)
def test_packs_write_rs_single_1000(rs_container, benchmark):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    hashkeys = benchmark(rs_container.add_objects_to_pack, data_content, compress=False)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys

@pytest.mark.benchmark(group="write_1000_packs", min_rounds=3)
def test_packs_write_rs(rs_container, benchmark):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    hashkeys = benchmark(rs_container.add_objects_to_pack, data_content, compress=False)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys

@pytest.mark.benchmark(group="write_1000_packs", min_rounds=3)
def test_packs_write_py(py_container, benchmark):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]


    hashkeys = benchmark(py_container.add_objects_to_pack, data_content, compress=False)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys
