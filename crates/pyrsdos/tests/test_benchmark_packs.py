import pytest
import hashlib

# @pytest.mark.benchmark(group="read_1000")
# def test_loose_read_rs(benchmark, rs_container):
#     """Add 1'000 objects to the container in loose form, and benchmark write and read speed."""
#     num_files = 1000
#     data_content = [str(i).encode("ascii") for i in range(num_files)]
#     hashkeys = []
#     for content in data_content:
#         hashkeys.append(rs_container.add_object(content))
#     expected_results = dict(zip(hashkeys, data_content))
#
#     # Note that here however the OS will be using the disk caches
#     results = benchmark(rs_container.get_objects_content, hashkeys)
#
#     assert results == expected_results
#
# @pytest.mark.benchmark(group="read_1000")
# def test_loose_read_py(benchmark, py_container):
#     """Add 1'000 objects to the container in loose form, and benchmark write and read speed."""
#     num_files = 1000
#     data_content = [str(i).encode("ascii") for i in range(num_files)]
#     hashkeys = []
#     for content in data_content:
#         hashkeys.append(py_container.add_object(content))
#     expected_results = dict(zip(hashkeys, data_content))
#
#     # Note that here however the OS will be using the disk caches
#     results = benchmark(py_container.get_objects_content, hashkeys)
#
#     assert results == expected_results
#
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
