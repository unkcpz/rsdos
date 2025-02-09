from rsdos import Container, CompressMode
import pytest
import hashlib
import random


@pytest.mark.parametrize(
    "compress_mode",
    [CompressMode.YES, CompressMode.NO],
)
def test_packs_read_single(tmp_path, compress_mode):
    """Add 1 objects to the container in loose form, and benchmark write and read speed."""
    cnt = Container(tmp_path)
    cnt.init_container()
    num_files = 1
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]
    expected_results_dict = dict(zip(expected_hashkeys, data_content))

    hashkeys = cnt.add_objects_to_pack(data_content, compress=compress_mode)
    random.shuffle(hashkeys)
    # Note that here however the OS will be using the disk caches
    result = cnt.get_object_content(hashkeys[0])

    assert result == expected_results_dict[hashkeys[0]]


@pytest.mark.parametrize(
    "compress_mode",
    [CompressMode.YES, CompressMode.NO],
)
def test_packs_read_many(tmp_path, compress_mode):
    """Add 10'00 objects to the container in loose form, and benchmark write and read speed."""
    cnt = Container(tmp_path)
    cnt.init_container()
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]
    expected_results_dict = dict(zip(expected_hashkeys, data_content))

    hashkeys = cnt.add_objects_to_pack(data_content, compress=compress_mode)
    random.shuffle(hashkeys)

    results = cnt.get_objects_content(hashkeys)

    assert results == expected_results_dict


@pytest.mark.parametrize(
    "compress_mode",
    [CompressMode.YES, CompressMode.NO],
)
def test_packs_write_single(tmp_path, compress_mode):
    """Add 1 objects to the container in packed form, and benchmark write and read speed."""
    cnt = Container(tmp_path)
    cnt.init_container()
    num_files = 1
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    hashkeys = cnt.add_objects_to_pack(data_content, compress=compress_mode)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys


@pytest.mark.parametrize(
    "compress_mode",
    [CompressMode.YES, CompressMode.NO],
)
def test_packs_write_many(tmp_path, compress_mode):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    cnt = Container(tmp_path)
    cnt.init_container()
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    hashkeys = cnt.add_objects_to_pack(data_content, compress=compress_mode)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys


@pytest.mark.parametrize(
    "compress_mode,nrepeat",
    [
        # 5 KiB, 5 bytes
        (CompressMode.YES, 5 * 1024),
        (CompressMode.NO, 5 * 1024),
        (CompressMode.YES, 5),
        (CompressMode.NO, 5),
    ],
)
def test_packs_write_different_size(tmp_path, compress_mode, nrepeat):
    """Add 10 objects to the container in packed form, and benchmark write and read speed."""
    cnt = Container(tmp_path)
    cnt.init_container()
    num_files = 10
    data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    hashkeys = cnt.add_objects_to_pack(data_content, compress=compress_mode)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys
