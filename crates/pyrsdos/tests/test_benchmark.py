import pytest
import hashlib

@pytest.mark.benchmark(group="read")
def test_loose_read_single(benchmark, container):
    """Add 1 objects to the container in loose form, and benchmark write and read speed."""
    content = str(5).encode("ascii")
    hashkey = container.add_object(content)

    # Note that here however the OS will be using the disk caches
    result = benchmark(container.get_object_content, hashkey)

    assert result == content

@pytest.mark.benchmark(group="read")
def test_loose_read(benchmark, container):
    """Add 1'000 objects to the container in loose form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(container.add_object(content))
    expected_results = dict(zip(hashkeys, data_content))

    # Note that here however the OS will be using the disk caches
    results = benchmark(container.get_objects_content, hashkeys)

    assert results == expected_results

@pytest.mark.benchmark(group="write", min_rounds=3)
def test_loose_write(container, benchmark):
    """Add 1'000 objects to the container in packed form, and benchmark write and read speed."""
    num_files = 1000
    data_content = [str(i).encode("ascii") for i in range(num_files)]
    expected_hashkeys = [
        hashlib.sha256(content).hexdigest() for content in data_content
    ]

    def write_loose(container, contents):
        retval = []
        for content in contents:
            retval.append(container.add_object(content))
        return retval

    hashkeys = benchmark(write_loose, container, data_content)

    assert len(hashkeys) == len(data_content)
    assert expected_hashkeys == hashkeys

@pytest.mark.benchmark(group="write", min_rounds=3)
def test_loose_write_single(container, benchmark):
    """Add 1 objects to the container in packed form, and benchmark write and read speed."""
    content = str('test').encode("ascii")
    expected_hashkey = hashlib.sha256(content).hexdigest()

    def write_loose(container, content):
        return container.add_object(content)

    hashkey = benchmark(write_loose, container, content)

    assert hashkey == expected_hashkey
