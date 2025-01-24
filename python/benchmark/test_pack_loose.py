import pytest
from disk_objectstore import Container as PyContainer
from pathlib import Path
import shutil
from rsdos import CompressMode, Container as RsContainer

def reset_packs(folder_path: Path):
    # Remove the folder and its contents
    if folder_path.exists():
        shutil.rmtree(folder_path)
    
    # Recreate the empty folder
    Path.mkdir(folder_path)

@pytest.mark.parametrize(
    "compress_mode,nrepeat", 
    [
        # 5 MiB, 5 KiB, 5 bytes
        (CompressMode.YES, 5 * 1024 * 1024),
        (CompressMode.NO, 5 * 1024 * 1024),
        (CompressMode.YES, 5 * 1024), 
        (CompressMode.NO, 5 * 1024),
        (CompressMode.YES, 5), 
        (CompressMode.NO, 5),
    ])
@pytest.mark.benchmark(group="pack_10")
def test_pack_loose_10_py(benchmark, tmp_path, compress_mode, nrepeat):
    """Add 10 objects to the container in loose form, and benchmark pack_all_loose speed."""
    with PyContainer(tmp_path) as cnt:
        cnt.init_container(pack_size_target = 4 * 1024 * 1024 * 1024, compression_algorithm="zlib+1")

        num_files = 10
        data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
        hashkeys = []
        for content in data_content:
            hashkeys.append(cnt.add_object(content))

        def pack_all_loose():
            cnt.pack_all_loose(compress_mode)
            reset_packs(cnt.get_folder() / "packs")

            # delete all packs.idx-*
            for pack_file in cnt.get_folder().glob("packs.idx*"):
                pack_file.unlink()

            # clean up the db session for next run to avoid reuse
            cnt._get_session(create=True)
            cnt.close()

        benchmark(pack_all_loose)


@pytest.mark.parametrize(
    "compress_mode,nrepeat", 
    [
        # 5 MiB, 5 KiB, 5 bytes
        (CompressMode.YES, 5 * 1024 * 1024),
        (CompressMode.NO, 5 * 1024 * 1024),
        (CompressMode.YES, 5 * 1024), 
        (CompressMode.NO, 5 * 1024),
        (CompressMode.YES, 5), 
        (CompressMode.NO, 5),
    ])
@pytest.mark.benchmark(group="pack_10")
def test_pack_loose_10_rs(benchmark, tmp_path, compress_mode, nrepeat):
    """Add 10 objects to the container in loose form, and benchmark pack_all_loose speed."""
    cnt = RsContainer(tmp_path)
    cnt.init_container(pack_size_target = 4 * 1024 * 1024 * 1024, compression_algorithm="zlib+1")

    num_files = 10
    data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkey = cnt.add_object(content)
        hashkeys.append(hashkey)

    def pack_all_loose():
        cnt.pack_all_loose(compress_mode)
        reset_packs(cnt.get_folder() / "packs")

        # delete all packs.idx-*
        for pack_file in cnt.get_folder().glob("packs.idx*"):
            pack_file.unlink()

        # clean up the db session for next run to avoid reuse
        cnt._init_db()

    benchmark(pack_all_loose)

@pytest.mark.skip(reason="legacy dos can not support such large amount of open file handlers")
def test_pack_loose_too_many_open_files_py_xfail(benchmark, tmp_path):
    """xfail test to demostrat that legacy dos didn't gracefully drop the file handlers.

    To make it work, the container mush used in the context manage so that every iter of test will close file handlers.
    `with PyContainer(tmp_path) as cnt:`
    """
    cnt = PyContainer(tmp_path)
    cnt.init_container(pack_size_target = 4 * 1024 * 1024, compression_algorithm="zlib+1")

    compress_mode = CompressMode.NO

    num_files = 1000
    nrepeat = 64
    data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkey = cnt.add_object(content)
        hashkeys.append(hashkey)

    def pack_all_loose():
        cnt.pack_all_loose(compress_mode)
        reset_packs(cnt.get_folder() / "packs")

        # delete all packs.idx-*
        for pack_file in cnt.get_folder().glob("packs.idx*"):
            pack_file.unlink()

        cnt._get_session(create=True)


    # Note that here however the OS will be using the disk caches
    benchmark(pack_all_loose)

def test_pack_loose_too_many_open_files_py(benchmark, tmp_path):
    """previous xfail test to demostrat that legacy dos didn't gracefully drop the file handlers.
    """
    with PyContainer(tmp_path) as cnt:
        cnt.init_container(pack_size_target = 4 * 1024 * 1024, compression_algorithm="zlib+1")

        compress_mode = CompressMode.NO

        num_files = 1000
        nrepeat = 5 * 1024 # 5 KiB
        data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
        hashkeys = []
        for content in data_content:
            hashkey = cnt.add_object(content)
            hashkeys.append(hashkey)

        def pack_all_loose():
            cnt.pack_all_loose(compress_mode)
            reset_packs(cnt.get_folder() / "packs")

            # delete all packs.idx-*
            for pack_file in cnt.get_folder().glob("packs.idx*"):
                pack_file.unlink()

            cnt._get_session(create=True)


        # Note that here however the OS will be using the disk caches
        benchmark(pack_all_loose)

def test_pack_loose_too_many_open_files_rs(benchmark, tmp_path):
    """This will work in rsdos, since:

    1. the file handler is droped after io and 
    2. pack file only open once for a sequential write.
    """
    cnt = RsContainer(tmp_path)
    cnt.init_container(pack_size_target = 4 * 1024 * 1024, compression_algorithm="zlib+1")

    compress_mode = CompressMode.NO

    num_files = 1000
    nrepeat = 5 * 1024 # 5 KiB
    data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(cnt.add_object(content))

    def pack_all_loose():
        cnt.pack_all_loose(compress_mode)
        reset_packs(cnt.get_folder() / "packs")

        # delete all packs.idx-*
        for pack_file in cnt.get_folder().glob("packs.idx*"):
            pack_file.unlink()

        cnt._init_db()

    # Note that here however the OS will be using the disk caches
    benchmark(pack_all_loose)

@pytest.mark.parametrize(
    "compress_mode,nrepeat,pack_size", 
    [
        (CompressMode.NO, 64, 1024), 
        (CompressMode.NO, 64, 4 * 1024 * 1024),
    ])
@pytest.mark.benchmark(group="pack_cross_packed_files")
def test_pack_loose_over_many_packs_py(benchmark, tmp_path, compress_mode, nrepeat, pack_size):
    """The test to find out the delay for creating more packed files, 
    The first case will create many packed files, the second one has large target size that will have id = 0 pack open and written
    """
    with PyContainer(tmp_path) as cnt:
        cnt.init_container(pack_size_target = pack_size, compression_algorithm="zlib+1")

        num_files = 200
        data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
        hashkeys = []
        for content in data_content:
            hashkeys.append(cnt.add_object(content))

        def pack_all_loose():
            cnt.pack_all_loose(compress_mode)
            reset_packs(cnt.get_folder() / "packs")

            # delete all packs.idx-*
            for pack_file in cnt.get_folder().glob("packs.idx*"):
                pack_file.unlink()

            cnt._get_session(create=True)
            # the session is open in mem need to clean up for next run

        # Note that here however the OS will be using the disk caches
        benchmark(pack_all_loose)

@pytest.mark.parametrize(
    "compress_mode,nrepeat,pack_size", 
    [
        (CompressMode.NO, 64, 1024), 
        (CompressMode.NO, 64, 4 * 1024 * 1024),
    ])
@pytest.mark.benchmark(group="pack_cross_packed_files")
def test_pack_loose_over_many_packs_rs(benchmark, tmp_path, compress_mode, nrepeat, pack_size):
    """The test to find out the delay for creating more packed files, 
    The first case will create many packed files, the second one has large target size that will have id = 0 pack open and written
    """
    cnt = RsContainer(tmp_path)
    cnt.init_container(pack_size_target = pack_size, compression_algorithm="zlib+1")

    num_files = 200
    data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(cnt.add_object(content))

    def pack_all_loose():
        cnt.pack_all_loose(compress_mode)
        reset_packs(cnt.get_folder() / "packs")

        # delete all packs.idx-*
        for pack_file in cnt.get_folder().glob("packs.idx*"):
            pack_file.unlink()

        cnt._init_db()

    # Note that here however the OS will be using the disk caches
    benchmark(pack_all_loose)
