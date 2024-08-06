import pytest
from disk_objectstore import Container as PyContainer
from pathlib import Path
import shutil
from rsdos import Container as RsContainer

def reset_packs(folder_path: Path):
    # Remove the folder and its contents
    if folder_path.exists():
        shutil.rmtree(folder_path)
    
    # Recreate the empty folder
    Path.mkdir(folder_path)

@pytest.mark.benchmark(group="pack_10000")
def test_pack_loose_py(benchmark, tmp_path):
    """Add 10'000 objects to the container in loose form, and benchmark pack_all_loose speed."""
    cnt = PyContainer(tmp_path)
    cnt.init_container(pack_size_target = 4 * 1024 * 1024)

    num_files = 10000
    data_content = [f"test {i}".encode("ascii") for i in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(cnt.add_object(content))

    def pack_all_loose():
        cnt.pack_all_loose()
        reset_packs(cnt.get_folder() / "packs")

        # delete all packs.idx-*
        for pack_file in cnt.get_folder().glob("packs.idx*"):
            pack_file.unlink()

        cnt._get_session(create=True)
        # the session is open in mem need to clean up for next run
        cnt.close()


    # Note that here however the OS will be using the disk caches
    benchmark(pack_all_loose)


@pytest.mark.benchmark(group="pack_10000")
def test_pack_loose_rs(benchmark, tmp_path):
    """Add 10'000 objects to the container in loose form, and benchmark pack_all_loose speed."""
    cnt = RsContainer(tmp_path)
    cnt.init_container(pack_size_target = 4 * 1024 * 1024)

    num_files = 10000
    data_content = [f"test {i}".encode("ascii") for i in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkeys.append(cnt.add_object(content))

    def pack_all_loose():
        cnt.pack_all_loose()
        reset_packs(cnt.get_folder() / "packs")

        # delete all packs.idx-*
        for pack_file in cnt.get_folder().glob("packs.idx*"):
            pack_file.unlink()

        cnt._init_db()

    # Note that here however the OS will be using the disk caches
    benchmark(pack_all_loose)


