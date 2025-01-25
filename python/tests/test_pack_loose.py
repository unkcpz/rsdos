import pytest
from rsdos import Container, CompressMode


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
def test_pack_loose_10(tmp_path, compress_mode, nrepeat):
    """Add 10 objects to the container in loose form, and benchmark pack_all_loose speed."""
    cnt = Container(tmp_path)
    cnt.init_container(
        pack_size_target=4 * 1024 * 1024 * 1024, compression_algorithm="zlib+1"
    )

    num_files = 10
    data_content = [("8bytes0" * nrepeat).encode("ascii") for _ in range(num_files)]
    hashkeys = []
    for content in data_content:
        hashkey = cnt.add_object(content)
        hashkeys.append(hashkey)

    cnt.pack_all_loose(compress_mode)

    got = cnt.get_object_content(hashkeys[0])

    assert got == data_content[0]


@pytest.mark.parametrize(
    # 400 * 8 bytes = 3200 bytes
    "compress_mode,nrepeat,pack_size",
    [
        (CompressMode.NO, 400, "s"),
        (CompressMode.NO, 400, "l"),
    ],
)
def test_pack_loose_over_many_packs(
    tmp_path, compress_mode, nrepeat, pack_size, gen_n_bytes
):
    """The test to find out the delay for creating more packed files,
    The first case will create many packed files, the second one has large target size that will have id = 0 pack open and written
    """
    if pack_size == "s":
        pack_size_ = 1024
    elif pack_size == "l":
        pack_size_ = 1024 * 1024

    cnt = Container(tmp_path)
    cnt.init_container(pack_size_target=pack_size_, compression_algorithm="zlib+1")

    num_files = 200
    data_content = [
        (gen_n_bytes(8) * nrepeat).encode("ascii") for _ in range(num_files)
    ]
    hashkeys = []
    for content in data_content:
        hashkeys.append(cnt.add_object(content))

    cnt.pack_all_loose(compress_mode)

    got = cnt.get_object_content(hashkeys[0])
    assert got == data_content[0]

    got = cnt.get_object_content(hashkeys[-1])
    assert got == data_content[-1]

    if pack_size == "s":
        assert cnt.count_pack_file() > 1
