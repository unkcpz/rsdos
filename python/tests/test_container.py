from rsdos import Container
import tempfile
import os


def test_initialisation(tmp_path):
    """Test that the initialisation function works as expected."""
    container = Container(tmp_path)
    assert not container.is_initialised

    container.init_container()
    assert container.is_initialised


def test_add_loose_from_stream(rs_container):
    """Test adding an object from a stream (from an open file, for instance)."""
    # Write 1_000_000 bytes, which larger than a chunk
    content = b"0123456789" * 1000_000

    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_handle:
        temp_handle.write(content)

    with open(temp_handle.name, "rb") as read_handle:
        hashkey = rs_container.add_streamed_object(read_handle)

    read_content = rs_container.get_object_content(hashkey)

    assert read_content == content

    os.remove(temp_handle.name)
