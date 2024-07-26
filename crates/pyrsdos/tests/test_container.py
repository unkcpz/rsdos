from rsdos import Container

def test_initialisation(tmp_path):
    """Test that the initialisation function works as expected."""
    container = Container(tmp_path)
    assert not container.is_initialised

    container.init_container()
    assert container.is_initialised
