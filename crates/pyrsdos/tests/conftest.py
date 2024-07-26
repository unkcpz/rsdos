import pytest
from rsdos import Container

@pytest.fixture(scope="function")
def container(tmp_path):
    cnt = Container(tmp_path)
    cnt.init_container()
    yield cnt
