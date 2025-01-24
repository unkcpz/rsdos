import pytest
from rsdos import Container as RsContainer
from disk_objectstore import Container as PyContainer

@pytest.fixture(scope="function")
def rs_container(tmp_path):
    cnt = RsContainer(tmp_path)
    cnt.init_container()
    yield cnt

@pytest.fixture(scope="function")
def py_container(tmp_path):
    with PyContainer(tmp_path) as cnt:
        cnt.init_container()
        yield cnt
