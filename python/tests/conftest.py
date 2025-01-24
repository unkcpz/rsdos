import pytest
from rsdos import Container as RsContainer

@pytest.fixture(scope="function")
def rs_container(tmp_path):
    cnt = RsContainer(tmp_path)
    cnt.init_container()
    yield cnt

