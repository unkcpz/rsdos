import pytest
import string
import random
from rsdos import Container as RsContainer


@pytest.fixture(scope="function")
def rs_container(tmp_path):
    cnt = RsContainer(tmp_path)
    cnt.init_container()
    yield cnt


@pytest.fixture(scope="function")
def gen_n_bytes():
    def _get_n_bytes(n: int):
        ascii_chars = string.ascii_letters + string.digits + string.punctuation
        return "".join(random.choices(ascii_chars, k=n))

    return _get_n_bytes
