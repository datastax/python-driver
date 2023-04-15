import pytest

from tests.integration.cqlengine import setup_package, teardown_package

@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown_packages():
    setup_package()
    yield
    teardown_package()
