import pytest

from tests.integration import teardown_package as parent_teardown_package
from tests.integration.cqlengine import setup_package, teardown_package


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown_packages():
    setup_package()
    yield
    teardown_package()
    parent_teardown_package()