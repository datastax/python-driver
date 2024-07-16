# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings
import os

import pytest

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace_simple, drop_keyspace, CQLENG_ALLOW_SCHEMA_MANAGEMENT
from tests.integration import use_single_node, teardown_package as parent_teardown_package

from . import setup_connection, DEFAULT_KEYSPACE


@pytest.fixture(scope='package', autouse=True)
def cqlengine_fixture():
    warnings.simplefilter('always')  # for testing warnings, make sure all are let through
    os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'

    use_single_node()

    setup_connection(DEFAULT_KEYSPACE)
    create_keyspace_simple(DEFAULT_KEYSPACE, 1)

    yield

    drop_keyspace(DEFAULT_KEYSPACE)
    connection.unregister_connection("default")


@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown_packages():
    yield
    parent_teardown_package()
