# Copyright 2015 DataStax, Inc.
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

import os
import warnings

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace_simple, CQLENG_ALLOW_SCHEMA_MANAGEMENT

from tests.integration import get_server_versions, use_single_node, PROTOCOL_VERSION


def setup_package():
    warnings.simplefilter('always')  # for testing warnings, make sure all are let through
    os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'

    use_single_node()

    keyspace = 'cqlengine_test'
    connection.setup(['localhost'],
                     protocol_version=PROTOCOL_VERSION,
                     default_keyspace=keyspace)

    create_keyspace_simple(keyspace, 1)


def is_prepend_reversed():
    # do we have https://issues.apache.org/jira/browse/CASSANDRA-8733 ?
    ver, _ = get_server_versions()
    return not (ver >= (2, 0, 13) or ver >= (2, 1, 3))
