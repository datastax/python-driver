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

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace

from tests.integration import use_single_node, PROTOCOL_VERSION


def setup_package():
    use_single_node()

    keyspace = 'cqlengine_test'
    connection.setup(['localhost'],
                      protocol_version=PROTOCOL_VERSION,
                      default_keyspace=keyspace)

    create_keyspace(keyspace, replication_factor=1, strategy_class="SimpleStrategy")
