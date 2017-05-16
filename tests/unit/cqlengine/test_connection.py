# Copyright 2013-2017 DataStax, Inc.
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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.cqlengine import connection
from cassandra.query import dict_factory

from mock import Mock


class ConnectionTest(unittest.TestCase):

    no_registered_connection_msg = "doesn't exist in the registry"

    def setUp(self):
        super(ConnectionTest, self).setUp()
        self.assertFalse(
            connection._connections,
            'Test precondition not met: connections are registered: {cs}'.format(cs=connection._connections)
        )

    def test_set_session_without_existing_connection(self):
        """
        Users can set the default session without having a default connection set.
        """
        mock_session = Mock(
            row_factory=dict_factory,
            encoder=Mock(mapping={})
        )
        connection.set_session(mock_session)

    def test_get_session_fails_without_existing_connection(self):
        """
        Users can't get the default session without having a default connection set.
        """
        with self.assertRaisesRegexp(connection.CQLEngineException, self.no_registered_connection_msg):
            connection.get_session(connection=None)

    def test_get_cluster_fails_without_existing_connection(self):
        """
        Users can't get the default cluster without having a default connection set.
        """
        with self.assertRaisesRegexp(connection.CQLEngineException, self.no_registered_connection_msg):
            connection.get_cluster(connection=None)
