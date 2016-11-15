# Copyright 2013-2016 DataStax, Inc.
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


class ConnectionTest(unittest.TestCase):

    def test_set_session_without_existing_connection(self):
        """
        Users can't set the default session without having a default connection set.
        """
        dummy_session = object()

        expected_msg_excerpt = 'no default connection'
        with self.assertRaisesRegexp(connection.CQLEngineException, expected_msg_excerpt):
            connection.set_session(dummy_session)
