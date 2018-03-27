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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


from cassandra.cluster import Cluster
from tests import notwindows, is_windows
from tests.integration import use_cluster, CLUSTER_NAME, PROTOCOL_VERSION, execute_until_pass, \
    execute_with_long_wait_retry


def setup_module():
    if is_windows():
        return
    use_cluster(CLUSTER_NAME, [3], dse_cluster=True, dse_options={})


@notwindows
class DseCCMClusterTest(unittest.TestCase):
    """
    This class can be executed setting the DSE_VERSION variable, for example:
    DSE_VERSION=5.1.4 python2.7 -m nose tests/integration/standard/test_dse.py
    If CASSANDRA_VERSION is set instead, it will be converted to the corresponding DSE_VERSION
    """

    def test_basic(self):
        """
        Test basic connection and usage
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        result = execute_until_pass(session,
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        self.assertFalse(result)

        result = execute_with_long_wait_retry(session,
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        self.assertFalse(result)

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        self.assertFalse(result)

        result = session.execute("SELECT * FROM clustertests.cf0")
        self.assertEqual([('a', 'b', 'c')], result)

        execute_with_long_wait_retry(session, "DROP KEYSPACE clustertests")

        cluster.shutdown()
