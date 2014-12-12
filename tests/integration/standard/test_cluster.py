# Copyright 2013-2014 DataStax, Inc.
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

from tests.integration import use_singledc, PROTOCOL_VERSION

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import cassandra
from cassandra.query import SimpleStatement, TraceUnavailable
from cassandra.policies import RoundRobinPolicy, ExponentialReconnectionPolicy, RetryPolicy, SimpleConvictionPolicy, HostDistance

from cassandra.cluster import Cluster, NoHostAvailable


def setup_module():
    use_singledc()


class ClusterTests(unittest.TestCase):

    def test_basic(self):
        """
        Test basic connection and usage
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        result = session.execute(
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        self.assertEqual(None, result)

        result = session.execute(
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        self.assertEqual(None, result)

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        self.assertEqual(None, result)

        result = session.execute("SELECT * FROM clustertests.cf0")
        self.assertEqual([('a', 'b', 'c')], result)

        cluster.shutdown()

    def test_connect_on_keyspace(self):
        """
        Ensure clusters that connect on a keyspace, do
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        result = session.execute(
            """
            INSERT INTO test3rf.test (k, v) VALUES (8889, 8889)
            """)
        self.assertEqual(None, result)

        result = session.execute("SELECT * FROM test3rf.test")
        self.assertEqual([(8889, 8889)], result)

        # test_connect_on_keyspace
        session2 = cluster.connect('test3rf')
        result2 = session2.execute("SELECT * FROM test")
        self.assertEqual(result, result2)

    def test_set_keyspace_twice(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        session.execute("USE system")
        session.execute("USE system")

    def test_default_connections(self):
        """
        Ensure errors are not thrown when using non-default policies
        """

        Cluster(
            load_balancing_policy=RoundRobinPolicy(),
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 600.0),
            default_retry_policy=RetryPolicy(),
            conviction_policy_factory=SimpleConvictionPolicy,
            protocol_version=PROTOCOL_VERSION
        )

    def test_connect_to_already_shutdown_cluster(self):
        """
        Ensure you cannot connect to a cluster that's been shutdown
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.shutdown()
        self.assertRaises(Exception, cluster.connect)

    def test_auth_provider_is_callable(self):
        """
        Ensure that auth_providers are always callable
        """
        self.assertRaises(TypeError, Cluster, auth_provider=1, protocol_version=1)
        c = Cluster(protocol_version=1)
        self.assertRaises(TypeError, setattr, c, 'auth_provider', 1)

    def test_v2_auth_provider(self):
        """
        Check for v2 auth_provider compliance
        """
        bad_auth_provider = lambda x: {'username': 'foo', 'password': 'bar'}
        self.assertRaises(TypeError, Cluster, auth_provider=bad_auth_provider, protocol_version=2)
        c = Cluster(protocol_version=2)
        self.assertRaises(TypeError, setattr, c, 'auth_provider', bad_auth_provider)

    def test_conviction_policy_factory_is_callable(self):
        """
        Ensure that conviction_policy_factory are always callable
        """

        self.assertRaises(ValueError, Cluster, conviction_policy_factory=1)

    def test_connect_to_bad_hosts(self):
        """
        Ensure that a NoHostAvailable Exception is thrown
        when a cluster cannot connect to given hosts
        """

        cluster = Cluster(['127.1.2.9', '127.1.2.10'],
                          protocol_version=PROTOCOL_VERSION)
        self.assertRaises(NoHostAvailable, cluster.connect)

    def test_cluster_settings(self):
        """
        Test connection setting getters and setters
        """
        if PROTOCOL_VERSION >= 3:
            raise unittest.SkipTest("min/max requests and core/max conns aren't used with v3 protocol")

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)

        min_requests_per_connection = cluster.get_min_requests_per_connection(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MIN_REQUESTS, min_requests_per_connection)
        cluster.set_min_requests_per_connection(HostDistance.LOCAL, min_requests_per_connection + 1)
        self.assertEqual(cluster.get_min_requests_per_connection(HostDistance.LOCAL), min_requests_per_connection + 1)

        max_requests_per_connection = cluster.get_max_requests_per_connection(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MAX_REQUESTS, max_requests_per_connection)
        cluster.set_max_requests_per_connection(HostDistance.LOCAL, max_requests_per_connection + 1)
        self.assertEqual(cluster.get_max_requests_per_connection(HostDistance.LOCAL), max_requests_per_connection + 1)

        core_connections_per_host = cluster.get_core_connections_per_host(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST, core_connections_per_host)
        cluster.set_core_connections_per_host(HostDistance.LOCAL, core_connections_per_host + 1)
        self.assertEqual(cluster.get_core_connections_per_host(HostDistance.LOCAL), core_connections_per_host + 1)

        max_connections_per_host = cluster.get_max_connections_per_host(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST, max_connections_per_host)
        cluster.set_max_connections_per_host(HostDistance.LOCAL, max_connections_per_host + 1)
        self.assertEqual(cluster.get_max_connections_per_host(HostDistance.LOCAL), max_connections_per_host + 1)

    def test_submit_schema_refresh(self):
        """
        Ensure new new schema is refreshed after submit_schema_refresh()
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.connect()
        self.assertNotIn("newkeyspace", cluster.metadata.keyspaces)

        other_cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = other_cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE newkeyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        future = cluster.submit_schema_refresh()
        future.result()

        self.assertIn("newkeyspace", cluster.metadata.keyspaces)

    def test_trace(self):
        """
        Ensure trace can be requested for async and non-async queries
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        self.assertRaises(TypeError, session.execute, "SELECT * FROM system.local", trace=True)

        def check_trace(trace):
            self.assertIsNot(None, trace.request_type)
            self.assertIsNot(None, trace.duration)
            self.assertIsNot(None, trace.started_at)
            self.assertIsNot(None, trace.coordinator)
            self.assertIsNot(None, trace.events)

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement, trace=True)
        check_trace(statement.trace)

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement)
        self.assertEqual(None, statement.trace)

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2, trace=True)
        future.result()
        check_trace(future.get_query_trace())

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2)
        future.result()
        self.assertEqual(None, future.get_query_trace())

        prepared = session.prepare("SELECT * FROM system.local")
        future = session.execute_async(prepared, parameters=(), trace=True)
        future.result()
        check_trace(future.get_query_trace())

    def test_trace_timeout(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        future = session.execute_async(statement, trace=True)
        future.result()
        self.assertRaises(TraceUnavailable, future.get_query_trace, -1.0)

    def test_string_coverage(self):
        """
        Ensure str(future) returns without error
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        future = session.execute_async(statement)

        self.assertIn(query, str(future))
        future.result()

        self.assertIn(query, str(future))
        self.assertIn('result', str(future))
