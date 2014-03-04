try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

import cassandra
from cassandra.query import SimpleStatement, TraceUnavailable
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.policies import RoundRobinPolicy, ExponentialReconnectionPolicy, RetryPolicy, SimpleConvictionPolicy, HostDistance

from cassandra.cluster import Cluster, NoHostAvailable


class ClusterTests(unittest.TestCase):

    def test_basic(self):
        """
        Test basic connection and usage
        """

        cluster = Cluster()
        session = cluster.connect()
        result = session.execute(
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        self.assertEquals(None, result)

        result = session.execute(
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        self.assertEquals(None, result)

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        self.assertEquals(None, result)

        result = session.execute("SELECT * FROM clustertests.cf0")
        self.assertEquals([('a', 'b', 'c')], result)

        cluster.shutdown()

    def test_connect_on_keyspace(self):
        """
        Ensure clusters that connect on a keyspace, do
        """

        cluster = Cluster()
        session = cluster.connect()
        result = session.execute(
            """
            INSERT INTO test3rf.test (k, v) VALUES (8889, 8889)
            """)
        self.assertEquals(None, result)

        result = session.execute("SELECT * FROM test3rf.test")
        self.assertEquals([(8889, 8889)], result)

        # test_connect_on_keyspace
        session2 = cluster.connect('test3rf')
        result2 = session2.execute("SELECT * FROM test")
        self.assertEquals(result, result2)

    def test_set_keyspace_twice(self):
        cluster = Cluster()
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
            connection_class=AsyncoreConnection
        )

    def test_double_shutdown(self):
        """
        Ensure that a cluster can be shutdown twice, without error
        """

        cluster = Cluster()
        cluster.shutdown()

        try:
            cluster.shutdown()
            self.fail('A double cluster.shutdown() should throw an error.')
        except Exception as e:
            self.assertIn('The Cluster was already shutdown', str(e))

    def test_connect_to_already_shutdown_cluster(self):
        """
        Ensure you cannot connect to a cluster that's been shutdown
        """

        cluster = Cluster()
        cluster.shutdown()
        self.assertRaises(Exception, cluster.connect)

    def test_auth_provider_is_callable(self):
        """
        Ensure that auth_providers are always callable
        """

        self.assertRaises(ValueError, Cluster, auth_provider=1)

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

        cluster = Cluster(['127.1.2.9', '127.1.2.10'])
        self.assertRaises(NoHostAvailable, cluster.connect)

    def test_cluster_settings(self):
        """
        Test connection setting getters and setters
        """

        cluster = Cluster()

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

        cluster = Cluster()
        cluster.connect()
        self.assertNotIn("newkeyspace", cluster.metadata.keyspaces)

        other_cluster = Cluster()
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

        cluster = Cluster()
        session = cluster.connect()

        self.assertRaises(TypeError, session.execute, "SELECT * FROM system.local", trace=True)

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement, trace=True)
        self.assertEqual(query, statement.trace.parameters['query'])

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement)
        self.assertEqual(None, statement.trace)

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2, trace=True)
        future.result()
        self.assertEqual(query, future.get_query_trace().parameters['query'])

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2)
        future.result()
        self.assertEqual(None, future.get_query_trace())

    def test_trace_timeout(self):
        cluster = Cluster()
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

        cluster = Cluster()
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        future = session.execute_async(statement)

        self.assertIn(query, str(future))
        future.result()

        self.assertIn(query, str(future))
        self.assertIn('result', str(future))
