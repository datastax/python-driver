import unittest
from mock import Mock

from cassandra.cluster import Cluster, NoHostAvailable

class ClusterTests(unittest.TestCase):

    def test_basic(self):
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

    def test_connect_to_bad_hosts(self):
        cluster = Cluster(['127.1.2.9', '127.1.2.10'])
        self.assertRaises(NoHostAvailable, cluster.connect)

    def test_init_kwargs(self):
        def foo(*args, **kwargs):
            return Mock()

        for kw in ('auth_provider', 'load_balancing_policy_factory',
                   'retry_policy_factory', 'conviction_policy_factory'):
            kwargs = {kw: 123}
            self.assertRaises(ValueError, Cluster, **kwargs)

            kwargs = {kw: foo}
            c = Cluster(**kwargs)
            self.assertEquals(getattr(c, kw), foo)

        for kw in ('contact_points', 'port', 'compression', 'metrics_enabled',
                   'reconnection_policy', 'sockopts', 'max_schema_agreement_wait'):
            kwargs = {kw: (1, 2, 3)}
            c = Cluster(**kwargs)
            self.assertEquals(getattr(c, kw), (1, 2, 3))

    def test_submit_schema_refresh(self):
        cluster = Cluster()
        cluster.connect()

        other_cluster = Cluster()
        session = other_cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE newkeyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        self.assertNotIn("newkeyspace", cluster.metadata.keyspaces)

        future = cluster.submit_schema_refresh()
        future.result()

        self.assertIn("newkeyspace", cluster.metadata.keyspaces)

    def test_on_down_and_up(self):
        cluster = Cluster()
        session = cluster.connect()
        host = cluster.metadata.all_hosts()[0]
        host.monitor.signal_connection_failure(None)
        cluster.on_down(host)
        self.assertNotEqual(None, cluster.control_connection._reconnection_handler)
        self.assertNotIn(host, session._pools)
        host_reconnector = host._reconnection_handler
        self.assertNotEqual(None, host_reconnector)

        host.monitor.is_up = True

        cluster.on_up(host)

        self.assertEqual(None, host._reconnection_handler)
        self.assertTrue(host_reconnector._cancelled)
        self.assertIn(host, session._pools)
