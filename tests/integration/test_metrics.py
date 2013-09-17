import unittest
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel, WriteTimeout, Unavailable, ReadTimeout

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.decoder import QueryMessage
from tests.integration import get_node, get_cluster


class MetricsTests(unittest.TestCase):

    def test_connection_error(self):
        """
        Trigger and ensure connection_errors are counted
        """

        # DISCUSS: Doesn't trigger code coverage on cassandra.metrics.on_connection_error(). Find new example.
        cluster = Cluster(metrics_enabled=True)
        session = cluster.connect()

        # Test write
        session.execute("USE test3rf")

        # Force kill cluster
        get_cluster().stop(wait=True, gently=False)
        try:
            self.assertRaises(NoHostAvailable, session.execute, "USE test3rf")
        finally:
            get_cluster().start(wait_for_binary_proto=True)

    def test_write_timeout(self):
        """
        Trigger and ensure write_timeouts are counted
        Write a key, value pair. Force kill a node without waiting for the cluster to register the death.
        Attempt a write at cl.ALL and receive a WriteTimeout.
        """

        cluster = Cluster(metrics_enabled=True)
        session = cluster.connect()

        # Test write
        session.execute("INSERT INTO test3rf.test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT v FROM test3rf.test WHERE k=%(k)s", consistency_level=ConsistencyLevel.ALL)
        results = session.execute(query, {'k': 1})
        self.assertEqual(1, results[0].v)

        # Force kill ccm node
        get_node(1).stop(wait=False, gently=False)

        try:
            # Test write
            query = SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
            self.assertRaises(WriteTimeout, session.execute, query)
            self.assertEqual(1, cluster.metrics.stats.write_timeouts)

        finally:
            get_node(1).start(wait_other_notice=True, wait_for_binary_proto=True)

    def test_read_timeout(self):
        """
        Trigger and ensure read_timeouts are counted
        Write a key, value pair. Force kill a node without waiting for the cluster to register the death.
        Attempt a read at cl.ALL and receive a ReadTimeout.
        """

        cluster = Cluster(metrics_enabled=True)
        session = cluster.connect()

        # Test write
        session.execute("INSERT INTO test3rf.test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT v FROM test3rf.test WHERE k=%(k)s", consistency_level=ConsistencyLevel.ALL)
        results = session.execute(query, {'k': 1})
        self.assertEqual(1, results[0].v)

        # Force kill ccm node
        get_node(1).stop(wait=False, gently=False)

        try:
            # Test read
            query = SimpleStatement("SELECT v FROM test3rf.test WHERE k=%(k)s", consistency_level=ConsistencyLevel.ALL)
            self.assertRaises(ReadTimeout, session.execute, query, {'k': 1})
            self.assertEqual(1, cluster.metrics.stats.read_timeouts)

        finally:
            get_node(1).start(wait_other_notice=True, wait_for_binary_proto=True)

    def test_unavailable(self):
        """
        Trigger and ensure unavailables are counted
        Write a key, value pair. Kill a node while waiting for the cluster to register the death.
        Attempt an insert/read at cl.ALL and receive a Unavailable Exception.
        """

        cluster = Cluster(metrics_enabled=True)
        session = cluster.connect()

        # Test write
        session.execute("INSERT INTO test3rf.test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT v FROM test3rf.test WHERE k=%(k)s", consistency_level=ConsistencyLevel.ALL)
        results = session.execute(query, {'k': 1})
        self.assertEqual(1, results[0].v)

        # Force kill ccm node
        get_node(1).stop(wait=True, gently=True)

        try:
            # Test write
            query = SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
            self.assertRaises(Unavailable, session.execute, query)
            self.assertEqual(1, cluster.metrics.stats.unavailables)

            # Test write
            query = SimpleStatement("SELECT v FROM test3rf.test WHERE k=%(k)s", consistency_level=ConsistencyLevel.ALL)
            self.assertRaises(Unavailable, session.execute, query, {'k': 1})
            self.assertEqual(2, cluster.metrics.stats.unavailables)
        finally:
            get_node(1).start(wait_other_notice=True, wait_for_binary_proto=True)

    def test_other_error(self):
        # TODO: Bootstrapping or Overloaded cases
        pass


    def test_ignore(self):
        # TODO: Look for ways to generate ignores
        pass


    def test_retry(self):
        # TODO: Look for ways to generate retries
        pass
