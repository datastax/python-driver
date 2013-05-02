import unittest

from cassandra.cluster import Cluster

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
        self.assertEquals([{'a': 'a', 'b': 'b', 'c': 'c'}], result)

        cluster.shutdown()

    def test_submit_schema_refresh(self):
        cluster = Cluster()
        cluster.connect()

        other_cluster = Cluster()
        session = other_cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE newkeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        self.assertNotIn("newkeyspace", cluster.metadata.keyspaces)

        future = cluster.submit_schema_refresh()
        future.result()

        self.assertIn("newkeyspace", cluster.metadata.keyspaces)
