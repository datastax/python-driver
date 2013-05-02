import unittest

from cassandra.cluster import Cluster

class ClusterTests(unittest.TestCase):

    def testBasic(self):
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
