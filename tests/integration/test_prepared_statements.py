try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement

class PreparedStatementTests(unittest.TestCase):

    def test_basic(self):
        cluster = Cluster()
        session = cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE preparedtests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        session.set_keyspace("preparedtests")
        session.execute(
            """
            CREATE TABLE cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)

        prepared = session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind(('a', 'b', 'c'))

        session.execute(bound)

        prepared = session.prepare(
           """
           SELECT * FROM cf0 WHERE a=?
           """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind(('a'))
        results = session.execute(bound)
        self.assertEquals(results, [('a', 'b', 'c')])
