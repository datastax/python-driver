try:
    import unittest2 as unittest
except ImportError:
    import unittest
from cassandra import InvalidRequest

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement

class PreparedStatementTests(unittest.TestCase):

    def test_basic(self):
        """
        Test basic PreparedStatement usage
        """

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

    def test_missing_primary_key(self):
        """
        Ensure an InvalidRequest is thrown
        when prepared statements are missing the primary key
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (v) VALUES  (?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1,))
        self.assertRaises(InvalidRequest, session.execute, bound)

    def test_too_many_bind_values(self):
        """
        Ensure a ValueError is thrown when attempting to bind too many variables
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (v) VALUES  (?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        self.assertRaises(ValueError, prepared.bind, (1,2))

    def test_none_values(self):
        """
        Ensure binding None is handled correctly
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        session.execute(bound)

        prepared = session.prepare(
           """
           SELECT * FROM test3rf.test WHERE k=?
           """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind((1,))
        results = session.execute(bound)
        self.assertEquals(results[0].v, None)

    def test_async_binding(self):
        """
        Ensure None binding over async queries
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        future = session.execute_async(prepared, (873, None))
        future.result()

        prepared = session.prepare(
           """
           SELECT * FROM test3rf.test WHERE k=?
           """)
        self.assertIsInstance(prepared, PreparedStatement)

        future = session.execute_async(prepared, (873,))
        results = future.result()
        self.assertEquals(results[0].v, None)
