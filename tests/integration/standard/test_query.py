try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from cassandra.query import (PreparedStatement, BoundStatement, ValueSequence,
                             SimpleStatement, BatchStatement, BatchType,
                             dict_factory)
from cassandra.cluster import Cluster
from cassandra.policies import HostDistance

from tests.integration import get_server_versions


class QueryTest(unittest.TestCase):

    def test_query(self):
        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        self.assertIsInstance(bound, BoundStatement)
        self.assertEqual(2, len(bound.values))
        session.execute(bound)
        self.assertEqual(bound.routing_key, '\x00\x00\x00\x01')

    def test_value_sequence(self):
        """
        Test the output of ValueSequences()
        """

        my_user_ids = ('alice', 'bob', 'charles')
        self.assertEqual(str(ValueSequence(my_user_ids)), "( 'alice' , 'bob' , 'charles' )")

    def test_trace_prints_okay(self):
        """
        Code coverage to ensure trace prints to string without error
        """

        cluster = Cluster()
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement, trace=True)

        # Ensure this does not throw an exception
        str(statement.trace)
        for event in statement.trace.events:
            str(event)

    def test_trace_ignores_row_factory(self):
        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = dict_factory

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement, trace=True)

        # Ensure this does not throw an exception
        str(statement.trace)
        for event in statement.trace.events:
            str(event)


class PreparedStatementTests(unittest.TestCase):

    def test_routing_key(self):
        """
        Simple code coverage to ensure routing_keys can be accessed
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        self.assertEqual(bound.routing_key, '\x00\x00\x00\x01')

    def test_empty_routing_key_indexes(self):
        """
        Ensure when routing_key_indexes are blank,
        the routing key should be None
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)
        prepared.routing_key_indexes = None

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        self.assertEqual(bound.routing_key, None)

    def test_predefined_routing_key(self):
        """
        Basic test that ensures _set_routing_key()
        overrides the current routing key
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        bound._set_routing_key('fake_key')
        self.assertEqual(bound.routing_key, 'fake_key')

    def test_multiple_routing_key_indexes(self):
        """
        Basic test that uses a fake routing_key_index
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)
        prepared.routing_key_indexes = {0: {0: 0}, 1: {1: 1}}

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, 2))
        self.assertEqual(bound.routing_key, '\x04\x00\x00\x00\x04\x00\x00\x00')

    def test_bound_keyspace(self):
        """
        Ensure that bound.keyspace works as expected
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, 2))
        self.assertEqual(bound.keyspace, 'test3rf')

        bound.prepared_statement.column_metadata = None
        self.assertEqual(bound.keyspace, None)


class PrintStatementTests(unittest.TestCase):
    """
    Test that shows the format used when printing Statements
    """

    def test_simple_statement(self):
        """
        Highlight the format of printing SimpleStatements
        """

        ss = SimpleStatement('SELECT * FROM test3rf.test')
        self.assertEqual(str(ss),
                         '<SimpleStatement query="SELECT * FROM test3rf.test", consistency=ONE>')

    def test_prepared_statement(self):
        """
        Highlight the difference between Prepared and Bound statements
        """

        cluster = Cluster()
        session = cluster.connect()

        prepared = session.prepare('INSERT INTO test3rf.test (k, v) VALUES (?, ?)')

        self.assertEqual(str(prepared),
                         '<PreparedStatement query="INSERT INTO test3rf.test (k, v) VALUES (?, ?)", consistency=ONE>')

        bound = prepared.bind((1, 2))
        self.assertEqual(str(bound),
                         '<BoundStatement query="INSERT INTO test3rf.test (k, v) VALUES (?, ?)", values=(1, 2), consistency=ONE>')


class BatchStatementTests(unittest.TestCase):

    def setUp(self):
        cass_version, _ = get_server_versions()
        if cass_version < (2, 0):
            raise unittest.SkipTest(
                "Cassandra 2.0+ is required for BATCH operations, currently testing against %r"
                % (cass_version,))

        self.cluster = Cluster(protocol_version=2)
        self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    def test_string_statements(self):
        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", (i, i))

        self.session.execute(batch)
        self.session.execute_async(batch).result()

    def test_simple_statements(self):
        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)"), (i, i))

        self.session.execute(batch)
        self.session.execute_async(batch).result()

    def test_prepared_statements(self):
        prepared = self.session.prepare("INSERT INTO test3rf.test (k, v) VALUES (?, ?)")

        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(prepared, (i, i))

        self.session.execute(batch)
        self.session.execute_async(batch).result()

    def test_bound_statements(self):
        prepared = self.session.prepare("INSERT INTO test3rf.test (k, v) VALUES (?, ?)")

        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(prepared.bind((i, i)))

        self.session.execute(batch)
        self.session.execute_async(batch).result()

    def test_no_parameters(self):
        batch = BatchStatement(BatchType.LOGGED)
        batch.add("INSERT INTO test3rf.test (k, v), VALUES (0, 0)")
        batch.add("INSERT INTO test3rf.test (k, v), VALUES (1, 1)", ())
        batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v), VALUES (2, 2)"))
        batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v), VALUES (3, 3)"), ())

        prepared = self.session.prepare("INSERT INTO test3rf.test (k, v) VALUES (4, 4)")
        batch.add(prepared)
        batch.add(prepared, ())
        batch.add(prepared.bind([]))
        batch.add(prepared.bind([]), ())

        self.assertRaises(ValueError, batch.add, prepared.bind([]), (1, 2, 3))
