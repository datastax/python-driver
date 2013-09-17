import unittest
from cassandra.query import PreparedStatement, BoundStatement, ValueSequence, SimpleStatement
from cassandra.cluster import Cluster


class QueryTest(unittest.TestCase):
    # TODO: Cover routing keys
    # def test_query(self):
    #     cluster = Cluster()
    #     session = cluster.connect()
    #
    #     prepared = session.prepare(
    #         """
    #         INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
    #         """)
    #
    #     self.assertIsInstance(prepared, PreparedStatement)
    #     bound = prepared.bind((1, None))
    #     self.assertIsInstance(bound, BoundStatement)
    #     session.execute(bound)
    #
    #     print bound.routing_key

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
