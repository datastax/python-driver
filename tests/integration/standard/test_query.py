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
import os

from cassandra.concurrent import execute_concurrent


try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra import ConsistencyLevel
from cassandra.query import (PreparedStatement, BoundStatement, SimpleStatement,
                             BatchStatement, BatchType, dict_factory)
from cassandra.cluster import Cluster
from cassandra.policies import HostDistance

from tests.integration import PROTOCOL_VERSION


class QueryTests(unittest.TestCase):

    def test_query(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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
        self.assertEqual(bound.routing_key, b'\x00\x00\x00\x01')

    def test_trace_prints_okay(self):
        """
        Code coverage to ensure trace prints to string without error
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        session.execute(statement, trace=True)

        # Ensure this does not throw an exception
        str(statement.trace)
        for event in statement.trace.events:
            str(event)

    def test_trace_ignores_row_factory(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        self.assertEqual(bound.routing_key, b'\x00\x00\x00\x01')

    def test_empty_routing_key_indexes(self):
        """
        Ensure when routing_key_indexes are blank,
        the routing key should be None
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)
        prepared.routing_key_indexes = {0: {0: 0}, 1: {1: 1}}

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, 2))
        self.assertEqual(bound.routing_key, b'\x04\x00\x00\x00\x04\x00\x00\x00')

    def test_bound_keyspace(self):
        """
        Ensure that bound.keyspace works as expected
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

        ss = SimpleStatement('SELECT * FROM test3rf.test', consistency_level=ConsistencyLevel.ONE)
        self.assertEqual(str(ss),
                         '<SimpleStatement query="SELECT * FROM test3rf.test", consistency=ONE>')

    def test_prepared_statement(self):
        """
        Highlight the difference between Prepared and Bound statements
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare('INSERT INTO test3rf.test (k, v) VALUES (?, ?)')
        prepared.consistency_level = ConsistencyLevel.ONE

        self.assertEqual(str(prepared),
                         '<PreparedStatement query="INSERT INTO test3rf.test (k, v) VALUES (?, ?)", consistency=ONE>')

        bound = prepared.bind((1, 2))
        self.assertEqual(str(bound),
                         '<BoundStatement query="INSERT INTO test3rf.test (k, v) VALUES (?, ?)", values=(1, 2), consistency=ONE>')


class BatchStatementTests(unittest.TestCase):

    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2.0+ is required for BATCH operations, currently testing against %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        if PROTOCOL_VERSION < 3:
            self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect()

        self.session.execute("TRUNCATE test3rf.test")

    def tearDown(self):
        self.cluster.shutdown()

    def confirm_results(self):
        keys = set()
        values = set()
        results = self.session.execute("SELECT * FROM test3rf.test")
        for result in results:
            keys.add(result.k)
            values.add(result.v)

        self.assertEqual(set(range(10)), keys)
        self.assertEqual(set(range(10)), values)

    def test_string_statements(self):
        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", (i, i))

        self.session.execute(batch)
        self.session.execute_async(batch).result()
        self.confirm_results()

    def test_simple_statements(self):
        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)"), (i, i))

        self.session.execute(batch)
        self.session.execute_async(batch).result()
        self.confirm_results()

    def test_prepared_statements(self):
        prepared = self.session.prepare("INSERT INTO test3rf.test (k, v) VALUES (?, ?)")

        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(prepared, (i, i))

        self.session.execute(batch)
        self.session.execute_async(batch).result()
        self.confirm_results()

    def test_bound_statements(self):
        prepared = self.session.prepare("INSERT INTO test3rf.test (k, v) VALUES (?, ?)")

        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(prepared.bind((i, i)))

        self.session.execute(batch)
        self.session.execute_async(batch).result()
        self.confirm_results()

    def test_no_parameters(self):
        batch = BatchStatement(BatchType.LOGGED)
        batch.add("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        batch.add("INSERT INTO test3rf.test (k, v) VALUES (1, 1)", ())
        batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (2, 2)"))
        batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (3, 3)"), ())

        prepared = self.session.prepare("INSERT INTO test3rf.test (k, v) VALUES (4, 4)")
        batch.add(prepared)
        batch.add(prepared, ())
        batch.add(prepared.bind([]))
        batch.add(prepared.bind([]), ())

        batch.add("INSERT INTO test3rf.test (k, v) VALUES (5, 5)", ())
        batch.add("INSERT INTO test3rf.test (k, v) VALUES (6, 6)", ())
        batch.add("INSERT INTO test3rf.test (k, v) VALUES (7, 7)", ())
        batch.add("INSERT INTO test3rf.test (k, v) VALUES (8, 8)", ())
        batch.add("INSERT INTO test3rf.test (k, v) VALUES (9, 9)", ())

        self.assertRaises(ValueError, batch.add, prepared.bind([]), (1))
        self.assertRaises(ValueError, batch.add, prepared.bind([]), (1, 2))
        self.assertRaises(ValueError, batch.add, prepared.bind([]), (1, 2, 3))

        self.session.execute(batch)
        self.confirm_results()


class SerialConsistencyTests(unittest.TestCase):
    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2.0+ is required for BATCH operations, currently testing against %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        if PROTOCOL_VERSION < 3:
            self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect()

    def test_conditional_update(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = SimpleStatement(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=1",
            serial_consistency_level=ConsistencyLevel.SERIAL)
        result = self.session.execute(statement)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].applied)

        statement = SimpleStatement(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0",
            serial_consistency_level=ConsistencyLevel.SERIAL)
        result = self.session.execute(statement)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].applied)

    def test_conditional_update_with_prepared_statements(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = self.session.prepare(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=2")

        statement.serial_consistency_level = ConsistencyLevel.SERIAL
        result = self.session.execute(statement)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].applied)

        statement = self.session.prepare(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0")
        bound = statement.bind(())
        bound.serial_consistency_level = ConsistencyLevel.SERIAL
        result = self.session.execute(statement)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].applied)

    def test_bad_consistency_level(self):
        statement = SimpleStatement("foo")
        self.assertRaises(ValueError, setattr, statement, 'serial_consistency_level', ConsistencyLevel.ONE)
        self.assertRaises(ValueError, SimpleStatement, 'foo', serial_consistency_level=ConsistencyLevel.ONE)


class LightweightTransactionTests(unittest.TestCase):
    def setUp(self):
        """
        Test is skipped if run with cql version < 2

        """
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2.0+ is required for Lightweight transactions, currently testing against %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()

        ddl = '''
            CREATE TABLE test3rf.lwt (
                k int PRIMARY KEY,
                v int )'''
        self.session.execute(ddl)

    def tearDown(self):
        """
        Shutdown cluster
        """
        self.session.execute("DROP TABLE test3rf.lwt")
        self.cluster.shutdown()

    def test_no_connection_refused_on_timeout(self):
        """
        Test for PYTHON-91 "Connection closed after LWT timeout"
        Verifies that connection to the cluster is not shut down when timeout occurs.
        Number of iterations can be specified with LWT_ITERATIONS environment variable.
        Default value is 1000
        """
        insert_statement = self.session.prepare("INSERT INTO test3rf.lwt (k, v) VALUES (0, 0) IF NOT EXISTS")
        delete_statement = self.session.prepare("DELETE FROM test3rf.lwt WHERE k = 0 IF EXISTS")

        iterations = int(os.getenv("LWT_ITERATIONS", 1000))
        print("Started test for %d iterations" % iterations)

        # Prepare series of parallel statements
        statements_and_params = []
        for i in range(iterations):
            statements_and_params.append((insert_statement, ()))
            statements_and_params.append((delete_statement, ()))

        received_timeout = False
        results = execute_concurrent(self.session, statements_and_params, raise_on_first_error=False)
        for (success, result) in results:
            if success:
                continue
            # In this case result is an exception
            if type(result).__name__ == "NoHostAvailable":
                self.fail("PYTHON-91: Disconnected from Cassandra: %s" % result.message)
                break
            if type(result).__name__ == "WriteTimeout":
                print("Timeout: %s" % result.message)
                received_timeout = True
                continue
            self.fail("Unexpected exception %s: %s" % (type(result).__name__, result.message))
            break

        # Make sure test passed
        self.assertTrue(received_timeout)


class NamedTupleFactoryAndNumericColNamesTests(unittest.TestCase):
    """
    Test for PYTHON-122: Improve Error Handling/Reporting for named_tuple_factory and Numeric Column Names
    """
    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()

        ddl = '''
            CREATE TABLE test3rf.ToBeIndexed ( key blob PRIMARY KEY, "626972746864617465" blob )
            WITH COMPACT STORAGE'''

        self.session.execute(ddl)

    def tearDown(self):
        """
        Shutdown cluster
        """
        self.session.execute("DROP TABLE test3rf.ToBeIndexed")
        self.cluster.shutdown()

    def test_no_exception_on_select(self):
        """
        no exception on SELECT for numeric column name
        """
        try:
            self.session.execute('SELECT * FROM test3rf.ToBeIndexed')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)

    def test_can_select_using_alias(self):
        """
        can SELECT "<numeric col name>" AS aliases
        """
        try:
            self.session.execute('SELECT key, "626972746864617465" AS my_col from test3rf.ToBeIndexed')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)

    def test_can_select_with_dict_factory(self):
        """
        can SELECT numeric column  using  dict_factory
        """
        self.session.row_factory = dict_factory
        try:
            self.session.execute('SELECT * FROM test3rf.ToBeIndexed')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)
