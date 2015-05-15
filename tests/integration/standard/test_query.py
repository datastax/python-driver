# Copyright 2013-2015 DataStax, Inc.
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

from tests.integration import use_singledc, PROTOCOL_VERSION, get_server_versions


def setup_module():
    use_singledc()


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

        cluster.shutdown()

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

        cluster.shutdown()

    def test_trace_id_to_query(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        self.assertIsNone(statement.trace_id)
        future = session.execute_async(statement, trace=True)

        # query should have trace_id, even before trace is obtained
        future.result()
        self.assertIsNotNone(statement.trace_id)

        cluster.shutdown()

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

        cluster.shutdown()

    def test_client_ip_in_trace(self):
        """
        Test to validate that client trace contains client ip information.

        creates a simple query and ensures that the client trace information is present. This will
        only be the case if the c* version is 3.0 or greater


        @since 3.0
        @jira_ticket PYTHON-235
        @expected_result client address should be present in C* >= 3, otherwise should be none.

        @test_category tracing
+       """
        #The current version on the trunk doesn't have the version set to 3.0 yet.
        #For now we will use the protocol version. Once they update the version on C* trunk
        #we can use the C*. See below
        #self._cass_version, self._cql_version = get_server_versions()
        #if self._cass_version < (3, 0):
        #   raise unittest.SkipTest("Client IP was not present in trace until C* 3.0")
        if PROTOCOL_VERSION < 4:
             raise unittest.SkipTest(
                 "Protocol 4+ is required for client ip tracing, currently testing against %r"
                 % (PROTOCOL_VERSION,))

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        response_future = session.execute_async(statement, trace=True)
        response_future.result(10.0)
        current_host = response_future._current_host.address
        # Fetch the client_ip from the trace.
        trace = response_future.get_query_trace(2.0)
        client_ip = trace.client
        # Ensure that ip is set for c* >3
        self.assertIsNotNone(client_ip,"Client IP was not set in trace with C* > 3.0")
        self.assertEqual(client_ip,current_host,"Client IP from trace did not match the expected value")
        cluster.shutdown()


class PreparedStatementTests(unittest.TestCase):

    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    def test_routing_key(self):
        """
        Simple code coverage to ensure routing_keys can be accessed
        """
        prepared = self.session.prepare(
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
        prepared = self.session.prepare(
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
        prepared = self.session.prepare(
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
        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        prepared.routing_key_indexes = [0, 1]
        bound = prepared.bind((1, 2))
        self.assertEqual(bound.routing_key, b'\x00\x04\x00\x00\x00\x01\x00\x00\x04\x00\x00\x00\x02\x00')

        prepared.routing_key_indexes = [1, 0]
        bound = prepared.bind((1, 2))
        self.assertEqual(bound.routing_key, b'\x00\x04\x00\x00\x00\x02\x00\x00\x04\x00\x00\x00\x01\x00')

    def test_bound_keyspace(self):
        """
        Ensure that bound.keyspace works as expected
        """
        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, 2))
        self.assertEqual(bound.keyspace, 'test3rf')


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

        cluster.shutdown()


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
                "Protocol 2.0+ is required for Serial Consistency, currently testing against %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        if PROTOCOL_VERSION < 3:
            self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    def test_conditional_update(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = SimpleStatement(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=1",
            serial_consistency_level=ConsistencyLevel.SERIAL)
        # crazy test, but PYTHON-299
        # TODO: expand to check more parameters get passed to statement, and on to messages
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.SERIAL)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].applied)

        statement = SimpleStatement(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0",
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].applied)

    def test_conditional_update_with_prepared_statements(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = self.session.prepare(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=2")

        statement.serial_consistency_level = ConsistencyLevel.SERIAL
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.SERIAL)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].applied)

        statement = self.session.prepare(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0")
        bound = statement.bind(())
        bound.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        future = self.session.execute_async(bound)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        self.assertEqual(1, len(result))
        self.assertTrue(result[0].applied)

    def test_conditional_update_with_batch_statements(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = BatchStatement(serial_consistency_level=ConsistencyLevel.SERIAL)
        statement.add("UPDATE test3rf.test SET v=1 WHERE k=0 IF v=1")
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.SERIAL)
        self.assertEqual(1, len(result))
        self.assertFalse(result[0].applied)

        statement = BatchStatement(serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)
        statement.add("UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0")
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
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
            else:
                # In this case result is an exception
                if type(result).__name__ == "NoHostAvailable":
                    self.fail("PYTHON-91: Disconnected from Cassandra: %s" % result.message)
                if type(result).__name__ == "WriteTimeout":
                    received_timeout = True
                    continue
                if type(result).__name__ == "ReadTimeout":
                    continue
                self.fail("Unexpected exception %s: %s" % (type(result).__name__, result.message))

        # Make sure test passed
        self.assertTrue(received_timeout)


class BatchStatementDefaultRoutingKeyTests(unittest.TestCase):
    # Test for PYTHON-126: BatchStatement.add() should set the routing key of the first added prepared statement

    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2.0+ is required for BATCH operations, currently testing against %r"
                % (PROTOCOL_VERSION,))
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        query = """
                INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
                """
        self.simple_statement = SimpleStatement(query, routing_key='ss_rk', keyspace='keyspace_name')
        self.prepared = self.session.prepare(query)

    def tearDown(self):
        self.cluster.shutdown()

    def test_rk_from_bound(self):
        """
        batch routing key is inherited from BoundStatement
        """
        bound = self.prepared.bind((1, None))
        batch = BatchStatement()
        batch.add(bound)
        self.assertIsNotNone(batch.routing_key)
        self.assertEqual(batch.routing_key, bound.routing_key)

    def test_rk_from_simple(self):
        """
        batch routing key is inherited from SimpleStatement
        """
        batch = BatchStatement()
        batch.add(self.simple_statement)
        self.assertIsNotNone(batch.routing_key)
        self.assertEqual(batch.routing_key, self.simple_statement.routing_key)

    def test_inherit_first_rk_bound(self):
        """
        compound batch inherits the first routing key of the first added statement (bound statement is first)
        """
        bound = self.prepared.bind((100000000, None))
        batch = BatchStatement()
        batch.add("ss with no rk")
        batch.add(bound)
        batch.add(self.simple_statement)

        for i in range(3):
            batch.add(self.prepared, (i, i))

        self.assertIsNotNone(batch.routing_key)
        self.assertEqual(batch.routing_key, bound.routing_key)

    def test_inherit_first_rk_simple_statement(self):
        """
        compound batch inherits the first routing key of the first added statement (Simplestatement is first)
        """
        bound = self.prepared.bind((1, None))
        batch = BatchStatement()
        batch.add("ss with no rk")
        batch.add(self.simple_statement)
        batch.add(bound)

        for i in range(10):
            batch.add(self.prepared, (i, i))

        self.assertIsNotNone(batch.routing_key)
        self.assertEqual(batch.routing_key, self.simple_statement.routing_key)

    def test_inherit_first_rk_prepared_param(self):
        """
        compound batch inherits the first routing key of the first added statement (prepared statement is first)
        """
        bound = self.prepared.bind((2, None))
        batch = BatchStatement()
        batch.add("ss with no rk")
        batch.add(self.prepared, (1, 0))
        batch.add(bound)
        batch.add(self.simple_statement)

        self.assertIsNotNone(batch.routing_key)
        self.assertEqual(batch.routing_key, self.prepared.bind((1, 0)).routing_key)
