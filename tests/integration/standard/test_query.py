# Copyright 2013-2017 DataStax, Inc.
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
from cassandra import DriverException

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa
import logging
from cassandra import ProtocolVersion
from cassandra import ConsistencyLevel, Unavailable, InvalidRequest, cluster
from cassandra.query import (PreparedStatement, BoundStatement, SimpleStatement,
                             BatchStatement, BatchType, dict_factory, TraceUnavailable)
from cassandra.cluster import Cluster, NoHostAvailable, ExecutionProfile
from cassandra.policies import HostDistance, RoundRobinPolicy, WhiteListRoundRobinPolicy
from tests.integration import use_singledc, PROTOCOL_VERSION, BasicSharedKeyspaceUnitTestCase, get_server_versions, \
    greaterthanprotocolv3, MockLoggingHandler, get_supported_protocol_versions, local, get_cluster, setup_keyspace, \
    USE_CASS_EXTERNAL, greaterthanorequalcass40
from tests import notwindows
from tests.integration import greaterthanorequalcass30, get_node

import time
import re

def setup_module():
    if not USE_CASS_EXTERNAL:
        use_singledc(start=False)
        ccm_cluster = get_cluster()
        ccm_cluster.clear()
        # This is necessary because test_too_many_statements may
        # timeout otherwise
        config_options = {'write_request_timeout_in_ms': '20000'}
        ccm_cluster.set_configuration_options(config_options)
        ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

    setup_keyspace()
    global CASS_SERVER_VERSION
    CASS_SERVER_VERSION = get_server_versions()[0]


class QueryTests(BasicSharedKeyspaceUnitTestCase):

    def test_query(self):

        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """.format(self.keyspace_name))

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        self.assertIsInstance(bound, BoundStatement)
        self.assertEqual(2, len(bound.values))
        self.session.execute(bound)
        self.assertEqual(bound.routing_key, b'\x00\x00\x00\x01')

    def test_trace_prints_okay(self):
        """
        Code coverage to ensure trace prints to string without error
        """

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        rs = self.session.execute(statement, trace=True)

        # Ensure this does not throw an exception
        trace = rs.get_query_trace()
        self.assertTrue(trace.events)
        str(trace)
        for event in trace.events:
            str(event)

    def test_row_error_message(self):
        """
        Test to validate, new column deserialization message
        @since 3.7.0
        @jira_ticket PYTHON-361
        @expected_result Special failed decoding message should be present

        @test_category tracing
        """
        self.session.execute("CREATE TABLE {0}.{1} (k int PRIMARY KEY, v timestamp)".format(self.keyspace_name,self.function_table_name))
        ss = SimpleStatement("INSERT INTO {0}.{1} (k, v) VALUES (1, 1000000000000000)".format(self.keyspace_name, self.function_table_name))
        self.session.execute(ss)
        with self.assertRaises(DriverException) as context:
            self.session.execute("SELECT * FROM {0}.{1}".format(self.keyspace_name, self.function_table_name))
        self.assertIn("Failed decoding result column", str(context.exception))

    def test_trace_id_to_resultset(self):

        future = self.session.execute_async("SELECT * FROM system.local", trace=True)

        # future should have the current trace
        rs = future.result()
        future_trace = future.get_query_trace()
        self.assertIsNotNone(future_trace)

        rs_trace = rs.get_query_trace()
        self.assertEqual(rs_trace, future_trace)
        self.assertTrue(rs_trace.events)
        self.assertEqual(len(rs_trace.events), len(future_trace.events))

        self.assertListEqual([rs_trace], rs.get_all_query_traces())

    def test_trace_ignores_row_factory(self):
        self.session.row_factory = dict_factory

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        rs = self.session.execute(statement, trace=True)

        # Ensure this does not throw an exception
        trace = rs.get_query_trace()
        self.assertTrue(trace.events)
        str(trace)
        for event in trace.events:
            str(event)

    @local
    @greaterthanprotocolv3
    def test_client_ip_in_trace(self):
        """
        Test to validate that client trace contains client ip information.

        creates a simple query and ensures that the client trace information is present. This will
        only be the case if the c* version is 2.2 or greater

        @since 2.6.0
        @jira_ticket PYTHON-435
        @expected_result client address should be present in C* >= 2.2, otherwise should be none.

        @test_category tracing
        #The current version on the trunk doesn't have the version set to 2.2 yet.
        #For now we will use the protocol version. Once they update the version on C* trunk
        #we can use the C*. See below
        #self._cass_version, self._cql_version = get_server_versions()
        #if self._cass_version < (2, 2):
        #   raise unittest.SkipTest("Client IP was not present in trace until C* 2.2")
        """

        # Make simple query with trace enabled
        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        response_future = self.session.execute_async(statement, trace=True)
        response_future.result()

        # Fetch the client_ip from the trace.
        trace = response_future.get_query_trace(max_wait=10.0)
        client_ip = trace.client

        # Ip address should be in the local_host range
        pat = re.compile("127.0.0.\d{1,3}")

        # Ensure that ip is set
        self.assertIsNotNone(client_ip, "Client IP was not set in trace with C* >= 2.2")
        self.assertTrue(pat.match(client_ip), "Client IP from trace did not match the expected value")

    def test_trace_cl(self):
        """
        Test to ensure that CL is set correctly honored when executing trace queries.

        @since 3.3
        @jira_ticket PYTHON-435
        @expected_result Consistency Levels set on get_query_trace should be honored
        """
        # Execute a query
        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        response_future = self.session.execute_async(statement, trace=True)
        response_future.result()
        with self.assertRaises(Unavailable):
            response_future.get_query_trace(query_cl=ConsistencyLevel.THREE)
        # Try again with a smattering of other CL's
        self.assertIsNotNone(response_future.get_query_trace(max_wait=2.0, query_cl=ConsistencyLevel.TWO).trace_id)
        response_future = self.session.execute_async(statement, trace=True)
        response_future.result()
        self.assertIsNotNone(response_future.get_query_trace(max_wait=2.0, query_cl=ConsistencyLevel.ONE).trace_id)
        response_future = self.session.execute_async(statement, trace=True)
        response_future.result()
        with self.assertRaises(InvalidRequest):
            self.assertIsNotNone(response_future.get_query_trace(max_wait=2.0, query_cl=ConsistencyLevel.ANY).trace_id)
        self.assertIsNotNone(response_future.get_query_trace(max_wait=2.0, query_cl=ConsistencyLevel.QUORUM).trace_id)

    @notwindows
    def test_incomplete_query_trace(self):
        """
        Tests to ensure that partial tracing works.

        Creates a table and runs an insert. Then attempt a query with tracing enabled. After the query is run we delete the
        duration information associated with the trace, and attempt to populate the tracing information.
        Should fail with wait_for_complete=True, succeed for False.

        @since 3.0.0
        @jira_ticket PYTHON-438
        @expected_result tracing comes back sans duration

        @test_category tracing
        """

        # Create table and run insert, then select
        self.session.execute("CREATE TABLE {0} (k INT, i INT, PRIMARY KEY(k, i))".format(self.keyspace_table_name))
        self.session.execute("INSERT INTO {0} (k, i) VALUES (0, 1)".format(self.keyspace_table_name))

        response_future = self.session.execute_async("SELECT i FROM {0} WHERE k=0".format(self.keyspace_table_name), trace=True)
        response_future.result()

        self.assertEqual(len(response_future._query_traces), 1)
        trace = response_future._query_traces[0]
        self.assertTrue(self._wait_for_trace_to_populate(trace.trace_id))

        # Delete trace duration from the session (this is what the driver polls for "complete")
        delete_statement = SimpleStatement("DELETE duration FROM system_traces.sessions WHERE session_id = {0}".format(trace.trace_id), consistency_level=ConsistencyLevel.ALL)
        self.session.execute(delete_statement)
        self.assertTrue(self._wait_for_trace_to_delete(trace.trace_id))

        # should raise because duration is not set
        self.assertRaises(TraceUnavailable, trace.populate, max_wait=0.2, wait_for_complete=True)
        self.assertFalse(trace.events)

        # should get the events with wait False
        trace.populate(wait_for_complete=False)
        self.assertIsNone(trace.duration)
        self.assertIsNotNone(trace.trace_id)
        self.assertIsNotNone(trace.request_type)
        self.assertIsNotNone(trace.parameters)
        self.assertTrue(trace.events)  # non-zero list len
        self.assertIsNotNone(trace.started_at)

    def _wait_for_trace_to_populate(self, trace_id):
        count = 0
        retry_max = 10
        while(not self._is_trace_present(trace_id) and count < retry_max):
            time.sleep(.2)
            count += 1
        return count != retry_max

    def _wait_for_trace_to_delete(self, trace_id):
        count = 0
        retry_max = 10
        while(self._is_trace_present(trace_id) and count < retry_max):
            time.sleep(.2)
            count += 1
        return count != retry_max

    def _is_trace_present(self, trace_id):
        select_statement = SimpleStatement("SElECT duration FROM system_traces.sessions WHERE session_id = {0}".format(trace_id), consistency_level=ConsistencyLevel.ALL)
        ssrs = self.session.execute(select_statement)
        if not len(ssrs.current_rows) or ssrs[0].duration is None:
            return False
        return True

    def test_query_by_id(self):
        """
        Test to ensure column_types are set as part of the result set

        @since 3.8
        @jira_ticket PYTHON-648
        @expected_result column_names should be preset.

        @test_category queries basic
        """
        create_table = "CREATE TABLE {0}.{1} (id int primary key, m map<int, text>)".format(self.keyspace_name, self.function_table_name)
        self.session.execute(create_table)

        self.session.execute("insert into "+self.keyspace_name+"."+self.function_table_name+" (id, m) VALUES ( 1, {1: 'one', 2: 'two', 3:'three'})")
        results1 = self.session.execute("select id, m from {0}.{1}".format(self.keyspace_name, self.function_table_name))

        self.assertIsNotNone(results1.column_types)
        self.assertEqual(results1.column_types[0].typename, 'int')
        self.assertEqual(results1.column_types[1].typename, 'map')
        self.assertEqual(results1.column_types[0].cassname, 'Int32Type')
        self.assertEqual(results1.column_types[1].cassname, 'MapType')
        self.assertEqual(len(results1.column_types[0].subtypes), 0)
        self.assertEqual(len(results1.column_types[1].subtypes), 2)
        self.assertEqual(results1.column_types[1].subtypes[0].typename, "int")
        self.assertEqual(results1.column_types[1].subtypes[1].typename, "varchar")
        self.assertEqual(results1.column_types[1].subtypes[0].cassname, "Int32Type")
        self.assertEqual(results1.column_types[1].subtypes[1].cassname, "VarcharType")

    def test_column_names(self):
        """
        Test to validate the columns are present on the result set.
        Preforms a simple query against a table then checks to ensure column names are correct and present and correct.

        @since 3.0.0
        @jira_ticket PYTHON-439
        @expected_result column_names should be preset.

        @test_category queries basic
        """
        create_table = """CREATE TABLE {0}.{1}(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score INT,
                        PRIMARY KEY (user, game, year, month, day)
                        )""".format(self.keyspace_name, self.function_table_name)


        self.session.execute(create_table)
        result_set = self.session.execute("SELECT * FROM {0}.{1}".format(self.keyspace_name, self.function_table_name))
        self.assertIsNotNone(result_set.column_types)

        self.assertEqual(result_set.column_names, [u'user', u'game', u'year', u'month', u'day', u'score'])

    @greaterthanorequalcass30
    def test_basic_json_query(self):
        insert_query = SimpleStatement("INSERT INTO test3rf.test(k, v) values (1, 1)", consistency_level = ConsistencyLevel.QUORUM)
        json_query = SimpleStatement("SELECT JSON * FROM test3rf.test where k=1", consistency_level = ConsistencyLevel.QUORUM)

        self.session.execute(insert_query)
        results = self.session.execute(json_query)
        self.assertEqual(results.column_names, ["[json]"])
        self.assertEqual(results[0][0], '{"k": 1, "v": 1}')


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


class ForcedHostSwitchPolicy(RoundRobinPolicy):

    def make_query_plan(self, working_keyspace=None, query=None):
        if hasattr(self, 'counter'):
            self.counter += 1
        else:
            self.counter = 0
        index = self.counter % 3
        a = list(self._live_hosts)
        value = [a[index]]
        return value


class PreparedStatementMetdataTest(unittest.TestCase):

    def test_prepared_metadata_generation(self):
        """
        Test to validate that result metadata is appropriately populated across protocol version

        In protocol version 1 result metadata is retrieved everytime the statement is issued. In all
        other protocol versions it's set once upon the prepare, then re-used. This test ensures that it manifests
        it's self the same across multiple protocol versions.

        @since 3.6.0
        @jira_ticket PYTHON-71
        @expected_result result metadata is consistent.
        """

        base_line = None
        for proto_version in get_supported_protocol_versions():
            beta_flag = True if proto_version in ProtocolVersion.BETA_VERSIONS else False
            cluster = Cluster(protocol_version=proto_version, allow_beta_protocol_version=beta_flag)

            session = cluster.connect()
            select_statement = session.prepare("SELECT * FROM system.local")
            if proto_version == 1:
                self.assertEqual(select_statement.result_metadata, None)
            else:
                self.assertNotEqual(select_statement.result_metadata, None)
            future = session.execute_async(select_statement)
            results = future.result()
            if base_line is None:
                base_line = results[0]._asdict().keys()
            else:
                self.assertEqual(base_line, results[0]._asdict().keys())
            cluster.shutdown()


class PreparedStatementArgTest(unittest.TestCase):

    def test_prepare_on_all_hosts(self):
        """
        Test to validate prepare_on_all_hosts flag is honored.

        Use a special ForcedHostSwitchPolicy to ensure prepared queries are cycled over nodes that should not
        have them prepared. Check the logs to insure they are being re-prepared on those nodes

        @since 3.4.0
        @jira_ticket PYTHON-556
        @expected_result queries will have to re-prepared on hosts that aren't the control connection
        """
        white_list = ForcedHostSwitchPolicy()
        clus = Cluster(
            load_balancing_policy=white_list,
            protocol_version=PROTOCOL_VERSION, prepare_on_all_hosts=False, reprepare_on_up=False)
        self.addCleanup(clus.shutdown)

        session = clus.connect(wait_for_all_pools=True)
        mock_handler = MockLoggingHandler()
        logger = logging.getLogger(cluster.__name__)
        logger.addHandler(mock_handler)
        select_statement = session.prepare("SELECT * FROM system.local")
        session.execute(select_statement)
        session.execute(select_statement)
        session.execute(select_statement)
        self.assertEqual(2, mock_handler.get_message_count('debug', "Re-preparing"))


    def test_prepare_batch_statement(self):
        """
        Test to validate a prepared statement used inside a batch statement is correctly handled
        by the driver

        @since 3.10
        @jira_ticket PYTHON-706
        @expected_result queries will have to re-prepared on hosts that aren't the control connection
        and the batch statement will be sent.
        """
        white_list = ForcedHostSwitchPolicy()
        clus = Cluster(
            load_balancing_policy=white_list,
            protocol_version=PROTOCOL_VERSION, prepare_on_all_hosts=False,
            reprepare_on_up=False)
        self.addCleanup(clus.shutdown)

        table = "test3rf.%s" % self._testMethodName.lower()

        session = clus.connect(wait_for_all_pools=True)

        session.execute("DROP TABLE IF EXISTS %s" % table)
        session.execute("CREATE TABLE %s (k int PRIMARY KEY, v int )" % table)

        insert_statement = session.prepare("INSERT INTO %s (k, v) VALUES  (?, ?)" % table)

        # This is going to query a host where the query
        # is not prepared
        batch_statement = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        batch_statement.add(insert_statement, (1, 2))
        session.execute(batch_statement)
        select_results = session.execute(SimpleStatement("SELECT * FROM %s WHERE k = 1" % table,
                                                         consistency_level=ConsistencyLevel.ALL))
        first_row = select_results[0][:2]
        self.assertEqual((1, 2), first_row)

    def test_prepare_batch_statement_after_alter(self):
        """
        Test to validate a prepared statement used inside a batch statement is correctly handled
        by the driver. The metadata might be updated when a table is altered. This tests combines
        queries not being prepared and an update of the prepared statement metadata

        @since 3.10
        @jira_ticket PYTHON-706
        @expected_result queries will have to re-prepared on hosts that aren't the control connection
        and the batch statement will be sent.
        """
        white_list = ForcedHostSwitchPolicy()
        clus = Cluster(
            load_balancing_policy=white_list,
            protocol_version=PROTOCOL_VERSION, prepare_on_all_hosts=False,
            reprepare_on_up=False)
        self.addCleanup(clus.shutdown)

        table = "test3rf.%s" % self._testMethodName.lower()

        session = clus.connect(wait_for_all_pools=True)

        session.execute("DROP TABLE IF EXISTS %s" % table)
        session.execute("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, d int)" % table)
        insert_statement = session.prepare("INSERT INTO %s (k, b, d) VALUES  (?, ?, ?)" % table)

        # Altering the table might trigger an update in the insert metadata
        session.execute("ALTER TABLE %s ADD c int" % table)

        values_to_insert = [(1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)]

        # We query the three hosts in order (due to the ForcedHostSwitchPolicy)
        # the first three queries will have to be repreapred and the rest should
        # work as normal batch prepared statements
        for i in range(10):
            value_to_insert = values_to_insert[i % len(values_to_insert)]
            batch_statement = BatchStatement(consistency_level=ConsistencyLevel.ONE)
            batch_statement.add(insert_statement, value_to_insert)
            session.execute(batch_statement)

        select_results = session.execute("SELECT * FROM %s" % table)
        expected_results = [(1, None, 2, None, 3), (2, None, 3, None, 4),
             (3, None, 4, None, 5), (4, None, 5, None, 6)]
        
        self.assertEqual(set(expected_results), set(select_results._current_rows))


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


class BatchStatementTests(BasicSharedKeyspaceUnitTestCase):

    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2.0+ is required for BATCH operations, currently testing against %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        if PROTOCOL_VERSION < 3:
            self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect(wait_for_all_pools=True)

    def tearDown(self):
        self.cluster.shutdown()

    def confirm_results(self):
        keys = set()
        values = set()
        # Assuming the test data is inserted at default CL.ONE, we need ALL here to guarantee we see
        # everything inserted
        results = self.session.execute(SimpleStatement("SELECT * FROM test3rf.test",
                                                       consistency_level=ConsistencyLevel.ALL))
        for result in results:
            keys.add(result.k)
            values.add(result.v)

        self.assertEqual(set(range(10)), keys, msg=results)
        self.assertEqual(set(range(10)), values, msg=results)

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

    def test_unicode(self):
        ddl = '''
            CREATE TABLE test3rf.testtext (
                k int PRIMARY KEY,
                v text )'''
        self.session.execute(ddl)
        unicode_text = u'Fran\u00E7ois'
        query = u'INSERT INTO test3rf.testtext (k, v) VALUES (%s, %s)'
        try:
            batch = BatchStatement(BatchType.LOGGED)
            batch.add(u"INSERT INTO test3rf.testtext (k, v) VALUES (%s, %s)", (0, unicode_text))
            self.session.execute(batch)
        finally:
            self.session.execute("DROP TABLE test3rf.testtext")

    def test_too_many_statements(self):
        max_statements = 0xFFFF
        ss = SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        b = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.ONE)

        # max works
        b.add_all([ss] * max_statements, [None] * max_statements)
        self.session.execute(b)

        # max + 1 raises
        self.assertRaises(ValueError, b.add, ss)

        # also would have bombed trying to encode
        b._statements_and_parameters.append((False, ss.query_string, ()))
        self.assertRaises(NoHostAvailable, self.session.execute, b)


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
        self.assertTrue(result)
        self.assertFalse(result[0].applied)

        statement = SimpleStatement(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0",
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        self.assertTrue(result)
        self.assertTrue(result[0].applied)

    def test_conditional_update_with_prepared_statements(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = self.session.prepare(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=2")

        statement.serial_consistency_level = ConsistencyLevel.SERIAL
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.SERIAL)
        self.assertTrue(result)
        self.assertFalse(result[0].applied)

        statement = self.session.prepare(
            "UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0")
        bound = statement.bind(())
        bound.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        future = self.session.execute_async(bound)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        self.assertTrue(result)
        self.assertTrue(result[0].applied)

    def test_conditional_update_with_batch_statements(self):
        self.session.execute("INSERT INTO test3rf.test (k, v) VALUES (0, 0)")
        statement = BatchStatement(serial_consistency_level=ConsistencyLevel.SERIAL)
        statement.add("UPDATE test3rf.test SET v=1 WHERE k=0 IF v=1")
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.SERIAL)
        self.assertTrue(result)
        self.assertFalse(result[0].applied)

        statement = BatchStatement(serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)
        statement.add("UPDATE test3rf.test SET v=1 WHERE k=0 IF v=0")
        self.assertEqual(statement.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        future = self.session.execute_async(statement)
        result = future.result()
        self.assertEqual(future.message.serial_consistency_level, ConsistencyLevel.LOCAL_SERIAL)
        self.assertTrue(result)
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
                if type(result).__name__ == "WriteFailure":
                    received_timeout = True
                    continue
                if type(result).__name__ == "ReadTimeout":
                    continue
                if type(result).__name__ == "ReadFailure":
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


class MaterializedViewQueryTest(BasicSharedKeyspaceUnitTestCase):

    def setUp(self):
        if CASS_SERVER_VERSION < (3, 0):
            raise unittest.SkipTest("Materialized views require Cassandra 3.0+")

    def test_mv_filtering(self):
        """
        Test to ensure that cql filtering where clauses are properly supported in the python driver.

        test_mv_filtering Tests that various complex MV where clauses produce the correct results. It also validates that
        these results and the grammar is supported appropriately.

        @since 3.0.0
        @jira_ticket PYTHON-399
        @expected_result Materialized view where clauses should produce the appropriate results.

        @test_category materialized_view
        """
        create_table = """CREATE TABLE {0}.scores(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score INT,
                        PRIMARY KEY (user, game, year, month, day)
                        )""".format(self.keyspace_name)

        self.session.execute(create_table)

        create_mv_alltime = """CREATE MATERIALIZED VIEW {0}.alltimehigh AS
                        SELECT * FROM {0}.scores
                        WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY (game, score, user, year, month, day)
                        WITH CLUSTERING ORDER BY (score DESC)""".format(self.keyspace_name)

        create_mv_dailyhigh = """CREATE MATERIALIZED VIEW {0}.dailyhigh AS
                        SELECT * FROM {0}.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL
                        PRIMARY KEY ((game, year, month, day), score, user)
                        WITH CLUSTERING ORDER BY (score DESC)""".format(self.keyspace_name)

        create_mv_monthlyhigh = """CREATE MATERIALIZED VIEW {0}.monthlyhigh AS
                        SELECT * FROM {0}.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY ((game, year, month), score, user, day)
                        WITH CLUSTERING ORDER BY (score DESC)""".format(self.keyspace_name)

        create_mv_filtereduserhigh = """CREATE MATERIALIZED VIEW {0}.filtereduserhigh AS
                        SELECT * FROM {0}.scores
                        WHERE user in ('jbellis', 'pcmanus') AND game IS NOT NULL AND score IS NOT NULL AND year is NOT NULL AND day is not NULL and month IS NOT NULL
                        PRIMARY KEY (game, score, user, year, month, day)
                        WITH CLUSTERING ORDER BY (score DESC)""".format(self.keyspace_name)

        self.session.execute(create_mv_alltime)
        self.session.execute(create_mv_dailyhigh)
        self.session.execute(create_mv_monthlyhigh)
        self.session.execute(create_mv_filtereduserhigh)

        self.addCleanup(self.session.execute, "DROP MATERIALIZED VIEW {0}.alltimehigh".format(self.keyspace_name))
        self.addCleanup(self.session.execute, "DROP MATERIALIZED VIEW {0}.dailyhigh".format(self.keyspace_name))
        self.addCleanup(self.session.execute, "DROP MATERIALIZED VIEW {0}.monthlyhigh".format(self.keyspace_name))
        self.addCleanup(self.session.execute, "DROP MATERIALIZED VIEW {0}.filtereduserhigh".format(self.keyspace_name))

        prepared_insert = self.session.prepare("""INSERT INTO {0}.scores (user, game, year, month, day, score) VALUES  (?, ?, ? ,? ,?, ?)""".format(self.keyspace_name))

        bound = prepared_insert.bind(('pcmanus', 'Coup', 2015, 5, 1, 4000))
        self.session.execute(bound)
        bound = prepared_insert.bind(('jbellis', 'Coup', 2015, 5, 3, 1750))
        self.session.execute(bound)
        bound = prepared_insert.bind(('yukim', 'Coup', 2015, 5, 3, 2250))
        self.session.execute(bound)
        bound = prepared_insert.bind(('tjake', 'Coup', 2015, 5, 3, 500))
        self.session.execute(bound)
        bound = prepared_insert.bind(('iamaleksey', 'Coup', 2015, 6, 1, 2500))
        self.session.execute(bound)
        bound = prepared_insert.bind(('tjake', 'Coup', 2015, 6, 2, 1000))
        self.session.execute(bound)
        bound = prepared_insert.bind(('pcmanus', 'Coup', 2015, 6, 2, 2000))
        self.session.execute(bound)
        bound = prepared_insert.bind(('jmckenzie', 'Coup', 2015, 6, 9, 2700))
        self.session.execute(bound)
        bound = prepared_insert.bind(('jbellis', 'Coup', 2015, 6, 20, 3500))
        self.session.execute(bound)
        bound = prepared_insert.bind(('jbellis', 'Checkers', 2015, 6, 20, 1200))
        self.session.execute(bound)
        bound = prepared_insert.bind(('jbellis', 'Chess', 2015, 6, 21, 3500))
        self.session.execute(bound)
        bound = prepared_insert.bind(('pcmanus', 'Chess', 2015, 1, 25, 3200))
        self.session.execute(bound)

        # Test simple statement and alltime high filtering
        query_statement = SimpleStatement("SELECT * FROM {0}.alltimehigh WHERE game='Coup'".format(self.keyspace_name),
                                          consistency_level=ConsistencyLevel.QUORUM)
        results = self.session.execute(query_statement)
        self.assertEqual(results[0].game, 'Coup')
        self.assertEqual(results[0].year, 2015)
        self.assertEqual(results[0].month, 5)
        self.assertEqual(results[0].day, 1)
        self.assertEqual(results[0].score, 4000)
        self.assertEqual(results[0].user, "pcmanus")

        # Test prepared statement and daily high filtering
        prepared_query = self.session.prepare("SELECT * FROM {0}.dailyhigh WHERE game=? AND year=? AND month=? and day=?".format(self.keyspace_name))
        bound_query = prepared_query.bind(("Coup", 2015, 6, 2))
        results = self.session.execute(bound_query)
        self.assertEqual(results[0].game, 'Coup')
        self.assertEqual(results[0].year, 2015)
        self.assertEqual(results[0].month, 6)
        self.assertEqual(results[0].day, 2)
        self.assertEqual(results[0].score, 2000)
        self.assertEqual(results[0].user, "pcmanus")

        self.assertEqual(results[1].game, 'Coup')
        self.assertEqual(results[1].year, 2015)
        self.assertEqual(results[1].month, 6)
        self.assertEqual(results[1].day, 2)
        self.assertEqual(results[1].score, 1000)
        self.assertEqual(results[1].user, "tjake")

        # Test montly high range queries
        prepared_query = self.session.prepare("SELECT * FROM {0}.monthlyhigh WHERE game=? AND year=? AND month=? and score >= ? and score <= ?".format(self.keyspace_name))
        bound_query = prepared_query.bind(("Coup", 2015, 6, 2500, 3500))
        results = self.session.execute(bound_query)
        self.assertEqual(results[0].game, 'Coup')
        self.assertEqual(results[0].year, 2015)
        self.assertEqual(results[0].month, 6)
        self.assertEqual(results[0].day, 20)
        self.assertEqual(results[0].score, 3500)
        self.assertEqual(results[0].user, "jbellis")

        self.assertEqual(results[1].game, 'Coup')
        self.assertEqual(results[1].year, 2015)
        self.assertEqual(results[1].month, 6)
        self.assertEqual(results[1].day, 9)
        self.assertEqual(results[1].score, 2700)
        self.assertEqual(results[1].user, "jmckenzie")

        self.assertEqual(results[2].game, 'Coup')
        self.assertEqual(results[2].year, 2015)
        self.assertEqual(results[2].month, 6)
        self.assertEqual(results[2].day, 1)
        self.assertEqual(results[2].score, 2500)
        self.assertEqual(results[2].user, "iamaleksey")

        # Test filtered user high scores
        query_statement = SimpleStatement("SELECT * FROM {0}.filtereduserhigh WHERE game='Chess'".format(self.keyspace_name),
                                          consistency_level=ConsistencyLevel.QUORUM)
        results = self.session.execute(query_statement)
        self.assertEqual(results[0].game, 'Chess')
        self.assertEqual(results[0].year, 2015)
        self.assertEqual(results[0].month, 6)
        self.assertEqual(results[0].day, 21)
        self.assertEqual(results[0].score, 3500)
        self.assertEqual(results[0].user, "jbellis")

        self.assertEqual(results[1].game, 'Chess')
        self.assertEqual(results[1].year, 2015)
        self.assertEqual(results[1].month, 1)
        self.assertEqual(results[1].day, 25)
        self.assertEqual(results[1].score, 3200)
        self.assertEqual(results[1].user, "pcmanus")


class UnicodeQueryTest(BasicSharedKeyspaceUnitTestCase):

    def setUp(self):
        ddl = '''
            CREATE TABLE {0}.{1} (
            k int PRIMARY KEY,
            v text )'''.format(self.keyspace_name, self.function_table_name)
        self.session.execute(ddl)

    def tearDown(self):
        self.session.execute("DROP TABLE {0}.{1}".format(self.keyspace_name,self.function_table_name))

    def test_unicode(self):
        """
        Test to validate that unicode query strings are handled appropriately by various query types

        @since 3.0.0
        @jira_ticket PYTHON-334
        @expected_result no unicode exceptions are thrown

        @test_category query
        """

        unicode_text = u'Fran\u00E7ois'
        batch = BatchStatement(BatchType.LOGGED)
        batch.add(u"INSERT INTO {0}.{1} (k, v) VALUES (%s, %s)".format(self.keyspace_name, self.function_table_name), (0, unicode_text))
        self.session.execute(batch)
        self.session.execute(u"INSERT INTO {0}.{1} (k, v) VALUES (%s, %s)".format(self.keyspace_name, self.function_table_name), (0, unicode_text))
        prepared = self.session.prepare(u"INSERT INTO {0}.{1} (k, v) VALUES (?, ?)".format(self.keyspace_name, self.function_table_name))
        bound = prepared.bind((1, unicode_text))
        self.session.execute(bound)


class BaseKeyspaceTests():
    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect(wait_for_all_pools=True)
        cls.ks_name = cls.__name__.lower()

        cls.alternative_ks = "alternative_keyspace"
        cls.table_name = "table_query_keyspace_tests"

        ddl = """CREATE KEYSPACE {0} WITH replication =
                        {{'class': 'SimpleStrategy',
                        'replication_factor': '{1}'}}""".format(cls.ks_name, 1)
        cls.session.execute(ddl)

        ddl = """CREATE KEYSPACE {0} WITH replication =
                                {{'class': 'SimpleStrategy',
                                'replication_factor': '{1}'}}""".format(cls.alternative_ks, 1)
        cls.session.execute(ddl)

        ddl = '''
                CREATE TABLE {0}.{1} (
                    k int PRIMARY KEY,
                    v int )'''.format(cls.ks_name, cls.table_name)
        cls.session.execute(ddl)
        ddl = '''
                CREATE TABLE {0}.{1} (
                    k int PRIMARY KEY,
                    v int )'''.format(cls.alternative_ks, cls.table_name)
        cls.session.execute(ddl)

        cls.session.execute("INSERT INTO {}.{} (k, v) VALUES (1, 1)".format(cls.ks_name, cls.table_name))
        cls.session.execute("INSERT INTO {}.{} (k, v) VALUES (2, 2)".format(cls.alternative_ks, cls.table_name))

    @classmethod
    def tearDownClass(cls):
        ddl = "DROP KEYSPACE {}".format(cls.alternative_ks)
        cls.session.execute(ddl)
        ddl = "DROP KEYSPACE {}".format(cls.ks_name)
        cls.session.execute(ddl)
        cls.cluster.shutdown()

class QueryKeyspaceTests(BaseKeyspaceTests):

    def test_setting_keyspace(self):
        """
        Test the basic functionality of PYTHON-678, the keyspace can be set
        independently of the query and read the results

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        self._check_set_keyspace_in_statement(self.session)

    def test_setting_keyspace_and_session(self):
        """
        Test we can still send the keyspace independently even the session
        connects to a keyspace when it's created

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        cluster = Cluster(protocol_version=ProtocolVersion.V5, allow_beta_protocol_version=True)
        session = cluster.connect(self.alternative_ks)
        self.addCleanup(cluster.shutdown)

        self._check_set_keyspace_in_statement(session)

    def test_setting_keyspace_and_session_after_created(self):
        """
        Test we can still send the keyspace independently even the session
        connects to a different keyspace after being created

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        cluster = Cluster(protocol_version=ProtocolVersion.V5, allow_beta_protocol_version=True)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)

        session.set_keyspace(self.alternative_ks)
        self._check_set_keyspace_in_statement(session)

    def test_setting_keyspace_and_same_session(self):
        """
        Test we can still send the keyspace independently even if the session
        is connected to the sent keyspace

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        cluster = Cluster(protocol_version=ProtocolVersion.V5, allow_beta_protocol_version=True)
        session = cluster.connect(self.ks_name)
        self.addCleanup(cluster.shutdown)

        self._check_set_keyspace_in_statement(session)


@greaterthanorequalcass40
class SimpleWithKeyspaceTests(QueryKeyspaceTests, unittest.TestCase):
    @unittest.skip
    def test_lower_protocol(self):
        cluster = Cluster(protocol_version=ProtocolVersion.V4)
        session = cluster.connect(self.ks_name)
        self.addCleanup(cluster.shutdown)

        simple_stmt = SimpleStatement("SELECT * from {}".format(self.table_name), keyspace=self.ks_name)
        # This raises cassandra.cluster.NoHostAvailable: ('Unable to complete the operation against
        # any hosts', {<Host: 127.0.0.3 datacenter1>: UnsupportedOperation('Keyspaces may only be
        # set on queries with protocol version 5 or higher. Consider setting Cluster.protocol_version to 5.',),
        # <Host: 127.0.0.2 datacenter1>: ConnectionException('Host has been marked down or removed',),
        # <Host: 127.0.0.1 datacenter1>: ConnectionException('Host has been marked down or removed',)})
        with self.assertRaises(NoHostAvailable):
            session.execute(simple_stmt)
            
    def _check_set_keyspace_in_statement(self, session):
        simple_stmt = SimpleStatement("SELECT * from {}".format(self.table_name), keyspace=self.ks_name)
        results = session.execute(simple_stmt)
        self.assertEqual(results[0], (1, 1))

        simple_stmt = SimpleStatement("SELECT * from {}".format(self.table_name))
        simple_stmt.keyspace = self.ks_name
        results = session.execute(simple_stmt)
        self.assertEqual(results[0], (1, 1))


@greaterthanorequalcass40
class BatchWithKeyspaceTests(QueryKeyspaceTests, unittest.TestCase):
    def _check_set_keyspace_in_statement(self, session):
        batch_stmt = BatchStatement()
        for i in range(10):
            batch_stmt.add("INSERT INTO {} (k, v) VALUES (%s, %s)".format(self.table_name), (i, i))

        batch_stmt.keyspace = self.ks_name
        session.execute(batch_stmt)
        self.confirm_results()

    def confirm_results(self):
        keys = set()
        values = set()
        # Assuming the test data is inserted at default CL.ONE, we need ALL here to guarantee we see
        # everything inserted
        results = self.session.execute(SimpleStatement("SELECT * FROM {}.{}".format(self.ks_name, self.table_name),
                                                       consistency_level=ConsistencyLevel.ALL))
        for result in results:
            keys.add(result.k)
            values.add(result.v)

        self.assertEqual(set(range(10)), keys, msg=results)
        self.assertEqual(set(range(10)), values, msg=results)


@greaterthanorequalcass40
class PreparedWithKeyspaceTests(BaseKeyspaceTests, unittest.TestCase):

    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, allow_beta_protocol_version=True)
        self.session = self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    def test_prepared_with_keyspace_explicit(self):
        """
        Test the basic functionality of PYTHON-678, the keyspace can be set
        independently of the query and read the results

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        query = "SELECT * from {} WHERE k = ?".format(self.table_name)
        prepared_statement = self.session.prepare(query, keyspace=self.ks_name)

        results = self.session.execute(prepared_statement, (1, ))
        self.assertEqual(results[0], (1, 1))

        prepared_statement_alternative = self.session.prepare(query, keyspace=self.alternative_ks)

        self.assertNotEqual(prepared_statement.query_id, prepared_statement_alternative.query_id)

        results = self.session.execute(prepared_statement_alternative, (2,))
        self.assertEqual(results[0], (2, 2))

    def test_reprepare_after_host_is_down(self):
        """
        Test that Cluster._prepare_all_queries is called and the
        when a node comes up and the queries succeed later

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        mock_handler = MockLoggingHandler()
        logger = logging.getLogger(cluster.__name__)
        logger.addHandler(mock_handler)
        get_node(1).stop(wait=True, gently=True, wait_other_notice=True)

        only_first = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]))
        self.cluster.add_execution_profile("only_first", only_first)

        query = "SELECT v from {} WHERE k = ?".format(self.table_name)
        prepared_statement = self.session.prepare(query, keyspace=self.ks_name)
        prepared_statement_alternative = self.session.prepare(query, keyspace=self.alternative_ks)

        get_node(1).start(wait_for_binary_proto=True, wait_other_notice=True)

        # We wait for cluster._prepare_all_queries to be called
        time.sleep(5)
        self.assertEqual(1, mock_handler.get_message_count('debug', 'Preparing all known prepared statements'))
        results = self.session.execute(prepared_statement, (1,), execution_profile="only_first")
        self.assertEqual(results[0], (1, ))

        results = self.session.execute(prepared_statement_alternative, (2,), execution_profile="only_first")
        self.assertEqual(results[0], (2, ))

    def test_prepared_not_found(self):
        """
        Test to if a query fails on a node that didn't have
        the query prepared, it is re-prepared as expected and then
        the query is executed

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the query is executed and the results retrieved

        @test_category query
        """
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, allow_beta_protocol_version=True)
        session = self.cluster.connect("system")
        self.addCleanup(cluster.shutdown)

        cluster.prepare_on_all_hosts = False
        query = "SELECT k from {} WHERE k = ?".format(self.table_name)
        prepared_statement = session.prepare(query, keyspace=self.ks_name)

        for _ in range(10):
            results = session.execute(prepared_statement, (1, ))
            self.assertEqual(results[0], (1,))

    def test_prepared_in_query_keyspace(self):
        """
        Test to the the keyspace can be set in the query

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the results are retrieved correctly

        @test_category query
        """
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, allow_beta_protocol_version=True)
        session = self.cluster.connect()
        self.addCleanup(cluster.shutdown)

        query = "SELECT k from {}.{} WHERE k = ?".format(self.ks_name, self.table_name)
        prepared_statement = session.prepare(query)
        results = session.execute(prepared_statement, (1,))
        self.assertEqual(results[0], (1,))

        query = "SELECT k from {}.{} WHERE k = ?".format(self.alternative_ks, self.table_name)
        prepared_statement = session.prepare(query)
        results = session.execute(prepared_statement, (2,))
        self.assertEqual(results[0], (2,))

    def test_prepared_in_query_keyspace_and_explicit(self):
        """
        Test to the the keyspace set explicitly is ignored if it is
        specified as well in the query

        @since 3.12
        @jira_ticket PYTHON-678
        @expected_result the keyspace set explicitly is ignored and
        the results are retrieved correctly

        @test_category query
        """
        query = "SELECT k from {}.{} WHERE k = ?".format(self.ks_name, self.table_name)
        prepared_statement = self.session.prepare(query, keyspace="system")
        results = self.session.execute(prepared_statement, (1,))
        self.assertEqual(results[0], (1,))

        query = "SELECT k from {}.{} WHERE k = ?".format(self.ks_name, self.table_name)
        prepared_statement = self.session.prepare(query, keyspace=self.alternative_ks)
        results = self.session.execute(prepared_statement, (1,))
        self.assertEqual(results[0], (1,))
