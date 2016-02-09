# Copyright 2013-2016 DataStax, Inc.
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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from collections import deque
from mock import patch
import time
from uuid import uuid4

import cassandra, os
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.concurrent import execute_concurrent
from cassandra.policies import (RoundRobinPolicy, ExponentialReconnectionPolicy,
                                RetryPolicy, SimpleConvictionPolicy, HostDistance,
                                WhiteListRoundRobinPolicy)
from cassandra.protocol import MAX_SUPPORTED_VERSION
from cassandra.query import SimpleStatement, TraceUnavailable

from tests.integration import use_singledc, PROTOCOL_VERSION, get_server_versions, get_node, CASSANDRA_VERSION, execute_until_pass, execute_with_long_wait_retry, CONTACT_POINTS
from tests.integration.util import assert_quiescent_pool_state


def setup_module():
    use_singledc()


class ClusterTests(unittest.TestCase):

    def test_raise_error_on_control_connection_timeout(self):
        """
        Test for initial control connection timeout

        test_raise_error_on_control_connection_timeout tests that the driver times out after the set initial connection
        timeout. It first pauses node1, essentially making it unreachable. It then attempts to create a Cluster object
        via connecting to node1 with a timeout of 1 second, and ensures that a NoHostAvailable is raised, along with
        an OperationTimedOut for 1 second.

        @expected_errors NoHostAvailable When node1 is paused, and a connection attempt is made.
        @since 2.6.0
        @jira_ticket PYTHON-206
        @expected_result NoHostAvailable exception should be raised after 1 second.

        @test_category connection
        """

        get_node(1).pause()

        cluster = Cluster(contact_points=CONTACT_POINTS, protocol_version=PROTOCOL_VERSION, connect_timeout=1)

        with self.assertRaisesRegexp(NoHostAvailable, "OperationTimedOut\('errors=Timed out creating connection \(1 seconds\)"):
            cluster.connect()

        get_node(1).resume()

    def test_basic(self):
        """
        Test basic connection and usage
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()
        result = execute_until_pass(session,
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        self.assertFalse(result)

        result = execute_with_long_wait_retry(session,
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        self.assertFalse(result)

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        self.assertFalse(result)

        result = session.execute("SELECT * FROM clustertests.cf0")
        self.assertEqual([('a', 'b', 'c')], result)

        execute_with_long_wait_retry(session, "DROP KEYSPACE clustertests")

        cluster.shutdown()

    def test_protocol_negotiation(self):
        """
        Test for protocol negotiation

        test_protocol_negotiation tests that the driver will select the correct protocol version to match
        the correct cassandra version. Please note that 2.1.5 has a
        bug https://issues.apache.org/jira/browse/CASSANDRA-9451 that will cause this test to fail
        that will cause this to not pass. It was rectified in 2.1.6

        @since 2.6.0
        @jira_ticket PYTHON-240
        @expected_result the correct protocol version should be selected

        @test_category connection
        """

        cluster = Cluster(contact_points=CONTACT_POINTS)
        self.assertEqual(cluster.protocol_version,  MAX_SUPPORTED_VERSION)
        session = cluster.connect()
        updated_protocol_version = session._protocol_version
        updated_cluster_version = cluster.protocol_version
        # Make sure the correct protocol was selected by default
        if CASSANDRA_VERSION >= '2.2':
            self.assertEqual(updated_protocol_version, 4)
            self.assertEqual(updated_cluster_version, 4)
        elif CASSANDRA_VERSION >= '2.1':
            self.assertEqual(updated_protocol_version, 3)
            self.assertEqual(updated_cluster_version, 3)
        elif CASSANDRA_VERSION >= '2.0':
            self.assertEqual(updated_protocol_version, 2)
            self.assertEqual(updated_cluster_version, 2)
        else:
            self.assertEqual(updated_protocol_version, 1)
            self.assertEqual(updated_cluster_version, 1)

        cluster.shutdown()

    def test_connect_on_keyspace(self):
        """
        Ensure clusters that connect on a keyspace, do
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()
        result = session.execute(
            """
            INSERT INTO test3rf.test (k, v) VALUES (8889, 8889)
            """)
        self.assertFalse(result)

        result = session.execute("SELECT * FROM test3rf.test")
        self.assertEqual([(8889, 8889)], result)

        # test_connect_on_keyspace
        session2 = cluster.connect('test3rf')
        result2 = session2.execute("SELECT * FROM test")
        self.assertEqual(result, result2)
        cluster.shutdown()

    def test_set_keyspace_twice(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()
        session.execute("USE system")
        session.execute("USE system")
        cluster.shutdown()

    def test_default_connections(self):
        """
        Ensure errors are not thrown when using non-default policies
        """

        Cluster(
            load_balancing_policy=RoundRobinPolicy(),
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 600.0),
            default_retry_policy=RetryPolicy(),
            conviction_policy_factory=SimpleConvictionPolicy,
            protocol_version=PROTOCOL_VERSION,
            contact_points=CONTACT_POINTS
        )

    def test_connect_to_already_shutdown_cluster(self):
        """
        Ensure you cannot connect to a cluster that's been shutdown
        """
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        cluster.shutdown()
        self.assertRaises(Exception, cluster.connect)

    def test_auth_provider_is_callable(self):
        """
        Ensure that auth_providers are always callable
        """
        self.assertRaises(TypeError, Cluster, auth_provider=1, protocol_version=1)
        c = Cluster(protocol_version=1, contact_points=CONTACT_POINTS)
        self.assertRaises(TypeError, setattr, c, 'auth_provider', 1)

    def test_v2_auth_provider(self):
        """
        Check for v2 auth_provider compliance
        """
        bad_auth_provider = lambda x: {'username': 'foo', 'password': 'bar'}
        self.assertRaises(TypeError, Cluster, auth_provider=bad_auth_provider, protocol_version=2)
        c = Cluster(protocol_version=2, contact_points=CONTACT_POINTS)
        self.assertRaises(TypeError, setattr, c, 'auth_provider', bad_auth_provider)

    def test_conviction_policy_factory_is_callable(self):
        """
        Ensure that conviction_policy_factory are always callable
        """

        self.assertRaises(ValueError, Cluster, conviction_policy_factory=1)

    def test_connect_to_bad_hosts(self):
        """
        Ensure that a NoHostAvailable Exception is thrown
        when a cluster cannot connect to given hosts
        """

        if os.environ.get("IP") == "IPV6":
            cluster = Cluster(contact_points=['::9', '::10'],
                          protocol_version=PROTOCOL_VERSION)

        else:
            cluster = Cluster(contact_points=['127.1.2.9', '127.1.2.10'],
                              protocol_version=PROTOCOL_VERSION)

        self.assertRaises(NoHostAvailable, cluster.connect)

    def test_cluster_settings(self):
        """
        Test connection setting getters and setters
        """
        if PROTOCOL_VERSION >= 3:
            raise unittest.SkipTest("min/max requests and core/max conns aren't used with v3 protocol")

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)

        min_requests_per_connection = cluster.get_min_requests_per_connection(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MIN_REQUESTS, min_requests_per_connection)
        cluster.set_min_requests_per_connection(HostDistance.LOCAL, min_requests_per_connection + 1)
        self.assertEqual(cluster.get_min_requests_per_connection(HostDistance.LOCAL), min_requests_per_connection + 1)

        max_requests_per_connection = cluster.get_max_requests_per_connection(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MAX_REQUESTS, max_requests_per_connection)
        cluster.set_max_requests_per_connection(HostDistance.LOCAL, max_requests_per_connection + 1)
        self.assertEqual(cluster.get_max_requests_per_connection(HostDistance.LOCAL), max_requests_per_connection + 1)

        core_connections_per_host = cluster.get_core_connections_per_host(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST, core_connections_per_host)
        cluster.set_core_connections_per_host(HostDistance.LOCAL, core_connections_per_host + 1)
        self.assertEqual(cluster.get_core_connections_per_host(HostDistance.LOCAL), core_connections_per_host + 1)

        max_connections_per_host = cluster.get_max_connections_per_host(HostDistance.LOCAL)
        self.assertEqual(cassandra.cluster.DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST, max_connections_per_host)
        cluster.set_max_connections_per_host(HostDistance.LOCAL, max_connections_per_host + 1)
        self.assertEqual(cluster.get_max_connections_per_host(HostDistance.LOCAL), max_connections_per_host + 1)

    def test_refresh_schema(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        # full schema refresh, with wait
        cluster.refresh_schema_metadata()
        self.assertIsNot(original_meta, cluster.metadata.keyspaces)
        self.assertEqual(original_meta, cluster.metadata.keyspaces)

        cluster.shutdown()

    def test_refresh_schema_keyspace(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        original_system_meta = original_meta['system']

        # only refresh one keyspace
        cluster.refresh_keyspace_metadata('system')
        current_meta = cluster.metadata.keyspaces
        self.assertIs(original_meta, current_meta)
        current_system_meta = current_meta['system']
        self.assertIsNot(original_system_meta, current_system_meta)
        self.assertEqual(original_system_meta.as_cql_query(), current_system_meta.as_cql_query())
        cluster.shutdown()

    def test_refresh_schema_table(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        original_system_meta = original_meta['system']
        original_system_schema_meta = original_system_meta.tables['local']

        # only refresh one table
        cluster.refresh_table_metadata('system', 'local')
        current_meta = cluster.metadata.keyspaces
        current_system_meta = current_meta['system']
        current_system_schema_meta = current_system_meta.tables['local']
        self.assertIs(original_meta, current_meta)
        self.assertIs(original_system_meta, current_system_meta)
        self.assertIsNot(original_system_schema_meta, current_system_schema_meta)
        self.assertEqual(original_system_schema_meta.as_cql_query(), current_system_schema_meta.as_cql_query())
        cluster.shutdown()

    def test_refresh_schema_type(self):
        if get_server_versions()[0] < (2, 1, 0):
            raise unittest.SkipTest('UDTs were introduced in Cassandra 2.1')

        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest('UDTs are not specified in change events for protocol v2')
            # We may want to refresh types on keyspace change events in that case(?)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        keyspace_name = 'test1rf'
        type_name = self._testMethodName

        execute_until_pass(session, 'CREATE TYPE IF NOT EXISTS %s.%s (one int, two text)' % (keyspace_name, type_name))
        original_meta = cluster.metadata.keyspaces
        original_test1rf_meta = original_meta[keyspace_name]
        original_type_meta = original_test1rf_meta.user_types[type_name]

        # only refresh one type
        cluster.refresh_user_type_metadata('test1rf', type_name)
        current_meta = cluster.metadata.keyspaces
        current_test1rf_meta = current_meta[keyspace_name]
        current_type_meta = current_test1rf_meta.user_types[type_name]
        self.assertIs(original_meta, current_meta)
        self.assertEqual(original_test1rf_meta.export_as_string(), current_test1rf_meta.export_as_string())
        self.assertIsNot(original_type_meta, current_type_meta)
        self.assertEqual(original_type_meta.as_cql_query(), current_type_meta.as_cql_query())
        session.shutdown()

    def test_refresh_schema_no_wait(self):

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, max_schema_agreement_wait=10,
                          contact_points=CONTACT_POINTS, load_balancing_policy=WhiteListRoundRobinPolicy(CONTACT_POINTS))
        session = cluster.connect()

        schema_ver = session.execute("SELECT schema_version FROM system.local WHERE key='local'")[0][0]
        new_schema_ver = uuid4()
        session.execute("UPDATE system.local SET schema_version=%s WHERE key='local'", (new_schema_ver,))

        try:
            agreement_timeout = 1

            # cluster agreement wait exceeded
            c = Cluster(protocol_version=PROTOCOL_VERSION, max_schema_agreement_wait=agreement_timeout, contact_points=CONTACT_POINTS)
            c.connect()
            self.assertTrue(c.metadata.keyspaces)

            # cluster agreement wait used for refresh
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            self.assertRaisesRegexp(Exception, r"Schema metadata was not refreshed.*", c.refresh_schema_metadata)
            end_time = time.time()
            self.assertGreaterEqual(end_time - start_time, agreement_timeout)
            self.assertIs(original_meta, c.metadata.keyspaces)
            
            # refresh wait overrides cluster value
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            c.refresh_schema_metadata(max_schema_agreement_wait=0)
            end_time = time.time()
            self.assertLess(end_time - start_time, agreement_timeout)
            self.assertIsNot(original_meta, c.metadata.keyspaces)
            self.assertEqual(original_meta, c.metadata.keyspaces)

            c.shutdown()

            refresh_threshold = 0.5
            # cluster agreement bypass
            c = Cluster(protocol_version=PROTOCOL_VERSION, max_schema_agreement_wait=0, contact_points=CONTACT_POINTS)
            start_time = time.time()
            s = c.connect()
            end_time = time.time()
            self.assertLess(end_time - start_time, refresh_threshold)
            self.assertTrue(c.metadata.keyspaces)

            # cluster agreement wait used for refresh
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            c.refresh_schema_metadata()
            end_time = time.time()
            self.assertLess(end_time - start_time, refresh_threshold)
            self.assertIsNot(original_meta, c.metadata.keyspaces)
            self.assertEqual(original_meta, c.metadata.keyspaces)
            
            # refresh wait overrides cluster value
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            self.assertRaisesRegexp(Exception, r"Schema metadata was not refreshed.*", c.refresh_schema_metadata,
                                    max_schema_agreement_wait=agreement_timeout)
            end_time = time.time()
            self.assertGreaterEqual(end_time - start_time, agreement_timeout)
            self.assertIs(original_meta, c.metadata.keyspaces)
            c.shutdown()
        finally:
            # TODO once fixed this connect call
            session = cluster.connect()
            session.execute("UPDATE system.local SET schema_version=%s WHERE key='local'", (schema_ver,))

        cluster.shutdown()

    def test_trace(self):
        """
        Ensure trace can be requested for async and non-async queries
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        def check_trace(trace):
            self.assertIsNotNone(trace.request_type)
            self.assertIsNotNone(trace.duration)
            self.assertIsNotNone(trace.started_at)
            self.assertIsNotNone(trace.coordinator)
            self.assertIsNotNone(trace.events)

        result = session.execute( "SELECT * FROM system.local", trace=True)
        check_trace(result.get_query_trace())

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        result = session.execute(statement, trace=True)
        check_trace(result.get_query_trace())

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        result = session.execute(statement)
        self.assertIsNone(result.get_query_trace())

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2, trace=True)
        future.result()
        check_trace(future.get_query_trace())

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2)
        future.result()
        self.assertIsNone(future.get_query_trace())

        prepared = session.prepare("SELECT * FROM system.local")
        future = session.execute_async(prepared, parameters=(), trace=True)
        future.result()
        check_trace(future.get_query_trace())
        cluster.shutdown()

    def test_trace_timeout(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        future = session.execute_async(statement, trace=True)
        future.result()
        self.assertRaises(TraceUnavailable, future.get_query_trace, -1.0)
        cluster.shutdown()

    def test_string_coverage(self):
        """
        Ensure str(future) returns without error
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        future = session.execute_async(statement)

        self.assertIn(query, str(future))
        future.result()

        self.assertIn(query, str(future))
        self.assertIn('result', str(future))
        cluster.shutdown()

    def test_idle_heartbeat(self):
        interval = 2
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, idle_heartbeat_interval=interval, contact_points=CONTACT_POINTS)
        if PROTOCOL_VERSION < 3:
            cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        session = cluster.connect()

        # This test relies on impl details of connection req id management to see if heartbeats 
        # are being sent. May need update if impl is changed
        connection_request_ids = {}
        for h in cluster.get_connection_holders():
            for c in h.get_connections():
                # make sure none are idle (should have startup messages)
                self.assertFalse(c.is_idle)
                with c.lock:
                    connection_request_ids[id(c)] = deque(c.request_ids)  # copy of request ids

        # let two heatbeat intervals pass (first one had startup messages in it)
        time.sleep(2 * interval + interval/2)

        connections = [c for holders in cluster.get_connection_holders() for c in holders.get_connections()]

        # make sure requests were sent on all connections
        for c in connections:
            expected_ids = connection_request_ids[id(c)]
            expected_ids.rotate(-1)
            with c.lock:
                self.assertListEqual(list(c.request_ids), list(expected_ids))

        # assert idle status
        self.assertTrue(all(c.is_idle for c in connections))

        # send messages on all connections
        statements_and_params = [("SELECT release_version FROM system.local", ())] * len(cluster.metadata.all_hosts())
        results = execute_concurrent(session, statements_and_params)
        for success, result in results:
            self.assertTrue(success)

        # assert not idle status
        self.assertFalse(any(c.is_idle if not c.is_control_connection else False for c in connections))

        # holders include session pools and cc
        holders = cluster.get_connection_holders()
        self.assertIn(cluster.control_connection, holders)
        self.assertEqual(len(holders), len(cluster.metadata.all_hosts()) + 1)  # hosts pools, 1 for cc

        # include additional sessions
        session2 = cluster.connect()

        holders = cluster.get_connection_holders()
        self.assertIn(cluster.control_connection, holders)
        self.assertEqual(len(holders), 2 * len(cluster.metadata.all_hosts()) + 1)  # 2 sessions' hosts pools, 1 for cc

        cluster._idle_heartbeat.stop()
        cluster._idle_heartbeat.join()
        assert_quiescent_pool_state(self, cluster)

        cluster.shutdown()

    @patch('cassandra.cluster.Cluster.idle_heartbeat_interval', new=0.1)
    def test_idle_heartbeat_disabled(self):
        self.assertTrue(Cluster.idle_heartbeat_interval)

        # heartbeat disabled with '0'
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, idle_heartbeat_interval=0, contact_points=CONTACT_POINTS)
        self.assertEqual(cluster.idle_heartbeat_interval, 0)
        session = cluster.connect()

        # let two heatbeat intervals pass (first one had startup messages in it)
        time.sleep(2 * Cluster.idle_heartbeat_interval)

        connections = [c for holders in cluster.get_connection_holders() for c in holders.get_connections()]

        # assert not idle status (should never get reset because there is not heartbeat)
        self.assertFalse(any(c.is_idle for c in connections))

        cluster.shutdown()

    def test_pool_management(self):
        # Ensure that in_flight and request_ids quiesce after cluster operations
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, idle_heartbeat_interval=0, contact_points=CONTACT_POINTS)  # no idle heartbeat here, pool management is tested in test_idle_heartbeat
        session = cluster.connect()
        session2 = cluster.connect()

        # prepare
        p = session.prepare("SELECT * FROM system.local WHERE key=?")
        self.assertTrue(session.execute(p, ('local',)))

        # simple
        self.assertTrue(session.execute("SELECT * FROM system.local WHERE key='local'"))

        # set keyspace
        session.set_keyspace('system')
        session.set_keyspace('system_traces')

        # use keyspace
        session.execute('USE system')
        session.execute('USE system_traces')

        # refresh schema
        cluster.refresh_schema_metadata()
        cluster.refresh_schema_metadata(max_schema_agreement_wait=0)

        assert_quiescent_pool_state(self, cluster)

        cluster.shutdown()


