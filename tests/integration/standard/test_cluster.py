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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from collections import deque
from copy import copy
from mock import Mock, call, patch
import time
from uuid import uuid4
import logging

import cassandra
from cassandra.cluster import Cluster, Session, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent
from cassandra.policies import (RoundRobinPolicy, ExponentialReconnectionPolicy,
                                RetryPolicy, SimpleConvictionPolicy, HostDistance,
                                AddressTranslator, TokenAwarePolicy, HostFilterPolicy)
from cassandra import ConsistencyLevel

from cassandra.pool import Host
from cassandra.query import SimpleStatement, TraceUnavailable, tuple_factory

from tests import notwindows
from tests.integration import use_singledc, PROTOCOL_VERSION, get_server_versions, CASSANDRA_VERSION, \
    execute_until_pass, execute_with_long_wait_retry, get_node, MockLoggingHandler, get_unsupported_lower_protocol, \
    get_unsupported_upper_protocol, protocolv5, local, CASSANDRA_IP
from tests.integration.util import assert_quiescent_pool_state
import sys


def setup_module():
    use_singledc()


class IgnoredHostPolicy(RoundRobinPolicy):

    def __init__(self, ignored_hosts):
        self.ignored_hosts = ignored_hosts
        RoundRobinPolicy.__init__(self)

    def distance(self, host):
        if(str(host) in self.ignored_hosts):
            return HostDistance.IGNORED
        else:
            return HostDistance.LOCAL


class ClusterTests(unittest.TestCase):
    @local
    def test_ignored_host_up(self):
        """
        Test to ensure that is_up is not set by default on ignored hosts

        @since 3.6
        @jira_ticket PYTHON-551
        @expected_result ignored hosts should have None set for is_up

        @test_category connection
        """
        ingored_host_policy = IgnoredHostPolicy(["127.0.0.2", "127.0.0.3"])
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, load_balancing_policy=ingored_host_policy)
        session = cluster.connect()
        for host in cluster.metadata.all_hosts():
            if str(host) == "127.0.0.1":
                self.assertTrue(host.is_up)
            else:
                self.assertIsNone(host.is_up)
        cluster.shutdown()

    @local
    def test_host_resolution(self):
        """
        Test to insure A records are resolved appropriately.

        @since 3.3
        @jira_ticket PYTHON-415
        @expected_result hostname will be transformed into IP

        @test_category connection
        """
        cluster = Cluster(contact_points=["localhost"], protocol_version=PROTOCOL_VERSION, connect_timeout=1)
        self.assertTrue('127.0.0.1' in cluster.contact_points_resolved)

    @local
    def test_host_duplication(self):
        """
        Ensure that duplicate hosts in the contact points are surfaced in the cluster metadata

        @since 3.3
        @jira_ticket PYTHON-103
        @expected_result duplicate hosts aren't surfaced in cluster.metadata

        @test_category connection
        """
        cluster = Cluster(contact_points=["localhost", "127.0.0.1", "localhost", "localhost", "localhost"], protocol_version=PROTOCOL_VERSION, connect_timeout=1)
        cluster.connect(wait_for_all_pools=True)
        self.assertEqual(len(cluster.metadata.all_hosts()), 3)
        cluster.shutdown()
        cluster = Cluster(contact_points=["127.0.0.1", "localhost"], protocol_version=PROTOCOL_VERSION, connect_timeout=1)
        cluster.connect(wait_for_all_pools=True)
        self.assertEqual(len(cluster.metadata.all_hosts()), 3)
        cluster.shutdown()

    @local
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
        cluster = Cluster(contact_points=['127.0.0.1'], protocol_version=PROTOCOL_VERSION, connect_timeout=1)

        with self.assertRaisesRegexp(NoHostAvailable, "OperationTimedOut\('errors=Timed out creating connection \(1 seconds\)"):
            cluster.connect()
        cluster.shutdown()

        get_node(1).resume()

    def test_basic(self):
        """
        Test basic connection and usage
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

    def test_session_host_parameter(self):
        """
        Test for protocol negotiation

        Very that NoHostAvailable is risen in Session.__init__ when there are no valid connections and that
        no error is arisen otherwise, despite maybe being some invalid hosts

        @since 3.9
        @jira_ticket PYTHON-665
        @expected_result NoHostAvailable when the driver is unable to connect to a valid host,
        no exception otherwise

        @test_category connection
        """
        # Test with empty list
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        with self.assertRaises(NoHostAvailable):
            Session(cluster, [])
        cluster.shutdown()

        # Test with only invalid
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        with self.assertRaises(NoHostAvailable):
            Session(cluster, [Host("1.2.3.4", SimpleConvictionPolicy)])
        cluster.shutdown()

        # Test with valid and invalid hosts
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        Session(cluster, [Host(x, SimpleConvictionPolicy) for x in
                                      ("127.0.0.1", "127.0.0.2", "1.2.3.4")])
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

        cluster = Cluster()
        self.assertLessEqual(cluster.protocol_version,  cassandra.ProtocolVersion.MAX_SUPPORTED)
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

    def test_invalid_protocol_negotation(self):
        """
        Test for protocol negotiation when explicit versions are set

        If an explicit protocol version that is not compatible with the server version is set
        an exception should be thrown. It should not attempt to negotiate

        for reference supported protocol version to server versions is as follows/

        1.2 -> 1
        2.0 -> 2, 1
        2.1 -> 3, 2, 1
        2.2 -> 4, 3, 2, 1
        3.X -> 4, 3

        @since 3.6.0
        @jira_ticket PYTHON-537
        @expected_result downgrading should not be allowed when explicit protocol versions are set.

        @test_category connection
        """

        upper_bound = get_unsupported_upper_protocol()
        if upper_bound is not None:
            cluster = Cluster(protocol_version=upper_bound)
            with self.assertRaises(NoHostAvailable):
                cluster.connect()
            cluster.shutdown()

        lower_bound = get_unsupported_lower_protocol()
        if lower_bound is not None:
            cluster = Cluster(protocol_version=lower_bound)
            with self.assertRaises(NoHostAvailable):
                cluster.connect()
            cluster.shutdown()

    def test_connect_on_keyspace(self):
        """
        Ensure clusters that connect on a keyspace, do
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        result = session.execute(
            """
            INSERT INTO test1rf.test (k, v) VALUES (8889, 8889)
            """)
        self.assertFalse(result)

        result = session.execute("SELECT * FROM test1rf.test")
        self.assertEqual([(8889, 8889)], result, "Rows in ResultSet are {0}".format(result.current_rows))

        # test_connect_on_keyspace
        session2 = cluster.connect('test1rf')
        result2 = session2.execute("SELECT * FROM test")
        self.assertEqual(result, result2)
        cluster.shutdown()

    def test_set_keyspace_twice(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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
            protocol_version=PROTOCOL_VERSION
        )

    def test_connect_to_already_shutdown_cluster(self):
        """
        Ensure you cannot connect to a cluster that's been shutdown
        """
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.shutdown()
        self.assertRaises(Exception, cluster.connect)

    def test_auth_provider_is_callable(self):
        """
        Ensure that auth_providers are always callable
        """
        self.assertRaises(TypeError, Cluster, auth_provider=1, protocol_version=1)
        c = Cluster(protocol_version=1)
        self.assertRaises(TypeError, setattr, c, 'auth_provider', 1)

    def test_v2_auth_provider(self):
        """
        Check for v2 auth_provider compliance
        """
        bad_auth_provider = lambda x: {'username': 'foo', 'password': 'bar'}
        self.assertRaises(TypeError, Cluster, auth_provider=bad_auth_provider, protocol_version=2)
        c = Cluster(protocol_version=2)
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

        cluster = Cluster(['127.1.2.9', '127.1.2.10'],
                          protocol_version=PROTOCOL_VERSION)
        self.assertRaises(NoHostAvailable, cluster.connect)

    def test_cluster_settings(self):
        """
        Test connection setting getters and setters
        """
        if PROTOCOL_VERSION >= 3:
            raise unittest.SkipTest("min/max requests and core/max conns aren't used with v3 protocol")

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)

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
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        # full schema refresh, with wait
        cluster.refresh_schema_metadata()
        self.assertIsNot(original_meta, cluster.metadata.keyspaces)
        self.assertEqual(original_meta, cluster.metadata.keyspaces)

        cluster.shutdown()

    def test_refresh_schema_keyspace(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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
        cluster.shutdown()

    @local
    @notwindows
    def test_refresh_schema_no_wait(self):
        contact_points = [CASSANDRA_IP]
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, max_schema_agreement_wait=10,
                          contact_points=contact_points,
                          load_balancing_policy=HostFilterPolicy(
                              RoundRobinPolicy(), lambda host: host.address == CASSANDRA_IP
                          ))
        session = cluster.connect()

        schema_ver = session.execute("SELECT schema_version FROM system.local WHERE key='local'")[0][0]
        new_schema_ver = uuid4()
        session.execute("UPDATE system.local SET schema_version=%s WHERE key='local'", (new_schema_ver,))


        try:
            agreement_timeout = 1

            # cluster agreement wait exceeded
            c = Cluster(protocol_version=PROTOCOL_VERSION, max_schema_agreement_wait=agreement_timeout)
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
            c = Cluster(protocol_version=PROTOCOL_VERSION, max_schema_agreement_wait=0)
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

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        result = session.execute( "SELECT * FROM system.local", trace=True)
        self._check_trace(result.get_query_trace())

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        result = session.execute(statement, trace=True)
        self._check_trace(result.get_query_trace())

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        result = session.execute(statement)
        self.assertIsNone(result.get_query_trace())

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2, trace=True)
        future.result()
        self._check_trace(future.get_query_trace())

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2)
        future.result()
        self.assertIsNone(future.get_query_trace())

        prepared = session.prepare("SELECT * FROM system.local")
        future = session.execute_async(prepared, parameters=(), trace=True)
        future.result()
        self._check_trace(future.get_query_trace())
        cluster.shutdown()

    def test_trace_unavailable(self):
        """
        First checks that TraceUnavailable is arisen if the
        max_wait parameter is negative

        Then checks that TraceUnavailable is arisen if the
        result hasn't been set yet

        @since 3.10
        @jira_ticket PYTHON-196
        @expected_result TraceUnavailable is arisen in both cases

        @test_category query
                """
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.addCleanup(cluster.shutdown)
        session = cluster.connect()

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)

        max_retry_count = 10
        for i in range(max_retry_count):
            future = session.execute_async(statement, trace=True)
            future.result()
            try:
                result = future.get_query_trace(-1.0)
                # In case the result has time to come back before this timeout due to a race condition
                self._check_trace(result)
            except TraceUnavailable:
                break
        else:
            raise Exception("get_query_trace didn't raise TraceUnavailable after {} tries".format(max_retry_count))


        for i in range(max_retry_count):
            future = session.execute_async(statement, trace=True)
            try:
                result = future.get_query_trace(max_wait=120)
                # In case the result has been set check the trace
                self._check_trace(result)
            except TraceUnavailable:
                break
        else:
            raise Exception("get_query_trace didn't raise TraceUnavailable after {} tries".format(max_retry_count))

    def test_string_coverage(self):
        """
        Ensure str(future) returns without error
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, idle_heartbeat_interval=interval)
        if PROTOCOL_VERSION < 3:
            cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        session = cluster.connect(wait_for_all_pools=True)

        # This test relies on impl details of connection req id management to see if heartbeats
        # are being sent. May need update if impl is changed
        connection_request_ids = {}
        for h in cluster.get_connection_holders():
            for c in h.get_connections():
                # make sure none are idle (should have startup messages
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
        session2 = cluster.connect(wait_for_all_pools=True)

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
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, idle_heartbeat_interval=0)
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
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, idle_heartbeat_interval=0)  # no idle heartbeat here, pool management is tested in test_idle_heartbeat
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

    @local
    def test_profile_load_balancing(self):
        """
        Tests that profile load balancing policies are honored.

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result Execution Policy should be used when applicable.

        @test_category config_profiles
        """
        query = "select release_version from system.local"
        node1 = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(
                RoundRobinPolicy(), lambda host: host.address == CASSANDRA_IP
            )
        )
        with Cluster(execution_profiles={'node1': node1}) as cluster:
            session = cluster.connect(wait_for_all_pools=True)

            # default is DCA RR for all hosts
            expected_hosts = set(cluster.metadata.all_hosts())
            queried_hosts = set()
            for _ in expected_hosts:
                rs = session.execute(query)
                queried_hosts.add(rs.response_future._current_host)
            self.assertEqual(queried_hosts, expected_hosts)

            # by name we should only hit the one
            expected_hosts = set(h for h in cluster.metadata.all_hosts() if h.address == CASSANDRA_IP)
            queried_hosts = set()
            for _ in cluster.metadata.all_hosts():
                rs = session.execute(query, execution_profile='node1')
                queried_hosts.add(rs.response_future._current_host)
            self.assertEqual(queried_hosts, expected_hosts)

            # use a copied instance and override the row factory
            # assert last returned value can be accessed as a namedtuple so we can prove something different
            named_tuple_row = rs[0]
            self.assertIsInstance(named_tuple_row, tuple)
            self.assertTrue(named_tuple_row.release_version)

            tmp_profile = copy(node1)
            tmp_profile.row_factory = tuple_factory
            queried_hosts = set()
            for _ in cluster.metadata.all_hosts():
                rs = session.execute(query, execution_profile=tmp_profile)
                queried_hosts.add(rs.response_future._current_host)
            self.assertEqual(queried_hosts, expected_hosts)
            tuple_row = rs[0]
            self.assertIsInstance(tuple_row, tuple)
            with self.assertRaises(AttributeError):
                tuple_row.release_version

            # make sure original profile is not impacted
            self.assertTrue(session.execute(query, execution_profile='node1')[0].release_version)

    def test_setting_lbp_legacy(self):
        cluster = Cluster()
        self.addCleanup(cluster.shutdown)
        cluster.load_balancing_policy = RoundRobinPolicy()
        self.assertEqual(
            list(cluster.load_balancing_policy.make_query_plan()), []
        )
        cluster.connect()
        self.assertNotEqual(
            list(cluster.load_balancing_policy.make_query_plan()), []
        )

    def test_profile_lb_swap(self):
        """
        Tests that profile load balancing policies are not shared

        Creates two LBP, runs a few queries, and validates that each LBP is execised
        seperately between EP's

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result LBP should not be shared.

        @test_category config_profiles
        """
        query = "select release_version from system.local"
        rr1 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        rr2 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        exec_profiles = {'rr1': rr1, 'rr2': rr2}
        with Cluster(execution_profiles=exec_profiles) as cluster:
            session = cluster.connect(wait_for_all_pools=True)

            # default is DCA RR for all hosts
            expected_hosts = set(cluster.metadata.all_hosts())
            rr1_queried_hosts = set()
            rr2_queried_hosts = set()

            rs = session.execute(query, execution_profile='rr1')
            rr1_queried_hosts.add(rs.response_future._current_host)
            rs = session.execute(query, execution_profile='rr2')
            rr2_queried_hosts.add(rs.response_future._current_host)

            self.assertEqual(rr2_queried_hosts, rr1_queried_hosts)

    def test_ta_lbp(self):
        """
        Test that execution profiles containing token aware LBP can be added

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result Queries can run

        @test_category config_profiles
        """
        query = "select release_version from system.local"
        ta1 = ExecutionProfile()
        with Cluster() as cluster:
            session = cluster.connect()
            cluster.add_execution_profile("ta1", ta1)
            rs = session.execute(query, execution_profile='ta1')

    def test_clone_shared_lbp(self):
        """
        Tests that profile load balancing policies are shared on clone

        Creates one LBP clones it, and ensures that the LBP is shared between
        the two EP's

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result LBP is shared

        @test_category config_profiles
        """
        query = "select release_version from system.local"
        rr1 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        exec_profiles = {'rr1': rr1}
        with Cluster(execution_profiles=exec_profiles) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            self.assertGreater(len(cluster.metadata.all_hosts()), 1, "We only have one host connected at this point")

            rr1_clone = session.execution_profile_clone_update('rr1', row_factory=tuple_factory)
            cluster.add_execution_profile("rr1_clone", rr1_clone)
            rr1_queried_hosts = set()
            rr1_clone_queried_hosts = set()
            rs = session.execute(query, execution_profile='rr1')
            rr1_queried_hosts.add(rs.response_future._current_host)
            rs = session.execute(query, execution_profile='rr1_clone')
            rr1_clone_queried_hosts.add(rs.response_future._current_host)
            self.assertNotEqual(rr1_clone_queried_hosts, rr1_queried_hosts)

    def test_missing_exec_prof(self):
        """
        Tests to verify that using an unknown profile raises a ValueError

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result ValueError

        @test_category config_profiles
        """
        query = "select release_version from system.local"
        rr1 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        rr2 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        exec_profiles = {'rr1': rr1, 'rr2': rr2}
        with Cluster(execution_profiles=exec_profiles) as cluster:
            session = cluster.connect()
            with self.assertRaises(ValueError):
                session.execute(query, execution_profile='rr3')

    @local
    def test_profile_pool_management(self):
        """
        Tests that changes to execution profiles correctly impact our cluster's pooling

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result pools should be correctly updated as EP's are added and removed

        @test_category config_profiles
        """

        node1 = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(
                RoundRobinPolicy(), lambda host: host.address == "127.0.0.1"
            )
        )
        node2 = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(
                RoundRobinPolicy(), lambda host: host.address == "127.0.0.2"
            )
        )
        with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: node1, 'node2': node2}) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            pools = session.get_pool_state()
            # there are more hosts, but we connected to the ones in the lbp aggregate
            self.assertGreater(len(cluster.metadata.all_hosts()), 2)
            self.assertEqual(set(h.address for h in pools), set(('127.0.0.1', '127.0.0.2')))

            # dynamically update pools on add
            node3 = ExecutionProfile(
                load_balancing_policy=HostFilterPolicy(
                    RoundRobinPolicy(), lambda host: host.address == "127.0.0.3"
                )
            )
            cluster.add_execution_profile('node3', node3)
            pools = session.get_pool_state()
            self.assertEqual(set(h.address for h in pools), set(('127.0.0.1', '127.0.0.2', '127.0.0.3')))

    @local
    def test_add_profile_timeout(self):
        """
        Tests that EP Timeouts are honored.

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result EP timeouts should override defaults

        @test_category config_profiles
        """
        max_retry_count = 10
        for i in range(max_retry_count):
            node1 = ExecutionProfile(
                load_balancing_policy=HostFilterPolicy(
                    RoundRobinPolicy(), lambda host: host.address == "127.0.0.1"
                )
            )
            with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: node1}) as cluster:
                session = cluster.connect(wait_for_all_pools=True)
                pools = session.get_pool_state()
                self.assertGreater(len(cluster.metadata.all_hosts()), 2)
                self.assertEqual(set(h.address for h in pools), set(('127.0.0.1',)))

                node2 = ExecutionProfile(
                    load_balancing_policy=HostFilterPolicy(
                        RoundRobinPolicy(), lambda host: host.address in ["127.0.0.2", "127.0.0.3"]
                    )
                )

                start = time.time()
                try:
                    self.assertRaises(cassandra.OperationTimedOut, cluster.add_execution_profile,
                                      'profile_{0}'.format(i),
                                      node2, pool_wait_timeout=sys.float_info.min)
                    break
                except AssertionError:
                    end = time.time()
                    self.assertAlmostEqual(start, end, 1)
        else:
            raise Exception("add_execution_profile didn't timeout after {0} retries".format(max_retry_count))

    def test_replicas_are_queried(self):
        """
        Test that replicas are queried first for TokenAwarePolicy. A table with RF 1
        is created. All the queries should go to that replica when TokenAwarePolicy
        is used.
        Then using HostFilterPolicy the replica is excluded from the considered hosts.
        By checking the trace we verify that there are no more replicas.

        @since 3.5
        @jira_ticket PYTHON-653
        @expected_result the replicas are queried for HostFilterPolicy

        @test_category metadata
        """
        queried_hosts = set()
        with Cluster(protocol_version=PROTOCOL_VERSION,
                     load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy())) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            session.execute('''
                    CREATE TABLE test1rf.table_with_big_key (
                        k1 int,
                        k2 int,
                        k3 int,
                        k4 int,
                        PRIMARY KEY((k1, k2, k3), k4))''')
            prepared = session.prepare("""SELECT * from test1rf.table_with_big_key
                                          WHERE k1 = ? AND k2 = ? AND k3 = ? AND k4 = ?""")
            for i in range(10):
                result = session.execute(prepared, (i, i, i, i), trace=True)
                trace = result.response_future.get_query_trace(query_cl=ConsistencyLevel.ALL)
                queried_hosts = self._assert_replica_queried(trace, only_replicas=True)
                last_i = i

        only_replica = queried_hosts.pop()
        log = logging.getLogger(__name__)
        log.info("The only replica found was: {}".format(only_replica))
        available_hosts = [host for host in ["127.0.0.1", "127.0.0.2", "127.0.0.3"] if host != only_replica]
        with Cluster(contact_points=available_hosts,
                     protocol_version=PROTOCOL_VERSION,
                     load_balancing_policy=HostFilterPolicy(RoundRobinPolicy(),
                     predicate=lambda host: host.address != only_replica)) as cluster:

            session = cluster.connect(wait_for_all_pools=True)
            prepared = session.prepare("""SELECT * from test1rf.table_with_big_key
                                          WHERE k1 = ? AND k2 = ? AND k3 = ? AND k4 = ?""")
            for _ in range(10):
                result = session.execute(prepared, (last_i, last_i, last_i, last_i), trace=True)
                trace = result.response_future.get_query_trace(query_cl=ConsistencyLevel.ALL)
                self._assert_replica_queried(trace, only_replicas=False)

            session.execute('''DROP TABLE test1rf.table_with_big_key''')

    def _assert_replica_queried(self, trace, only_replicas=True):
        queried_hosts = set()
        for row in trace.events:
            queried_hosts.add(row.source)
        if only_replicas:
            self.assertEqual(len(queried_hosts), 1, "The hosts queried where {}".format(queried_hosts))
        else:
            self.assertGreater(len(queried_hosts), 1, "The host queried was {}".format(queried_hosts))
        return queried_hosts

    def _check_trace(self, trace):
        self.assertIsNotNone(trace.request_type)
        self.assertIsNotNone(trace.duration)
        self.assertIsNotNone(trace.started_at)
        self.assertIsNotNone(trace.coordinator)
        self.assertIsNotNone(trace.events)


class LocalHostAdressTranslator(AddressTranslator):

    def __init__(self, addr_map=None):
        self.addr_map = addr_map

    def translate(self, addr):
        new_addr = self.addr_map.get(addr)
        return new_addr

@local
class TestAddressTranslation(unittest.TestCase):

    def test_address_translator_basic(self):
        """
        Test host address translation

        Uses a custom Address Translator to map all ip back to one.
        Validates AddressTranslator invocation by ensuring that only meta data associated with single
        host is populated

        @since 3.3
        @jira_ticket PYTHON-69
        @expected_result only one hosts' metadata will be populated

        @test_category metadata
        """
        lh_ad = LocalHostAdressTranslator({'127.0.0.1': '127.0.0.1', '127.0.0.2': '127.0.0.1', '127.0.0.3': '127.0.0.1'})
        c = Cluster(address_translator=lh_ad)
        c.connect()
        self.assertEqual(len(c.metadata.all_hosts()), 1)
        c.shutdown()

    def test_address_translator_with_mixed_nodes(self):
        """
        Test host address translation

        Uses a custom Address Translator to map ip's of non control_connection nodes to each other
        Validates AddressTranslator invocation by ensuring that metadata for mapped hosts is also mapped

        @since 3.3
        @jira_ticket PYTHON-69
        @expected_result metadata for crossed hosts will also be crossed

        @test_category metadata
        """
        adder_map = {'127.0.0.1': '127.0.0.1', '127.0.0.2': '127.0.0.3', '127.0.0.3': '127.0.0.2'}
        lh_ad = LocalHostAdressTranslator(adder_map)
        c = Cluster(address_translator=lh_ad)
        c.connect()
        for host in c.metadata.all_hosts():
            self.assertEqual(adder_map.get(str(host)), host.broadcast_address)
        c.shutdown()

@local
class ContextManagementTest(unittest.TestCase):
    load_balancing_policy = HostFilterPolicy(
        RoundRobinPolicy(), lambda host: host.address == CASSANDRA_IP
    )
    cluster_kwargs = {'execution_profiles': {EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=
                                                                                    load_balancing_policy)},
                      'schema_metadata_enabled': False,
                      'token_metadata_enabled': False}

    def test_no_connect(self):
        """
        Test cluster context without connecting.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result context should still be valid

        @test_category configuration
        """
        with Cluster() as cluster:
            self.assertFalse(cluster.is_shutdown)
        self.assertTrue(cluster.is_shutdown)

    def test_simple_nested(self):
        """
        Test cluster and session contexts nested in one another.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result cluster/session should be crated and shutdown appropriately.

        @test_category configuration
        """
        with Cluster(**self.cluster_kwargs) as cluster:
            with cluster.connect() as session:
                self.assertFalse(cluster.is_shutdown)
                self.assertFalse(session.is_shutdown)
                self.assertTrue(session.execute('select release_version from system.local')[0])
            self.assertTrue(session.is_shutdown)
        self.assertTrue(cluster.is_shutdown)

    def test_cluster_no_session(self):
        """
        Test cluster context without session context.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result Session should be created correctly. Cluster should shutdown outside of context

        @test_category configuration
        """
        with Cluster(**self.cluster_kwargs) as cluster:
            session = cluster.connect()
            self.assertFalse(cluster.is_shutdown)
            self.assertFalse(session.is_shutdown)
            self.assertTrue(session.execute('select release_version from system.local')[0])
        self.assertTrue(session.is_shutdown)
        self.assertTrue(cluster.is_shutdown)

    def test_session_no_cluster(self):
        """
        Test session context without cluster context.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result session should be created correctly. Session should shutdown correctly outside of context

        @test_category configuration
        """
        cluster = Cluster(**self.cluster_kwargs)
        unmanaged_session = cluster.connect()
        with cluster.connect() as session:
            self.assertFalse(cluster.is_shutdown)
            self.assertFalse(session.is_shutdown)
            self.assertFalse(unmanaged_session.is_shutdown)
            self.assertTrue(session.execute('select release_version from system.local')[0])
        self.assertTrue(session.is_shutdown)
        self.assertFalse(cluster.is_shutdown)
        self.assertFalse(unmanaged_session.is_shutdown)
        unmanaged_session.shutdown()
        self.assertTrue(unmanaged_session.is_shutdown)
        self.assertFalse(cluster.is_shutdown)
        cluster.shutdown()
        self.assertTrue(cluster.is_shutdown)


class HostStateTest(unittest.TestCase):

    def test_down_event_with_active_connection(self):
        """
        Test to ensure that on down calls to clusters with connections still active don't result in
        a host being marked down. The second part of the test kills the connection then invokes
        on_down, and ensures the state changes for host's metadata.

        @since 3.7
        @jira_ticket PYTHON-498
        @expected_result host should never be toggled down while a connection is active.

        @test_category connection
        """
        with Cluster(protocol_version=PROTOCOL_VERSION) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            random_host = cluster.metadata.all_hosts()[0]
            cluster.on_down(random_host, False)
            for _ in range(10):
                new_host = cluster.metadata.all_hosts()[0]
                self.assertTrue(new_host.is_up, "Host was not up on iteration {0}".format(_))
                time.sleep(.01)

            pool = session._pools.get(random_host)
            pool.shutdown()
            cluster.on_down(random_host, False)
            was_marked_down = False
            for _ in range(20):
                new_host = cluster.metadata.all_hosts()[0]
                if not new_host.is_up:
                    was_marked_down = True
                    break
                time.sleep(.01)
            self.assertTrue(was_marked_down)

@local
class DontPrepareOnIgnoredHostsTest(unittest.TestCase):
    ignored_addresses = ['127.0.0.3']
    ignore_node_3_policy = IgnoredHostPolicy(ignored_addresses)

    def test_prepare_on_ignored_hosts(self):

        cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                          load_balancing_policy=self.ignore_node_3_policy)
        session = cluster.connect()
        cluster.reprepare_on_up, cluster.prepare_on_all_hosts = True, False

        hosts = cluster.metadata.all_hosts()
        session.execute("CREATE KEYSPACE clustertests "
                        "WITH replication = "
                        "{'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("CREATE TABLE clustertests.tab (a text, PRIMARY KEY (a))")
        # assign to an unused variable so cluster._prepared_statements retains
        # reference
        _ = session.prepare("INSERT INTO clustertests.tab (a) VALUES ('a')")  # noqa

        cluster.connection_factory = Mock(wraps=cluster.connection_factory)

        unignored_address = '127.0.0.1'
        unignored_host = next(h for h in hosts if h.address == unignored_address)
        ignored_host = next(h for h in hosts if h.address in self.ignored_addresses)
        unignored_host.is_up = ignored_host.is_up = False

        cluster.on_up(unignored_host)
        cluster.on_up(ignored_host)

        # the length of mock_calls will vary, but all should use the unignored
        # address
        for c in cluster.connection_factory.mock_calls:
            self.assertEqual(call(unignored_address), c)
        cluster.shutdown()

@local
class DuplicateRpcTest(unittest.TestCase):

    load_balancing_policy = HostFilterPolicy(RoundRobinPolicy(),
                                             lambda host: host.address == "127.0.0.1")

    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, load_balancing_policy=self.load_balancing_policy)
        self.session = self.cluster.connect()
        self.session.execute("UPDATE system.peers SET rpc_address = '127.0.0.1' WHERE peer='127.0.0.2'")

    def tearDown(self):
        self.session.execute("UPDATE system.peers SET rpc_address = '127.0.0.2' WHERE peer='127.0.0.2'")
        self.cluster.shutdown()

    def test_duplicate(self):
        """
        Test duplicate RPC addresses.

        Modifies the system.peers table to make hosts have the same rpc address. Ensures such hosts are filtered out and a message is logged

        @since 3.4
        @jira_ticket PYTHON-366
        @expected_result only one hosts' metadata will be populated

        @test_category metadata
        """
        mock_handler = MockLoggingHandler()
        logger = logging.getLogger(cassandra.cluster.__name__)
        logger.addHandler(mock_handler)
        test_cluster = Cluster(protocol_version=PROTOCOL_VERSION, load_balancing_policy=self.load_balancing_policy)
        test_cluster.connect()
        warnings = mock_handler.messages.get("warning")
        self.assertEqual(len(warnings), 1)
        self.assertTrue('multiple' in warnings[0])
        logger.removeHandler(mock_handler)
        test_cluster.shutdown()



@protocolv5
class BetaProtocolTest(unittest.TestCase):

    @protocolv5
    def test_invalid_protocol_version_beta_option(self):
        """
        Test cluster connection with protocol v5 and beta flag not set

        @since 3.7.0
        @jira_ticket PYTHON-614
        @expected_result client shouldn't connect with V5 and no beta flag set

        @test_category connection
        """

        cluster = Cluster(protocol_version=cassandra.ProtocolVersion.MAX_SUPPORTED, allow_beta_protocol_version=False)
        try:
            with self.assertRaises(NoHostAvailable):
                cluster.connect()
        except Exception as e:
            self.fail("Unexpected error encountered {0}".format(e.message))

    @protocolv5
    def test_valid_protocol_version_beta_options_connect(self):
        """
        Test cluster connection with protocol version 5 and beta flag set

        @since 3.7.0
        @jira_ticket PYTHON-614
        @expected_result client should connect with protocol v5 and beta flag set.

        @test_category connection
        """
        cluster = Cluster(protocol_version=cassandra.ProtocolVersion.MAX_SUPPORTED, allow_beta_protocol_version=True)
        session = cluster.connect()
        self.assertEqual(cluster.protocol_version, cassandra.ProtocolVersion.MAX_SUPPORTED)
        self.assertTrue(session.execute("select release_version from system.local")[0])
        cluster.shutdown()
