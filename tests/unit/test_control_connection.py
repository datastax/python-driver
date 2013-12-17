try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from mock import Mock, ANY

from concurrent.futures import ThreadPoolExecutor

from cassandra import OperationTimedOut
from cassandra.decoder import ResultMessage
from cassandra.cluster import ControlConnection, Cluster, _Scheduler
from cassandra.pool import Host
from cassandra.policies import (SimpleConvictionPolicy, RoundRobinPolicy,
                                ConstantReconnectionPolicy)

PEER_IP = "foobar"

class MockMetadata(object):

    def __init__(self):
        self.hosts = {
            "192.168.1.0": Host("192.168.1.0", SimpleConvictionPolicy),
            "192.168.1.1": Host("192.168.1.1", SimpleConvictionPolicy),
            "192.168.1.2": Host("192.168.1.2", SimpleConvictionPolicy)
        }
        for host in self.hosts.values():
            host.set_up()

        self.cluster_name = None
        self.partitioner = None
        self.token_map = {}

    def get_host(self, rpc_address):
        return self.hosts.get(rpc_address)

    def all_hosts(self):
        return self.hosts.values()

    def rebuild_token_map(self, partitioner, token_map):
        self.partitioner = partitioner
        self.token_map = token_map


class MockCluster(object):

    max_schema_agreement_wait = Cluster.max_schema_agreement_wait
    load_balancing_policy = RoundRobinPolicy()
    reconnection_policy = ConstantReconnectionPolicy(2)
    down_host = None
    contact_points = []

    def __init__(self):
        self.metadata = MockMetadata()
        self.added_hosts = []
        self.removed_hosts = []
        self.scheduler = Mock(spec=_Scheduler)
        self.executor = Mock(spec=ThreadPoolExecutor)

    def add_host(self, address, signal=False):
        host = Host(address, SimpleConvictionPolicy)
        self.added_hosts.append(host)
        return host

    def remove_host(self, host):
        self.removed_hosts.append(host)

    def on_up(self, host):
        pass

    def on_down(self, host, is_host_addition):
        self.down_host = host


class MockConnection(object):

    is_defunct = False

    def __init__(self):
        self.host = "192.168.1.0"
        self.local_results = [
            ["schema_version", "cluster_name", "data_center", "rack", "partitioner", "tokens"],
            [["a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", ["0", "100", "200"]]]
        ]

        self.peer_results = [
            ["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens"],
            [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"]],
             ["192.168.1.2", "10.0.0.2", "a", "dc1", "rack1", ["2", "102", "202"]]]
        ]

    def wait_for_responses(self, peer_query, local_query, timeout=None):
        local_response = ResultMessage(
            kind=ResultMessage.KIND_ROWS, results=self.local_results)
        peer_response = ResultMessage(
            kind=ResultMessage.KIND_ROWS, results=self.peer_results)
        return (peer_response, local_response)


class FakeTime(object):

    def __init__(self):
        self.clock = 0

    def time(self):
        return self.clock

    def sleep(self, amount):
        self.clock += amount


class ControlConnectionTest(unittest.TestCase):

    def setUp(self):
        self.cluster = MockCluster()
        self.connection = MockConnection()
        self.time = FakeTime()

        self.control_connection = ControlConnection(self.cluster, timeout=0.01)
        self.control_connection._connection = self.connection
        self.control_connection._time = self.time

    def test_wait_for_schema_agreement(self):
        """
        Basic test with all schema versions agreeing
        """
        self.assertTrue(self.control_connection.wait_for_schema_agreement())
        # the control connection should not have slept at all
        self.assertEqual(self.time.clock, 0)

    def test_wait_for_schema_agreement_fails(self):
        """
        Make sure the control connection sleeps and retries
        """
        # change the schema version on one node
        self.connection.peer_results[1][1][2] = 'b'
        self.assertFalse(self.control_connection.wait_for_schema_agreement())
        # the control connection should have slept until it hit the limit
        self.assertGreaterEqual(self.time.clock, Cluster.max_schema_agreement_wait)

    def test_wait_for_schema_agreement_skipping(self):
        """
        If rpc_address or schema_version isn't set, the host should be skipped
        """
        # an entry with no schema_version
        self.connection.peer_results[1].append(
            ["192.168.1.3", "10.0.0.3", None, "dc1", "rack1", ["3", "103", "203"]]
        )
        # an entry with a different schema_version and no rpc_address
        self.connection.peer_results[1].append(
            [None, None, "b", "dc1", "rack1", ["4", "104", "204"]]
        )

        # change the schema version on one of the existing entries
        self.connection.peer_results[1][1][3] = 'c'
        self.cluster.metadata.get_host('192.168.1.1').is_up = False

        self.assertTrue(self.control_connection.wait_for_schema_agreement())
        self.assertEqual(self.time.clock, 0)

    def test_wait_for_schema_agreement_rpc_lookup(self):
        """
        If the rpc_address is 0.0.0.0, the "peer" column should be used instead.
        """
        self.connection.peer_results[1].append(
            ["0.0.0.0", PEER_IP, "b", "dc1", "rack1", ["3", "103", "203"]]
        )
        host = Host("0.0.0.0", SimpleConvictionPolicy)
        self.cluster.metadata.hosts[PEER_IP] = host
        host.is_up = False

        # even though the new host has a different schema version, it's
        # marked as down, so the control connection shouldn't care
        self.assertTrue(self.control_connection.wait_for_schema_agreement())
        self.assertEqual(self.time.clock, 0)

        # but once we mark it up, the control connection will care
        host.is_up = True
        self.assertFalse(self.control_connection.wait_for_schema_agreement())
        self.assertGreaterEqual(self.time.clock, Cluster.max_schema_agreement_wait)

    def test_refresh_nodes_and_tokens(self):
        self.control_connection.refresh_node_list_and_token_map()
        meta = self.cluster.metadata
        self.assertEqual(meta.partitioner, 'Murmur3Partitioner')
        self.assertEqual(meta.cluster_name, 'foocluster')

        # check token map
        self.assertEqual(sorted(meta.all_hosts()), sorted(meta.token_map.keys()))
        for token_list in meta.token_map.values():
            self.assertEqual(3, len(token_list))

        # check datacenter/rack
        for host in meta.all_hosts():
            self.assertEqual(host.datacenter, "dc1")
            self.assertEqual(host.rack, "rack1")

    def test_refresh_nodes_and_tokens_no_partitioner(self):
        """
        Test handling of an unknown partitioner.
        """
        # set the partitioner column to None
        self.connection.local_results[1][0][4] = None
        self.control_connection.refresh_node_list_and_token_map()
        meta = self.cluster.metadata
        self.assertEqual(meta.partitioner, None)
        self.assertEqual(meta.token_map, {})

    def test_refresh_nodes_and_tokens_add_host(self):
        self.connection.peer_results[1].append(
            ["192.168.1.3", "10.0.0.3", "a", "dc1", "rack1", ["3", "103", "203"]]
        )
        self.control_connection.refresh_node_list_and_token_map()
        self.assertEqual(1, len(self.cluster.added_hosts))
        self.assertEqual(self.cluster.added_hosts[0].address, "192.168.1.3")
        self.assertEqual(self.cluster.added_hosts[0].datacenter, "dc1")
        self.assertEqual(self.cluster.added_hosts[0].rack, "rack1")

    def test_refresh_nodes_and_tokens_remove_host(self):
        del self.connection.peer_results[1][1]
        self.control_connection.refresh_node_list_and_token_map()
        self.assertEqual(1, len(self.cluster.removed_hosts))
        self.assertEqual(self.cluster.removed_hosts[0].address, "192.168.1.2")

    def test_refresh_nodes_and_tokens_timeout(self):

        def bad_wait_for_responses(*args, **kwargs):
            self.assertEqual(kwargs['timeout'], self.control_connection._timeout)
            raise OperationTimedOut()

        self.connection.wait_for_responses = bad_wait_for_responses
        self.control_connection.refresh_node_list_and_token_map()
        self.cluster.executor.submit.assert_called_with(self.control_connection._reconnect)

    def test_refresh_schema_timeout(self):

        def bad_wait_for_responses(*args, **kwargs):
            self.assertEqual(kwargs['timeout'], self.control_connection._timeout)
            raise OperationTimedOut()

        self.connection.wait_for_responses = bad_wait_for_responses
        self.control_connection.refresh_schema()
        self.cluster.executor.submit.assert_called_with(self.control_connection._reconnect)

    def test_handle_topology_change(self):
        event = {
            'change_type': 'NEW_NODE',
            'address': ('1.2.3.4', 9000)
        }
        self.control_connection._handle_topology_change(event)
        self.cluster.scheduler.schedule.assert_called_with(ANY, self.cluster.add_host, '1.2.3.4', signal=True)

        event = {
            'change_type': 'REMOVED_NODE',
            'address': ('1.2.3.4', 9000)
        }
        self.control_connection._handle_topology_change(event)
        self.cluster.scheduler.schedule.assert_called_with(ANY, self.cluster.remove_host, None)

        event = {
            'change_type': 'MOVED_NODE',
            'address': ('1.2.3.4', 9000)
        }
        self.control_connection._handle_topology_change(event)
        self.cluster.scheduler.schedule.assert_called_with(ANY, self.control_connection.refresh_node_list_and_token_map)

    def test_handle_status_change(self):
        event = {
            'change_type': 'UP',
            'address': ('1.2.3.4', 9000)
        }
        self.control_connection._handle_status_change(event)
        self.cluster.scheduler.schedule.assert_called_with(ANY, self.cluster.add_host, '1.2.3.4', signal=True)

        # do the same with a known Host
        event = {
            'change_type': 'UP',
            'address': ('192.168.1.0', 9000)
        }
        self.control_connection._handle_status_change(event)
        host = self.cluster.metadata.hosts['192.168.1.0']
        self.cluster.scheduler.schedule.assert_called_with(ANY, self.cluster.on_up, host)

        self.cluster.scheduler.schedule.reset_mock()
        event = {
            'change_type': 'DOWN',
            'address': ('1.2.3.4', 9000)
        }
        self.control_connection._handle_status_change(event)
        self.assertFalse(self.cluster.scheduler.schedule.called)

        # do the same with a known Host
        event = {
            'change_type': 'DOWN',
            'address': ('192.168.1.0', 9000)
        }
        self.control_connection._handle_status_change(event)
        host = self.cluster.metadata.hosts['192.168.1.0']
        self.assertIs(host, self.cluster.down_host)

    def test_handle_schema_change(self):

        for change_type in ('CREATED', 'DROPPED'):
            event = {
                'change_type': change_type,
                'keyspace': 'ks1',
                'table': 'table1'
            }
            self.control_connection._handle_schema_change(event)
            self.cluster.executor.submit.assert_called_with(self.control_connection.refresh_schema, 'ks1')

            event['table'] = None
            self.control_connection._handle_schema_change(event)
            self.cluster.executor.submit.assert_called_with(self.control_connection.refresh_schema, None)

        event = {
            'change_type': 'UPDATED',
            'keyspace': 'ks1',
            'table': 'table1'
        }
        self.control_connection._handle_schema_change(event)
        self.cluster.executor.submit.assert_called_with(self.control_connection.refresh_schema, 'ks1', 'table1')

        event['table'] = None
        self.control_connection._handle_schema_change(event)
        self.cluster.executor.submit.assert_called_with(self.control_connection.refresh_schema, 'ks1', None)
