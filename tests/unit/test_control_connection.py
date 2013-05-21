import unittest

from cassandra.decoder import ResultMessage
from cassandra.cluster import ControlConnection, Cluster
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy

PEER_IP = "foobar"

class MockMetadata(object):

    def __init__(self):
        self.hosts = {
            "192.168.1.0": Host("192.168.1.0", SimpleConvictionPolicy),
            "192.168.1.1": Host("192.168.1.1", SimpleConvictionPolicy),
            "192.168.1.2": Host("192.168.1.2", SimpleConvictionPolicy)
        }

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

    def __init__(self):
        self.metadata = MockMetadata()
        self.added_hosts = []
        self.removed_hosts = []

    def add_host(self, address, signal=False):
        host = Host(address, SimpleConvictionPolicy)
        self.added_hosts.append(host)
        return host

    def remove_host(self, host):
        self.removed_hosts.append(host)

class MockConnection(object):

    def __init__(self):
        self.host = "192.168.1.0"
        self.local_results = [
            {
                "schema_version": "a",
                "cluster_name": "foocluster",
                "data_center": "dc1",
                "rack": "rack1",
                "partitioner": "Murmur3Partitioner",
                "tokens": ['0', '100', '200']
            }
        ]

        self.peer_results = [
            {
                "rpc_address": "192.168.1.1",
                "peer": "10.0.0.1",
                "schema_version": "a",
                "data_center": "dc1",
                "rack": "rack1",
                "tokens": ['1', '101', '201']
            },
            {
                "rpc_address": "192.168.1.2",
                "peer": "10.0.0.2",
                "schema_version": "a",
                "data_center": "dc1",
                "rack": "rack1",
                "tokens": ['2', '102', '202']
            }
        ]

    def wait_for_responses(self, peer_query, local_query):
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

        self.control_connection = ControlConnection(self.cluster)
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
        self.connection.peer_results[1]["schema_version"] = 'b'
        self.assertFalse(self.control_connection.wait_for_schema_agreement())
        # the control connection should have slept until it hit the limit
        self.assertGreaterEqual(self.time.clock, Cluster.max_schema_agreement_wait)

    def test_wait_for_schema_agreement_skipping(self):
        """
        If rpc_address or schema_version isn't set, the host should be skipped
        """
        self.connection.peer_results.append({
            "rpc_address": "192.168.1.3",
            "schema_version": None
        })
        self.connection.peer_results.append({
            "rpc_address": None,
            "schema_version": "b"
        })

        self.connection.peer_results[0]['schema_version'] = 'c'
        self.cluster.metadata.get_host('192.168.1.1').monitor.is_up = False

        self.assertTrue(self.control_connection.wait_for_schema_agreement())
        self.assertEqual(self.time.clock, 0)

    def test_wait_for_schema_agreement_rpc_lookup(self):
        """
        If the rpc_address is 0.0.0.0, the "peer" column should be used instead.
        """
        self.connection.peer_results.append({
            "rpc_address": "0.0.0.0",
            "schema_version": "b",
            "peer": PEER_IP
        })
        host = Host("0.0.0.0", SimpleConvictionPolicy)
        self.cluster.metadata.hosts[PEER_IP] = host
        host.monitor.is_up = False

        # even though the new host has a different schema version, it's
        # marked as down, so the control connection shouldn't care
        self.assertTrue(self.control_connection.wait_for_schema_agreement())
        self.assertEqual(self.time.clock, 0)

        # but once we mark it up, the control connection will care
        host.monitor.is_up = True
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
        self.connection.local_results[0]["partitioner"] = None
        self.control_connection.refresh_node_list_and_token_map()
        meta = self.cluster.metadata
        self.assertEqual(meta.partitioner, None)
        self.assertEqual(meta.token_map, {})

    def test_refresh_nodes_and_tokens_add_host(self):
        self.connection.peer_results.append({
            "rpc_address": "192.168.1.3",
            "peer": "10.0.0.3",
            "schema_version": "a",
            "data_center": "dc1",
            "rack": "rack1",
            "tokens": ['3', '103', '203']
        })
        self.control_connection.refresh_node_list_and_token_map()
        self.assertEqual(1, len(self.cluster.added_hosts))
        self.assertEqual(self.cluster.added_hosts[0].address, "192.168.1.3")
        self.assertEqual(self.cluster.added_hosts[0].datacenter, "dc1")
        self.assertEqual(self.cluster.added_hosts[0].rack, "rack1")

    def test_refresh_nodes_and_tokens_remove_host(self):
        del self.connection.peer_results[1]
        self.control_connection.refresh_node_list_and_token_map()
        self.assertEqual(1, len(self.cluster.removed_hosts))
        self.assertEqual(self.cluster.removed_hosts[0].address, "192.168.1.2")
