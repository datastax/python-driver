import unittest

from cassandra.decoder import ResultMessage
from cassandra.cluster import _ControlConnection, MAX_SCHEMA_AGREEMENT_WAIT
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy

PEER_IP = "foobar"

class MockMetadata(object):

    def __init__(self):
        self.hosts = {
            "192.168.1.1": Host("192.168.1.1", SimpleConvictionPolicy),
            "192.168.1.2": Host("192.168.1.2", SimpleConvictionPolicy)
        }

    def get_host(self, rpc_address):
        return self.hosts.get(rpc_address)

    def all_hosts(self):
        return self.hosts.values()


class MockCluster(object):

    def __init__(self):
        self.metadata = MockMetadata()


class MockConnection(object):

    def __init__(self):
        self.local_results = [
            {
                "schema_version": "a",
                "cluster_name": "foocluster",
                "data_center": "dc1",
                "rack": "rack1",
                "partitioner": "Murmur3Partitioner",
                "tokens": [0, 100, 200]
            }
        ]

        self.peer_results = [
            {
                "rpc_address": "192.168.1.1",
                "peer": "10.0.0.1",
                "schema_version": "a",
                "data_center": "dc1",
                "rack": "rack1",
                "tokens": [1, 101, 201]
            },
            {
                "rpc_address": "192.168.1.2",
                "peer": "10.0.0.2",
                "schema_version": "a",
                "data_center": "dc1",
                "rack": "rack1",
                "tokens": [1, 101, 201]
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

        self.control_connection = _ControlConnection(self.cluster)
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
        self.assertGreaterEqual(self.time.clock, MAX_SCHEMA_AGREEMENT_WAIT)

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
        self.assertGreaterEqual(self.time.clock, MAX_SCHEMA_AGREEMENT_WAIT)
