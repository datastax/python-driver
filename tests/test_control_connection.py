import unittest

from cassandra.decoder import ResultMessage
from cassandra.cluster import _ControlConnection, MAX_SCHEMA_AGREEMENT_WAIT
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy

PEER_IP = "foobar"

class MockMetadata(object):

    def __init__(self):
        self.hosts = {
            "ip1": Host("ip1", SimpleConvictionPolicy),
            "ip2": Host("ip2", SimpleConvictionPolicy)
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
        self.local_schema = "a"
        self.remote_schemas = {
            "ip1": "a",
            "ip2": "a"
        }

    def wait_for_responses(self, peer_query, local_query):
        local_results = [{"schema_version": self.local_schema}]
        local_response = ResultMessage(kind=ResultMessage.KIND_ROWS, results=local_results)

        peer_results = [{"rpc_address": ip, "schema_version": schema_ver, "peer": PEER_IP}
                        for ip, schema_ver in self.remote_schemas.iteritems()]
        peer_response = ResultMessage(kind=ResultMessage.KIND_ROWS, results=peer_results)

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
        self.connection.remote_schemas['ip2'] = 'b'
        self.assertFalse(self.control_connection.wait_for_schema_agreement())
        # the control connection should have slept until it hit the limit
        self.assertGreaterEqual(self.time.clock, MAX_SCHEMA_AGREEMENT_WAIT)

    def test_wait_for_schema_agreement_skipping(self):
        """
        If rpc_address or schema_version isn't set, the host should be skipped
        """
        self.connection.remote_schemas['ip3'] = None
        self.connection.remote_schemas[None] = 'b'

        self.connection.remote_schemas['ip1'] = 'c'
        self.cluster.metadata.get_host('ip1').monitor.is_up = False

        self.assertTrue(self.control_connection.wait_for_schema_agreement())
        self.assertEqual(self.time.clock, 0)

    def test_wait_for_schema_agreement_rpc_lookup(self):
        """
        If the rpc_address is 0.0.0.0, the "peer" column should be used instead.
        """
        self.connection.remote_schemas["0.0.0.0"] = "b"
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
