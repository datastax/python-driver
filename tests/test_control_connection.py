import unittest

from cassandra.decoder import ResultMessage
from cassandra.cluster import _ControlConnection, MAX_SCHEMA_AGREEMENT_WAIT
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy

class MockMetadata(object):

    def get_host(self, rpc_address):
        return Host(rpc_address, SimpleConvictionPolicy)

    def all_hosts(self):
        return ['ip1', 'ip2']

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

        peer_results = [{"rpc_address": ip, "schema_version": schema_ver}
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
        self.assertTrue(self.control_connection.wait_for_schema_agreement())

    def test_wait_for_schema_agreement_fails(self):
        self.connection.remote_schemas['ip2'] = 'b'
        self.assertFalse(self.control_connection.wait_for_schema_agreement())
        self.assertGreaterEqual(self.time.clock, MAX_SCHEMA_AGREEMENT_WAIT)
