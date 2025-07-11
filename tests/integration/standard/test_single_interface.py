# Copyright DataStax, Inc.
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

import unittest
import pytest

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from packaging.version import Version
from tests.integration import use_singledc, PROTOCOL_VERSION, \
    remove_cluster, greaterthanorequalcass40, \
    CASSANDRA_VERSION, TestCluster, DEFAULT_SINGLE_INTERFACE_PORT


def setup_module():
    if CASSANDRA_VERSION >= Version('4-a'):
        remove_cluster()
        use_singledc(use_single_interface=True)

def teardown_module():
    remove_cluster()


@greaterthanorequalcass40
class SingleInterfaceTest(unittest.TestCase):

    def setUp(self):
        self.cluster = TestCluster(port=DEFAULT_SINGLE_INTERFACE_PORT)
        self.session = self.cluster.connect()

    def tearDown(self):
        if self.cluster is not None:
            self.cluster.shutdown()

    # TODO: enable after https://github.com/scylladb/python-driver/issues/121 is fixed
    @unittest.skip('Fails on scylla due to the broadcast_rpc_port is None')
    def test_single_interface(self):
        """
        Test that we can connect to a multiple hosts bound to a single interface.
        """
        hosts = self.cluster.metadata._hosts
        broadcast_rpc_ports = []
        broadcast_ports = []
        assert len(hosts) == 3
        for endpoint, host in hosts.items():

            assert endpoint.address == host.broadcast_rpc_address
            assert endpoint.port == host.broadcast_rpc_port

            assert host.broadcast_rpc_port not in broadcast_rpc_ports, "Duplicate broadcast_rpc_port"
            broadcast_rpc_ports.append(host.broadcast_rpc_port)
            assert host.broadcast_port not in broadcast_ports, "Duplicate broadcast_port"
            broadcast_ports.append(host.broadcast_port)

        for _ in range(1, 100):
            self.session.execute(SimpleStatement("select * from system_distributed.view_build_status",
                                                 consistency_level=ConsistencyLevel.ALL))

        for pool in self.session.get_pools():
            assert 1 == pool.get_state()['open_count']
