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

from functools import partial
from six.moves import range
import sys
from threading import Thread, Event
import time
import weakref

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.query import SimpleStatement
from cassandra.cluster import NoHostAvailable, ConnectionShutdown, Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.protocol import QueryMessage
from cassandra.connection import Connection
from cassandra.policies import WhiteListRoundRobinPolicy, HostStateListener
from cassandra.pool import HostConnectionPool

from tests import is_monkey_patched
from tests.integration import use_singledc, PROTOCOL_VERSION, get_node, get_cluster, CASSANDRA_IP, local, remove_cluster

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None


def setup_module():
    remove_cluster()
    use_singledc(use_single_interface=True)


class SingleInterfaceTest(unittest.TestCase):

    def setUp(self):
        if PROTOCOL_VERSION < 5:
            raise unittest.SkipTest('Single interface test is only supported on 4.0 and later')
        contact_points = [get_cluster().nodelist()[0].network_interfaces['binary']]
        self.cluster = Cluster(contact_points=contact_points, protocol_version=PROTOCOL_VERSION, load_balancing_policy=
                            WhiteListRoundRobinPolicy([CASSANDRA_IP]),  allow_server_port_discovery=True)
        self.session = self.cluster.connect()

    def tearDown(self):
        if self.cluster is not None:
            self.cluster.shutdown()
        remove_cluster()

    def test_single_interface(self):
        """
        Test that we can connect to a multiple hosts bound to a single interface

        @since 4.0
        """
        hosts = self.cluster.metadata._hosts
        interface = None
        rpc_ports = []
        broadcast_ports = []
        for key, host in hosts.iteritems():
            if interface is None:
                interface = key[0]
            else:
                self.assertEqual(interface, key[0])

            self.assertEqual(key[1], host.rpc_port)
            self.assertEquals(interface, host.address)
            self.assertEquals(interface, host.broadcast_address)
            if host.rpc_port in rpc_ports:
                self.fail("Duplicate rpc_port")
            rpc_ports.append(host.rpc_port)
            if host.broadcast_port in broadcast_ports:
                self.fail("Duplicate broadcast_port")
            broadcast_ports.append(host.broadcast_port)

        for _ in range(1, 100):
            self.session.execute(SimpleStatement("select * from system_distributed.view_build_status", consistency_level=ConsistencyLevel.ALL))

        for pool in self.session.get_pools():
            self.assertEquals(1, pool.get_state()['open_count'])
