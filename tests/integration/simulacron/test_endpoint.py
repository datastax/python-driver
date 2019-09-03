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
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from functools import total_ordering

from cassandra.cluster import Cluster
from cassandra.connection import DefaultEndPoint, EndPoint, EndPointFactory
from tests.integration import (PROTOCOL_VERSION, requiressimulacron)
from tests.integration.simulacron import SimulacronCluster


@total_ordering
class AddressEndPoint(EndPoint):

    def __init__(self, address, port=9042):
        self._address = address
        self._port = port

    @property
    def address(self):
        return self._address

    @property
    def port(self):
        return self._port

    def resolve(self):
        return self._address, self._port  # connection purpose

    def __eq__(self, other):
        return isinstance(other, AddressEndPoint) and \
            self.address == other.address

    def __hash__(self):
        return hash(self.address)

    def __lt__(self, other):
        return self.address < other.address

    def __str__(self):
        return str("%s" % self.address)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.address)


class AddressEndPointFactory(EndPointFactory):

    def create(self, row):
        addr = None
        if "rpc_address" in row:
            addr = row.get("rpc_address")
        if "native_transport_address" in row:
            addr = row.get("native_transport_address")
        if not addr or addr in ["0.0.0.0", "::"]:
            addr = row.get("peer")

        return AddressEndPoint(addr)


@requiressimulacron
class EndPointTests(SimulacronCluster):
    """
    Basic tests to validate the internal use of the EndPoint class.

    @since 3.18
    @jira_ticket PYTHON-1079
    @expected_result all the hosts are using the proper endpoint class
    """

    def test_default_endpoint(self):
        hosts = self.cluster.metadata.all_hosts()
        for host in hosts:
            self.assertIsNotNone(host.endpoint)
            self.assertIsInstance(host.endpoint, DefaultEndPoint)
            self.assertEqual(host.address, host.endpoint.address)
            self.assertEqual(host.broadcast_rpc_address, host.endpoint.address)

        self.assertIsInstance(self.cluster.control_connection._connection.endpoint, DefaultEndPoint)
        self.assertIsNotNone(self.cluster.control_connection._connection.endpoint)
        endpoints = [host.endpoint for host in hosts]
        self.assertIn(self.cluster.control_connection._connection.endpoint, endpoints)

    def test_custom_endpoint(self):
        cluster = Cluster(
            contact_points=[AddressEndPoint('127.0.0.1')],
            protocol_version=PROTOCOL_VERSION,
            endpoint_factory=AddressEndPointFactory(),
            compression=False,
        )
        cluster.connect(wait_for_all_pools=True)

        hosts = cluster.metadata.all_hosts()
        for host in hosts:
            self.assertIsNotNone(host.endpoint)
            self.assertIsInstance(host.endpoint, AddressEndPoint)
            self.assertEqual(str(host.endpoint), host.endpoint.address)
            self.assertEqual(host.address, host.endpoint.address)
            self.assertEqual(host.broadcast_rpc_address, host.endpoint.address)

        self.assertIsInstance(cluster.control_connection._connection.endpoint, AddressEndPoint)
        self.assertIsNotNone(cluster.control_connection._connection.endpoint)
        endpoints = [host.endpoint for host in hosts]
        self.assertIn(cluster.control_connection._connection.endpoint, endpoints)

        cluster.shutdown()
