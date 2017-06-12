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
# limitations under the License
#
#
#

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException
from tests.integration import use_singledc, PROTOCOL_VERSION, greaterthanorequalcass40, protocolv5
from tests.integration.datatype_utils import update_datatypes


def setup_module():
    use_singledc()
    update_datatypes()


class ControlConnectionTests(unittest.TestCase):
    def setUp(self):
        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest(
                "Native protocol 3,0+ is required for UDTs using %r"
                % (PROTOCOL_VERSION,))
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)

    def tearDown(self):
        try:
            self.session.execute("DROP KEYSPACE keyspacetodrop ")
        except (ConfigurationException):
            # we already removed the keyspace.
            pass
        self.cluster.shutdown()

    def test_drop_keyspace(self):
        """
        Test to validate that dropping a keyspace with user defined types doesn't kill the control connection.


        Creates a keyspace, and populates with a user defined type. It then records the control_connection's id. It
        will then drop the keyspace and get the id of the control_connection again. They should be the same. If they are
        not dropping the keyspace likely caused the control connection to be rebuilt.

        @since 2.7.0
        @jira_ticket PYTHON-358
        @expected_result the control connection is not killed

        @test_category connection
        """

        self.session = self.cluster.connect()
        self.session.execute("""
            CREATE KEYSPACE keyspacetodrop
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        self.session.set_keyspace("keyspacetodrop")
        self.session.execute("CREATE TYPE user (age int, name text)")
        self.session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")
        cc_id_pre_drop = id(self.cluster.control_connection._connection)
        self.session.execute("DROP KEYSPACE keyspacetodrop")
        cc_id_post_drop = id(self.cluster.control_connection._connection)
        self.assertEqual(cc_id_post_drop, cc_id_pre_drop)

    def test_get_control_connection_host(self):
        """
        Test to validate Cluster.get_control_connection_host() metadata

        @since 3.5.0
        @jira_ticket PYTHON-583
        @expected_result the control connection metadata should accurately reflect cluster state.

        @test_category metadata
        """

        host = self.cluster.get_control_connection_host()
        self.assertEqual(host, None)

        self.session = self.cluster.connect()
        cc_host = self.cluster.control_connection._connection.host

        host = self.cluster.get_control_connection_host()
        self.assertEqual(host.address, cc_host)
        self.assertEqual(host.is_up, True)

        # reconnect and make sure that the new host is reflected correctly
        self.cluster.control_connection._reconnect()
        new_host = self.cluster.get_control_connection_host()
        self.assertNotEqual(host, new_host)

    @greaterthanorequalcass40
    @protocolv5
    def test_control_connection_port_discovery(self):
        """
        Test to validate that if allow_server_port_discovery the correct port is discovered
        Unit tests already validate that the port can be picked up (or not) from the query. This validates
        it picks up the correct port from a real server and is able to connect.

        @since 4.0

        @test_category metadata
        """
        if PROTOCOL_VERSION < 5:
            raise unittest.SkipTest(
                "Native protocol 4,0+ is required to test allow_server_port_discovery")
        self.cluster.shutdown()
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, allow_server_port_discovery=True)

        host = self.cluster.get_control_connection_host()
        self.assertEqual(host, None)

        self.session = self.cluster.connect()
        cc_host = self.cluster.control_connection._connection.host

        host = self.cluster.get_control_connection_host()
        self.assertEqual(host.address, cc_host)
        self.assertEqual(host.is_up, True)
        hosts = self.cluster.metadata.all_hosts()
        self.assertEqual(3, len(hosts))

        for host in hosts:
            self.assertEquals(9042, host.rpc_port)
            self.assertEquals(7000, host.broadcast_port)

        # reconnect and make sure that the new host is reflected correctly
        self.cluster.control_connection._reconnect()
        new_host = self.cluster.get_control_connection_host()
        self.assertNotEqual(host, new_host)

    def test_control_connection_port_discovery_disabled(self):
        """
        Test to validate that if allow_server_port_discovery is disabled and it still connects using the configured
        port. A unit test already validates the choice of config vs result of the query.

        @since 4.0

        @test_category metadata
        """
        if PROTOCOL_VERSION < 5:
            raise unittest.SkipTest(
                "Native protocol 4,0+ is required to test allow_server_port_discovery")
        self.cluster.shutdown()
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, allow_server_port_discovery=False)

        host = self.cluster.get_control_connection_host()
        self.assertEqual(host, None)

        self.session = self.cluster.connect()
        cc_host = self.cluster.control_connection._connection.host

        host = self.cluster.get_control_connection_host()
        self.assertEqual(host.address, cc_host)
        self.assertEqual(host.is_up, True)
        hosts = self.cluster.metadata.all_hosts()
        self.assertEqual(3, len(hosts))

        for host in hosts:
            self.assertEquals(9042, host.rpc_port)
            self.assertEquals(7000, host.broadcast_port)

        # reconnect and make sure that the new host is reflected correctly
        self.cluster.control_connection._reconnect()
        new_host = self.cluster.get_control_connection_host()
        self.assertNotEqual(host, new_host)
