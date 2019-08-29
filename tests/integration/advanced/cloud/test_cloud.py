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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import six
from ssl import SSLContext, PROTOCOL_TLSv1

from cassandra import DriverException
from cassandra.cluster import NoHostAvailable
from cassandra.connection import SniEndPoint
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, ConstantReconnectionPolicy

from mock import patch

from tests.integration import requirescloudproxy
from tests.integration.util import wait_until_not_raised
from tests.integration.advanced.cloud import CloudProxyCluster, CLOUD_PROXY_SERVER


@requirescloudproxy
class CloudTests(CloudProxyCluster):
    def hosts_up(self):
        return [h for h in self.cluster.metadata.all_hosts() if h.is_up]

    def test_resolve_and_connect(self):
        self.connect(self.creds)

        self.assertEqual(len(self.hosts_up()), 3)
        for host in self.cluster.metadata.all_hosts():
            self.assertTrue(host.is_up)
            self.assertIsInstance(host.endpoint, SniEndPoint)
            self.assertEqual(str(host.endpoint), "{}:{}:{}".format(
                host.endpoint.address, host.endpoint.port, host.host_id))
            self.assertIn(host.endpoint._resolved_address, ("127.0.0.1", '::1'))

    def test_match_system_local(self):
        self.connect(self.creds)

        self.assertEqual(len(self.hosts_up()), 3)
        for host in self.cluster.metadata.all_hosts():
            row = self.session.execute('SELECT * FROM system.local', host=host).one()
            self.assertEqual(row.host_id, host.host_id)
            self.assertEqual(row.rpc_address, host.broadcast_rpc_address)

    def test_set_auth_provider(self):
        self.connect(self.creds)
        self.assertIsInstance(self.cluster.auth_provider, PlainTextAuthProvider)
        self.assertEqual(self.cluster.auth_provider.username, 'cassandra')
        self.assertEqual(self.cluster.auth_provider.password, 'cassandra')

    def test_support_leaving_the_auth_unset(self):
        with self.assertRaises(NoHostAvailable):
            self.connect(self.creds_no_auth)
        self.assertIsNone(self.cluster.auth_provider)

    def test_support_overriding_auth_provider(self):
        try:
            self.connect(self.creds, auth_provider=PlainTextAuthProvider('invalid', 'invalid'))
        except:
            pass  # this will fail soon when sni_single_endpoint is updated
        self.assertIsInstance(self.cluster.auth_provider, PlainTextAuthProvider)
        self.assertEqual(self.cluster.auth_provider.username, 'invalid')
        self.assertEqual(self.cluster.auth_provider.password, 'invalid')

    def test_support_overriding_ssl_context(self):
        with self.assertRaises(DriverException):
            # will fail since the ssl_context is
            self.connect(self.creds, ssl_context=SSLContext(PROTOCOL_TLSv1))

    def test_error_when_bundle_doesnt_exist(self):
        try:
            self.connect('/invalid/path/file.zip')
        except Exception as e:
            if six.PY2:
                self.assertIsInstance(e, IOError)
            else:
                self.assertIsInstance(e, FileNotFoundError)

    def test_load_balancing_policy_is_dcawaretokenlbp(self):
        self.connect(self.creds)
        self.assertIsInstance(self.cluster.profile_manager.default.load_balancing_policy,
                              TokenAwarePolicy)
        self.assertIsInstance(self.cluster.profile_manager.default.load_balancing_policy._child_policy,
                              DCAwareRoundRobinPolicy)

    def test_resolve_and_reconnect_on_node_down(self):

        self.connect(self.creds,
                     idle_heartbeat_interval=1, idle_heartbeat_timeout=1,
                     reconnection_policy=ConstantReconnectionPolicy(50))

        self.assertEqual(len(self.hosts_up()), 3)
        CLOUD_PROXY_SERVER.stop_node(1)
        wait_until_not_raised(
            lambda: self.assertEqual(len(self.hosts_up()), 2),
            0.02, 250)

        host = [h for h in self.cluster.metadata.all_hosts() if not h.is_up][0]
        with patch.object(SniEndPoint, "resolve", wraps=host.endpoint.resolve) as mocked_resolve:
            CLOUD_PROXY_SERVER.start_node(1)
            wait_until_not_raised(
                lambda: self.assertEqual(len(self.hosts_up()), 3),
                0.02, 250)
            mocked_resolve.assert_called_once()

    def test_metadata_unreachable(self):
        with self.assertRaises(DriverException) as cm:
            self.connect(self.creds_unreachable, connect_timeout=1)

        self.assertIn('Unable to connect to the metadata service', str(cm.exception))

    def test_metadata_ssl_error(self):
        with self.assertRaises(DriverException) as cm:
            self.connect(self.creds, ssl_context=SSLContext(PROTOCOL_TLSv1))

        self.assertIn('Unable to connect to the metadata', str(cm.exception))
