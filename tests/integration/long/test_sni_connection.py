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
import socket
import ssl
import unittest
from unittest.mock import patch

from cassandra import ProtocolVersion
from cassandra.cluster import Cluster, ControlConnection, ProfileManager
from cassandra.connection import SniEndPoint
from cassandra.datastax.cloud import CloudConfig
from cassandra.policies import ConstantReconnectionPolicy
from tests.integration import (
    get_cluster, remove_cluster, CASSANDRA_IP
)
from tests.integration.long.test_ssl import setup_cluster_ssl, CLIENT_CA_CERTS, DRIVER_CERTFILE, DRIVER_KEYFILE


class SniConnectionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl(client_auth=True)

    @classmethod
    def tearDownClass(cls):
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        remove_cluster()

    def _mocked_cloud_config(self, cloud_config, create_pyopenssl_context):
        config = CloudConfig.from_dict({})
        config.sni_host = 'proxy.datastax.com'
        config.sni_port = 9042
        config.host_ids = ['8c4b6ed7-f505-4226-b7a4-41f322520c1f', '2e25021d-8d72-41a7-a247-3da85c5d92d2']

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ssl_context.load_verify_locations(CLIENT_CA_CERTS)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_cert_chain(certfile=DRIVER_CERTFILE, keyfile=DRIVER_KEYFILE)
        config.ssl_context = ssl_context

        return config

    def _mocked_proxy_dns_resolution(self):
        return [
            # return wrong IP at first position, so that we make sure all SNI endpoints
            # do not start with first IP only
            (socket.AF_UNIX, socket.SOCK_STREAM, 0, None, ('100.101.102.103', 9042)),
            (socket.AF_UNIX, socket.SOCK_STREAM, 0, None, (CASSANDRA_IP, 9042))
        ]

    def _mocked_refresh_node_list_and_token_map(self, connection, preloaded_results=None,
                                                force_token_rebuild=False):
        return

    def _mocked_refresh_schema(self, connection, preloaded_results=None, schema_agreement_wait=None,
                               force=False, **kwargs):
        return

    def _mocked_check_supported(self):
        return

    # Tests verifies that driver can connect to SNI endpoint even when one IP
    # returned by the DNS resolution of SNI does not respond. Mocked SNI resolution method
    # returns two IPs where only one corresponds to online C* cluster started with CCM.
    def test_round_robin_dns_resolution(self):
        with patch('cassandra.datastax.cloud.get_cloud_config', self._mocked_cloud_config):
            with patch.object(SniEndPoint, '_resolve_proxy_addresses', self._mocked_proxy_dns_resolution):
                # Mock below three functions, because host ID returned from proxy will not match ID present in C*
                # Network connection should be already made, so we can consider our test successful
                with patch.object(ControlConnection, '_refresh_node_list_and_token_map',
                                  self._mocked_refresh_node_list_and_token_map):
                    with patch.object(ControlConnection, '_refresh_schema',
                                      self._mocked_refresh_schema):
                        with patch.object(ProfileManager, 'check_supported', self._mocked_check_supported):
                            cloud_config = {
                                'secure_connect_bundle': '/path/to/secure-connect-dbname.zip'
                            }
                            cluster = Cluster(cloud=cloud_config, protocol_version=ProtocolVersion.V4, reconnection_policy=ConstantReconnectionPolicy(10))
                            session = cluster.connect()
                            session.shutdown()
                            cluster.shutdown()
