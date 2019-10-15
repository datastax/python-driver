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

from cassandra.cluster import Cluster
from tests.integration import CASSANDRA_IP, USE_CASS_EXTERNAL, use_cluster, PROTOCOL_VERSION


class MisconfiguredAuthenticationTests(unittest.TestCase):
    """ One node (not the contact point) has password auth. The rest of the nodes have no auth """
    @classmethod
    def setUpClass(cls):
        if not USE_CASS_EXTERNAL:
            ccm_cluster = use_cluster(cls.__name__, [3], start=False)
            node3 = ccm_cluster.nodes['node3']
            node3.set_configuration_options(values={
                'authenticator': 'PasswordAuthenticator',
                'authorizer': 'CassandraAuthorizer',
            })
            ccm_cluster.start()

            cls.ccm_cluster = ccm_cluster

    def test_connect_no_auth_provider(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=[CASSANDRA_IP])
        cluster.connect()
        cluster.refresh_nodes()
        down_hosts = [host for host in cluster.metadata.all_hosts() if not host.is_up]
        self.assertEqual(len(down_hosts), 1)
        cluster.shutdown()

    @classmethod
    def tearDownClass(cls):
        if not USE_CASS_EXTERNAL:
            cls.ccm_cluster.stop()
