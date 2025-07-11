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

from cassandra.cluster import NoHostAvailable
from tests.integration import use_singledc, get_cluster, remove_cluster, local, TestCluster
from tests.util import wait_until, wait_until_not_raised

import unittest
import pytest


def setup_module():
    use_singledc(start=False)
    ccm_cluster = get_cluster()
    ccm_cluster.stop()
    config_options = {'native_transport_port': 9046}
    ccm_cluster.set_configuration_options(config_options)
    # can't use wait_for_binary_proto cause ccm tries on port 9042
    ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)


def teardown_module():
    remove_cluster()


class CustomClusterTests(unittest.TestCase):

    @local
    def test_connection_honor_cluster_port(self):
        """
        Test that the initial contact point and discovered nodes honor
        the cluster port on new connection.

        All hosts should be marked as up and we should be able to execute queries on it.
        """
        cluster = TestCluster()
        with pytest.raises(NoHostAvailable):
            cluster.connect()  # should fail on port 9042

        cluster = TestCluster(port=9046)
        session = cluster.connect(wait_for_all_pools=True)

        wait_until(lambda: len(cluster.metadata.all_hosts()) == 3, 1, 5)
        for host in cluster.metadata.all_hosts():
            assert host.is_up
            session.execute("select * from system.local where key='local'", host=host)
