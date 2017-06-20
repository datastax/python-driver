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


from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, HostStateListener

from tests.integration import use_singledc
from tests.integration.long.utils import start, stop


import time

def setup_module():
    use_singledc()


class TrackDownListener(HostStateListener):
    hosts_marked_down = []

    def on_down(self, host):
        self.hosts_marked_down.append(host)


class ConnectionTest(unittest.TestCase):

    def test_heart_beat_timeout(self):
        """
        Test to ensure the hosts are marked as down after a OTO is received.
        Also to ensure this happens within the expected timeout
        @since 3.10
        @jira_ticket PYTHON-762
        @expected_result all the hosts have been marked as down at some point

        @test_category metadata
        """
        idle_heartbeat_timeout = 5
        idle_heartbeat_interval = 1

        listener = TrackDownListener()
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(),
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          idle_heartbeat_timeout=idle_heartbeat_timeout,
                          executor_threads=16)
        cluster.connect(wait_for_all_pools=True)
        cluster.register_listener(listener)

        self.addCleanup(cluster.shutdown)

        [stop(i) for i in [1, 2, 3]]

        # We allow from some extra time for all the hosts to be to on_down
        time.sleep((idle_heartbeat_timeout + idle_heartbeat_interval) + 2)

        for host in cluster.metadata.all_hosts():
            self.assertIn(host, listener.hosts_marked_down)

        [start(i) for i in [1, 2, 3]]
