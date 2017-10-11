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

from tests.integration import use_singledc

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import HostFilterPolicy, RoundRobinPolicy,  SimpleConvictionPolicy, WhiteListRoundRobinPolicy
from cassandra.pool import Host

from tests.integration import PROTOCOL_VERSION, local

from concurrent.futures import wait as wait_futures


def setup_module():
    use_singledc()


class HostFilterPolicyTests(unittest.TestCase):

    def test_predicate_changes(self):
        """
        Test to validate hostfilter reacts correctly when the predicate return
        a different subset of the hosts
        HostFilterPolicy
        @since 3.8
        @jira_ticket PYTHON-961
        @expected_result the excluded hosts are ignored

        @test_category policy
        """
        external_event = True
        contact_point = "127.0.0.1"

        single_host = {Host(contact_point, SimpleConvictionPolicy)}
        all_hosts = {Host("127.0.0.{}".format(i), SimpleConvictionPolicy) for i in (1, 2, 3)}

        predicate = lambda host: host.address == contact_point if external_event else True
        cluster = Cluster((contact_point,), load_balancing_policy=HostFilterPolicy(RoundRobinPolicy(),
                                                                                 predicate=predicate),
                          protocol_version=PROTOCOL_VERSION, topology_event_refresh_window=0,
                          status_event_refresh_window=0)
        session = cluster.connect(wait_for_all_pools=True)

        queried_hosts = set()
        for _ in range(10):
            response = session.execute("SELECT * from system.local")
            queried_hosts.update(response.response_future.attempted_hosts)

        self.assertEqual(queried_hosts, single_host)

        external_event = False
        futures = session.update_created_pools()
        wait_futures(futures, timeout=cluster.connect_timeout)

        queried_hosts = set()
        for _ in range(10):
            response = session.execute("SELECT * from system.local")
            queried_hosts.update(response.response_future.attempted_hosts)
        self.assertEqual(queried_hosts, all_hosts)


class WhiteListRoundRobinPolicyTests(unittest.TestCase):

    @local
    def test_only_connects_to_subset(self):
        only_connect_hosts = {"127.0.0.1", "127.0.0.2"}
        white_list = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(only_connect_hosts))
        cluster = Cluster(execution_profiles={"white_list": white_list})
        #cluster = Cluster(load_balancing_policy=WhiteListRoundRobinPolicy(only_connect_hosts))
        session = cluster.connect(wait_for_all_pools=True)
        queried_hosts = set()
        for _ in range(10):
            response = session.execute('SELECT * from system.local', execution_profile="white_list")
            queried_hosts.update(response.response_future.attempted_hosts)
        queried_hosts = set(host.address for host in queried_hosts)
        self.assertEqual(queried_hosts, only_connect_hosts)
