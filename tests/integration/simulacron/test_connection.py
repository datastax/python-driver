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

import logging
import time

from concurrent.futures import ThreadPoolExecutor

from mock import Mock

from cassandra import OperationTimedOut
from cassandra.cluster import (EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile,
                               _Scheduler)
from cassandra.policies import HostStateListener, RoundRobinPolicy
from tests.integration import (CASSANDRA_VERSION, PROTOCOL_VERSION,
                               requiressimulacron)
from tests.integration.util import assert_quiescent_pool_state
from tests.integration.simulacron.utils import (NO_THEN, PrimeOptions,
                                                prime_query, prime_request,
                                                start_and_prime_cluster_defaults,
                                                start_and_prime_singledc,
                                                stop_simulacron,
                                                clear_queries)


class TrackDownListener(HostStateListener):
    hosts_marked_down = []

    def on_down(self, host):
        self.hosts_marked_down.append(host)

class ThreadTracker(ThreadPoolExecutor):
    called_functions = []

    def submit(self, fn, *args, **kwargs):
        self.called_functions.append(fn.__name__)
        return super(ThreadTracker, self).submit(fn, *args, **kwargs)

@requiressimulacron
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
        number_of_dcs = 3
        nodes_per_dc = 100

        query_to_prime = "INSERT INTO test3rf.test (k, v) VALUES (0, 1);"

        idle_heartbeat_timeout = 5
        idle_heartbeat_interval = 1

        start_and_prime_cluster_defaults(number_of_dcs, nodes_per_dc, CASSANDRA_VERSION)
        self.addCleanup(stop_simulacron)

        listener = TrackDownListener()
        executor = ThreadTracker(max_workers=16)

        # We need to disable compression since it's not supported in simulacron
        cluster = Cluster(compression=False,
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          idle_heartbeat_timeout=idle_heartbeat_timeout,
                          executor_threads=16,
                          execution_profiles={
                              EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())})
        self.addCleanup(cluster.shutdown)

        cluster.scheduler.shutdown()
        cluster.executor = executor
        cluster.scheduler = _Scheduler(executor)

        session = cluster.connect(wait_for_all_pools=True)
        cluster.register_listener(listener)

        log = logging.getLogger()
        log.setLevel('CRITICAL')
        self.addCleanup(log.setLevel, "DEBUG")

        prime_query(query_to_prime, then=NO_THEN)

        futures = []
        for _ in range(number_of_dcs * nodes_per_dc):
            future = session.execute_async(query_to_prime)
            futures.append(future)

        for f in futures:
            f._event.wait()
            self.assertIsInstance(f._final_exception, OperationTimedOut)

        prime_request(PrimeOptions(then=NO_THEN))

        # We allow from some extra time for all the hosts to be to on_down
        # The callbacks should start happening after idle_heartbeat_timeout + idle_heartbeat_interval
        time.sleep((idle_heartbeat_timeout + idle_heartbeat_interval) * 2)

        for host in cluster.metadata.all_hosts():
            self.assertIn(host, listener.hosts_marked_down)

        # In this case HostConnection._replace shouldn't be called
        self.assertNotIn("_replace", executor.called_functions)

        clear_queries()
        # We leave some time for the connections to recover
        time.sleep((idle_heartbeat_timeout + idle_heartbeat_interval) * 3)
        
        assert_quiescent_pool_state(self, cluster)

    def test_callbacks_and_pool_when_oto(self):
        """
        Test to ensure the callbacks are correcltly called and the connection
        is returned when there is an OTO
        @since 3.12
        @jira_ticket PYTHON-630
        @expected_result the connection is correctly returned to the pool
        after an OTO, also the only the errback is called and not the callback
        when the message finally arrives.

        @test_category metadata
        """
        start_and_prime_singledc()
        self.addCleanup(stop_simulacron)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)

        query_to_prime = "SELECT * from testkesypace.testtable"

        server_delay = 2  # seconds
        prime_query(query_to_prime, then={"delay_in_ms": server_delay * 1000})

        future = session.execute_async(query_to_prime, timeout=1)
        callback, errback = Mock(name='callback'), Mock(name='errback')
        future.add_callbacks(callback, errback)
        self.assertRaises(OperationTimedOut, future.result)

        assert_quiescent_pool_state(self, cluster)

        time.sleep(server_delay + 1)
        # PYTHON-630 -- only the errback should be called
        errback.assert_called_once()
        callback.assert_not_called()
