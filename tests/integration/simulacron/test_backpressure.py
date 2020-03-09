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
import select
import time

from cassandra import OperationTimedOut
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy
from tests.integration import requiressimulacron
from tests.integration.simulacron import SimulacronBase, PROTOCOL_VERSION
from tests.integration.simulacron.utils import ResumeReads, PauseReads, prime_request, start_and_prime_singledc


@requiressimulacron
class TCPBackpressureTests(SimulacronBase):
    def setUp(self):
        self.callback_successes = 0
        self.callback_errors = 0

    def callback_success(self, results):
        self.callback_successes += 1

    def callback_error(self, results):
        self.callback_errors += 1

    def _fill_buffers(self, session, query, expected_blocked=3):
        futures = []
        buffer = '1' * 10000
        for _ in range(1000):
            for i in range(100):
                future = session.execute_async(query, [buffer])
                future.add_callbacks(callback=self.callback_success, errback=self.callback_error)
                futures.append(future)

            total_blocked = 0
            for pool in session.get_pools():
                # The transport conditional here is for twisted
                socket = pool._connection.transport.socket if hasattr(pool._connection, 'transport') else pool._connection._socket
                readable, writeable, err = select.select([], [socket], [], 0)
                if not writeable:
                    total_blocked += 1
            if total_blocked == expected_blocked:
                break
            # If we go nuts writing, we can actually fill the entire TCP send buffer and use all of our
            # request IDs before the kernel tells us we can't write anymore
            time.sleep(.01)
        else:
            raise Exception("Unable to fill TCP send buffer on expected number of nodes")
        return futures

    def test_paused_connections(self):
        """  Verify all requests come back as expected if node resumes within query timeout """
        start_and_prime_singledc()
        profile = ExecutionProfile(request_timeout=500, load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads())
        futures = self._fill_buffers(session, query)

        # We're trying to verify here that the application is still queuing requests even though
        # TCP isn't letting us send any additional data out the socket.
        # In the other drivers, they inspect the application write queue buffer directly to verify
        # that no more data is being sent out. Since that implementation is hidden behind the various
        # event loops, it's easier (though perhaps slightly less precise) to check that the number
        # of inflight requests is positive and hasn't changed within a short period of time
        pools = session.get_pools()
        old_in_flight_per_connection = [pool._connection.in_flight for pool in pools]
        time.sleep(.5)
        current_in_flight_per_connection = [pool._connection.in_flight for pool in pools]
        for old_in_flight, current_in_flight in zip(old_in_flight_per_connection, current_in_flight_per_connection):
            self.assertEqual(old_in_flight, current_in_flight)
            self.assertGreater(current_in_flight, 0)

        prime_request(ResumeReads())

        for future in futures:
            future.result()

        # Verify that we can continue sending queries without any problems
        for host in session.cluster.metadata.all_hosts():
            session.execute(query, ["a"], host=host)

    def test_traffic_to_non_paused_nodes(self):
        """ Verify when one node is paused, traffic is still routed to other nodes """
        start_and_prime_singledc()
        profile = ExecutionProfile(request_timeout=500, load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads(dc_id=0, node_id=1))
        futures = self._fill_buffers(session, query, expected_blocked=1)

        # Wait for the two non-blocked hosts to get to 0 in-flight requests
        for i in range(1000):
            in_flight = [pool._connection.in_flight for pool in session.get_pools()]
            if len([num for num in in_flight if num == 0]) == 2:
                break
            time.sleep(.1)
        else:
            raise Exception("Expected two nodes to drop to 0 in-flight requests")

        prime_request(ResumeReads())

        for future in futures:
            future.result()

        in_flight = [pool._connection.in_flight for pool in session.get_pools()]
        self.assertTrue(len([num for num in in_flight if num == 0]) == 3)

        # Verify that we can continue sending queries without any problems
        for host in session.cluster.metadata.all_hosts():
            session.execute(query, ["a"], host=host)

    def test_queued_requests_timeout(self):
        """ Verify that queued requests timeout as expected """
        start_and_prime_singledc()
        profile = ExecutionProfile(request_timeout=.1, load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads())

        futures = self._fill_buffers(session, query)
        time.sleep(.1)

        successes = 0
        for future in futures:
            try:
                future.result()
                successes += 1
            except OperationTimedOut:
                pass

        # Simulacron will respond to a couple queries before cutting off reads, so we'll just verify
        # that only "a few" successes happened here
        self.assertLess(successes, 50)
        self.assertLess(self.callback_successes, 50)
        self.assertEqual(self.callback_errors, len(futures) - self.callback_successes)
