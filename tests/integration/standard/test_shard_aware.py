# Copyright 2020 ScyllaDB, Inc.
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
import os
import time
import random
from subprocess import run
import logging

try:
    from concurrent.futures import ThreadPoolExecutor, as_completed
except ImportError:
    from futures import ThreadPoolExecutor, as_completed  # noqa

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa
import pytest

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, ConstantReconnectionPolicy
from cassandra import OperationTimedOut, ConsistencyLevel

from tests.integration import use_cluster, get_node, PROTOCOL_VERSION

LOGGER = logging.getLogger(__name__)


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 4 --memory 2048M"
    use_cluster('shared_aware', [3], start=True)


class TestShardAwareIntegration(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1"], protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()
        LOGGER.info(cls.cluster.is_shard_aware())
        LOGGER.info(cls.cluster.shard_aware_stats())

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def verify_same_shard_in_tracing(self, results, shard_name):
        traces = results.get_query_trace()
        events = traces.events
        for event in events:
            LOGGER.info("%s %s", event.thread_name, event.description)
        for event in events:
            self.assertEqual(event.thread_name, shard_name)
        self.assertIn('querying locally', "\n".join([event.description for event in events]))

        trace_id = results.response_future.get_query_trace_ids()[0]
        traces = self.session.execute("SELECT * FROM system_traces.events WHERE session_id = %s", (trace_id,))
        events = [event for event in traces]
        for event in events:
            LOGGER.info("%s %s", event.thread, event.activity)
        for event in events:
            self.assertEqual(event.thread, shard_name)
        self.assertIn('querying locally', "\n".join([event.activity for event in events]))

    def create_ks_and_cf(self):
        self.session.execute(
            """
            DROP KEYSPACE IF EXISTS preparedtests
            """
        )
        self.session.execute(
            """
            CREATE KEYSPACE preparedtests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
            """)

        self.session.execute("USE preparedtests")
        self.session.execute(
            """
            CREATE TABLE cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)

    @staticmethod
    def create_data(session):
        session.execute("USE preparedtests")
        prepared = session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        bound = prepared.bind(('a', 'b', 'c'))
        session.execute(bound)
        bound = prepared.bind(('e', 'f', 'g'))
        session.execute(bound)
        bound = prepared.bind(('100000', 'f', 'g'))
        session.execute(bound)

    def query_data(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            SELECT * FROM cf0 WHERE a=? AND b=?
            """)

        bound = prepared.bind(('a', 'b'))
        results = session.execute(bound, trace=True)
        self.assertEqual(results, [('a', 'b', 'c')])
        if verify_in_tracing:
            self.verify_same_shard_in_tracing(results, "shard 1")

        bound = prepared.bind(('100000', 'f'))
        results = session.execute(bound, trace=True)
        self.assertEqual(results, [('100000', 'f', 'g')])

        if verify_in_tracing:
            self.verify_same_shard_in_tracing(results, "shard 0")

        bound = prepared.bind(('e', 'f'))
        results = session.execute(bound, trace=True)

        if verify_in_tracing:
            self.verify_same_shard_in_tracing(results, "shard 1")

    def test_all_tracing_coming_one_shard(self):
        """
        Testing that shard aware driver is sending the requests to the correct shards

        using the traces to validate that all the action been executed on the the same shard.
        this test is using prepared SELECT statements for this validation
        """

        self.create_ks_and_cf()
        self.create_data(self.session)
        self.query_data(self.session)

    def test_connect_from_multiple_clients(self):
        """
        verify that connection from multiple clients at the same time, are handled gracefully even
        if shard are randomly(round-robin) acquired
        """
        self.create_ks_and_cf()

        number_of_clients = 15
        session_list = [self.session] + [self.cluster.connect() for _ in range(number_of_clients)]

        with ThreadPoolExecutor(number_of_clients) as pool:
            futures = [pool.submit(self.create_data, session) for session in session_list]
            for result in as_completed(futures):
                print(result)

            futures = [pool.submit(self.query_data, session) for session in session_list]
            for result in as_completed(futures):
                print(result)

    def test_closing_connections(self):
        """
        Verify that reconnection is working as expected, when connection are being closed.
        """
        self.create_ks_and_cf()
        self.create_data(self.session)
        self.query_data(self.session)

        for i in range(6):
            assert self.session._pools
            pool = list(self.session._pools.values())[0]
            if not pool._connections:
                continue
            shard_id = random.choice(list(pool._connections.keys()))
            pool._connections.get(shard_id).close()
            time.sleep(5)
            self.query_data(self.session, verify_in_tracing=False)

        time.sleep(10)
        self.query_data(self.session)

    @pytest.mark.skip
    def test_blocking_connections(self):
        """
        Verify that reconnection is working as expected, when connection are being blocked.
        """
        res = run('which iptables'.split(' '))
        if not res.returncode == 0:
            self.skipTest("iptables isn't installed")

        self.create_ks_and_cf()
        self.create_data(self.session)
        self.query_data(self.session)

        node1_ip_address, node1_port = get_node(1).network_interfaces['binary']

        def remove_iptables():
            run(('sudo iptables -t filter -D INPUT -p tcp --dport {node1_port} '
                 '--destination {node1_ip_address}/32 -j REJECT --reject-with icmp-port-unreachable'
                 ).format(node1_ip_address=node1_ip_address, node1_port=node1_port).split(' ')
                )

        self.addCleanup(remove_iptables)

        for i in range(3):
            run(('sudo iptables -t filter -A INPUT -p tcp --dport {node1_port} '
                 '--destination {node1_ip_address}/32 -j REJECT --reject-with icmp-port-unreachable'
                 ).format(node1_ip_address=node1_ip_address, node1_port=node1_port).split(' ')
                )
            time.sleep(5)
            try:
                self.query_data(self.session, verify_in_tracing=False)
            except OperationTimedOut:
                pass
            remove_iptables()
            time.sleep(5)
            self.query_data(self.session, verify_in_tracing=False)

        self.query_data(self.session)
