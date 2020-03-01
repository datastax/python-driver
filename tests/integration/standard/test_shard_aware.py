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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

from tests.integration import use_cluster


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 4 --memory 2048M"
    use_cluster('shared_aware', [1], start=True)


class TestShardAwareIntegration(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(protocol_version=4, load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def verify_same_shard_in_tracing(self, results, shard_name):
        traces = results.get_query_trace()
        events = traces.events
        for event in events:
            print(event.thread_name, event.description)
        for event in events:
            self.assertEqual(event.thread_name, shard_name)
        self.assertIn('querying locally', "\n".join([event.description for event in events]))

        trace_id = results.response_future.get_query_trace_ids()[0]
        traces = self.session.execute("SELECT * FROM system_traces.events WHERE session_id = %s", (trace_id, ))
        events = [event for event in traces]
        for event in events:
            print(event.thread, event.activity)
        for event in events:
            self.assertEqual(event.thread, shard_name)
        self.assertIn('querying locally', "\n".join([event.activity for event in events]))

    def test_all_tracing_coming_one_shard(self):
        """
        Testing that shard aware driver is sending the requests to the correct shards

        using the traces to validate that all the action been executed on the the same shard.
        this test is using prepared SELECT statements for this validation
        """

        self.session.execute(
            """
            DROP KEYSPACE IF EXISTS preparedtests
            """
        )
        self.session.execute(
            """
            CREATE KEYSPACE preparedtests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
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

        prepared = self.session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        bound = prepared.bind(('a', 'b', 'c'))

        self.session.execute(bound)

        bound = prepared.bind(('e', 'f', 'g'))

        self.session.execute(bound)

        bound = prepared.bind(('100000', 'f', 'g'))

        self.session.execute(bound)

        prepared = self.session.prepare(
            """
            SELECT * FROM cf0 WHERE a=? AND b=?
            """)

        bound = prepared.bind(('a', 'b'))
        results = self.session.execute(bound, trace=True)
        self.assertEqual(results, [('a', 'b', 'c')])

        self.verify_same_shard_in_tracing(results, "shard 1")

        bound = prepared.bind(('100000', 'f'))
        results = self.session.execute(bound, trace=True)
        self.assertEqual(results, [('100000', 'f', 'g')])

        self.verify_same_shard_in_tracing(results, "shard 0")

        bound = prepared.bind(('e', 'f'))
        results = self.session.execute(bound, trace=True)

        self.verify_same_shard_in_tracing(results, "shard 1")
