# Copyright 2013-2016 DataStax, Inc.
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

import time

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel, WriteTimeout, Unavailable, ReadTimeout

from cassandra.cluster import Cluster, NoHostAvailable
from tests.integration import get_cluster, get_node, use_singledc, PROTOCOL_VERSION, execute_until_pass, \
    CONTACT_POINTS, notipv6


def setup_module():
    use_singledc()


class MetricsTests(unittest.TestCase):

    def setUp(self):
        self.cluster = Cluster(metrics_enabled=True, protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        self.session = self.cluster.connect("test3rf")

    def tearDown(self):
        self.cluster.shutdown()

    # @notipv6
    def test_connection_error(self):
        """
        Trigger and ensure connection_errors are counted
        Stop all node with the driver knowing about the "DOWN" states.
        """


        # Test writes
        for i in range(0, 100):
            self.session.execute_async("INSERT INTO test (k, v) VALUES ({0}, {1})".format(i, i))

        # Stop the cluster
        get_cluster().stop(wait=True, gently=False)

        try:
            # Ensure the nodes are actually down
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(NoHostAvailable):
                self.session.execute(query)
        finally:
            get_cluster().start(wait_for_binary_proto=True, wait_other_notice=True)
            # Give some time for the cluster to come back up, for the next test
            time.sleep(5)

        self.assertGreater(self.cluster.metrics.stats.connection_errors, 0)

    # @notipv6
    def test_write_timeout(self):
        """
        Trigger and ensure write_timeouts are counted
        Write a key, value pair. Pause a node without the coordinator node knowing about the "DOWN" state.
        Attempt a write at cl.ALL and receive a WriteTimeout.
        """

        # Test write
        self.session.execute("INSERT INTO test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT * FROM test WHERE k=1", consistency_level=ConsistencyLevel.ALL)
        results = execute_until_pass(self.session, query)
        self.assertTrue(results)

        # Pause node so it shows as unreachable to coordinator
        get_node(1).pause()

        try:
            # Test write
            query = SimpleStatement("INSERT INTO test (k, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(WriteTimeout):
                self.session.execute(query, timeout=None)
            self.assertEqual(1, self.cluster.metrics.stats.write_timeouts)

        finally:
            get_node(1).resume()

    # @notipv6
    def test_read_timeout(self):
        """
        Trigger and ensure read_timeouts are counted
        Write a key, value pair. Pause a node without the coordinator node knowing about the "DOWN" state.
        Attempt a read at cl.ALL and receive a ReadTimeout.
        """


        # Test write
        self.session.execute("INSERT INTO test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT * FROM test WHERE k=1", consistency_level=ConsistencyLevel.ALL)
        results = execute_until_pass(self.session, query)
        self.assertTrue(results)

        # Pause node so it shows as unreachable to coordinator
        get_node(1).pause()

        try:
            # Test read
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(ReadTimeout):
                self.session.execute(query, timeout=None)
            self.assertEqual(1, self.cluster.metrics.stats.read_timeouts)

        finally:
            get_node(1).resume()


    # @notipv6
    def test_unavailable(self):
        """
        Trigger and ensure unavailables are counted
        Write a key, value pair. Stop a node with the coordinator node knowing about the "DOWN" state.
        Attempt an insert/read at cl.ALL and receive a Unavailable Exception.
        """

        # Test write
        self.session.execute("INSERT INTO test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT * FROM test WHERE k=1", consistency_level=ConsistencyLevel.ALL)
        results = execute_until_pass(self.session, query)
        self.assertTrue(results)

        # Stop node gracefully
        get_node(1).stop(wait=True, wait_other_notice=True)

        try:
            # Test write
            query = SimpleStatement("INSERT INTO test (k, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(Unavailable):
                self.session.execute(query)
            self.assertEqual(1, self.cluster.metrics.stats.unavailables)

            # Test write
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(Unavailable):
                self.session.execute(query, timeout=None)
            self.assertEqual(2, self.cluster.metrics.stats.unavailables)
        finally:
            get_node(1).start(wait_other_notice=True, wait_for_binary_proto=True)
            # Give some time for the cluster to come back up, for the next test
            time.sleep(5)

        self.cluster.shutdown()

    # def test_other_error(self):
    #     # TODO: Bootstrapping or Overloaded cases
    #     pass
    #
    # def test_ignore(self):
    #     # TODO: Look for ways to generate ignores
    #     pass
    #
    # def test_retry(self):
    #     # TODO: Look for ways to generate retries
    #     pass
