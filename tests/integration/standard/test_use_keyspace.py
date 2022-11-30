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

from mock import patch

from cassandra.connection import Connection
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, ConstantReconnectionPolicy
from cassandra import OperationTimedOut, ConsistencyLevel

from tests.integration import use_cluster, get_node, PROTOCOL_VERSION

LOGGER = logging.getLogger(__name__)

def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
    use_cluster('shared_aware', [3], start=True)



class TestUseKeyspace(unittest.TestCase):
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
    
    def test_set_keyspace_slow_connection(self):
        # Test that "USE keyspace" gets propagated
        # to all connections.
        #
        # Reproduces an issue #187 where some pending
        # connections for shards would not 
        # receive "USE keyspace".
        #
        # Simulate that scenario by adding an artifical
        # delay before sending "USE keyspace" on
        # connections.

        original_set_keyspace_blocking = Connection.set_keyspace_blocking
        def patched_set_keyspace_blocking(*args, **kwargs):
            time.sleep(1)
            return original_set_keyspace_blocking(*args, **kwargs)

        with patch.object(Connection, "set_keyspace_blocking", patched_set_keyspace_blocking):
            self.session.execute("CREATE KEYSPACE test_set_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
            self.session.execute("CREATE TABLE test_set_keyspace.set_keyspace_slow_connection(pk int, PRIMARY KEY(pk))")

            session2 = self.cluster.connect()
            session2.execute("USE test_set_keyspace")
            for i in range(200):
                session2.execute(f"SELECT * FROM set_keyspace_slow_connection WHERE pk = 1")
