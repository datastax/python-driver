import logging
import unittest
from cassandra import OperationType, RateLimitReached
from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy

from tests.integration import PROTOCOL_VERSION, use_cluster
import pytest

LOGGER = logging.getLogger(__name__)

def setup_module():
    use_cluster('rate_limit', [3], start=True)

class TestRateLimitExceededException(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1"], protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def test_rate_limit_exceeded(self):
        self.session.execute(
            """
            DROP KEYSPACE IF EXISTS ratetests
            """
        )
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS ratetests
            WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
            """)

        self.session.execute("USE ratetests")
        self.session.execute(
            """
            CREATE TABLE tbl (pk int PRIMARY KEY, v int)
            WITH per_partition_rate_limit = {'max_writes_per_second': 1}
            """)

        prepared = self.session.prepare(
            """
            INSERT INTO tbl (pk, v) VALUES (?, ?)
            """)

        # The rate limit is 1 write/s, so repeat the same query
        # until an error occurs, it should happen quickly
        def execute_write():
            for _ in range(1000):
                self.session.execute(prepared.bind((123, 456)))

        with pytest.raises(RateLimitReached) as context:
            execute_write()

        assert context.value.op_type == OperationType.Write
