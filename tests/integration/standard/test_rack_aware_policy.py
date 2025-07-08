import logging
import unittest

from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy, RackAwareRoundRobinPolicy

from tests.integration import PROTOCOL_VERSION, get_cluster, use_multidc

LOGGER = logging.getLogger(__name__)

def setup_module():
    use_multidc({'DC1': {'RC1': 2, 'RC2': 2}, 'DC2': {'RC1': 3}})

class RackAwareRoundRobinPolicyTests(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=[node.address() for node in get_cluster().nodelist()], protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=RackAwareRoundRobinPolicy("DC1", "RC1", used_hosts_per_remote_dc=0),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()
        cls.create_ks_and_cf(cls)
        cls.create_data(cls.session)
        cls.node1, cls.node2, cls.node3, cls.node4, cls.node5, cls.node6, cls.node7 = get_cluster().nodes.values()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def create_ks_and_cf(self):
        self.session.execute(
            """
            DROP KEYSPACE IF EXISTS test1
            """
        )
        self.session.execute(
            """
            CREATE KEYSPACE test1
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'replication_factor': 3
            }
            """)

        self.session.execute(
            """
            CREATE TABLE test1.table1 (pk int, ck int, v int, PRIMARY KEY (pk, ck));
            """)

    @staticmethod
    def create_data(session):
        prepared = session.prepare(
            """
            INSERT INTO test1.table1 (pk, ck, v) VALUES (?, ?, ?)
            """)

        for i in range(50):
            bound = prepared.bind((i, i%5, i%2))
            session.execute(bound)

    def test_rack_aware(self):
        prepared = self.session.prepare(
            """
            SELECT pk, ck, v FROM test1.table1 WHERE pk = ?
            """)

        for i in range (10):
            bound = prepared.bind([i])
            results = self.session.execute(bound)
            assert results == [(i, i%5, i%2)]
            coordinator = str(results.response_future.coordinator_host.endpoint)
            assert coordinator in set(["127.0.0.1:9042", "127.0.0.2:9042"])

        self.node2.stop(wait_other_notice=True, gently=True)

        for i in range (10):
            bound = prepared.bind([i])
            results = self.session.execute(bound)
            assert results == [(i, i%5, i%2)]
            coordinator =str(results.response_future.coordinator_host.endpoint)
            assert coordinator == "127.0.0.1:9042"

        self.node1.stop(wait_other_notice=True, gently=True)

        for i in range (10):
            bound = prepared.bind([i])
            results = self.session.execute(bound)
            assert results == [(i, i%5, i%2)]
            coordinator = str(results.response_future.coordinator_host.endpoint)
            assert coordinator in set(["127.0.0.3:9042", "127.0.0.4:9042"])
