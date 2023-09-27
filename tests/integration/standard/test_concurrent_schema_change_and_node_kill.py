import os
import logging
import unittest

from tests.integration import use_cluster, get_node, local, TestCluster

LOGGER = logging.getLogger(__name__)


def setup_module():
    use_cluster('test_concurrent_schema_change_and_node_kill', [3], start=True)

@local
class TestConcurrentSchemaChangeAndNodeKill(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster(max_schema_agreement_wait=120)
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def test_schema_change_after_node_kill(self):
        node2 = get_node(2)
        self.session.execute(
            "DROP KEYSPACE IF EXISTS ks_deadlock;")
        self.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS ks_deadlock "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2' };")
        self.session.set_keyspace('ks_deadlock')
        self.session.execute("CREATE TABLE IF NOT EXISTS some_table(k int, c int, v int, PRIMARY KEY (k, v));")
        self.session.execute("INSERT INTO some_table (k, c, v) VALUES (1, 2, 3);")
        node2.stop(wait=False, gently=False)
        self.session.execute("ALTER TABLE some_table ADD v2 int;", timeout=180)
        print(self.session.execute("SELECT * FROM some_table WHERE k = 1;").all())
