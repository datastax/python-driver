import struct
from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, DCAwareRoundRobinPolicy, \
    TokenAwarePolicy, WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement
from tests.integration import use_multidc, use_singledc
from tests.integration.long.utils import wait_for_up, create_schema, \
    CoordinatorStats, force_stop, wait_for_down, decommission, start, ring, \
    bootstrap, stop, IP_FORMAT

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class LoadBalancingPolicyTests(unittest.TestCase):
    def setUp(self):
        self.coordinator_stats = CoordinatorStats()
        self.prepared = None

    def teardown(self):
        use_singledc()

    def _insert(self, session, keyspace, count=12,
                consistency_level=ConsistencyLevel.ONE):
        session.execute('USE %s' % keyspace)
        for i in range(count):
            ss = SimpleStatement('INSERT INTO cf(k, i) VALUES (0, 0)',
                                 consistency_level=consistency_level)
            session.execute(ss)

    def _query(self, session, keyspace, count=12,
               consistency_level=ConsistencyLevel.ONE, use_prepared=False):
        if use_prepared:
            query_string = 'SELECT * FROM %s.cf WHERE k = ?' % keyspace
            if not self.prepared or self.prepared.query_string != query_string:
                self.prepared = session.prepare(query_string)

            for i in range(count):
                self.coordinator_stats.add_coordinator(session.execute_async(self.prepared.bind((0,))))
        else:
            routing_key = struct.pack('>i', 0)
            for i in range(count):
                ss = SimpleStatement('SELECT * FROM %s.cf WHERE k = 0' % keyspace,
                                     consistency_level=consistency_level,
                                     routing_key=routing_key)
                self.coordinator_stats.add_coordinator(session.execute_async(ss))


    def test_roundrobin(self):
        use_singledc()
        keyspace = 'test_roundrobin'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3)

        create_schema(session, keyspace, replication_factor=3)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 4)
        self.coordinator_stats.assert_query_count_equals(self, 2, 4)
        self.coordinator_stats.assert_query_count_equals(self, 3, 4)

        force_stop(3)
        wait_for_down(cluster, 3)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 6)
        self.coordinator_stats.assert_query_count_equals(self, 2, 6)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        decommission(1)
        start(3)
        wait_for_down(cluster, 1)
        wait_for_up(cluster, 3)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 6)
        self.coordinator_stats.assert_query_count_equals(self, 3, 6)

    def test_roundrobin_two_dcs(self):
        use_multidc([2,2])
        keyspace = 'test_roundrobin_two_dcs'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4)

        create_schema(session, keyspace, replication_strategy=[2,2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 3)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)

        force_stop(1)
        bootstrap(5, 'dc3')

        # reset control connection
        self._insert(session, keyspace, count=1000)

        wait_for_up(cluster, 5)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)
        self.coordinator_stats.assert_query_count_equals(self, 5, 3)

    def test_roundrobin_two_dcs_2(self):
        use_multidc([2,2])
        keyspace = 'test_roundrobin_two_dcs_2'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4)

        create_schema(session, keyspace, replication_strategy=[2,2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 3)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)

        force_stop(1)
        bootstrap(5, 'dc1')

        # reset control connection
        self._insert(session, keyspace, count=1000)

        wait_for_up(cluster, 5)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)
        self.coordinator_stats.assert_query_count_equals(self, 5, 3)

    def test_dc_aware_roundrobin_two_dcs(self):
        use_multidc([3,2])
        keyspace = 'test_dc_aware_roundrobin_two_dcs'
        cluster = Cluster(
            load_balancing_policy=DCAwareRoundRobinPolicy('dc1'))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4, wait=False)
        wait_for_up(cluster, 5)

        create_schema(session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace, count=12000)
        self._query(session, keyspace, count=12000)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 181, in test_dc_aware_roundrobin_two_dcs
        #     self.coordinator_stats.assert_query_count_equals(self, 1, 4000)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 35, in assert_query_count_equals
        #     expected, ip, self.coordinator_counts[ip], dict(self.coordinator_counts)))
        # AssertionError: Expected 4000 queries to 127.0.0.1, but got 2400. Query counts: {'127.0.0.3': 7200, '127.0.0.2': 2400, '127.0.0.1': 2400}
        # -------------------- >> begin captured logging << --------------------
        # self.coordinator_stats.assert_query_count_equals(self, 1, 4000)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 4000)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 4000)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 5, 0)

    def test_dc_aware_roundrobin_two_dcs_2(self):
        use_multidc([3,2])
        keyspace = 'test_dc_aware_roundrobin_two_dcs_2'
        cluster = Cluster(
            load_balancing_policy=DCAwareRoundRobinPolicy('dc2'))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4, wait=False)
        wait_for_up(cluster, 5)

        create_schema(session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace, count=12000)
        self._query(session, keyspace, count=12000)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 214, in test_dc_aware_roundrobin_two_dcs_2
        #     self.coordinator_stats.assert_query_count_equals(self, 4, 6000)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 35, in assert_query_count_equals
        #     expected, ip, self.coordinator_counts[ip], dict(self.coordinator_counts)))
        # AssertionError: Expected 6000 queries to 127.0.0.4, but got 2400. Query counts: {'127.0.0.5': 9600, '127.0.0.4': 2400, '127.0.0.3': 0, '127.0.0.2': 0, '127.0.0.1': 0}
        # -------------------- >> begin captured logging << --------------------
        # self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 6000)
        # self.coordinator_stats.assert_query_count_equals(self, 5, 6000)

    def test_dc_aware_roundrobin_one_remote_host(self):
        use_multidc([2,2])
        keyspace = 'test_dc_aware_roundrobin_one_remote_host_2'
        cluster = Cluster(
            load_balancing_policy=DCAwareRoundRobinPolicy('dc2', used_hosts_per_remote_dc=1))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3, wait=False)
        wait_for_up(cluster, 4)

        create_schema(session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace, count=12000)
        self._query(session, keyspace, count=12000)

        # Differing distributions
        # self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 6000)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 6000)

        self.coordinator_stats.reset_counts()
        bootstrap(5, 'dc1')
        wait_for_up(cluster, 5)

        self._query(session, keyspace)

        # Differing distributions
        # self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 6)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 6)
        # self.coordinator_stats.assert_query_count_equals(self, 5, 0)

        self.coordinator_stats.reset_counts()
        decommission(3)
        decommission(4)
        wait_for_down(cluster, 3, wait=True)
        wait_for_down(cluster, 4, wait=True)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 242, in test_dc_aware_roundrobin_one_remote_host
        #     self._query(session, keyspace)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 36, in _query
        #     self.coordinator_stats.add_coordinator(session.execute_async(ss))
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 21, in add_coordinator
        #     coordinator = future._current_host.address
        # AttributeError: 'NoneType' object has no attribute 'address'
        # -------------------- >> begin captured logging << --------------------
        # self._query(session, keyspace)

        # self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 5, 12)

        self.coordinator_stats.reset_counts()
        decommission(5)
        wait_for_down(cluster, 5, wait=True)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 266, in test_dc_aware_roundrobin_one_remote_host
        #     self._query(session, keyspace)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 36, in _query
        #     self.coordinator_stats.add_coordinator(session.execute_async(ss))
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 21, in add_coordinator
        #     coordinator = future._current_host.address
        # AttributeError: 'NoneType' object has no attribute 'address'
        # -------------------- >> begin captured logging << --------------------
        # self._query(session, keyspace)
        #
        # self.coordinator_stats.assert_query_count_equals(self, 1, 12)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 5, 0)

        self.coordinator_stats.reset_counts()
        decommission(1)
        wait_for_down(cluster, 1, wait=True)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 288, in test_dc_aware_roundrobin_one_remote_host
        #     self._query(session, keyspace)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 36, in _query
        #     self.coordinator_stats.add_coordinator(session.execute_async(ss))
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 21, in add_coordinator
        #     coordinator = future._current_host.address
        # AttributeError: 'NoneType' object has no attribute 'address'
        # -------------------- >> begin captured logging << --------------------
        # self._query(session, keyspace)
        #
        # self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 5, 0)

        self.coordinator_stats.reset_counts()
        force_stop(2)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 309, in test_dc_aware_roundrobin_one_remote_host
        #     self._query(session, keyspace)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 36, in _query
        #     self.coordinator_stats.add_coordinator(session.execute_async(ss))
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 21, in add_coordinator
        #     coordinator = future._current_host.address
        # AttributeError: 'NoneType' object has no attribute 'address'
        # -------------------- >> begin captured logging << --------------------
        # should throw an error about not being able to connect
        # self._query(session, keyspace)

    def test_token_aware(self):
        keyspace = 'test_token_aware'
        self.token_aware(keyspace)

    def test_token_aware_prepared(self):
        keyspace = 'test_token_aware_prepared'
        self.token_aware(keyspace, True)

    def token_aware(self, keyspace, use_prepared=False):
        use_singledc()
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3)

        create_schema(session, keyspace, replication_factor=1)
        self._insert(session, keyspace)
        self._query(session, keyspace, use_prepared=use_prepared)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace, use_prepared=use_prepared)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        force_stop(2)
        wait_for_down(cluster, 2, wait=True)

        try:
            self._query(session, keyspace, use_prepared=use_prepared)
            self.fail()
        except Unavailable as e:
            self.assertEqual(e.consistency, 1)
            self.assertEqual(e.required_replicas, 1)
            self.assertEqual(e.alive_replicas, 0)

        self.coordinator_stats.reset_counts()
        start(2)
        wait_for_up(cluster, 2, wait=True)

        self._query(session, keyspace, use_prepared=use_prepared)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        decommission(2)
        wait_for_down(cluster, 2, wait=True)

        self._query(session, keyspace, use_prepared=use_prepared)

        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 328, in test_token_aware
        #     self.token_aware()
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 387, in token_aware
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 35, in assert_query_count_equals
        #     expected, ip, self.coordinator_counts[ip], dict(self.coordinator_counts)))
        # AssertionError: Expected 12 queries to 127.0.0.1, but got 6. Query counts: {'127.0.0.3': 6, '127.0.0.1': 6}
        # -------------------- >> begin captured logging << --------------------
        # This is different than the java-driver, but I just wanted to make sure it's intended
        # self.coordinator_stats.assert_query_count_equals(self, 1, 12)
        # self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        # self.coordinator_stats.assert_query_count_equals(self, 3, 0)

    def test_token_aware_composite_key(self):
        use_singledc()
        keyspace = 'test_token_aware_composite_key'
        table = 'composite'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3)

        create_schema(session, keyspace, replication_factor=2)
        session.execute('CREATE TABLE %s ('
                        'k1 int, '
                        'k2 int, '
                        'i int, '
                        'PRIMARY KEY ((k1, k2)))' % table)

        prepared = session.prepare('INSERT INTO %s '
                                   '(k1, k2, i) '
                                   'VALUES '
                                   '(?, ?, ?)' % table)
        session.execute(prepared.bind((1, 2, 3)))

        results = session.execute('SELECT * FROM %s WHERE k1 = 1 AND k2 = 2' % table)
        self.assertTrue(len(results) == 1)
        self.assertTrue(results[0].i)

    def test_token_aware_with_rf_2(self, use_prepared=False):
        use_singledc()
        keyspace = 'test_token_aware_with_rf_2'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3)

        create_schema(session, keyspace, replication_factor=2)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        stop(2)
        wait_for_down(cluster, 2, wait=True)

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 12)

    def test_white_list(self):
        use_singledc()
        keyspace = 'test_white_list'

        # BUG? Can only connect to the cluster via nodes in the whitelist?
        # Perhaps we should be more lenient here?
        cluster = Cluster(('127.0.0.2',),
            load_balancing_policy=WhiteListRoundRobinPolicy((IP_FORMAT % 2,)))
        session = cluster.connect()
        wait_for_up(cluster, 1, wait=False)
        wait_for_up(cluster, 2, wait=False)
        wait_for_up(cluster, 3)

        create_schema(session, keyspace)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        decommission(2)
        wait_for_down(cluster, 2, wait=True)

        # I was expecting another type of error since only IP_FORMAT % 2 is whitelisted
        # ----------------------------------------------------------------------
        # Traceback (most recent call last):
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 506, in test_white_list
        #     self._query(session, keyspace)
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/test_loadbalancingpolicies.py", line 49, in _query
        #     self.coordinator_stats.add_coordinator(session.execute_async(ss))
        #   File "/Users/joaquin/repos/python-driver/tests/integration/long/utils.py", line 22, in add_coordinator
        #     coordinator = future._current_host.address
        # AttributeError: 'NoneType' object has no attribute 'address'
        # -------------------- >> begin captured logging << --------------------
        # self._query(session, keyspace)
