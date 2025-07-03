import time

import pytest

from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy

from tests.integration import PROTOCOL_VERSION, use_cluster
from tests.unit.test_host_connection_pool import LOGGER

CCM_CLUSTER = None

def setup_module():
    global CCM_CLUSTER

    CCM_CLUSTER = use_cluster('tablets', [3], start=True)


class TestTabletsIntegration:
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1", "127.0.0.2", "127.0.0.3"], protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()
        cls.create_ks_and_cf(cls.session)
        cls.create_data(cls.session)

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def verify_hosts_in_tracing(self, results, expected):
        traces = results.get_query_trace()
        events = traces.events
        host_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s %s", event.source, event.thread_name, event.description)
            host_set.add(event.source)

        assert len(host_set) == expected
        assert 'locally' in "\n".join([event.description for event in events])

        trace_id = results.response_future.get_query_trace_ids()[0]
        traces = self.session.execute("SELECT * FROM system_traces.events WHERE session_id = %s", (trace_id,))
        events = [event for event in traces]
        host_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s", event.source, event.activity)
            host_set.add(event.source)

        assert len(host_set) == expected
        assert 'locally' in "\n".join([event.activity for event in events])

    def get_tablet_record(self, query):
        metadata = self.session.cluster.metadata
        return metadata._tablets.get_tablet_for_key(query.keyspace, query.table, metadata.token_map.token_class.from_key(query.routing_key))

    def verify_same_shard_in_tracing(self, results):
        traces = results.get_query_trace()
        events = traces.events
        shard_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s %s", event.source, event.thread_name, event.description)
            shard_set.add(event.thread_name)

        assert len(shard_set) == 1
        assert 'locally' in "\n".join([event.description for event in events])

        trace_id = results.response_future.get_query_trace_ids()[0]
        traces = self.session.execute("SELECT * FROM system_traces.events WHERE session_id = %s", (trace_id,))
        events = [event for event in traces]
        shard_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s", event.thread, event.activity)
            shard_set.add(event.thread)

        assert len(shard_set) == 1
        assert 'locally' in "\n".join([event.activity for event in events])

    @classmethod
    def create_ks_and_cf(cls, session):
        session.execute(
            """
            DROP KEYSPACE IF EXISTS test1
            """
        )
        session.execute(
            """
            CREATE KEYSPACE test1
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'replication_factor': 2
            } AND tablets = {
                'initial': 8
            }
            """)

        session.execute(
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

    def query_data_shard_select(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            SELECT pk, ck, v FROM test1.table1 WHERE pk = ?
            """)

        bound = prepared.bind([(2)])
        results = session.execute(bound, trace=True)
        assert results == [(2, 2, 0)]
        if verify_in_tracing:
            self.verify_same_shard_in_tracing(results)

    def query_data_host_select(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            SELECT pk, ck, v FROM test1.table1 WHERE pk = ?
            """)

        bound = prepared.bind([(2)])
        results = session.execute(bound, trace=True)
        assert results == [(2, 2, 0)]
        if verify_in_tracing:
            self.verify_hosts_in_tracing(results, 1)

    def query_data_shard_insert(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            INSERT INTO test1.table1 (pk, ck, v) VALUES (?, ?, ?)
            """)

        bound = prepared.bind([(51), (1), (2)])
        results = session.execute(bound, trace=True)
        if verify_in_tracing:
            self.verify_same_shard_in_tracing(results)

    def query_data_host_insert(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            INSERT INTO test1.table1 (pk, ck, v) VALUES (?, ?, ?)
            """)

        bound = prepared.bind([(52), (1), (2)])
        results = session.execute(bound, trace=True)
        if verify_in_tracing:
            self.verify_hosts_in_tracing(results, 2)

    def test_tablets(self):
        self.query_data_host_select(self.session)
        self.query_data_host_insert(self.session)

    def test_tablets_shard_awareness(self):
        self.query_data_shard_select(self.session)
        self.query_data_shard_insert(self.session)

    def test_tablets_invalidation_drop_ks_while_reconnecting(self):
        def recreate_while_reconnecting(_):
            # Kill control connection
            conn = self.session.cluster.control_connection._connection
            self.session.cluster.control_connection._connection = None
            conn.close()

            # Drop and recreate ks and table to trigger tablets invalidation
            self.create_ks_and_cf(self.cluster.connect())

            # Start control connection
            self.session.cluster.control_connection._reconnect()

        self.run_tablets_invalidation_test(recreate_while_reconnecting)

    def test_tablets_invalidation_drop_ks(self):
        def drop_ks(_):
            # Drop and recreate ks and table to trigger tablets invalidation
            self.create_ks_and_cf(self.cluster.connect())
            time.sleep(3)

        self.run_tablets_invalidation_test(drop_ks)

    @pytest.mark.last
    def test_tablets_invalidation_decommission_non_cc_node(self):
        def decommission_non_cc_node(rec):
            # Drop and recreate ks and table to trigger tablets invalidation
            for node in CCM_CLUSTER.nodes.values():
                if self.cluster.control_connection._connection.endpoint.address == node.network_interfaces["storage"][0]:
                    # Ignore node that control connection is connected to
                    continue
                for replica in rec.replicas:
                    if str(replica[0]) == str(node.node_hostid):
                        node.decommission()
                        break
                else:
                    continue
                break
            else:
                assert False, "failed to find node to decommission"
            time.sleep(10)

        self.run_tablets_invalidation_test(decommission_non_cc_node)


    def run_tablets_invalidation_test(self, invalidate):
        # Make sure driver holds tablet info
        # By landing query to the host that is not in replica set
        bound = self.session.prepare(
            """
            SELECT pk, ck, v FROM test1.table1 WHERE pk = ?
            """).bind([(2)])

        rec = None
        for host in self.cluster.metadata.all_hosts():
            self.session.execute(bound, host=host)
            rec = self.get_tablet_record(bound)
            if rec is not None:
                break

        assert rec is not None, "failed to find tablet record"

        invalidate(rec)

        # Check if tablets information was purged
        assert self.get_tablet_record(bound) is None, "tablet was not deleted, invalidation did not work"
