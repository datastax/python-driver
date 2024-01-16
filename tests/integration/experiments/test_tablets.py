import time
import unittest
import pytest
import os
from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy

from tests.integration import PROTOCOL_VERSION, use_cluster
from tests.unit.test_host_connection_pool import LOGGER

def setup_module():
    use_cluster('tablets', [3], start=True, use_tablets=True)

class TestTabletsIntegration(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1", "127.0.0.2", "127.0.0.3"], protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()
        cls.create_ks_and_cf(cls)
        cls.create_data(cls.session)
    
    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def verify_same_host_in_tracing(self, results):
        traces = results.get_query_trace()
        events = traces.events
        host_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s %s", event.source, event.thread_name, event.description)
            host_set.add(event.source)
        
        self.assertEqual(len(host_set), 1)
        self.assertIn('locally', "\n".join([event.description for event in events]))

        trace_id = results.response_future.get_query_trace_ids()[0]
        traces = self.session.execute("SELECT * FROM system_traces.events WHERE session_id = %s", (trace_id,))
        events = [event for event in traces]
        host_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s", event.source, event.activity)
            host_set.add(event.source)
        
        self.assertEqual(len(host_set), 1)
        self.assertIn('locally', "\n".join([event.activity for event in events]))

    def verify_same_shard_in_tracing(self, results):
        traces = results.get_query_trace()
        events = traces.events
        shard_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s %s", event.source, event.thread_name, event.description)
            shard_set.add(event.thread_name)
            
        self.assertEqual(len(shard_set), 1)
        self.assertIn('locally', "\n".join([event.description for event in events]))

        trace_id = results.response_future.get_query_trace_ids()[0]
        traces = self.session.execute("SELECT * FROM system_traces.events WHERE session_id = %s", (trace_id,))
        events = [event for event in traces]
        shard_set = set()
        for event in events:
            LOGGER.info("TRACE EVENT: %s %s", event.thread, event.activity)
            shard_set.add(event.thread)
            
        self.assertEqual(len(shard_set), 1)
        self.assertIn('locally', "\n".join([event.activity for event in events]))
    
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
                'replication_factor': 1, 
                'initial_tablets': 8
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

    def query_data_shard_select(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            SELECT pk, ck, v FROM test1.table1 WHERE pk = ?
            """)

        bound = prepared.bind([(2)])
        results = session.execute(bound, trace=True)
        self.assertEqual(results, [(2, 2, 0)])
        if verify_in_tracing:
            self.verify_same_shard_in_tracing(results)

    def query_data_host_select(self, session, verify_in_tracing=True):
        prepared = session.prepare(
            """
            SELECT pk, ck, v FROM test1.table1 WHERE pk = ?
            """)

        bound = prepared.bind([(2)])
        results = session.execute(bound, trace=True)
        self.assertEqual(results, [(2, 2, 0)])
        if verify_in_tracing:
            self.verify_same_host_in_tracing(results)

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
            self.verify_same_host_in_tracing(results)

    def test_tablets(self):
        self.query_data_host_select(self.session)
        self.query_data_host_insert(self.session)

    def test_tablets_shard_awareness(self):
        self.query_data_shard_select(self.session)
        self.query_data_shard_insert(self.session)
