import unittest
from mock import Mock, ANY

from cassandra.cluster import Cluster, Session, ResponseFuture
from cassandra.connection import ConnectionException
from cassandra.decoder import (ReadTimeoutErrorMessage, WriteTimeoutErrorMessage,
                               UnavailableErrorMessage, ResultMessage, QueryMessage,
                               ConsistencyLevel)
from cassandra.policies import RetryPolicy
from cassandra.query import SimpleStatement

class ClusterTests(unittest.TestCase):

    def test_basic(self):
        cluster = Cluster()
        session = cluster.connect()
        result = session.execute(
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        self.assertEquals(None, result)

        result = session.execute(
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        self.assertEquals(None, result)

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        self.assertEquals(None, result)

        result = session.execute("SELECT * FROM clustertests.cf0")
        self.assertEquals([{'a': 'a', 'b': 'b', 'c': 'c'}], result)

        cluster.shutdown()

    def test_submit_schema_refresh(self):
        cluster = Cluster()
        cluster.connect()

        other_cluster = Cluster()
        session = other_cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE newkeyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        self.assertNotIn("newkeyspace", cluster.metadata.keyspaces)

        future = cluster.submit_schema_refresh()
        future.result()

        self.assertIn("newkeyspace", cluster.metadata.keyspaces)

    def test_on_down_and_up(self):
        cluster = Cluster()
        session = cluster.connect()
        host = cluster.metadata.all_hosts()[0]
        host.monitor.signal_connection_failure(None)
        cluster.on_down(host)
        self.assertNotEqual(None, cluster.control_connection._reconnection_handler)
        self.assertNotIn(host, session._pools)
        host_reconnector = host._reconnection_handler
        self.assertNotEqual(None, host_reconnector)

        host.monitor.is_up = True

        cluster.on_up(host)

        self.assertEqual(None, host._reconnection_handler)
        self.assertTrue(host_reconnector._cancelled)
        self.assertIn(host, session._pools)

        cluster.shutdown()


class ResponseFutureTests(unittest.TestCase):

    def test_result_message(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        pool = session._pools.get.return_value
        pool.is_shutdown = False

        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        rf = ResponseFuture(session, message, query)
        rf.send_request()

        session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)
        connection = pool.borrow_connection.return_value
        connection.send_msg.assert_called_once_with(message, cb=ANY)

        response = Mock(spec=ResultMessage, kind=ResultMessage.KIND_ROWS, results=[{'col': 'val'}])
        rf._set_result(response)

        result = rf.deliver()
        self.assertEqual(result, [{'col': 'val'}])

    def test_unknown_result_class(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        rf = ResponseFuture(session, Mock(), Mock())
        rf.send_request()

        rf._set_result(object())
        self.assertRaises(ConnectionException, rf.deliver)

    def test_set_keyspace_result(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        rf = ResponseFuture(session, Mock(), Mock())
        rf.send_request()

        result = Mock(spec=ResultMessage,
                      kind=ResultMessage.KIND_SET_KEYSPACE,
                      results="keyspace1")
        rf._set_result(result)
        self.assertEqual(None, rf.deliver())
        session.set_keyspace.assert_called_once_with('keyspace1')

    def test_schema_change_result(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        rf = ResponseFuture(session, Mock(), Mock())
        rf.send_request()

        result = Mock(spec=ResultMessage,
                      kind=ResultMessage.KIND_SCHEMA_CHANGE,
                      results={'keyspace': "keyspace1", "table": "table1"})
        rf._set_result(result)
        session.submit.assert_called_once_with(ANY, 'keyspace1', 'table1', ANY, rf)

    def test_other_result_message_kind(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        rf = ResponseFuture(session, Mock(), Mock())
        rf.send_request()

        rf._set_result(Mock(spec=ResultMessage, kind=999, results="foobar"))
        self.assertEqual('foobar', rf.deliver())

    def test_read_timeout_error_message(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        query = SimpleStatement("SELECT * FROM foo")
        query.retry_policy = Mock()
        query.retry_policy.on_read_timeout.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=ReadTimeoutErrorMessage)
        result.info = {}
        rf._set_result(result)

        self.assertRaises(Exception, rf.deliver)

    def test_write_timeout_error_message(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_write_timeout.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=WriteTimeoutErrorMessage)
        result.info = {}
        rf._set_result(result)
        self.assertRaises(Exception, rf.deliver)

    def test_unavailable_error_message(self):
        session = Mock(spec=Session)
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage)
        result.info = {}
        rf._set_result(result)
        self.assertRaises(Exception, rf.deliver)
