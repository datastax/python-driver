try:
    import unittest2 as unittest
except ImportError:
    import unittest

from mock import Mock, MagicMock, ANY

from cassandra import ConsistencyLevel
from cassandra.cluster import Session, ResponseFuture, NoHostAvailable
from cassandra.connection import ConnectionException
from cassandra.decoder import (ReadTimeoutErrorMessage, WriteTimeoutErrorMessage,
                               UnavailableErrorMessage, ResultMessage, QueryMessage,
                               OverloadedErrorMessage, IsBootstrappingErrorMessage,
                               PreparedQueryNotFound, PrepareMessage)
from cassandra.policies import RetryPolicy
from cassandra.pool import NoConnectionsAvailable
from cassandra.query import SimpleStatement

class ResponseFutureTests(unittest.TestCase):

    def make_basic_session(self):
        return Mock(spec=Session, row_factory=lambda *x: list(x))

    def make_session(self):
        session = self.make_basic_session()
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = False
        return session

    def make_response_future(self, session):
        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        return ResponseFuture(session, message, query)

    def test_result_message(self):
        session = self.make_basic_session()
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        pool = session._pools.get.return_value
        pool.is_shutdown = False

        rf = self.make_response_future(session)
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)
        connection = pool.borrow_connection.return_value
        connection.send_msg.assert_called_once_with(rf.message, cb=ANY)

        response = Mock(spec=ResultMessage, kind=ResultMessage.KIND_ROWS, results=[{'col': 'val'}])
        rf._set_result(response)

        result = rf.result()
        self.assertEqual(result, [{'col': 'val'}])

    def test_unknown_result_class(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()
        rf._set_result(object())
        self.assertRaises(ConnectionException, rf.result)

    def test_set_keyspace_result(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        result = Mock(spec=ResultMessage,
                      kind=ResultMessage.KIND_SET_KEYSPACE,
                      results="keyspace1")
        rf._set_result(result)
        self.assertEqual(None, rf.result())
        self.assertEqual(session.keyspace, 'keyspace1')

    def test_schema_change_result(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        result = Mock(spec=ResultMessage,
                      kind=ResultMessage.KIND_SCHEMA_CHANGE,
                      results={'keyspace': "keyspace1", "table": "table1"})
        rf._set_result(result)
        session.submit.assert_called_once_with(ANY, 'keyspace1', 'table1', ANY, rf)

    def test_other_result_message_kind(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()
        result = object()
        rf._set_result(Mock(spec=ResultMessage, kind=999, results=result))
        self.assertIs(result, rf.result())

    def test_read_timeout_error_message(self):
        session = self.make_session()
        query = SimpleStatement("SELECT * FROM foo")
        query.retry_policy = Mock()
        query.retry_policy.on_read_timeout.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=ReadTimeoutErrorMessage, info={})
        rf._set_result(result)

        self.assertRaises(Exception, rf.result)

    def test_write_timeout_error_message(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_write_timeout.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=WriteTimeoutErrorMessage, info={})
        rf._set_result(result)
        self.assertRaises(Exception, rf.result)

    def test_unavailable_error_message(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(result)
        self.assertRaises(Exception, rf.result)

    def test_retry_policy_says_ignore(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.IGNORE, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(result)
        self.assertEqual(None, rf.result())

    def test_retry_policy_says_retry(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETRY, ConsistencyLevel.ONE)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.QUORUM)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)
        connection = pool.borrow_connection.return_value
        connection.send_msg.assert_called_once_with(rf.message, cb=ANY)

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(result)

        session.submit.assert_called_once_with(rf._retry_task, True)
        self.assertEqual(1, rf._query_retries)

        # simulate the executor running this
        rf._retry_task(True)

        # it should try again with the same host since this was
        # an UnavailableException
        rf.session._pools.get.assert_called_with('ip1')
        pool.borrow_connection.assert_called_with(timeout=ANY)
        connection = pool.borrow_connection.return_value
        connection.send_msg.assert_called_with(rf.message, cb=ANY)

    def test_retry_with_different_host(self):
        session = self.make_session()
        pool = session._pools.get.return_value

        rf = self.make_response_future(session)
        rf.message.consistency_level = ConsistencyLevel.QUORUM
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)
        connection = pool.borrow_connection.return_value
        connection.send_msg.assert_called_once_with(rf.message, cb=ANY)
        self.assertEqual(ConsistencyLevel.QUORUM, rf.message.consistency_level)

        result = Mock(spec=OverloadedErrorMessage, info={})
        rf._set_result(result)

        session.submit.assert_called_once_with(rf._retry_task, False)
        # query_retries does not get incremented for Overloaded/Bootstrapping errors
        self.assertEqual(0, rf._query_retries)

        # simulate the executor running this
        rf._retry_task(False)

        # it should try with a different host
        rf.session._pools.get.assert_called_with('ip2')
        pool.borrow_connection.assert_called_with(timeout=ANY)
        connection = pool.borrow_connection.return_value
        connection.send_msg.assert_called_with(rf.message, cb=ANY)

        # the consistency level should be the same
        self.assertEqual(ConsistencyLevel.QUORUM, rf.message.consistency_level)

    def test_all_retries_fail(self):
        session = self.make_session()

        rf = self.make_response_future(session)
        rf.send_request()
        rf.session._pools.get.assert_called_once_with('ip1')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        rf._set_result(result)

        # simulate the executor running this
        session.submit.assert_called_once_with(rf._retry_task, False)
        rf._retry_task(False)

        # it should try with a different host
        rf.session._pools.get.assert_called_with('ip2')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        rf._set_result(result)

        # simulate the executor running this
        session.submit.assert_called_with(rf._retry_task, False)
        rf._retry_task(False)

        self.assertRaises(NoHostAvailable, rf.result)

    def test_all_pools_shutdown(self):
        session = self.make_basic_session()
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = True

        rf = ResponseFuture(session, Mock(), Mock())
        rf.send_request()
        self.assertRaises(NoHostAvailable, rf.result)

    def test_first_pool_shutdown(self):
        session = self.make_basic_session()
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']
        # first return a pool with is_shutdown=True, then is_shutdown=False
        session._pools.get.side_effect = [Mock(is_shutdown=True), Mock(is_shutdown=False)]

        rf = self.make_response_future(session)
        rf.send_request()

        response = Mock(spec=ResultMessage, kind=ResultMessage.KIND_ROWS, results=[{'col': 'val'}])
        rf._set_result(response)

        result = rf.result()
        self.assertEqual(result, [{'col': 'val'}])

    def test_timeout_getting_connection_from_pool(self):
        session = self.make_basic_session()
        session._load_balancer.make_query_plan.return_value = ['ip1', 'ip2']

        # the first pool will raise an exception on borrow_connection()
        exc = NoConnectionsAvailable()
        first_pool = Mock(is_shutdown=False)
        first_pool.borrow_connection.side_effect = exc
        second_pool = Mock(is_shutdown=False)

        session._pools.get.side_effect = [first_pool, second_pool]

        rf = self.make_response_future(session)
        rf.send_request()

        response = Mock(spec=ResultMessage, kind=ResultMessage.KIND_ROWS, results=[{'col': 'val'}])
        rf._set_result(response)
        self.assertEqual(rf.result(), [{'col': 'val'}])

        # make sure the exception is recorded correctly
        self.assertEqual(rf._errors, {'ip1': exc})

    def test_callback(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        rf.add_callback(self.assertEqual, [{'col': 'val'}])

        response = Mock(spec=ResultMessage, kind=ResultMessage.KIND_ROWS, results=[{'col': 'val'}])
        rf._set_result(response)

        result = rf.result()
        self.assertEqual(result, [{'col': 'val'}])

        # this should get called immediately now that the result is set
        rf.add_callback(self.assertEqual, [{'col': 'val'}])

    def test_errback(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query)
        rf.send_request()

        rf.add_errback(self.assertIsInstance, Exception)

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(result)
        self.assertRaises(Exception, rf.result)

        # this should get called immediately now that the error is set
        rf.add_errback(self.assertIsInstance, Exception)

    def test_add_callbacks(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        # test errback
        rf = ResponseFuture(session, message, query)
        rf.send_request()

        rf.add_callbacks(
            callback=self.assertEquals, callback_args=([{'col': 'val'}],),
            errback=self.assertIsInstance, errback_args=(Exception,))

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(result)
        self.assertRaises(Exception, rf.result)

        # test callback
        rf = ResponseFuture(session, message, query)
        rf.send_request()

        rf.add_callbacks(
            callback=self.assertEquals, callback_args=([{'col': 'val'}],),
            errback=self.assertIsInstance, errback_args=(Exception,))

        response = Mock(spec=ResultMessage, kind=ResultMessage.KIND_ROWS, results=[{'col': 'val'}])
        rf._set_result(response)
        self.assertEqual(rf.result(), [{'col': 'val'}])

    def test_prepared_query_not_found(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        session.cluster._prepared_statements = MagicMock(dict)
        prepared_statement = session.cluster._prepared_statements.__getitem__.return_value
        prepared_statement.query_string = "SELECT * FROM foobar"
        prepared_statement.keyspace = "FooKeyspace"
        rf._connection.keyspace = "FooKeyspace"

        result = Mock(spec=PreparedQueryNotFound, info='a' * 16)
        rf._set_result(result)

        session.submit.assert_called_once()
        args, kwargs = session.submit.call_args
        self.assertIsInstance(args[-1], PrepareMessage)
        self.assertEquals(args[-1].query, "SELECT * FROM foobar")

    def test_prepared_query_not_found_bad_keyspace(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        session.cluster._prepared_statements = MagicMock(dict)
        prepared_statement = session.cluster._prepared_statements.__getitem__.return_value
        prepared_statement.query_string = "SELECT * FROM foobar"
        prepared_statement.keyspace = "FooKeyspace"
        rf._connection.keyspace = "BarKeyspace"

        result = Mock(spec=PreparedQueryNotFound, info='a' * 16)
        rf._set_result(result)
        self.assertRaises(ValueError, rf.result)
