from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.connection import ConnectionPool

from mock import Mock
from cqlengine import management


class ConnectionPoolTestCase(BaseCassEngTestCase):
    """Test cassandra connection pooling."""

    def setUp(self):
        ConnectionPool.clear()

    def test_should_create_single_connection_on_request(self):
        """Should create a single connection on first request"""
        result = ConnectionPool.get()
        self.assertIsNotNone(result)
        self.assertEquals(0, ConnectionPool._queue.qsize())
        ConnectionPool._queue.put(result)
        self.assertEquals(1, ConnectionPool._queue.qsize())

    def test_should_close_connection_if_queue_is_full(self):
        """Should close additional connections if queue is full"""
        connections = [ConnectionPool.get() for x in range(10)]
        for conn in connections:
            ConnectionPool.put(conn)
        fake_conn = Mock()
        ConnectionPool.put(fake_conn)
        fake_conn.close.assert_called_once_with()

    def test_should_pop_connections_from_queue(self):
        """Should pull existing connections off of the queue"""
        conn = ConnectionPool.get()
        ConnectionPool.put(conn)
        self.assertEquals(1, ConnectionPool._queue.qsize())
        self.assertEquals(conn, ConnectionPool.get())
        self.assertEquals(0, ConnectionPool._queue.qsize())
    

class CreateKeyspaceTest(BaseCassEngTestCase):
    def test_create_succeeeds(self):
        management.create_keyspace('test_keyspace')
        management.delete_keyspace('test_keyspace')
    
    
    