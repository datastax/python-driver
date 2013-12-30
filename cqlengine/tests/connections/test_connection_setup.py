from unittest import TestCase
from mock import patch

from cqlengine.connection import setup as setup_connection
from cqlengine.connection import CQLConnectionError, Host


class OperationalErrorLoggingTest(TestCase):

    @patch('cqlengine.connection.ConnectionPool', return_value=None, autospec=True)
    def test_setup_hosts(self, PatchedConnectionPool):
        with self.assertRaises(CQLConnectionError):
            setup_connection(hosts=['localhost:abcd'])
            self.assertEqual(len(PatchedConnectionPool.mock_calls), 0)

        with self.assertRaises(CQLConnectionError):
            setup_connection(hosts=['localhost:9160:abcd'])
            self.assertEqual(len(PatchedConnectionPool.mock_calls), 0)

        setup_connection(hosts=['localhost:9161', 'remotehost'])
        self.assertEqual(len(PatchedConnectionPool.mock_calls), 1)
        self.assertEqual(PatchedConnectionPool.call_args[0][0], [Host('localhost', 9161), Host('remotehost', 9160)])
