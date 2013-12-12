from unittest import TestCase
from cql import OperationalError
from mock import MagicMock, patch, Mock

from cqlengine import ONE
from cqlengine.connection import ConnectionPool, Host


class OperationalErrorLoggingTest(TestCase):
    def test_logging(self):
        p = ConnectionPool([Host('127.0.0.1', '9160')])

        class MockConnection(object):
            host = 'localhost'
            port = 6379
            def cursor(self):
                raise OperationalError('test')


        with patch.object(p, 'get', return_value=MockConnection()):
            with self.assertRaises(OperationalError):
                p.execute("select * from system.peers", {}, ONE)
