from cqlengine.exceptions import CQLEngineException
from cqlengine.management import create_table, delete_table
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.connection import ConnectionPool, Host

from mock import Mock, MagicMock, MagicProxy, patch
from cqlengine import management
from cqlengine.tests.query.test_queryset import TestModel

from cql.thrifteries import ThriftConnection

class ConnectionPoolFailoverTestCase(BaseCassEngTestCase):
    """Test cassandra connection pooling."""

    def setUp(self):
        self.host = Host('127.0.0.1', '9160')
        self.pool = ConnectionPool([self.host])

    def test_totally_dead_pool(self):
        # kill the con
        with patch('cqlengine.connection.cql.connect') as mock:
            mock.side_effect=CQLEngineException
            with self.assertRaises(CQLEngineException):
                self.pool.execute("select * from system.peers", {})

    def test_dead_node(self):
        """
        tests that a single dead node doesn't mess up the pool
        """
        self.pool._hosts.append(self.host)

        # cursor mock needed so set_cql_version doesn't crap out
        ok_cur = MagicMock()

        ok_conn = MagicMock()
        ok_conn.return_value = ok_cur


        returns = [CQLEngineException(), ok_conn]

        def side_effect(*args, **kwargs):
            result = returns.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with patch('cqlengine.connection.cql.connect') as mock:
            mock.side_effect = side_effect
            conn = self.pool._create_connection()


class CreateKeyspaceTest(BaseCassEngTestCase):
    def test_create_succeeeds(self):
        management.create_keyspace('test_keyspace')
        management.delete_keyspace('test_keyspace')

class DeleteTableTest(BaseCassEngTestCase):

    def test_multiple_deletes_dont_fail(self):
        """

        """
        create_table(TestModel)

        delete_table(TestModel)
        delete_table(TestModel)
