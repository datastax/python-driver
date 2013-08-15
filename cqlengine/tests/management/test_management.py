from cqlengine.exceptions import CQLEngineException
from cqlengine.management import create_table, delete_table, get_fields, get_compaction_options
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.connection import ConnectionPool, Host

from mock import MagicMock, patch
from cqlengine import management, SizeTieredCompactionStrategy, LeveledCompactionStrategy
from cqlengine.tests.query.test_queryset import TestModel
from cqlengine.models import Model
from cqlengine import columns


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

class LowercaseKeyModel(Model):
    first_key = columns.Integer(primary_key=True)
    second_key = columns.Integer(primary_key=True)
    some_data = columns.Text()

class CapitalizedKeyModel(Model):
    firstKey = columns.Integer(primary_key=True)
    secondKey = columns.Integer(primary_key=True)
    someData = columns.Text()

class CapitalizedKeyTest(BaseCassEngTestCase):

    def test_table_definition(self):
        """ Tests that creating a table with capitalized column names succeedso """
        create_table(LowercaseKeyModel)
        create_table(CapitalizedKeyModel)

        delete_table(LowercaseKeyModel)
        delete_table(CapitalizedKeyModel)


class FirstModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()

class SecondModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()
    fourth_key = columns.Text()

class ThirdModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()
    # removed fourth key, but it should stay in the DB
    blah = columns.Map(columns.Text, columns.Text)

class FourthModel(Model):
    __table_name__ = 'first_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.UUID()
    third_key = columns.Text()
    # removed fourth key, but it should stay in the DB
    renamed = columns.Map(columns.Text, columns.Text, db_field='blah')

class AddColumnTest(BaseCassEngTestCase):
    def setUp(self):
        delete_table(FirstModel)

    def test_add_column(self):
        create_table(FirstModel)
        fields = get_fields(FirstModel)

        # this should contain the second key
        self.assertEqual(len(fields), 2)
        # get schema
        create_table(SecondModel)

        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 3)

        create_table(ThirdModel)
        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 4)

        create_table(FourthModel)
        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 4)

class CompactionModel(Model):
    __compaction__ = None
    cid = columns.UUID(primary_key=True)
    name = columns.Text()

class CompactionSizeTieredModel(Model):
    __compaction__ = SizeTieredCompactionStrategy
    cid = columns.UUID(primary_key=True)
    name = columns.Text()

class CompactionLeveledStrategyModel(Model):
    __compaction__ = LeveledCompactionStrategy
    cid = columns.UUID(primary_key=True)
    name = columns.Text()

import copy

class EmptyCompactionTest(BaseCassEngTestCase):
    def test_empty_compaction(self):
        self.model = copy.deepcopy(CompactionModel)
        result = get_compaction_options(self.model)
        self.assertIsNone(result)

class BaseCompactionTest(BaseCassEngTestCase):
    def assert_option_fails(self, key):
        # key is a normal_key, converted to
        # __compaction_key__

        key = "__compaction_{}__".format(key)

        with patch.object(self.model, key, 10), \
             self.assertRaises(CQLEngineException):
            get_compaction_options(self.model)

class SizeTieredCompactionTest(BaseCompactionTest):

    def setUp(self):
        self.model = copy.deepcopy(CompactionModel)
        self.model.__compaction__ = SizeTieredCompactionStrategy

    def test_size_tiered(self):
        result = get_compaction_options(self.model)
        assert result['class'] == SizeTieredCompactionStrategy

    def test_min_threshold(self):
        self.model.__compaction_min_threshold__ = 2
        result = get_compaction_options(self.model)
        assert result['min_threshold'] == 2

class LeveledCompactionTest(BaseCompactionTest):
    def setUp(self):
        self.model = copy.deepcopy(CompactionLeveledStrategyModel)

    def test_simple_leveled(self):
        result = get_compaction_options(self.model)
        assert result['class'] == LeveledCompactionStrategy

    def test_bucket_high_fails(self):
        self.assert_option_fails('bucket_high')

    def test_bucket_low_fails(self):
        self.assert_option_fails('bucket_low')

    def test_max_threshold_fails(self):
        self.assert_option_fails('max_threshold')

    def test_min_threshold_fails(self):
        self.assert_option_fails('min_threshold')

    def test_min_sstable_size_fails(self):
        self.assert_option_fails('min_sstable_size')

