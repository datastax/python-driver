from mock import MagicMock, patch

from cqlengine import ONE, ALL, NONE
from cqlengine.exceptions import CQLEngineException
from cqlengine.management import create_table, delete_table, get_fields, sync_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.connection import ConnectionPool, Host
from cqlengine import management
from cqlengine.tests.query.test_queryset import TestModel
from cqlengine.models import Model
from cqlengine import columns, SizeTieredCompactionStrategy, LeveledCompactionStrategy


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
                self.pool.execute("select * from system.peers", {}, ONE)

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

class PrimaryKeysOnlyModel(Model):
    __compaction__ = LeveledCompactionStrategy

    first_ey = columns.Integer(primary_key=True)
    second_key = columns.Integer(primary_key=True)


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


class ModelWithTableProperties(Model):
    # Set random table properties
    __bloom_filter_fp_chance__ = 0.76328
    __caching__ = ALL
    __comment__ = 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR'
    __default_time_to_live__ = 4756
    __gc_grace_seconds__ = 2063
    __index_interval__ = 98706
    __memtable_flush_period_in_ms__ = 43681
    __populate_io_cache_on_flush__ = True
    __read_repair_chance__ = 0.17985
    __replicate_on_write__ = False
    __dclocal_read_repair_chance__ = 0.50811

    key = columns.UUID(primary_key=True)


class TablePropertiesTests(BaseCassEngTestCase):

    def setUp(self):
        delete_table(ModelWithTableProperties)

    def test_set_table_properties(self):
        create_table(ModelWithTableProperties)
        self.assertDictContainsSubset({
            'bloom_filter_fp_chance': 0.76328,
            'caching': ALL,
            'comment': 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR',
            'default_time_to_live': 4756,
            'gc_grace_seconds': 2063,
            'index_interval': 98706,
            'memtable_flush_period_in_ms': 43681,
            'populate_io_cache_on_flush': True,
            'read_repair_chance': 0.17985,
            'replicate_on_write': False,
            # For some reason 'dclocal_read_repair_chance' in CQL is called
            # just 'local_read_repair_chance' in the schema table.
            # Source: https://issues.apache.org/jira/browse/CASSANDRA-6717
            'local_read_repair_chance': 0.50811,
        }, management.get_table_settings(ModelWithTableProperties))

    def test_table_property_update(self):
        ModelWithTableProperties.__bloom_filter_fp_chance__ = 0.66778
        ModelWithTableProperties.__caching__ = NONE
        ModelWithTableProperties.__comment__ = 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy'
        ModelWithTableProperties.__default_time_to_live__ = 65178
        ModelWithTableProperties.__gc_grace_seconds__ = 96362
        ModelWithTableProperties.__index_interval__ = 94207
        ModelWithTableProperties.__memtable_flush_period_in_ms__ = 60210
        ModelWithTableProperties.__populate_io_cache_on_flush__ = False
        ModelWithTableProperties.__read_repair_chance__ = 0.2989
        ModelWithTableProperties.__replicate_on_write__ = True
        ModelWithTableProperties.__dclocal_read_repair_chance__ = 0.12732

        sync_table(ModelWithTableProperties)

        self.assertDictContainsSubset({
            'bloom_filter_fp_chance': 0.66778,
            'caching': NONE,
            'comment': 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy',
            'default_time_to_live': 65178,
            'gc_grace_seconds': 96362,
            'index_interval': 94207,
            'memtable_flush_period_in_ms': 60210,
            'populate_io_cache_on_flush': False,
            'read_repair_chance': 0.2989,
            'replicate_on_write': True,
            'local_read_repair_chance': 0.12732,
        }, management.get_table_settings(ModelWithTableProperties))


class SyncTableTests(BaseCassEngTestCase):

    def setUp(self):
        delete_table(PrimaryKeysOnlyModel)

    def test_sync_table_works_with_primary_keys_only_tables(self):

        # This is "create table":

        sync_table(PrimaryKeysOnlyModel)

        # let's make sure settings persisted correctly:

        assert PrimaryKeysOnlyModel.__compaction__ == LeveledCompactionStrategy
        # blows up with DoesNotExist if table does not exist
        table_settings = management.get_table_settings(PrimaryKeysOnlyModel)
        # let make sure the flag we care about
        assert LeveledCompactionStrategy in table_settings['compaction_strategy_class']


        # Now we are "updating" the table:

        # setting up something to change
        PrimaryKeysOnlyModel.__compaction__ = SizeTieredCompactionStrategy

        # primary-keys-only tables do not create entries in system.schema_columns
        # table. Only non-primary keys are added to that table.
        # Our code must deal with that eventuality properly (not crash)
        # on subsequent runs of sync_table (which runs get_fields internally)
        get_fields(PrimaryKeysOnlyModel)
        sync_table(PrimaryKeysOnlyModel)

        table_settings = management.get_table_settings(PrimaryKeysOnlyModel)
        assert SizeTieredCompactionStrategy in table_settings['compaction_strategy_class']
