
from cqlengine.management import  get_fields, sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import management
from cqlengine.tests.query.test_queryset import TestModel
from cqlengine.models import Model
from cqlengine import columns, SizeTieredCompactionStrategy, LeveledCompactionStrategy


class CreateKeyspaceTest(BaseCassEngTestCase):
    def test_create_succeeeds(self):
        management.create_keyspace('test_keyspace')
        management.delete_keyspace('test_keyspace')

class DeleteTableTest(BaseCassEngTestCase):

    def test_multiple_deletes_dont_fail(self):
        """

        """
        sync_table(TestModel)

        drop_table(TestModel)
        drop_table(TestModel)

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
        sync_table(LowercaseKeyModel)
        sync_table(CapitalizedKeyModel)

        drop_table(LowercaseKeyModel)
        drop_table(CapitalizedKeyModel)


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
        drop_table(FirstModel)

    def test_add_column(self):
        sync_table(FirstModel)
        fields = get_fields(FirstModel)

        # this should contain the second key
        self.assertEqual(len(fields), 2)
        # get schema
        sync_table(SecondModel)

        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 3)

        sync_table(ThirdModel)
        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 4)

        sync_table(FourthModel)
        fields = get_fields(FirstModel)
        self.assertEqual(len(fields), 4)


class SyncTableTests(BaseCassEngTestCase):

    def setUp(self):
        drop_table(PrimaryKeysOnlyModel)

    def test_sync_table_works_with_primary_keys_only_tables(self):

        # This is "create table":

        sync_table(PrimaryKeysOnlyModel)

        # let's make sure settings persisted correctly:

        assert PrimaryKeysOnlyModel.__compaction__ == LeveledCompactionStrategy
        # blows up with DoesNotExist if table does not exist
        table_settings = management.get_table_settings(PrimaryKeysOnlyModel)
        # let make sure the flag we care about

        assert LeveledCompactionStrategy in table_settings.options['compaction_strategy_class']


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
        assert SizeTieredCompactionStrategy in table_settings.options['compaction_strategy_class']
