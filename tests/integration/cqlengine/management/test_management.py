# Copyright 2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock

from unittest import skipUnless
import warnings

from cassandra.cqlengine import CACHING_ALL, CACHING_NONE
from cassandra.cqlengine.connection import get_session, get_cluster
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine import management
from cassandra.cqlengine.management import get_fields, sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns, SizeTieredCompactionStrategy, LeveledCompactionStrategy

from tests.integration import CASSANDRA_VERSION, PROTOCOL_VERSION
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine.query.test_queryset import TestModel


class KeyspaceManagementTest(BaseCassEngTestCase):
    def test_create_drop_succeeeds(self):
        cluster = get_cluster()

        keyspace_ss = 'test_ks_ss'
        self.assertFalse(keyspace_ss in cluster.metadata.keyspaces)
        management.create_keyspace_simple(keyspace_ss, 2)
        self.assertTrue(keyspace_ss in cluster.metadata.keyspaces)

        management.drop_keyspace(keyspace_ss)

        self.assertFalse(keyspace_ss in cluster.metadata.keyspaces)
        with warnings.catch_warnings(record=True) as w:
            management.create_keyspace(keyspace_ss, strategy_class="SimpleStrategy", replication_factor=1)
            self.assertEqual(len(w), 1)
            self.assertEqual(w[-1].category, DeprecationWarning)
        self.assertTrue(keyspace_ss in cluster.metadata.keyspaces)

        management.drop_keyspace(keyspace_ss)
        self.assertFalse(keyspace_ss in cluster.metadata.keyspaces)

        keyspace_nts = 'test_ks_nts'
        self.assertFalse(keyspace_nts in cluster.metadata.keyspaces)
        management.create_keyspace_simple(keyspace_nts, 2)
        self.assertTrue(keyspace_nts in cluster.metadata.keyspaces)

        with warnings.catch_warnings(record=True) as w:
            management.delete_keyspace(keyspace_nts)
            self.assertEqual(len(w), 1)
            self.assertEqual(w[-1].category, DeprecationWarning)

        self.assertFalse(keyspace_nts in cluster.metadata.keyspaces)


class DropTableTest(BaseCassEngTestCase):

    def test_multiple_deletes_dont_fail(self):
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
        """ Tests that creating a table with capitalized column names succeeds """
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


class ModelWithTableProperties(Model):

    # Set random table properties
    __bloom_filter_fp_chance__ = 0.76328
    __caching__ = CACHING_ALL
    __comment__ = 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR'
    __gc_grace_seconds__ = 2063
    __populate_io_cache_on_flush__ = True
    __read_repair_chance__ = 0.17985
    __replicate_on_write__ = False
    __dclocal_read_repair_chance__ = 0.50811

    key = columns.UUID(primary_key=True)

# kind of a hack, but we only test this property on C >= 2.0
if CASSANDRA_VERSION >= '2.0.0':
    ModelWithTableProperties.__memtable_flush_period_in_ms__ = 43681
    ModelWithTableProperties.__index_interval__ = 98706
    ModelWithTableProperties.__default_time_to_live__ = 4756


class TablePropertiesTests(BaseCassEngTestCase):

    def setUp(self):
        drop_table(ModelWithTableProperties)

    def test_set_table_properties(self):

        sync_table(ModelWithTableProperties)
        expected = {'bloom_filter_fp_chance': 0.76328,
                    'comment': 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR',
                    'gc_grace_seconds': 2063,
                    'read_repair_chance': 0.17985,
                     # For some reason 'dclocal_read_repair_chance' in CQL is called
                     #  just 'local_read_repair_chance' in the schema table.
                     #  Source: https://issues.apache.org/jira/browse/CASSANDRA-6717
                     #  TODO: due to a bug in the native driver i'm not seeing the local read repair chance show up
                     # 'local_read_repair_chance': 0.50811,
                    }
        if CASSANDRA_VERSION <= '2.0.0':
            expected['caching'] = CACHING_ALL
            expected['replicate_on_write'] = False

        if CASSANDRA_VERSION == '2.0.0':
            expected['populate_io_cache_on_flush'] = True
            expected['index_interval'] = 98706

        if CASSANDRA_VERSION >= '2.0.0':
            expected['default_time_to_live'] = 4756
            expected['memtable_flush_period_in_ms'] = 43681

        options = management.get_table_settings(ModelWithTableProperties).options
        self.assertEqual(dict([(k, options.get(k)) for k in expected.keys()]),
                         expected)

    def test_table_property_update(self):
        ModelWithTableProperties.__bloom_filter_fp_chance__ = 0.66778
        ModelWithTableProperties.__caching__ = CACHING_NONE
        ModelWithTableProperties.__comment__ = 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy'
        ModelWithTableProperties.__gc_grace_seconds__ = 96362

        ModelWithTableProperties.__populate_io_cache_on_flush__ = False
        ModelWithTableProperties.__read_repair_chance__ = 0.2989
        ModelWithTableProperties.__replicate_on_write__ = True
        ModelWithTableProperties.__dclocal_read_repair_chance__ = 0.12732

        if CASSANDRA_VERSION >= '2.0.0':
            ModelWithTableProperties.__default_time_to_live__ = 65178
            ModelWithTableProperties.__memtable_flush_period_in_ms__ = 60210
            ModelWithTableProperties.__index_interval__ = 94207

        sync_table(ModelWithTableProperties)

        table_settings = management.get_table_settings(ModelWithTableProperties).options

        expected = {'bloom_filter_fp_chance': 0.66778,
                    'comment': 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy',
                    'gc_grace_seconds': 96362,
                    'read_repair_chance': 0.2989,
                    # 'local_read_repair_chance': 0.12732,
                    }
        if CASSANDRA_VERSION >= '2.0.0':
            expected['memtable_flush_period_in_ms'] = 60210
            expected['default_time_to_live'] = 65178

        if CASSANDRA_VERSION == '2.0.0':
            expected['index_interval'] = 94207

        # these featuers removed in cassandra 2.1
        if CASSANDRA_VERSION <= '2.0.0':
            expected['caching'] = CACHING_NONE
            expected['replicate_on_write'] = True
            expected['populate_io_cache_on_flush'] = False

        self.assertEqual(dict([(k, table_settings.get(k)) for k in expected.keys()]),
                         expected)


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


class NonModelFailureTest(BaseCassEngTestCase):
    class FakeModel(object):
        pass

    def test_failure(self):
        with self.assertRaises(CQLEngineException):
            sync_table(self.FakeModel)


@skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
def test_static_columns():
    class StaticModel(Model):
        id = columns.Integer(primary_key=True)
        c = columns.Integer(primary_key=True)
        name = columns.Text(static=True)

    drop_table(StaticModel)

    session = get_session()

    with mock.patch.object(session, "execute", wraps=session.execute) as m:
        sync_table(StaticModel)

    assert m.call_count > 0
    statement = m.call_args[0][0].query_string
    assert '"name" text static' in statement, statement

    # if we sync again, we should not apply an alter w/ a static
    sync_table(StaticModel)

    with mock.patch.object(session, "execute", wraps=session.execute) as m2:
        sync_table(StaticModel)

    assert len(m2.call_args_list) == 1
    assert "ALTER" not in m2.call_args[0][0].query_string
