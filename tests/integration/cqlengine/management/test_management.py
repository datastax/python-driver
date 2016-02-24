# Copyright 2013-2016 DataStax, Inc.
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
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import mock
import warnings

from cassandra.cqlengine import CACHING_ALL, CACHING_NONE
from cassandra.cqlengine.connection import get_session, get_cluster
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine import management
from cassandra.cqlengine.management import _get_non_pk_field_names, _get_table_metadata, sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns

from tests.integration import CASSANDRA_VERSION, PROTOCOL_VERSION
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine.query.test_queryset import TestModel


class KeyspaceManagementTest(BaseCassEngTestCase):
    def test_create_drop_succeeeds(self):
        cluster = get_cluster()

        keyspace_ss = 'test_ks_ss'
        self.assertNotIn(keyspace_ss, cluster.metadata.keyspaces)
        management.create_keyspace_simple(keyspace_ss, 2)
        self.assertIn(keyspace_ss, cluster.metadata.keyspaces)

        management.drop_keyspace(keyspace_ss)
        self.assertNotIn(keyspace_ss, cluster.metadata.keyspaces)

        keyspace_nts = 'test_ks_nts'
        self.assertNotIn(keyspace_nts, cluster.metadata.keyspaces)
        management.create_keyspace_network_topology(keyspace_nts, {'dc1': 1})
        self.assertIn(keyspace_nts, cluster.metadata.keyspaces)

        management.drop_keyspace(keyspace_nts)
        self.assertNotIn(keyspace_nts, cluster.metadata.keyspaces)


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

    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy'}}

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
        fields = _get_non_pk_field_names(_get_table_metadata(FirstModel))

        # this should contain the second key
        self.assertEqual(len(fields), 2)
        # get schema
        sync_table(SecondModel)

        fields = _get_non_pk_field_names(_get_table_metadata(FirstModel))
        self.assertEqual(len(fields), 3)

        sync_table(ThirdModel)
        fields = _get_non_pk_field_names(_get_table_metadata(FirstModel))
        self.assertEqual(len(fields), 4)

        sync_table(FourthModel)
        fields = _get_non_pk_field_names(_get_table_metadata(FirstModel))
        self.assertEqual(len(fields), 4)


class ModelWithTableProperties(Model):

    __options__ = {'bloom_filter_fp_chance': '0.76328',
                   'comment': 'TxfguvBdzwROQALmQBOziRMbkqVGFjqcJfVhwGR',
                   'gc_grace_seconds': '2063',
                   'read_repair_chance': '0.17985',
                   'dclocal_read_repair_chance': '0.50811'}

    key = columns.UUID(primary_key=True)


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
        options = management._get_table_metadata(ModelWithTableProperties).options
        self.assertEqual(dict([(k, options.get(k)) for k in expected.keys()]),
                         expected)

    def test_table_property_update(self):
        ModelWithTableProperties.__options__['bloom_filter_fp_chance'] = 0.66778
        ModelWithTableProperties.__options__['comment'] = 'xirAkRWZVVvsmzRvXamiEcQkshkUIDINVJZgLYSdnGHweiBrAiJdLJkVohdRy'
        ModelWithTableProperties.__options__['gc_grace_seconds'] = 96362

        ModelWithTableProperties.__options__['read_repair_chance'] = 0.2989
        ModelWithTableProperties.__options__['dclocal_read_repair_chance'] = 0.12732

        sync_table(ModelWithTableProperties)

        table_options = management._get_table_metadata(ModelWithTableProperties).options

        self.assertDictContainsSubset(ModelWithTableProperties.__options__, table_options)

    def test_bogus_option_update(self):
        sync_table(ModelWithTableProperties)
        option = 'no way will this ever be an option'
        try:
            ModelWithTableProperties.__options__[option] = 'what was I thinking?'
            self.assertRaisesRegexp(KeyError, "Invalid table option.*%s.*" % option, sync_table, ModelWithTableProperties)
        finally:
            ModelWithTableProperties.__options__.pop(option, None)


class SyncTableTests(BaseCassEngTestCase):

    def setUp(self):
        drop_table(PrimaryKeysOnlyModel)

    def test_sync_table_works_with_primary_keys_only_tables(self):

        sync_table(PrimaryKeysOnlyModel)
        # blows up with DoesNotExist if table does not exist
        table_meta = management._get_table_metadata(PrimaryKeysOnlyModel)

        self.assertIn('LeveledCompactionStrategy', table_meta.as_cql_query())

        PrimaryKeysOnlyModel.__options__['compaction']['class'] = 'SizeTieredCompactionStrategy'

        sync_table(PrimaryKeysOnlyModel)

        table_meta = management._get_table_metadata(PrimaryKeysOnlyModel)
        self.assertIn('SizeTieredCompactionStrategy', table_meta.as_cql_query())


class IndexModel(Model):

    __table_name__ = 'index_model'
    first_key = columns.UUID(primary_key=True)
    second_key = columns.Text(index=True)


class IndexCaseSensitiveModel(Model):

    __table_name__ = 'IndexModel'
    __table_name_case_sensitive__ = True
    first_key = columns.UUID(primary_key=True)
    second_key = columns.Text(index=True)


class IndexTests(BaseCassEngTestCase):

    def setUp(self):
        drop_table(IndexModel)

    def test_sync_index(self):

        sync_table(IndexModel)
        table_meta = management._get_table_metadata(IndexModel)
        self.assertIn("index_index_model_second_key", table_meta.indexes)

        # index already exists
        sync_table(IndexModel)
        table_meta = management._get_table_metadata(IndexModel)
        self.assertIn("index_index_model_second_key", table_meta.indexes)

    def test_sync_index_case_sensitive(self):

        sync_table(IndexCaseSensitiveModel)
        table_meta = management._get_table_metadata(IndexCaseSensitiveModel)
        self.assertIn("index_IndexModel_second_key", table_meta.indexes)

        # index already exists
        sync_table(IndexCaseSensitiveModel)
        table_meta = management._get_table_metadata(IndexCaseSensitiveModel)
        self.assertIn("index_IndexModel_second_key", table_meta.indexes)


class NonModelFailureTest(BaseCassEngTestCase):
    class FakeModel(object):
        pass

    def test_failure(self):
        with self.assertRaises(CQLEngineException):
            sync_table(self.FakeModel)

class StaticColumnTests(BaseCassEngTestCase):
    def test_static_columns(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest("Native protocol 2+ required, currently using: {0}".format(PROTOCOL_VERSION))

        class StaticModel(Model):
            id = columns.Integer(primary_key=True)
            c = columns.Integer(primary_key=True)
            name = columns.Text(static=True)

        drop_table(StaticModel)

        session = get_session()

        with mock.patch.object(session, "execute", wraps=session.execute) as m:
            sync_table(StaticModel)

        self.assertGreater(m.call_count, 0)
        statement = m.call_args[0][0].query_string
        self.assertIn('"name" text static', statement)

        # if we sync again, we should not apply an alter w/ a static
        sync_table(StaticModel)

        with mock.patch.object(session, "execute", wraps=session.execute) as m2:
            sync_table(StaticModel)

        self.assertEqual(len(m2.call_args_list), 0)
