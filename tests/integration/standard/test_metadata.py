# Copyright 2013-2015 DataStax, Inc.
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

import difflib
from mock import Mock
import six
import sys

from cassandra import AlreadyExists

from cassandra.cluster import Cluster
from cassandra.metadata import (Metadata, KeyspaceMetadata, TableMetadata, IndexMetadata,
                                Token, MD5Token, TokenMap, murmur3)
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host

from tests.integration import (get_cluster, use_singledc, PROTOCOL_VERSION,
                               get_server_versions)


def setup_module():
    use_singledc()


class SchemaMetadataTests(unittest.TestCase):

    ksname = "schemametadatatest"

    @property
    def cfname(self):
        return self._testMethodName.lower()

    def setUp(self):
        self._cass_version, self._cql_version = get_server_versions()

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        self.session.execute("CREATE KEYSPACE schemametadatatest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")

    def tearDown(self):
        self.session.execute("DROP KEYSPACE schemametadatatest")
        self.cluster.shutdown()

    def make_create_statement(self, partition_cols, clustering_cols=None, other_cols=None, compact=False):
        clustering_cols = clustering_cols or []
        other_cols = other_cols or []

        statement = "CREATE TABLE %s.%s (" % (self.ksname, self.cfname)
        if len(partition_cols) == 1 and not clustering_cols:
            statement += "%s text PRIMARY KEY, " % partition_cols[0]
        else:
            statement += ", ".join("%s text" % col for col in partition_cols)
            statement += ", "

        statement += ", ".join("%s text" % col for col in clustering_cols + other_cols)

        if len(partition_cols) != 1 or clustering_cols:
            statement += ", PRIMARY KEY ("

            if len(partition_cols) > 1:
                statement += "(" + ", ".join(partition_cols) + ")"
            else:
                statement += partition_cols[0]

            if clustering_cols:
                statement += ", "
                statement += ", ".join(clustering_cols)

            statement += ")"

        statement += ")"
        if compact:
            statement += " WITH COMPACT STORAGE"

        return statement

    def check_create_statement(self, tablemeta, original):
        recreate = tablemeta.as_cql_query(formatted=False)
        self.assertEqual(original, recreate[:len(original)])
        self.session.execute("DROP TABLE %s.%s" % (self.ksname, self.cfname))
        self.session.execute(recreate)

        # create the table again, but with formatting enabled
        self.session.execute("DROP TABLE %s.%s" % (self.ksname, self.cfname))
        recreate = tablemeta.as_cql_query(formatted=True)
        self.session.execute(recreate)

    def get_table_metadata(self):
        self.cluster.control_connection.refresh_schema()
        return self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

    def test_basic_table_meta_properties(self):
        create_statement = self.make_create_statement(["a"], [], ["b", "c"])
        self.session.execute(create_statement)

        self.cluster.control_connection.refresh_schema()

        meta = self.cluster.metadata
        self.assertNotEqual(meta.cluster_name, None)
        self.assertTrue(self.ksname in meta.keyspaces)
        ksmeta = meta.keyspaces[self.ksname]

        self.assertEqual(ksmeta.name, self.ksname)
        self.assertTrue(ksmeta.durable_writes)
        self.assertEqual(ksmeta.replication_strategy.name, 'SimpleStrategy')
        self.assertEqual(ksmeta.replication_strategy.replication_factor, 1)

        self.assertTrue(self.cfname in ksmeta.tables)
        tablemeta = ksmeta.tables[self.cfname]
        self.assertEqual(tablemeta.keyspace, ksmeta)
        self.assertEqual(tablemeta.name, self.cfname)

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        for option in tablemeta.options:
            self.assertIn(option, TableMetadata.recognized_options)

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual(
            [u'a', u'b', u'c', u'd', u'e', u'f'],
            sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"])
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        create_statement += " WITH CLUSTERING ORDER BY (c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd', u'e'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_compact(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"], compact=True)
        create_statement += " AND CLUSTERING ORDER BY (b ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns_compact(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d"], compact=True)
        create_statement += " AND CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"], compact=True)
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d"], compact=True)
        create_statement += " AND CLUSTERING ORDER BY (c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_ordering(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH CLUSTERING ORDER BY (b DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns_ordering(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b DESC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_ordering(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        create_statement += " WITH CLUSTERING ORDER BY (c DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_indexes(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)

        d_index = "CREATE INDEX d_index ON %s.%s (d)" % (self.ksname, self.cfname)
        e_index = "CREATE INDEX e_index ON %s.%s (e)" % (self.ksname, self.cfname)
        self.session.execute(d_index)
        self.session.execute(e_index)

        tablemeta = self.get_table_metadata()
        statements = tablemeta.export_as_string().strip()
        statements = [s.strip() for s in statements.split(';')]
        statements = list(filter(bool, statements))
        self.assertEqual(3, len(statements))
        self.assertEqual(d_index, statements[1])
        self.assertEqual(e_index, statements[2])

        # make sure indexes are included in KeyspaceMetadata.export_as_string()
        ksmeta = self.cluster.metadata.keyspaces[self.ksname]
        statement = ksmeta.export_as_string()
        self.assertIn('CREATE INDEX d_index', statement)
        self.assertIn('CREATE INDEX e_index', statement)

    def test_collection_indexes(self):
        if get_server_versions()[0] < (2, 1, 0):
            raise unittest.SkipTest("Secondary index on collections were introduced in Cassandra 2.1")

        self.session.execute("CREATE TABLE %s.%s (a int PRIMARY KEY, b map<text, text>)"
                             % (self.ksname, self.cfname))
        self.session.execute("CREATE INDEX index1 ON %s.%s (keys(b))"
                             % (self.ksname, self.cfname))

        tablemeta = self.get_table_metadata()
        self.assertIn('(keys(b))', tablemeta.export_as_string())

        self.session.execute("DROP INDEX %s.index1" % (self.ksname,))
        self.session.execute("CREATE INDEX index2 ON %s.%s (b)"
                             % (self.ksname, self.cfname))

        tablemeta = self.get_table_metadata()
        self.assertIn(' (b)', tablemeta.export_as_string())

        # test full indexes on frozen collections, if available
        if get_server_versions()[0] >= (2, 1, 3):
            self.session.execute("DROP TABLE %s.%s" % (self.ksname, self.cfname))
            self.session.execute("CREATE TABLE %s.%s (a int PRIMARY KEY, b frozen<map<text, text>>)"
                                 % (self.ksname, self.cfname))
            self.session.execute("CREATE INDEX index3 ON %s.%s (full(b))"
                                 % (self.ksname, self.cfname))

            tablemeta = self.get_table_metadata()
            self.assertIn('(full(b))', tablemeta.export_as_string())

    def test_compression_disabled(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH compression = {}"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.assertIn("compression = {}", tablemeta.export_as_string())


class TestCodeCoverage(unittest.TestCase):

    def test_export_schema(self):
        """
        Test export schema functionality
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.connect()

        self.assertIsInstance(cluster.metadata.export_schema_as_string(), six.string_types)

    def test_export_keyspace_schema(self):
        """
        Test export keyspace schema functionality
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.connect()

        for keyspace in cluster.metadata.keyspaces:
            keyspace_metadata = cluster.metadata.keyspaces[keyspace]
            self.assertIsInstance(keyspace_metadata.export_as_string(), six.string_types)
            self.assertIsInstance(keyspace_metadata.as_cql_query(), six.string_types)
        cluster.shutdown()

    def assert_equal_diff(self, received, expected):
        if received != expected:
            diff_string = '\n'.join(difflib.unified_diff(expected.split('\n'),
                                                         received.split('\n'),
                                                         'EXPECTED', 'RECEIVED',
                                                         lineterm=''))
            self.fail(diff_string)

    def test_export_keyspace_schema_udts(self):
        """
        Test udt exports
        """

        if get_server_versions()[0] < (2, 1, 0):
            raise unittest.SkipTest('UDTs were introduced in Cassandra 2.1')

        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest(
                "Protocol 3.0+ is required for UDT change events, currently testing against %r"
                % (PROTOCOL_VERSION,))

        if sys.version_info[0:2] != (2, 7):
            raise unittest.SkipTest('This test compares static strings generated from dict items, which may change orders. Test with 2.7.')

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE export_udts
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            AND durable_writes = true;
        """)
        session.execute("""
            CREATE TYPE export_udts.street (
                street_number int,
                street_name text)
        """)
        session.execute("""
            CREATE TYPE export_udts.zip (
                zipcode int,
                zip_plus_4 int)
        """)
        session.execute("""
            CREATE TYPE export_udts.address (
                street_address frozen<street>,
                zip_code frozen<zip>)
        """)
        session.execute("""
            CREATE TABLE export_udts.users (
            user text PRIMARY KEY,
            addresses map<text, frozen<address>>)
        """)

        expected_string = """CREATE KEYSPACE export_udts WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TYPE export_udts.street (
    street_number int,
    street_name text
);

CREATE TYPE export_udts.zip (
    zipcode int,
    zip_plus_4 int
);

CREATE TYPE export_udts.address (
    street_address frozen<street>,
    zip_code frozen<zip>
);

CREATE TABLE export_udts.users (
    user text PRIMARY KEY,
    addresses map<text, frozen<address>>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';"""

        self.assert_equal_diff(cluster.metadata.keyspaces['export_udts'].export_as_string(), expected_string)

        table_meta = cluster.metadata.keyspaces['export_udts'].tables['users']

        expected_string = """CREATE TABLE export_udts.users (
    user text PRIMARY KEY,
    addresses map<text, frozen<address>>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';"""

        self.assert_equal_diff(table_meta.export_as_string(), expected_string)

        cluster.shutdown()

    def test_case_sensitivity(self):
        """
        Test that names that need to be escaped in CREATE statements are
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        ksname = 'AnInterestingKeyspace'
        cfname = 'AnInterestingTable'

        session.execute("""
            CREATE KEYSPACE "%s"
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """ % (ksname,))
        session.execute("""
            CREATE TABLE "%s"."%s" (
                k int,
                "A" int,
                "B" int,
                "MyColumn" int,
                PRIMARY KEY (k, "A"))
            WITH CLUSTERING ORDER BY ("A" DESC)
            """ % (ksname, cfname))
        session.execute("""
            CREATE INDEX myindex ON "%s"."%s" ("MyColumn")
            """ % (ksname, cfname))

        ksmeta = cluster.metadata.keyspaces[ksname]
        schema = ksmeta.export_as_string()
        self.assertIn('CREATE KEYSPACE "AnInterestingKeyspace"', schema)
        self.assertIn('CREATE TABLE "AnInterestingKeyspace"."AnInterestingTable"', schema)
        self.assertIn('"A" int', schema)
        self.assertIn('"B" int', schema)
        self.assertIn('"MyColumn" int', schema)
        self.assertIn('PRIMARY KEY (k, "A")', schema)
        self.assertIn('WITH CLUSTERING ORDER BY ("A" DESC)', schema)
        self.assertIn('CREATE INDEX myindex ON "AnInterestingKeyspace"."AnInterestingTable" ("MyColumn")', schema)
        cluster.shutdown()

    def test_already_exists_exceptions(self):
        """
        Ensure AlreadyExists exception is thrown when hit
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        ksname = 'test3rf'
        cfname = 'test'

        ddl = '''
            CREATE KEYSPACE %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}'''
        self.assertRaises(AlreadyExists, session.execute, ddl % ksname)

        ddl = '''
            CREATE TABLE %s.%s (
                k int PRIMARY KEY,
                v int )'''
        self.assertRaises(AlreadyExists, session.execute, ddl % (ksname, cfname))
        cluster.shutdown()

    def test_replicas(self):
        """
        Ensure cluster.metadata.get_replicas return correctly when not attached to keyspace
        """
        if murmur3 is None:
            raise unittest.SkipTest('the murmur3 extension is not available')

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.assertEqual(cluster.metadata.get_replicas('test3rf', 'key'), [])

        cluster.connect('test3rf')

        self.assertNotEqual(list(cluster.metadata.get_replicas('test3rf', 'key')), [])
        host = list(cluster.metadata.get_replicas('test3rf', 'key'))[0]
        self.assertEqual(host.datacenter, 'dc1')
        self.assertEqual(host.rack, 'r1')
        cluster.shutdown()

    def test_token_map(self):
        """
        Test token mappings
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.connect('test3rf')
        ring = cluster.metadata.token_map.ring
        owners = list(cluster.metadata.token_map.token_to_host_owner[token] for token in ring)
        get_replicas = cluster.metadata.token_map.get_replicas

        for ksname in ('test1rf', 'test2rf', 'test3rf'):
            self.assertNotEqual(list(get_replicas(ksname, ring[0])), [])

        for i, token in enumerate(ring):
            self.assertEqual(set(get_replicas('test3rf', token)), set(owners))
            self.assertEqual(set(get_replicas('test2rf', token)), set([owners[(i + 1) % 3], owners[(i + 2) % 3]]))
            self.assertEqual(set(get_replicas('test1rf', token)), set([owners[(i + 1) % 3]]))
        cluster.shutdown()

    def test_legacy_tables(self):

        if get_server_versions()[0] < (2, 1, 0):
            raise unittest.SkipTest('Test schema output assumes 2.1.0+ options')

        if sys.version_info[0:2] != (2, 7):
            raise unittest.SkipTest('This test compares static strings generated from dict items, which may change orders. Test with 2.7.')

        cli_script = """CREATE KEYSPACE legacy
WITH placement_strategy = 'SimpleStrategy'
AND strategy_options = {replication_factor:1};

USE legacy;

CREATE COLUMN FAMILY simple_no_col
 WITH comparator = UTF8Type
 AND key_validation_class = UUIDType
 AND default_validation_class = UTF8Type;

CREATE COLUMN FAMILY simple_with_col
 WITH comparator = UTF8Type
 and key_validation_class = UUIDType
 and default_validation_class = UTF8Type
 AND column_metadata = [
 {column_name: col_with_meta, validation_class: UTF8Type}
 ];

CREATE COLUMN FAMILY composite_partition_no_col
 WITH comparator = UTF8Type
 AND key_validation_class = 'CompositeType(UUIDType,UTF8Type)'
 AND default_validation_class = UTF8Type;

CREATE COLUMN FAMILY composite_partition_with_col
 WITH comparator = UTF8Type
 AND key_validation_class = 'CompositeType(UUIDType,UTF8Type)'
 AND default_validation_class = UTF8Type
 AND column_metadata = [
 {column_name: col_with_meta, validation_class: UTF8Type}
 ];

CREATE COLUMN FAMILY nested_composite_key
 WITH comparator = UTF8Type
 and key_validation_class = 'CompositeType(CompositeType(UUIDType,UTF8Type), LongType)'
 and default_validation_class = UTF8Type
 AND column_metadata = [
 {column_name: full_name, validation_class: UTF8Type}
 ];

create column family composite_comp_no_col
  with column_type = 'Standard'
  and comparator = 'DynamicCompositeType(t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,b=>org.apache.cassandra.db.marshal.BytesType)'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'BytesType'
  and read_repair_chance = 0.0
  and dclocal_read_repair_chance = 0.1
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and cells_per_row_to_cache = '0'
  and default_time_to_live = 0
  and speculative_retry = 'NONE'
  and comment = 'Stores file meta data';

create column family composite_comp_with_col
  with column_type = 'Standard'
  and comparator = 'DynamicCompositeType(t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,b=>org.apache.cassandra.db.marshal.BytesType)'
  and default_validation_class = 'BytesType'
  and key_validation_class = 'BytesType'
  and read_repair_chance = 0.0
  and dclocal_read_repair_chance = 0.1
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and compaction_strategy = 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'
  and caching = 'KEYS_ONLY'
  and cells_per_row_to_cache = '0'
  and default_time_to_live = 0
  and speculative_retry = 'NONE'
  and comment = 'Stores file meta data'
  and column_metadata = [
    {column_name : 'b@6d616d6d616a616d6d61',
    validation_class : BytesType,
    index_name : 'idx_one',
    index_type : 0},
    {column_name : 'b@6869746d65776974686d75736963',
    validation_class : BytesType,
    index_name : 'idx_two',
    index_type : 0}]
  and compression_options = {'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor'};"""

        # note: the inner key type for legacy.nested_composite_key
        # (org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType, org.apache.cassandra.db.marshal.UTF8Type))
        # is a bit strange, but it replays in CQL with desired results
        expected_string = """CREATE KEYSPACE legacy WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

/*
Warning: Table legacy.composite_comp_with_col omitted because it has constructs not compatible with CQL (was created via legacy API).

Approximate structure, for reference:
(this should not be used to reproduce this schema)

CREATE TABLE legacy.composite_comp_with_col (
    key blob,
    t timeuuid,
    b blob,
    s text,
    "b@6869746d65776974686d75736963" blob,
    "b@6d616d6d616a616d6d61" blob,
    PRIMARY KEY (key, t, b, s)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (t ASC, b ASC, s ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = 'Stores file meta data'
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';
CREATE INDEX idx_two ON legacy.composite_comp_with_col ("b@6869746d65776974686d75736963");
CREATE INDEX idx_one ON legacy.composite_comp_with_col ("b@6d616d6d616a616d6d61");
*/

CREATE TABLE legacy.nested_composite_key (
    key 'org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UUIDType, org.apache.cassandra.db.marshal.UTF8Type)',
    key2 bigint,
    full_name text,
    PRIMARY KEY ((key, key2))
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';

CREATE TABLE legacy.composite_partition_with_col (
    key uuid,
    key2 text,
    col_with_meta text,
    PRIMARY KEY ((key, key2))
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';

CREATE TABLE legacy.composite_partition_no_col (
    key uuid,
    key2 text,
    column1 text,
    value text,
    PRIMARY KEY ((key, key2), column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';

CREATE TABLE legacy.simple_with_col (
    key uuid PRIMARY KEY,
    col_with_meta text
) WITH COMPACT STORAGE
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';

CREATE TABLE legacy.simple_no_col (
    key uuid,
    column1 text,
    value text,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';

/*
Warning: Table legacy.composite_comp_no_col omitted because it has constructs not compatible with CQL (was created via legacy API).

Approximate structure, for reference:
(this should not be used to reproduce this schema)

CREATE TABLE legacy.composite_comp_no_col (
    key blob,
    column1 'org.apache.cassandra.db.marshal.DynamicCompositeType(org.apache.cassandra.db.marshal.TimeUUIDType, org.apache.cassandra.db.marshal.BytesType, org.apache.cassandra.db.marshal.UTF8Type)',
    column2 text,
    value blob,
    PRIMARY KEY (key, column1, column1, column2)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (column1 ASC, column1 ASC, column2 ASC)
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = 'Stores file meta data'
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';
*/"""

        ccm = get_cluster()
        ccm.run_cli(cli_script)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        legacy_meta = cluster.metadata.keyspaces['legacy']
        self.assert_equal_diff(legacy_meta.export_as_string(), expected_string)

        session.execute('DROP KEYSPACE legacy')

        cluster.shutdown()


class TokenMetadataTest(unittest.TestCase):
    """
    Test of TokenMap creation and other behavior.
    """

    def test_token(self):
        expected_node_count = len(get_cluster().nodes)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cluster.connect()
        tmap = cluster.metadata.token_map
        self.assertTrue(issubclass(tmap.token_class, Token))
        self.assertEqual(expected_node_count, len(tmap.ring))
        cluster.shutdown()

    def test_getting_replicas(self):
        tokens = [MD5Token(str(i)) for i in range(0, (2 ** 127 - 1), 2 ** 125)]
        hosts = [Host("ip%d" % i, SimpleConvictionPolicy) for i in range(len(tokens))]
        token_to_primary_replica = dict(zip(tokens, hosts))
        keyspace = KeyspaceMetadata("ks", True, "SimpleStrategy", {"replication_factor": "1"})
        metadata = Mock(spec=Metadata, keyspaces={'ks': keyspace})
        token_map = TokenMap(MD5Token, token_to_primary_replica, tokens, metadata)

        # tokens match node tokens exactly
        for i, token in enumerate(tokens):
            expected_host = hosts[(i + 1) % len(hosts)]
            replicas = token_map.get_replicas("ks", token)
            self.assertEqual(set(replicas), set([expected_host]))

        # shift the tokens back by one
        for token, expected_host in zip(tokens, hosts):
            replicas = token_map.get_replicas("ks", MD5Token(str(token.value - 1)))
            self.assertEqual(set(replicas), set([expected_host]))

        # shift the tokens forward by one
        for i, token in enumerate(tokens):
            replicas = token_map.get_replicas("ks", MD5Token(str(token.value + 1)))
            expected_host = hosts[(i + 1) % len(hosts)]
            self.assertEqual(set(replicas), set([expected_host]))


class KeyspaceAlterMetadata(unittest.TestCase):
    """
    Test verifies that table metadata is preserved on keyspace alter
    """
    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        name = self._testMethodName.lower()
        crt_ks = '''
                CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = true''' % name
        self.session.execute(crt_ks)

    def tearDown(self):
        name = self._testMethodName.lower()
        self.session.execute('DROP KEYSPACE %s' % name)
        self.cluster.shutdown()

    def test_keyspace_alter(self):
        """
        Table info is preserved upon keyspace alter:
        Create table
        Verify schema
        Alter ks
        Verify that table metadata is still present

        PYTHON-173
        """
        name = self._testMethodName.lower()

        self.session.execute('CREATE TABLE %s.d (d INT PRIMARY KEY)' % name)
        original_keyspace_meta = self.cluster.metadata.keyspaces[name]
        self.assertEqual(original_keyspace_meta.durable_writes, True)
        self.assertEqual(len(original_keyspace_meta.tables), 1)

        self.session.execute('ALTER KEYSPACE %s WITH durable_writes = false' % name)
        new_keyspace_meta = self.cluster.metadata.keyspaces[name]
        self.assertNotEqual(original_keyspace_meta, new_keyspace_meta)
        self.assertEqual(new_keyspace_meta.durable_writes, False)


class IndexMapTests(unittest.TestCase):

    keyspace_name = 'index_map_tests'

    @property
    def table_name(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        try:
            if cls.keyspace_name in cls.cluster.metadata.keyspaces:
                cls.session.execute("DROP KEYSPACE %s" % cls.keyspace_name)

            cls.session.execute(
                """
                CREATE KEYSPACE %s
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                """ % cls.keyspace_name)
            cls.session.set_keyspace(cls.keyspace_name)
        except Exception:
            cls.cluster.shutdown()
            raise

    @classmethod
    def teardown_class(cls):
        try:
            cls.session.execute("DROP KEYSPACE %s" % cls.keyspace_name)
        finally:
            cls.cluster.shutdown()

    def create_basic_table(self):
        self.session.execute("CREATE TABLE %s (k int PRIMARY KEY, a int)" % self.table_name)

    def drop_basic_table(self):
        self.session.execute("DROP TABLE %s" % self.table_name)

    def test_index_updates(self):
        self.create_basic_table()

        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertNotIn('a_idx', ks_meta.indexes)
        self.assertNotIn('b_idx', ks_meta.indexes)
        self.assertNotIn('a_idx', table_meta.indexes)
        self.assertNotIn('b_idx', table_meta.indexes)

        self.session.execute("CREATE INDEX a_idx ON %s (a)" % self.table_name)
        self.session.execute("ALTER TABLE %s ADD b int" % self.table_name)
        self.session.execute("CREATE INDEX b_idx ON %s (b)" % self.table_name)

        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertIsInstance(ks_meta.indexes['a_idx'], IndexMetadata)
        self.assertIsInstance(ks_meta.indexes['b_idx'], IndexMetadata)
        self.assertIsInstance(table_meta.indexes['a_idx'], IndexMetadata)
        self.assertIsInstance(table_meta.indexes['b_idx'], IndexMetadata)

        # both indexes updated when index dropped
        self.session.execute("DROP INDEX a_idx")
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertNotIn('a_idx', ks_meta.indexes)
        self.assertIsInstance(ks_meta.indexes['b_idx'], IndexMetadata)
        self.assertNotIn('a_idx', table_meta.indexes)
        self.assertIsInstance(table_meta.indexes['b_idx'], IndexMetadata)

        # keyspace index updated when table dropped
        self.drop_basic_table()
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        self.assertNotIn(self.table_name, ks_meta.tables)
        self.assertNotIn('a_idx', ks_meta.indexes)
        self.assertNotIn('b_idx', ks_meta.indexes)

    def test_index_follows_alter(self):
        self.create_basic_table()

        idx = self.table_name + '_idx'
        self.session.execute("CREATE INDEX %s ON %s (a)" % (idx, self.table_name))
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertIsInstance(ks_meta.indexes[idx], IndexMetadata)
        self.assertIsInstance(table_meta.indexes[idx], IndexMetadata)
        self.session.execute('ALTER KEYSPACE %s WITH durable_writes = false' % self.keyspace_name)
        old_meta = ks_meta
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        self.assertIsNot(ks_meta, old_meta)
        table_meta = ks_meta.tables[self.table_name]
        self.assertIsInstance(ks_meta.indexes[idx], IndexMetadata)
        self.assertIsInstance(table_meta.indexes[idx], IndexMetadata)
        self.drop_basic_table()
