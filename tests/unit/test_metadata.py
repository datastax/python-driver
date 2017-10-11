# Copyright 2013-2017 DataStax, Inc.
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

from binascii import unhexlify
from mock import Mock
import os
import six
import timeit

import cassandra
from cassandra.marshal import uint16_unpack, uint16_pack
from cassandra.metadata import (Murmur3Token, MD5Token,
                                BytesToken, ReplicationStrategy,
                                NetworkTopologyStrategy, SimpleStrategy,
                                LocalStrategy, protect_name,
                                protect_names, protect_value, is_valid_name,
                                UserType, KeyspaceMetadata, get_schema_parser,
                                _UnknownStrategy, ColumnMetadata, TableMetadata,
                                IndexMetadata, Function, Aggregate,
                                Metadata, TokenMap)
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host


class StrategiesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        "Hook method for setting up class fixture before running tests in the class."
        if not hasattr(cls, 'assertItemsEqual'):
            cls.assertItemsEqual = cls.assertCountEqual

    def test_replication_strategy(self):
        """
        Basic code coverage testing that ensures different ReplicationStrategies
        can be initiated using parameters correctly.
        """

        rs = ReplicationStrategy()

        self.assertEqual(rs.create('OldNetworkTopologyStrategy', None), _UnknownStrategy('OldNetworkTopologyStrategy', None))
        fake_options_map = {'options': 'map'}
        uks = rs.create('OldNetworkTopologyStrategy', fake_options_map)
        self.assertEqual(uks, _UnknownStrategy('OldNetworkTopologyStrategy', fake_options_map))
        self.assertEqual(uks.make_token_replica_map({}, []), {})

        fake_options_map = {'dc1': '3'}
        self.assertIsInstance(rs.create('NetworkTopologyStrategy', fake_options_map), NetworkTopologyStrategy)
        self.assertEqual(rs.create('NetworkTopologyStrategy', fake_options_map).dc_replication_factors,
                         NetworkTopologyStrategy(fake_options_map).dc_replication_factors)

        fake_options_map = {'options': 'map'}
        self.assertIsNone(rs.create('SimpleStrategy', fake_options_map))

        fake_options_map = {'options': 'map'}
        self.assertIsInstance(rs.create('LocalStrategy', fake_options_map), LocalStrategy)

        fake_options_map = {'options': 'map', 'replication_factor': 3}
        self.assertIsInstance(rs.create('SimpleStrategy', fake_options_map), SimpleStrategy)
        self.assertEqual(rs.create('SimpleStrategy', fake_options_map).replication_factor,
                         SimpleStrategy(fake_options_map).replication_factor)

        self.assertEqual(rs.create('xxxxxxxx', fake_options_map), _UnknownStrategy('xxxxxxxx', fake_options_map))

        self.assertRaises(NotImplementedError, rs.make_token_replica_map, None, None)
        self.assertRaises(NotImplementedError, rs.export_for_schema)

    def test_nts_make_token_replica_map(self):
        token_to_host_owner = {}

        dc1_1 = Host('dc1.1', SimpleConvictionPolicy)
        dc1_2 = Host('dc1.2', SimpleConvictionPolicy)
        dc1_3 = Host('dc1.3', SimpleConvictionPolicy)
        for host in (dc1_1, dc1_2, dc1_3):
            host.set_location_info('dc1', 'rack1')
        token_to_host_owner[MD5Token(0)] = dc1_1
        token_to_host_owner[MD5Token(100)] = dc1_2
        token_to_host_owner[MD5Token(200)] = dc1_3

        dc2_1 = Host('dc2.1', SimpleConvictionPolicy)
        dc2_2 = Host('dc2.2', SimpleConvictionPolicy)
        dc2_1.set_location_info('dc2', 'rack1')
        dc2_2.set_location_info('dc2', 'rack1')
        token_to_host_owner[MD5Token(1)] = dc2_1
        token_to_host_owner[MD5Token(101)] = dc2_2

        dc3_1 = Host('dc3.1', SimpleConvictionPolicy)
        dc3_1.set_location_info('dc3', 'rack3')
        token_to_host_owner[MD5Token(2)] = dc3_1

        ring = [MD5Token(0),
                MD5Token(1),
                MD5Token(2),
                MD5Token(100),
                MD5Token(101),
                MD5Token(200)]

        nts = NetworkTopologyStrategy({'dc1': 2, 'dc2': 2, 'dc3': 1})
        replica_map = nts.make_token_replica_map(token_to_host_owner, ring)

        self.assertItemsEqual(replica_map[MD5Token(0)], (dc1_1, dc1_2, dc2_1, dc2_2, dc3_1))

    def test_nts_token_performance(self):
        """
        Tests to ensure that when rf exceeds the number of nodes available, that we dont'
        needlessly iterate trying to construct tokens for nodes that don't exist.

        @since 3.7
        @jira_ticket PYTHON-379
        @expected_result timing with 1500 rf should be same/similar to 3rf if we have 3 nodes

        @test_category metadata
        """

        token_to_host_owner = {}
        ring = []
        dc1hostnum = 3
        current_token = 0
        vnodes_per_host = 500
        for i in range(dc1hostnum):

            host = Host('dc1.{0}'.format(i), SimpleConvictionPolicy)
            host.set_location_info('dc1', "rack1")
            for vnode_num in range(vnodes_per_host):
                md5_token = MD5Token(current_token+vnode_num)
                token_to_host_owner[md5_token] = host
                ring.append(md5_token)
            current_token += 1000

        nts = NetworkTopologyStrategy({'dc1': 3})
        start_time = timeit.default_timer()
        nts.make_token_replica_map(token_to_host_owner, ring)
        elapsed_base = timeit.default_timer() - start_time

        nts = NetworkTopologyStrategy({'dc1': 1500})
        start_time = timeit.default_timer()
        nts.make_token_replica_map(token_to_host_owner, ring)
        elapsed_bad = timeit.default_timer() - start_time
        difference = elapsed_bad - elapsed_base
        self.assertTrue(difference < 1 and difference > -1)

    def test_nts_make_token_replica_map_multi_rack(self):
        token_to_host_owner = {}

        # (A) not enough distinct racks, first skipped is used
        dc1_1 = Host('dc1.1', SimpleConvictionPolicy)
        dc1_2 = Host('dc1.2', SimpleConvictionPolicy)
        dc1_3 = Host('dc1.3', SimpleConvictionPolicy)
        dc1_4 = Host('dc1.4', SimpleConvictionPolicy)
        dc1_1.set_location_info('dc1', 'rack1')
        dc1_2.set_location_info('dc1', 'rack1')
        dc1_3.set_location_info('dc1', 'rack2')
        dc1_4.set_location_info('dc1', 'rack2')
        token_to_host_owner[MD5Token(0)] = dc1_1
        token_to_host_owner[MD5Token(100)] = dc1_2
        token_to_host_owner[MD5Token(200)] = dc1_3
        token_to_host_owner[MD5Token(300)] = dc1_4

        # (B) distinct racks, but not contiguous
        dc2_1 = Host('dc2.1', SimpleConvictionPolicy)
        dc2_2 = Host('dc2.2', SimpleConvictionPolicy)
        dc2_3 = Host('dc2.3', SimpleConvictionPolicy)
        dc2_1.set_location_info('dc2', 'rack1')
        dc2_2.set_location_info('dc2', 'rack1')
        dc2_3.set_location_info('dc2', 'rack2')
        token_to_host_owner[MD5Token(1)] = dc2_1
        token_to_host_owner[MD5Token(101)] = dc2_2
        token_to_host_owner[MD5Token(201)] = dc2_3

        ring = [MD5Token(0),
                MD5Token(1),
                MD5Token(100),
                MD5Token(101),
                MD5Token(200),
                MD5Token(201),
                MD5Token(300)]

        nts = NetworkTopologyStrategy({'dc1': 3, 'dc2': 2})
        replica_map = nts.make_token_replica_map(token_to_host_owner, ring)

        token_replicas = replica_map[MD5Token(0)]
        self.assertItemsEqual(token_replicas, (dc1_1, dc1_2, dc1_3, dc2_1, dc2_3))

    def test_nts_make_token_replica_map_empty_dc(self):
        host = Host('1', SimpleConvictionPolicy)
        host.set_location_info('dc1', 'rack1')
        token_to_host_owner = {MD5Token(0): host}
        ring = [MD5Token(0)]
        nts = NetworkTopologyStrategy({'dc1': 1, 'dc2': 0})

        replica_map = nts.make_token_replica_map(token_to_host_owner, ring)
        self.assertEqual(set(replica_map[MD5Token(0)]), set([host]))

    def test_nts_export_for_schema(self):
        strategy = NetworkTopologyStrategy({'dc1': '1', 'dc2': '2'})
        self.assertEqual("{'class': 'NetworkTopologyStrategy', 'dc1': '1', 'dc2': '2'}",
                         strategy.export_for_schema())

    def test_simple_strategy_make_token_replica_map(self):
        host1 = Host('1', SimpleConvictionPolicy)
        host2 = Host('2', SimpleConvictionPolicy)
        host3 = Host('3', SimpleConvictionPolicy)
        token_to_host_owner = {
            MD5Token(0): host1,
            MD5Token(100): host2,
            MD5Token(200): host3
        }
        ring = [MD5Token(0), MD5Token(100), MD5Token(200)]

        rf1_replicas = SimpleStrategy({'replication_factor': '1'}).make_token_replica_map(token_to_host_owner, ring)
        self.assertItemsEqual(rf1_replicas[MD5Token(0)], [host1])
        self.assertItemsEqual(rf1_replicas[MD5Token(100)], [host2])
        self.assertItemsEqual(rf1_replicas[MD5Token(200)], [host3])

        rf2_replicas = SimpleStrategy({'replication_factor': '2'}).make_token_replica_map(token_to_host_owner, ring)
        self.assertItemsEqual(rf2_replicas[MD5Token(0)], [host1, host2])
        self.assertItemsEqual(rf2_replicas[MD5Token(100)], [host2, host3])
        self.assertItemsEqual(rf2_replicas[MD5Token(200)], [host3, host1])

        rf3_replicas = SimpleStrategy({'replication_factor': '3'}).make_token_replica_map(token_to_host_owner, ring)
        self.assertItemsEqual(rf3_replicas[MD5Token(0)], [host1, host2, host3])
        self.assertItemsEqual(rf3_replicas[MD5Token(100)], [host2, host3, host1])
        self.assertItemsEqual(rf3_replicas[MD5Token(200)], [host3, host1, host2])

    def test_ss_equals(self):
        self.assertNotEqual(SimpleStrategy({'replication_factor': '1'}), NetworkTopologyStrategy({'dc1': 2}))


class NameEscapingTest(unittest.TestCase):

    def test_protect_name(self):
        """
        Test cassandra.metadata.protect_name output
        """
        self.assertEqual(protect_name('tests'), 'tests')
        self.assertEqual(protect_name('test\'s'), '"test\'s"')
        self.assertEqual(protect_name('test\'s'), "\"test's\"")
        self.assertEqual(protect_name('tests ?!@#$%^&*()'), '"tests ?!@#$%^&*()"')
        self.assertEqual(protect_name('1'), '"1"')
        self.assertEqual(protect_name('1test'), '"1test"')

    def test_protect_names(self):
        """
        Test cassandra.metadata.protect_names output
        """
        self.assertEqual(protect_names(['tests']), ['tests'])
        self.assertEqual(protect_names(
            [
                'tests',
                'test\'s',
                'tests ?!@#$%^&*()',
                '1'
            ]),
            [
                'tests',
                "\"test's\"",
                '"tests ?!@#$%^&*()"',
                '"1"'
            ])

    def test_protect_value(self):
        """
        Test cassandra.metadata.protect_value output
        """
        self.assertEqual(protect_value(True), "true")
        self.assertEqual(protect_value(False), "false")
        self.assertEqual(protect_value(3.14), '3.14')
        self.assertEqual(protect_value(3), '3')
        self.assertEqual(protect_value('test'), "'test'")
        self.assertEqual(protect_value('test\'s'), "'test''s'")
        self.assertEqual(protect_value(None), 'NULL')

    def test_is_valid_name(self):
        """
        Test cassandra.metadata.is_valid_name output
        """
        self.assertEqual(is_valid_name(None), False)
        self.assertEqual(is_valid_name('test'), True)
        self.assertEqual(is_valid_name('Test'), False)
        self.assertEqual(is_valid_name('t_____1'), True)
        self.assertEqual(is_valid_name('test1'), True)
        self.assertEqual(is_valid_name('1test1'), False)

        invalid_keywords = cassandra.metadata.cql_keywords - cassandra.metadata.cql_keywords_unreserved
        for keyword in invalid_keywords:
            self.assertEqual(is_valid_name(keyword), False)


class GetReplicasTest(unittest.TestCase):
    def _get_replicas(self, token_klass):
        tokens = [token_klass(i) for i in range(0, (2 ** 127 - 1), 2 ** 125)]
        hosts = [Host("ip%d" % i, SimpleConvictionPolicy) for i in range(len(tokens))]
        token_to_primary_replica = dict(zip(tokens, hosts))
        keyspace = KeyspaceMetadata("ks", True, "SimpleStrategy", {"replication_factor": "1"})
        metadata = Mock(spec=Metadata, keyspaces={'ks': keyspace})
        token_map = TokenMap(token_klass, token_to_primary_replica, tokens, metadata)

        # tokens match node tokens exactly
        for i, token in enumerate(tokens):
            expected_host = hosts[(i + 1) % len(hosts)]
            replicas = token_map.get_replicas("ks", token)
            self.assertEqual(set(replicas), {expected_host})

        # shift the tokens back by one
        for token, expected_host in zip(tokens, hosts):
            replicas = token_map.get_replicas("ks", token_klass(token.value - 1))
            self.assertEqual(set(replicas), {expected_host})

        # shift the tokens forward by one
        for i, token in enumerate(tokens):
            replicas = token_map.get_replicas("ks", token_klass(token.value + 1))
            expected_host = hosts[(i + 1) % len(hosts)]
            self.assertEqual(set(replicas), {expected_host})

    def test_murmur3_tokens(self):
        self._get_replicas(Murmur3Token)

    def test_md5_tokens(self):
        self._get_replicas(MD5Token)

    def test_bytes_tokens(self):
        self._get_replicas(BytesToken)


class Murmur3TokensTest(unittest.TestCase):

    def test_murmur3_init(self):
        murmur3_token = Murmur3Token(cassandra.metadata.MIN_LONG - 1)
        self.assertEqual(str(murmur3_token), '<Murmur3Token: -9223372036854775809>')

    def test_python_vs_c(self):
        from cassandra.murmur3 import _murmur3 as mm3_python
        try:
            from cassandra.cmurmur3 import murmur3 as mm3_c

            iterations = 100
            for _ in range(iterations):
                for len in range(0, 32):  # zero to one block plus full range of tail lengths
                    key = os.urandom(len)
                    self.assertEqual(mm3_python(key), mm3_c(key))

        except ImportError:
            raise unittest.SkipTest('The cmurmur3 extension is not available')

    def test_murmur3_python(self):
        from cassandra.murmur3 import _murmur3
        self._verify_hash(_murmur3)

    def test_murmur3_c(self):
        try:
            from cassandra.cmurmur3 import murmur3
            self._verify_hash(murmur3)
        except ImportError:
            raise unittest.SkipTest('The cmurmur3 extension is not available')

    def _verify_hash(self, fn):
        self.assertEqual(fn(six.b('123')), -7468325962851647638)
        self.assertEqual(fn(b'\x00\xff\x10\xfa\x99' * 10), 5837342703291459765)
        self.assertEqual(fn(b'\xfe' * 8), -8927430733708461935)
        self.assertEqual(fn(b'\x10' * 8), 1446172840243228796)
        self.assertEqual(fn(six.b(str(cassandra.metadata.MAX_LONG))), 7162290910810015547)


class MD5TokensTest(unittest.TestCase):

    def test_md5_tokens(self):
        md5_token = MD5Token(cassandra.metadata.MIN_LONG - 1)
        self.assertEqual(md5_token.hash_fn('123'), 42767516990368493138776584305024125808)
        self.assertEqual(md5_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 28528976619278518853815276204542453639)
        self.assertEqual(str(md5_token), '<MD5Token: %s>' % -9223372036854775809)


class BytesTokensTest(unittest.TestCase):

    def test_bytes_tokens(self):
        bytes_token = BytesToken(unhexlify(six.b('01')))
        self.assertEqual(bytes_token.value, six.b('\x01'))
        self.assertEqual(str(bytes_token), "<BytesToken: %s>" % bytes_token.value)
        self.assertEqual(bytes_token.hash_fn('123'), '123')
        self.assertEqual(bytes_token.hash_fn(123), 123)
        self.assertEqual(bytes_token.hash_fn(str(cassandra.metadata.MAX_LONG)), str(cassandra.metadata.MAX_LONG))

    def test_from_string(self):
        from_unicode = BytesToken.from_string(six.text_type('0123456789abcdef'))
        from_bin = BytesToken.from_string(six.b('0123456789abcdef'))
        self.assertEqual(from_unicode, from_bin)
        self.assertIsInstance(from_unicode.value, six.binary_type)
        self.assertIsInstance(from_bin.value, six.binary_type)

    def test_comparison(self):
        tok = BytesToken.from_string(six.text_type('0123456789abcdef'))
        token_high_order = uint16_unpack(tok.value[0:2])
        self.assertLess(BytesToken(uint16_pack(token_high_order - 1)), tok)
        self.assertGreater(BytesToken(uint16_pack(token_high_order + 1)), tok)

    def test_comparison_unicode(self):
        value = six.b('\'_-()"\xc2\xac')
        t0 = BytesToken(value)
        t1 = BytesToken.from_string('00')
        self.assertGreater(t0, t1)
        self.assertFalse(t0 < t1)


class KeyspaceMetadataTest(unittest.TestCase):

    def test_export_as_string_user_types(self):
        keyspace_name = 'test'
        keyspace = KeyspaceMetadata(keyspace_name, True, 'SimpleStrategy', dict(replication_factor=3))
        keyspace.user_types['a'] = UserType(keyspace_name, 'a', ['one', 'two'], ['c', 'int'])
        keyspace.user_types['b'] = UserType(keyspace_name, 'b', ['one', 'two', 'three'], ['d', 'int', 'a'])
        keyspace.user_types['c'] = UserType(keyspace_name, 'c', ['one'], ['int'])
        keyspace.user_types['d'] = UserType(keyspace_name, 'd', ['one'], ['c'])

        self.assertEqual("""CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TYPE test.c (
    one int
);

CREATE TYPE test.a (
    one c,
    two int
);

CREATE TYPE test.d (
    one c
);

CREATE TYPE test.b (
    one d,
    two int,
    three a
);""", keyspace.export_as_string())


class UserTypesTest(unittest.TestCase):

    def test_as_cql_query(self):
        field_types = ['varint', 'ascii', 'frozen<tuple<varint, ascii>>']
        udt = UserType("ks1", "mytype", ["a", "b", "c"], field_types)
        self.assertEqual("CREATE TYPE ks1.mytype (a varint, b ascii, c frozen<tuple<varint, ascii>>)", udt.as_cql_query(formatted=False))

        self.assertEqual("""CREATE TYPE ks1.mytype (
    a varint,
    b ascii,
    c frozen<tuple<varint, ascii>>
);""", udt.export_as_string())

    def test_as_cql_query_name_escaping(self):
        udt = UserType("MyKeyspace", "MyType", ["AbA", "keyspace"], ['ascii', 'ascii'])
        self.assertEqual('CREATE TYPE "MyKeyspace"."MyType" ("AbA" ascii, "keyspace" ascii)', udt.as_cql_query(formatted=False))


class IndexTest(unittest.TestCase):

    def test_build_index_as_cql(self):
        column_meta = Mock()
        column_meta.name = 'column_name_here'
        column_meta.table.name = 'table_name_here'
        column_meta.table.keyspace_name = 'keyspace_name_here'
        column_meta.table.columns = {column_meta.name: column_meta}
        parser = get_schema_parser(Mock(), '2.1.0', 0.1)

        row = {'index_name': 'index_name_here', 'index_type': 'index_type_here'}
        index_meta = parser._build_index_metadata(column_meta, row)
        self.assertEqual(index_meta.as_cql_query(),
                'CREATE INDEX index_name_here ON keyspace_name_here.table_name_here (column_name_here)')

        row['index_options'] = '{ "class_name": "class_name_here" }'
        row['index_type'] = 'CUSTOM'
        index_meta = parser._build_index_metadata(column_meta, row)
        self.assertEqual(index_meta.as_cql_query(),
                "CREATE CUSTOM INDEX index_name_here ON keyspace_name_here.table_name_here (column_name_here) USING 'class_name_here'")


class UnicodeIdentifiersTests(unittest.TestCase):
    """
    Exercise cql generation with unicode characters. Keyspace, Table, and Index names
    cannot have special chars because C* names files by those identifiers, but they are
    tested anyway.

    Looking for encoding errors like PYTHON-447
    """

    name = six.text_type(b'\'_-()"\xc2\xac'.decode('utf-8'))

    def test_keyspace_name(self):
        km = KeyspaceMetadata(self.name, False, 'SimpleStrategy', {'replication_factor': 1})
        km.export_as_string()

    def test_table_name(self):
        tm = TableMetadata(self.name, self.name)
        tm.export_as_string()

    def test_column_name_single_partition(self):
        tm = TableMetadata('ks', 'table')
        cm = ColumnMetadata(tm, self.name, u'int')
        tm.columns[cm.name] = cm
        tm.partition_key.append(cm)
        tm.export_as_string()

    def test_column_name_single_partition_single_clustering(self):
        tm = TableMetadata('ks', 'table')
        cm = ColumnMetadata(tm, self.name, u'int')
        tm.columns[cm.name] = cm
        tm.partition_key.append(cm)
        cm = ColumnMetadata(tm, self.name + 'x', u'int')
        tm.columns[cm.name] = cm
        tm.clustering_key.append(cm)
        tm.export_as_string()

    def test_column_name_multiple_partition(self):
        tm = TableMetadata('ks', 'table')
        cm = ColumnMetadata(tm, self.name, u'int')
        tm.columns[cm.name] = cm
        tm.partition_key.append(cm)
        cm = ColumnMetadata(tm, self.name + 'x', u'int')
        tm.columns[cm.name] = cm
        tm.partition_key.append(cm)
        tm.export_as_string()

    def test_index(self):
        im = IndexMetadata(self.name, self.name, self.name, kind='', index_options={'target': self.name})
        im.export_as_string()
        im = IndexMetadata(self.name, self.name, self.name, kind='CUSTOM', index_options={'target': self.name, 'class_name': 'Class'})
        im.export_as_string()

    def test_function(self):
        fm = Function(self.name, self.name, (u'int', u'int'), (u'x', u'y'), u'int', u'language', self.name, False)
        fm.export_as_string()

    def test_aggregate(self):
        am = Aggregate(self.name, self.name, (u'text',), self.name, u'text', self.name, self.name, u'text')
        am.export_as_string()

    def test_user_type(self):
        um = UserType(self.name, self.name, [self.name, self.name], [u'int', u'text'])
        um.export_as_string()


class HostsTests(unittest.TestCase):
    def test_iterate_all_hosts_and_modify(self):
        """
        PYTHON-572
        """
        metadata = Metadata()
        metadata.add_or_return_host(Host('dc1.1', SimpleConvictionPolicy))
        metadata.add_or_return_host(Host('dc1.2', SimpleConvictionPolicy))

        self.assertEqual(len(metadata.all_hosts()), 2)

        for host in metadata.all_hosts():  # this would previously raise in Py3
            metadata.remove_host(host)

        self.assertEqual(len(metadata.all_hosts()), 0)
