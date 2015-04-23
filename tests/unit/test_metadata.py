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

from mock import Mock

import cassandra
from cassandra.cqltypes import IntegerType, AsciiType, TupleType
from cassandra.metadata import (Murmur3Token, MD5Token,
                                BytesToken, ReplicationStrategy,
                                NetworkTopologyStrategy, SimpleStrategy,
                                LocalStrategy, NoMurmur3, protect_name,
                                protect_names, protect_value, is_valid_name,
                                UserType, KeyspaceMetadata, Metadata,
                                _UnknownStrategy)
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

        non_valid_keywords = cassandra.metadata._keywords - cassandra.metadata._unreserved_keywords
        for keyword in non_valid_keywords:
            self.assertEqual(is_valid_name(keyword), False)


class TokensTest(unittest.TestCase):

    def test_murmur3_tokens(self):
        try:
            murmur3_token = Murmur3Token(cassandra.metadata.MIN_LONG - 1)
            self.assertEqual(murmur3_token.hash_fn('123'), -7468325962851647638)
            self.assertEqual(murmur3_token.hash_fn(b'\x00\xff\x10\xfa\x99' * 10), 5837342703291459765)
            self.assertEqual(murmur3_token.hash_fn(b'\xfe' * 8), -8927430733708461935)
            self.assertEqual(murmur3_token.hash_fn(b'\x10' * 8), 1446172840243228796)
            self.assertEqual(murmur3_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 7162290910810015547)
            self.assertEqual(str(murmur3_token), '<Murmur3Token: -9223372036854775809>')
        except NoMurmur3:
            raise unittest.SkipTest('The murmur3 extension is not available')

    def test_md5_tokens(self):
        md5_token = MD5Token(cassandra.metadata.MIN_LONG - 1)
        self.assertEqual(md5_token.hash_fn('123'), 42767516990368493138776584305024125808)
        self.assertEqual(md5_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 28528976619278518853815276204542453639)
        self.assertEqual(str(md5_token), '<MD5Token: %s>' % -9223372036854775809)

    def test_bytes_tokens(self):
        bytes_token = BytesToken(str(cassandra.metadata.MIN_LONG - 1))
        self.assertEqual(bytes_token.hash_fn('123'), '123')
        self.assertEqual(bytes_token.hash_fn(123), 123)
        self.assertEqual(bytes_token.hash_fn(str(cassandra.metadata.MAX_LONG)), str(cassandra.metadata.MAX_LONG))
        self.assertEqual(str(bytes_token), "<BytesToken: -9223372036854775809>")

        try:
            bytes_token = BytesToken(cassandra.metadata.MIN_LONG - 1)
            self.fail('Tokens for ByteOrderedPartitioner should be only strings')
        except TypeError:
            pass


class KeyspaceMetadataTest(unittest.TestCase):

    def test_export_as_string_user_types(self):
        keyspace_name = 'test'
        keyspace = KeyspaceMetadata(keyspace_name, True, 'SimpleStrategy', dict(replication_factor=3))
        keyspace.user_types['a'] = UserType(keyspace_name, 'a', ['one', 'two'],
                                            [self.mock_user_type('UserType', 'c'),
                                             self.mock_user_type('IntType', 'int')])
        keyspace.user_types['b'] = UserType(keyspace_name, 'b', ['one', 'two', 'three'],
                                            [self.mock_user_type('UserType', 'd'),
                                             self.mock_user_type('IntType', 'int'),
                                             self.mock_user_type('UserType', 'a')])
        keyspace.user_types['c'] = UserType(keyspace_name, 'c', ['one'],
                                            [self.mock_user_type('IntType', 'int')])
        keyspace.user_types['d'] = UserType(keyspace_name, 'd', ['one'],
                                            [self.mock_user_type('UserType', 'c')])

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

    def mock_user_type(self, cassname, typename):
        return Mock(**{'cassname': cassname, 'typename': typename, 'cql_parameterized_type.return_value': typename})


class UserTypesTest(unittest.TestCase):

    def test_as_cql_query(self):
        field_types = [IntegerType, AsciiType, TupleType.apply_parameters([IntegerType, AsciiType])]
        udt = UserType("ks1", "mytype", ["a", "b", "c"], field_types)
        self.assertEqual("CREATE TYPE ks1.mytype (a varint, b ascii, c frozen<tuple<varint, ascii>>);", udt.as_cql_query(formatted=False))

        self.assertEqual("""CREATE TYPE ks1.mytype (
    a varint,
    b ascii,
    c frozen<tuple<varint, ascii>>
);""", udt.as_cql_query(formatted=True))

    def test_as_cql_query_name_escaping(self):
        udt = UserType("MyKeyspace", "MyType", ["AbA", "keyspace"], [AsciiType, AsciiType])
        self.assertEqual('CREATE TYPE "MyKeyspace"."MyType" ("AbA" ascii, "keyspace" ascii);', udt.as_cql_query(formatted=False))


class IndexTest(unittest.TestCase):

    def test_build_index_as_cql(self):
        column_meta = Mock()
        column_meta.name = 'column_name_here'
        column_meta.table.name = 'table_name_here'
        column_meta.table.keyspace.name = 'keyspace_name_here'
        meta_model = Metadata()

        row = {'index_name': 'index_name_here', 'index_type': 'index_type_here'}
        index_meta = meta_model._build_index_metadata(column_meta, row)
        self.assertEqual(index_meta.as_cql_query(),
                'CREATE INDEX index_name_here ON keyspace_name_here.table_name_here (column_name_here)')

        row['index_options'] = '{ "class_name": "class_name_here" }'
        row['index_type'] = 'CUSTOM'
        index_meta = meta_model._build_index_metadata(column_meta, row)
        self.assertEqual(index_meta.as_cql_query(),
                "CREATE CUSTOM INDEX index_name_here ON keyspace_name_here.table_name_here (column_name_here) USING 'class_name_here'")
