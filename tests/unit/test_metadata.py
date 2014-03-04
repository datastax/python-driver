try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

import cassandra
from cassandra.metadata import (Murmur3Token, MD5Token,
                                BytesToken, ReplicationStrategy,
                                NetworkTopologyStrategy, SimpleStrategy,
                                LocalStrategy, NoMurmur3, protect_name,
                                protect_names, protect_value, is_valid_name)
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host


class TestStrategies(unittest.TestCase):

    def test_replication_strategy(self):
        """
        Basic code coverage testing that ensures different ReplicationStrategies
        can be initiated using parameters correctly.
        """

        rs = ReplicationStrategy()

        self.assertEqual(rs.create('OldNetworkTopologyStrategy', None), None)
        self.assertEqual(rs.create('xxxxxxOldNetworkTopologyStrategy', None), None)

        fake_options_map = {'options': 'map'}
        self.assertIsInstance(rs.create('NetworkTopologyStrategy', fake_options_map), NetworkTopologyStrategy)
        self.assertEqual(rs.create('NetworkTopologyStrategy', fake_options_map).dc_replication_factors,
                         NetworkTopologyStrategy(fake_options_map).dc_replication_factors)

        self.assertIsInstance(rs.create('xxxxxxNetworkTopologyStrategy', fake_options_map), NetworkTopologyStrategy)
        self.assertEqual(rs.create('xxxxxxNetworkTopologyStrategy', fake_options_map).dc_replication_factors,
                         NetworkTopologyStrategy(fake_options_map).dc_replication_factors)

        fake_options_map = {'options': 'map'}
        self.assertEqual(rs.create('SimpleStrategy', fake_options_map), None)
        self.assertEqual(rs.create('xxxxxxSimpleStrategy', fake_options_map), None)

        fake_options_map = {'options': 'map'}
        self.assertIsInstance(rs.create('LocalStrategy', fake_options_map), LocalStrategy)
        self.assertIsInstance(rs.create('xxxxxxLocalStrategy', fake_options_map), LocalStrategy)

        fake_options_map = {'options': 'map', 'replication_factor': 3}
        self.assertIsInstance(rs.create('SimpleStrategy', fake_options_map), SimpleStrategy)
        self.assertEqual(rs.create('SimpleStrategy', fake_options_map).replication_factor,
                         SimpleStrategy(fake_options_map['replication_factor']).replication_factor)

        self.assertIsInstance(rs.create('xxxxxxSimpleStrategy', fake_options_map), SimpleStrategy)
        self.assertEqual(rs.create('xxxxxxSimpleStrategy', fake_options_map).replication_factor,
                         SimpleStrategy(fake_options_map['replication_factor']).replication_factor)

        self.assertEqual(rs.create('xxxxxxxx', fake_options_map), None)

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
        # TODO: Cover NetworkTopologyStrategy.export_for_schema()
        pass

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

        rf1_replicas = SimpleStrategy(1).make_token_replica_map(token_to_host_owner, ring)
        self.assertItemsEqual(rf1_replicas[MD5Token(0)], [host1])
        self.assertItemsEqual(rf1_replicas[MD5Token(100)], [host2])
        self.assertItemsEqual(rf1_replicas[MD5Token(200)], [host3])

        rf2_replicas = SimpleStrategy(2).make_token_replica_map(token_to_host_owner, ring)
        self.assertItemsEqual(rf2_replicas[MD5Token(0)], [host1, host2])
        self.assertItemsEqual(rf2_replicas[MD5Token(100)], [host2, host3])
        self.assertItemsEqual(rf2_replicas[MD5Token(200)], [host3, host1])

        rf3_replicas = SimpleStrategy(3).make_token_replica_map(token_to_host_owner, ring)
        self.assertItemsEqual(rf3_replicas[MD5Token(0)], [host1, host2, host3])
        self.assertItemsEqual(rf3_replicas[MD5Token(100)], [host2, host3, host1])
        self.assertItemsEqual(rf3_replicas[MD5Token(200)], [host3, host1, host2])


class TestNameEscaping(unittest.TestCase):

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
        self.assertEqual(protect_value(True), "True")
        self.assertEqual(protect_value(False), "False")
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


class TestTokens(unittest.TestCase):

    def test_murmur3_tokens(self):
        try:
            murmur3_token = Murmur3Token(cassandra.metadata.MIN_LONG - 1)
            self.assertEqual(murmur3_token.hash_fn('123'), -7468325962851647638)
            self.assertEqual(murmur3_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 7162290910810015547)
            self.assertEqual(str(murmur3_token), '<Murmur3Token: -9223372036854775809L>')
        except NoMurmur3:
            raise unittest.SkipTest('The murmur3 extension is not available')

    def test_md5_tokens(self):
        md5_token = MD5Token(cassandra.metadata.MIN_LONG - 1)
        self.assertEqual(md5_token.hash_fn('123'), 42767516990368493138776584305024125808L)
        self.assertEqual(md5_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 28528976619278518853815276204542453639L)
        self.assertEqual(str(md5_token), '<MD5Token: -9223372036854775809L>')

    def test_bytes_tokens(self):
        bytes_token = BytesToken(str(cassandra.metadata.MIN_LONG - 1))
        self.assertEqual(bytes_token.hash_fn('123'), '123')
        self.assertEqual(bytes_token.hash_fn(123), 123)
        self.assertEqual(bytes_token.hash_fn(str(cassandra.metadata.MAX_LONG)), str(cassandra.metadata.MAX_LONG))
        self.assertEqual(str(bytes_token), "<BytesToken: '-9223372036854775809'>")

        try:
            bytes_token = BytesToken(cassandra.metadata.MIN_LONG - 1)
            self.fail('Tokens for ByteOrderedPartitioner should be only strings')
        except TypeError:
            pass
