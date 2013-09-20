import unittest
import cassandra
from cassandra.cluster import Cluster
from cassandra.metadata import TableMetadata, Murmur3Token, MD5Token, BytesToken


class TestMetadata(unittest.TestCase):

    def test_protect_name(self):
        """
        Test TableMetadata.protect_name output
        """

        table_metadata = TableMetadata('ks_name', 'table_name')

        self.assertEqual(table_metadata.protect_name('tests'), 'tests')
        self.assertEqual(table_metadata.protect_name('test\'s'), '"test\'s"')
        self.assertEqual(table_metadata.protect_name('test\'s'), "\"test's\"")
        self.assertEqual(table_metadata.protect_name('tests ?!@#$%^&*()'), '"tests ?!@#$%^&*()"')
        self.assertEqual(table_metadata.protect_name('1'), '"1"')
        self.assertEqual(table_metadata.protect_name('1test'), '"1test"')

    def test_protect_names(self):
        """
        Test TableMetadata.protect_names output
        """

        table_metadata = TableMetadata('ks_name', 'table_name')

        self.assertEqual(table_metadata.protect_names(['tests']), ['tests'])
        self.assertEqual(table_metadata.protect_names(
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
        Test TableMetadata.protect_value output
        """

        table_metadata = TableMetadata('ks_name', 'table_name')

        self.assertEqual(table_metadata.protect_value(True), "True")
        self.assertEqual(table_metadata.protect_value(False), "False")
        self.assertEqual(table_metadata.protect_value(3.14), '3.14')
        self.assertEqual(table_metadata.protect_value(3), '3')
        self.assertEqual(table_metadata.protect_value('test'), "'test'")
        self.assertEqual(table_metadata.protect_value('test\'s'), "'test''s'")
        self.assertEqual(table_metadata.protect_value(None), 'NULL')

    def test_is_valid_name(self):
        """
        Test TableMetadata.is_valid_name output
        """

        table_metadata = TableMetadata('ks_name', 'table_name')

        self.assertEqual(table_metadata.is_valid_name(None), False)
        self.assertEqual(table_metadata.is_valid_name('test'), True)
        self.assertEqual(table_metadata.is_valid_name('Test'), False)
        self.assertEqual(table_metadata.is_valid_name('t_____1'), True)
        self.assertEqual(table_metadata.is_valid_name('test1'), True)
        self.assertEqual(table_metadata.is_valid_name('1test1'), False)

        non_valid_keywords = cassandra.metadata._keywords - cassandra.metadata._unreserved_keywords
        for keyword in non_valid_keywords:
            self.assertEqual(table_metadata.is_valid_name(keyword), False)

    def test_token_values(self):
        """
        Spot check token classes and values
        """

        # spot check murmur3
        murmur3_token = Murmur3Token(cassandra.metadata.MIN_LONG - 1)
        self.assertEqual(murmur3_token.hash_fn('123'), -7468325962851647638)
        self.assertEqual(murmur3_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 7162290910810015547)

        md5_token = MD5Token(cassandra.metadata.MIN_LONG - 1)
        self.assertEqual(md5_token.hash_fn('123'), 42767516990368493138776584305024125808L)
        self.assertEqual(md5_token.hash_fn(str(cassandra.metadata.MAX_LONG)), 28528976619278518853815276204542453639L)

        bytes_token = BytesToken(str(cassandra.metadata.MIN_LONG - 1))
        self.assertEqual(bytes_token.hash_fn('123'), '123')
        self.assertEqual(bytes_token.hash_fn(123), 123)
        self.assertEqual(bytes_token.hash_fn(str(cassandra.metadata.MAX_LONG)), str(cassandra.metadata.MAX_LONG))

        try:
            bytes_token = BytesToken(cassandra.metadata.MIN_LONG - 1)
            self.fail('Tokens for ByteOrderedPartitioner should be only strings')
        except TypeError:
            pass
