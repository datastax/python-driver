try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from decimal import Decimal
from datetime import datetime
from uuid import uuid1, uuid4

try:
    from blist import sortedset
except ImportError:
    sortedset = set  # noqa

from cassandra import InvalidRequest
from cassandra.cluster import Cluster

from tests.integration import get_server_versions


class TypeTests(unittest.TestCase):

    def setUp(self):
        self._cass_version, self._cql_version = get_server_versions()

    def test_blob_type_as_string(self):
        c = Cluster()
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE typetests_blob1
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}
            """)
        s.set_keyspace("typetests_blob1")
        s.execute("""
            CREATE TABLE mytable (
                a ascii,
                b blob,
                PRIMARY KEY (a)
            )
        """)

        params = [
            'key1',
            'blobyblob'.encode('hex')
        ]

        query = 'INSERT INTO mytable (a, b) VALUES (%s, %s)'

        if self._cql_version >= (3, 1, 0):
            # Blob values can't be specified using string notation in CQL 3.1.0 and
            # above which is used by default in Cassandra 2.0.
            msg = r'.*Invalid STRING constant \(.*?\) for b of type blob.*'
            self.assertRaisesRegexp(InvalidRequest, msg, s.execute, query, params)
            return

        s.execute(query, params)
        expected_vals = [
           'key1',
           'blobyblob'
        ]

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

    def test_blob_type_as_bytearray(self):
        c = Cluster()
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE typetests_blob2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}
            """)
        s.set_keyspace("typetests_blob2")
        s.execute("""
            CREATE TABLE mytable (
                a ascii,
                b blob,
                PRIMARY KEY (a)
            )
        """)

        params = [
            'key1',
            bytearray('blob1', 'hex')
        ]

        query = 'INSERT INTO mytable (a, b) VALUES (%s, %s);'
        s.execute(query, params)

        expected_vals = [
            'key1',
            bytearray('blob1', 'hex')
        ]

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

    def test_basic_types(self):
        c = Cluster()
        s = c.connect()
        s.execute("""
            CREATE KEYSPACE typetests
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}
            """)
        s.set_keyspace("typetests")
        s.execute("""
            CREATE TABLE mytable (
                a text,
                b text,
                c ascii,
                d bigint,
                f boolean,
                g decimal,
                h double,
                i float,
                j inet,
                k int,
                l list<text>,
                m set<int>,
                n map<text, int>,
                o text,
                p timestamp,
                q uuid,
                r timeuuid,
                s varchar,
                t varint,
                PRIMARY KEY (a, b)
            )
        """)

        v1_uuid = uuid1()
        v4_uuid = uuid4()
        mydatetime = datetime(2013, 12, 31, 23, 59, 59, 999000)

        params = [
            "sometext",
            "sometext",
            "ascii",  # ascii
            12345678923456789,  # bigint
            True,  # boolean
            Decimal('1.234567890123456789'),  # decimal
            0.000244140625,  # double
            1.25,  # float
            "1.2.3.4",  # inet
            12345,  # int
            ['a', 'b', 'c'],  # list<text> collection
            set([1, 2, 3]),  # set<int> collection
            {'a': 1, 'b': 2},  # map<text, int> collection
            "text",  # text
            mydatetime,  # timestamp
            v4_uuid,  # uuid
            v1_uuid,  # timeuuid
            u"sometext\u1234",  # varchar
            123456789123456789123456789  # varint
        ]

        expected_vals = (
            "sometext",
            "sometext",
            "ascii",  # ascii
            12345678923456789,  # bigint
            True,  # boolean
            Decimal('1.234567890123456789'),  # decimal
            0.000244140625,  # double
            1.25,  # float
            "1.2.3.4",  # inet
            12345,  # int
            ('a', 'b', 'c'),  # list<text> collection
            sortedset((1, 2, 3)),  # set<int> collection
            {'a': 1, 'b': 2},  # map<text, int> collection
            "text",  # text
            mydatetime,  # timestamp
            v4_uuid,  # uuid
            v1_uuid,  # timeuuid
            u"sometext\u1234",  # varchar
            123456789123456789123456789  # varint
        )

        s.execute("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, params)

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

        # try the same thing with a prepared statement
        prepared = s.prepare("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)

        s.execute(prepared.bind(params))

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

        # query with prepared statement
        prepared = s.prepare("""
            SELECT a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        results = s.execute(prepared.bind(()))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

        # query with prepared statement, no explicit columns
        prepared = s.prepare("""SELECT * FROM mytable""")
        results = s.execute(prepared.bind(()))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

    def test_timezone_aware_datetimes(self):
        """ Ensure timezone-aware datetimes are converted to timestamps correctly """
        try:
            import pytz
        except ImportError, exc:
            raise unittest.SkipTest('pytz is not available: %r' % (exc,))

        dt = datetime(1997, 8, 29, 11, 14)
        eastern_tz = pytz.timezone('US/Eastern')
        eastern_tz.localize(dt)

        c = Cluster()
        s = c.connect()

        s.execute("""CREATE KEYSPACE tz_aware_test
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("tz_aware_test")
        s.execute("CREATE TABLE mytable (a ascii PRIMARY KEY, b timestamp)")

        # test non-prepared statement
        s.execute("INSERT INTO mytable (a, b) VALUES ('key1', %s)", parameters=(dt,))
        result = s.execute("SELECT b FROM mytable WHERE a='key1'")[0].b
        self.assertEquals(dt.utctimetuple(), result.utctimetuple())

        # test prepared statement
        prepared = s.prepare("INSERT INTO mytable (a, b) VALUES ('key2', ?)")
        s.execute(prepared, parameters=(dt,))
        result = s.execute("SELECT b FROM mytable WHERE a='key2'")[0].b
        self.assertEquals(dt.utctimetuple(), result.utctimetuple())
