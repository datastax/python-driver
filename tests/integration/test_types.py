try:
    import unittest2 as unittest
except ImportError:
    import unittest

from decimal import Decimal
from datetime import datetime
from uuid import uuid1, uuid4

from cassandra.cluster import Cluster

class TypeTests(unittest.TestCase):

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
                e blob,
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
                s varint,
                PRIMARY KEY (a, b)
            )
        """)

        v1_uuid = uuid1()
        v4_uuid = uuid4()
        mydatetime = datetime(2013, 1, 1, 1, 1, 1)

        params = [
            "sometext",
            "sometext",
            "ascii",  # ascii
            12345678923456789,  # bigint
            "blob".encode('hex'),  # blob
            True,  # boolean
            Decimal('1.234567890123456789'),  # decimal
            0.000244140625,  # double
            1.25,  # float
            "1.2.3.4",  # inet
            12345,  # int
            ['a', 'b', 'c'],  # list<text> collection
            {1, 2, 3},  # set<int> collection
            {'a': 1, 'b': 2},  # map<text, int> collection
            "text",  # text
            mydatetime,  # timestamp
            v4_uuid,  # uuid
            v1_uuid,  # timeuuid
            123456789123456789123456789  # varint
        ]

        expected_vals = (
            "sometext",
            "sometext",
            "ascii",  # ascii
            12345678923456789,  # bigint
            "blob",  # blob
            True,  # boolean
            Decimal('1.234567890123456789'),  # decimal
            0.000244140625,  # double
            1.25,  # float
            "1.2.3.4",  # inet
            12345,  # int
            ('a', 'b', 'c'),  # list<text> collection
            {1, 2, 3},  # set<int> collection
            {'a': 1, 'b': 2},  # map<text, int> collection
            "text",  # text
            mydatetime,  # timestamp
            v4_uuid,  # uuid
            v1_uuid,  # timeuuid
            123456789123456789123456789  # varint
        )

        s.execute("""
            INSERT INTO mytable (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, params)

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

        # try the same thing with a prepared statement
        prepared = s.prepare("""
            INSERT INTO mytable (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)

        params[4] = 'blob'
        s.execute(prepared.bind(params))

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

        # query with prepared statement
        prepared = s.prepare("""
            SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s FROM mytable
            """)
        results = s.execute(prepared.bind(()))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)

        # query with prepared statement, no explicit columns
        prepared = s.prepare("""SELECT * FROM mytable""")
        results = s.execute(prepared.bind(()))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEquals(expected, actual)
