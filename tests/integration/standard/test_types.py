# Copyright 2013-2014 DataStax, Inc.
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
from tests.integration.datatype_utils import get_sample, DATA_TYPE_PRIMITIVES, DATA_TYPE_NON_PRIMITIVE_NAMES

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import logging
log = logging.getLogger(__name__)

from decimal import Decimal
from datetime import datetime
import six
from uuid import uuid1, uuid4

from cassandra import InvalidRequest
from cassandra.cluster import Cluster
from cassandra.cqltypes import Int32Type, EMPTY
from cassandra.query import dict_factory
from cassandra.util import OrderedDict, SortedSet

from tests.integration import get_server_versions, PROTOCOL_VERSION


class TypeTests(unittest.TestCase):

    def setUp(self):
        self._cass_version, self._cql_version = get_server_versions()

    def test_blob_type_as_string(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
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
            b'blobyblob'
        ]

        query = 'INSERT INTO mytable (a, b) VALUES (%s, %s)'

        # In python 3, the 'bytes' type is treated as a blob, so we can
        # correctly encode it with hex notation.
        # In python2, we don't treat the 'str' type as a blob, so we'll encode it
        # as a string literal and have the following failure.
        if six.PY2 and self._cql_version >= (3, 1, 0):
            # Blob values can't be specified using string notation in CQL 3.1.0 and
            # above which is used by default in Cassandra 2.0.
            if self._cass_version >= (2, 1, 0):
                msg = r'.*Invalid STRING constant \(.*?\) for "b" of type blob.*'
            else:
                msg = r'.*Invalid STRING constant \(.*?\) for b of type blob.*'
            self.assertRaisesRegexp(InvalidRequest, msg, s.execute, query, params)
            return
        elif six.PY2:
            params[1] = params[1].encode('hex')

        s.execute(query, params)
        expected_vals = [
            'key1',
            bytearray(b'blobyblob')
        ]

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

    def test_blob_type_as_bytearray(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
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
            bytearray(b'blob1')
        ]

        query = 'INSERT INTO mytable (a, b) VALUES (%s, %s);'
        s.execute(query, params)

        expected_vals = [
            'key1',
            bytearray(b'blob1')
        ]

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

    create_type_table = """
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
        """

    def test_basic_types(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()
        s.execute("""
            CREATE KEYSPACE typetests
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}
            """)
        s.set_keyspace("typetests")
        s.execute(self.create_type_table)

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
            ['a', 'b', 'c'],  # list<text> collection
            SortedSet((1, 2, 3)),  # set<int> collection
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
            self.assertEqual(expected, actual)

        # try the same thing with a prepared statement
        prepared = s.prepare("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)

        s.execute(prepared.bind(params))

        results = s.execute("SELECT * FROM mytable")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

        # query with prepared statement
        prepared = s.prepare("""
            SELECT a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        results = s.execute(prepared.bind(()))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

        # query with prepared statement, no explicit columns
        prepared = s.prepare("""SELECT * FROM mytable""")
        results = s.execute(prepared.bind(()))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

    def test_empty_strings_and_nones(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()
        s.execute("""
            CREATE KEYSPACE test_empty_strings_and_nones
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}
            """)
        s.set_keyspace("test_empty_strings_and_nones")
        s.execute(self.create_type_table)

        s.execute("INSERT INTO mytable (a, b) VALUES ('a', 'b')")
        s.row_factory = dict_factory
        results = s.execute("""
            SELECT c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        self.assertTrue(all(x is None for x in results[0].values()))

        prepared = s.prepare("""
            SELECT c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        results = s.execute(prepared.bind(()))
        self.assertTrue(all(x is None for x in results[0].values()))

        # insert empty strings for string-like fields and fetch them
        s.execute("INSERT INTO mytable (a, b, c, o, s, l, n) VALUES ('a', 'b', %s, %s, %s, %s, %s)",
                  ('', '', '', [''], {'': 3}))
        self.assertEqual(
            {'c': '', 'o': '', 's': '', 'l': [''], 'n': OrderedDict({'': 3})},
            s.execute("SELECT c, o, s, l, n FROM mytable WHERE a='a' AND b='b'")[0])

        self.assertEqual(
            {'c': '', 'o': '', 's': '', 'l': [''], 'n': OrderedDict({'': 3})},
            s.execute(s.prepare("SELECT c, o, s, l, n FROM mytable WHERE a='a' AND b='b'"), [])[0])

        # non-string types shouldn't accept empty strings
        for col in ('d', 'f', 'g', 'h', 'i', 'k', 'l', 'm', 'n', 'q', 'r', 't'):
            query = "INSERT INTO mytable (a, b, %s) VALUES ('a', 'b', %%s)" % (col, )
            try:
                s.execute(query, [''])
            except InvalidRequest:
                pass
            else:
                self.fail("Expected an InvalidRequest error when inserting an "
                          "emptry string for column %s" % (col, ))

            prepared = s.prepare("INSERT INTO mytable (a, b, %s) VALUES ('a', 'b', ?)" % (col, ))
            try:
                s.execute(prepared, [''])
            except TypeError:
                pass
            else:
                self.fail("Expected an InvalidRequest error when inserting an "
                          "emptry string for column %s with a prepared statement" % (col, ))

        # insert values for all columns
        values = ['a', 'b', 'a', 1, True, Decimal('1.0'), 0.1, 0.1,
                  "1.2.3.4", 1, ['a'], set([1]), {'a': 1}, 'a',
                  datetime.now(), uuid4(), uuid1(), 'a', 1]
        s.execute("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)

        # then insert None, which should null them out
        null_values = values[:2] + ([None] * (len(values) - 2))
        s.execute("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, null_values)

        results = s.execute("""
            SELECT c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        prepared = s.prepare("""
            SELECT c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        results = s.execute(prepared.bind(()))
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        # do the same thing again, but use a prepared statement to insert the nulls
        s.execute("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)
        prepared = s.prepare("""
            INSERT INTO mytable (a, b, c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
        s.execute(prepared, null_values)

        results = s.execute("""
            SELECT c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        prepared = s.prepare("""
            SELECT c, d, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t FROM mytable
            """)
        results = s.execute(prepared.bind(()))
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

    def test_empty_values(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()
        s.execute("""
            CREATE KEYSPACE test_empty_values
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}
            """)
        s.set_keyspace("test_empty_values")
        s.execute("CREATE TABLE mytable (a text PRIMARY KEY, b int)")
        s.execute("INSERT INTO mytable (a, b) VALUES ('a', blobAsInt(0x))")
        try:
            Int32Type.support_empty_values = True
            results = s.execute("SELECT b FROM mytable WHERE a='a'")[0]
            self.assertIs(EMPTY, results.b)
        finally:
            Int32Type.support_empty_values = False

    def test_timezone_aware_datetimes(self):
        """ Ensure timezone-aware datetimes are converted to timestamps correctly """
        try:
            import pytz
        except ImportError as exc:
            raise unittest.SkipTest('pytz is not available: %r' % (exc,))

        dt = datetime(1997, 8, 29, 11, 14)
        eastern_tz = pytz.timezone('US/Eastern')
        eastern_tz.localize(dt)

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""CREATE KEYSPACE tz_aware_test
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("tz_aware_test")
        s.execute("CREATE TABLE mytable (a ascii PRIMARY KEY, b timestamp)")

        # test non-prepared statement
        s.execute("INSERT INTO mytable (a, b) VALUES ('key1', %s)", parameters=(dt,))
        result = s.execute("SELECT b FROM mytable WHERE a='key1'")[0].b
        self.assertEqual(dt.utctimetuple(), result.utctimetuple())

        # test prepared statement
        prepared = s.prepare("INSERT INTO mytable (a, b) VALUES ('key2', ?)")
        s.execute(prepared, parameters=(dt,))
        result = s.execute("SELECT b FROM mytable WHERE a='key2'")[0].b
        self.assertEqual(dt.utctimetuple(), result.utctimetuple())

    def test_tuple_type(self):
        """
        Basic test of tuple functionality
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        # use this encoder in order to insert tuples
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("""CREATE KEYSPACE test_tuple_type
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("test_tuple_type")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<tuple<ascii, int, boolean>>)")

        # test non-prepared statement
        complete = ('foo', 123, True)
        s.execute("INSERT INTO mytable (a, b) VALUES (0, %s)", parameters=(complete,))
        result = s.execute("SELECT b FROM mytable WHERE a=0")[0]
        self.assertEqual(complete, result.b)

        partial = ('bar', 456)
        partial_result = partial + (None,)
        s.execute("INSERT INTO mytable (a, b) VALUES (1, %s)", parameters=(partial,))
        result = s.execute("SELECT b FROM mytable WHERE a=1")[0]
        self.assertEqual(partial_result, result.b)

        # test single value tuples
        subpartial = ('zoo',)
        subpartial_result = subpartial + (None, None)
        s.execute("INSERT INTO mytable (a, b) VALUES (2, %s)", parameters=(subpartial,))
        result = s.execute("SELECT b FROM mytable WHERE a=2")[0]
        self.assertEqual(subpartial_result, result.b)

        # test prepared statement
        prepared = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(prepared, parameters=(3, complete))
        s.execute(prepared, parameters=(4, partial))
        s.execute(prepared, parameters=(5, subpartial))

        # extra items in the tuple should result in an error
        self.assertRaises(ValueError, s.execute, prepared, parameters=(0, (1, 2, 3, 4, 5, 6)))

        prepared = s.prepare("SELECT b FROM mytable WHERE a=?")
        self.assertEqual(complete, s.execute(prepared, (3,))[0].b)
        self.assertEqual(partial_result, s.execute(prepared, (4,))[0].b)
        self.assertEqual(subpartial_result, s.execute(prepared, (5,))[0].b)

    def test_tuple_type_varying_lengths(self):
        """
        Test tuple types of lengths of 1, 2, 3, and 384 to ensure edge cases work
        as expected.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        # set the row_factory to dict_factory for programmatic access
        # set the encoder for tuples for the ability to write tuples
        s.row_factory = dict_factory
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("""CREATE KEYSPACE test_tuple_type_varying_lengths
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("test_tuple_type_varying_lengths")

        # programmatically create the table with tuples of said sizes
        lengths = (1, 2, 3, 384)
        value_schema = []
        for i in lengths:
            value_schema += [' v_%s frozen<tuple<%s>>' % (i, ', '.join(['int'] * i))]
        s.execute("CREATE TABLE mytable (k int PRIMARY KEY, %s)" % (', '.join(value_schema),))

        # insert tuples into same key using different columns
        # and verify the results
        for i in lengths:
            # ensure tuples of larger sizes throw an error
            created_tuple = tuple(range(0, i + 1))
            self.assertRaises(InvalidRequest, s.execute, "INSERT INTO mytable (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            # ensure tuples of proper sizes are written and read correctly
            created_tuple = tuple(range(0, i))

            s.execute("INSERT INTO mytable (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM mytable WHERE k=0", (i,))[0]
            self.assertEqual(tuple(created_tuple), result['v_%s' % i])

    def test_tuple_primitive_subtypes(self):
        """
        Ensure tuple subtypes are appropriately handled.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("""CREATE KEYSPACE test_tuple_primitive_subtypes
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("test_tuple_primitive_subtypes")

        s.execute("CREATE TABLE mytable ("
                  "k int PRIMARY KEY, "
                  "v frozen<tuple<%s>>)" % ','.join(DATA_TYPE_PRIMITIVES))

        for i in range(len(DATA_TYPE_PRIMITIVES)):
            # create tuples to be written and ensure they match with the expected response
            # responses have trailing None values for every element that has not been written
            created_tuple = [get_sample(DATA_TYPE_PRIMITIVES[j]) for j in range(i + 1)]
            response_tuple = tuple(created_tuple + [None for j in range(len(DATA_TYPE_PRIMITIVES) - i - 1)])
            written_tuple = tuple(created_tuple)

            s.execute("INSERT INTO mytable (k, v) VALUES (%s, %s)", (i, written_tuple))

            result = s.execute("SELECT v FROM mytable WHERE k=%s", (i,))[0]
            self.assertEqual(response_tuple, result.v)

    def test_tuple_non_primitive_subtypes(self):
        """
        Ensure tuple subtypes are appropriately handled for maps, sets, and lists.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        # set the row_factory to dict_factory for programmatic access
        # set the encoder for tuples for the ability to write tuples
        s.row_factory = dict_factory
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("""CREATE KEYSPACE test_tuple_non_primitive_subtypes
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("test_tuple_non_primitive_subtypes")

        values = []

        # create list values
        for datatype in DATA_TYPE_PRIMITIVES:
            values.append('v_{} frozen<tuple<list<{}>>>'.format(len(values), datatype))

        # create set values
        for datatype in DATA_TYPE_PRIMITIVES:
            values.append('v_{} frozen<tuple<set<{}>>>'.format(len(values), datatype))

        # create map values
        for datatype in DATA_TYPE_PRIMITIVES:
            datatype_1 = datatype_2 = datatype
            if datatype == 'blob':
                # unhashable type: 'bytearray'
                datatype_1 = 'ascii'
            values.append('v_{} frozen<tuple<map<{}, {}>>>'.format(len(values), datatype_1, datatype_2))

        # make sure we're testing all non primitive data types in the future
        if set(DATA_TYPE_NON_PRIMITIVE_NAMES) != set(['tuple', 'list', 'map', 'set']):
            raise NotImplemented('Missing datatype not implemented: {}'.format(
                set(DATA_TYPE_NON_PRIMITIVE_NAMES) - set(['tuple', 'list', 'map', 'set'])
            ))

        # create table
        s.execute("CREATE TABLE mytable ("
                  "k int PRIMARY KEY, "
                  "%s)" % ', '.join(values))

        i = 0
        # test tuple<list<datatype>>
        for datatype in DATA_TYPE_PRIMITIVES:
            created_tuple = tuple([[get_sample(datatype)]])
            s.execute("INSERT INTO mytable (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM mytable WHERE k=0", (i,))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
            i += 1

        # test tuple<set<datatype>>
        for datatype in DATA_TYPE_PRIMITIVES:
            created_tuple = tuple([SortedSet([get_sample(datatype)])])
            s.execute("INSERT INTO mytable (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM mytable WHERE k=0", (i,))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
            i += 1

        # test tuple<map<datatype, datatype>>
        for datatype in DATA_TYPE_PRIMITIVES:
            if datatype == 'blob':
                # unhashable type: 'bytearray'
                created_tuple = tuple([{get_sample('ascii'): get_sample(datatype)}])
            else:
                created_tuple = tuple([{get_sample(datatype): get_sample(datatype)}])

            s.execute("INSERT INTO mytable (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM mytable WHERE k=0", (i,))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
            i += 1

    def nested_tuples_schema_helper(self, depth):
        """
        Helper method for creating nested tuple schema
        """

        if depth == 0:
            return 'int'
        else:
            return 'tuple<%s>' % self.nested_tuples_schema_helper(depth - 1)

    def nested_tuples_creator_helper(self, depth):
        """
        Helper method for creating nested tuples
        """

        if depth == 0:
            return 303
        else:
            return (self.nested_tuples_creator_helper(depth - 1), )

    def test_nested_tuples(self):
        """
        Ensure nested are appropriately handled.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        # set the row_factory to dict_factory for programmatic access
        # set the encoder for tuples for the ability to write tuples
        s.row_factory = dict_factory
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("""CREATE KEYSPACE test_nested_tuples
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("test_nested_tuples")

        # create a table with multiple sizes of nested tuples
        s.execute("CREATE TABLE mytable ("
                  "k int PRIMARY KEY, "
                  "v_1 frozen<%s>,"
                  "v_2 frozen<%s>,"
                  "v_3 frozen<%s>,"
                  "v_128 frozen<%s>"
                  ")" % (self.nested_tuples_schema_helper(1),
                         self.nested_tuples_schema_helper(2),
                         self.nested_tuples_schema_helper(3),
                         self.nested_tuples_schema_helper(128)))

        for i in (1, 2, 3, 128):
            # create tuple
            created_tuple = self.nested_tuples_creator_helper(i)

            # write tuple
            s.execute("INSERT INTO mytable (k, v_%s) VALUES (%s, %s)", (i, i, created_tuple))

            # verify tuple was written and read correctly
            result = s.execute("SELECT v_%s FROM mytable WHERE k=%s", (i, i))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])

    def test_tuples_with_nulls(self):
        """
        Test tuples with null and empty string fields.
        """
        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""CREATE KEYSPACE test_tuples_with_nulls
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}""")
        s.set_keyspace("test_tuples_with_nulls")

        s.execute("CREATE TABLE mytable (k int PRIMARY KEY, t frozen<tuple<text, int, uuid, blob>>)")

        insert = s.prepare("INSERT INTO mytable (k, t) VALUES (0, ?)")
        s.execute(insert, [(None, None, None, None)])

        result = s.execute("SELECT * FROM mytable WHERE k=0")
        self.assertEquals((None, None, None, None), result[0].t)

        read = s.prepare("SELECT * FROM mytable WHERE k=0")
        self.assertEquals((None, None, None, None), s.execute(read)[0].t)

        # also test empty strings where compatible
        s.execute(insert, [('', None, None, '')])
        result = s.execute("SELECT * FROM mytable WHERE k=0")
        self.assertEquals(('', None, None, ''), result[0].t)
        self.assertEquals(('', None, None, ''), s.execute(read)[0].t)

        c.shutdown()

    def test_unicode_query_string(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        query = u"SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = 'ef\u2052ef' AND columnfamily_name = %s"
        s.execute(query, (u"fe\u2051fe",))
