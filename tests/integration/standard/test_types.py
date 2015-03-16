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

from collections import namedtuple
from decimal import Decimal
from datetime import datetime, date, time
from functools import partial
import six
from uuid import uuid1, uuid4

from cassandra import InvalidRequest
from cassandra.cluster import Cluster
from cassandra.cqltypes import Int32Type, EMPTY
from cassandra.query import dict_factory
from cassandra.util import OrderedMap, sortedset

from tests.integration import get_server_versions, use_singledc, PROTOCOL_VERSION

# defined in module scope for pickling in OrderedMap
nested_collection_udt = namedtuple('nested_collection_udt', ['m', 't', 'l', 's'])
nested_collection_udt_nested = namedtuple('nested_collection_udt_nested', ['m', 't', 'l', 's', 'u'])


def setup_module():
    use_singledc()


class TypeTests(unittest.TestCase):

    _types_table_created = False

    @classmethod
    def setup_class(cls):
        cls._cass_version, cls._cql_version = get_server_versions()

        cls._col_types = ['text',
                          'ascii',
                          'bigint',
                          'boolean',
                          'decimal',
                          'double',
                          'float',
                          'inet',
                          'int',
                          'list<text>',
                          'set<int>',
                          'map<text,int>',
                          'timestamp',
                          'uuid',
                          'timeuuid',
                          'varchar',
                          'varint']

        if cls._cass_version >= (2, 1, 4):
            cls._col_types.extend(('date', 'time'))

        cls._session = Cluster(protocol_version=PROTOCOL_VERSION).connect()
        cls._session.execute("CREATE KEYSPACE typetests WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        cls._session.set_keyspace("typetests")

    @classmethod
    def teardown_class(cls):
        cls._session.execute("DROP KEYSPACE typetests")
        cls._session.cluster.shutdown()

    def test_blob_type_as_string(self):
        s = self._session

        s.execute("""
            CREATE TABLE blobstring (
                a ascii,
                b blob,
                PRIMARY KEY (a)
            )
        """)

        params = [
            'key1',
            b'blobyblob'
        ]

        query = 'INSERT INTO blobstring (a, b) VALUES (%s, %s)'

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

        results = s.execute("SELECT * FROM blobstring")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

    def test_blob_type_as_bytearray(self):
        s = self._session
        s.execute("""
            CREATE TABLE blobbytes (
                a ascii,
                b blob,
                PRIMARY KEY (a)
            )
        """)

        params = [
            'key1',
            bytearray(b'blob1')
        ]

        query = 'INSERT INTO blobbytes (a, b) VALUES (%s, %s);'
        s.execute(query, params)

        expected_vals = [
            'key1',
            bytearray(b'blob1')
        ]

        results = s.execute("SELECT * FROM blobbytes")

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(expected, actual)

    def _create_all_types_table(self):
        if not self._types_table_created:
            TypeTests._col_names = ["%s_col" % col_type.translate(None, '<> ,') for col_type in self._col_types]
            cql = "CREATE TABLE alltypes ( key int PRIMARY KEY, %s)" % ','.join("%s %s" % name_type for name_type in zip(self._col_names, self._col_types))
            self._session.execute(cql)
            TypeTests._types_table_created = True

    def test_basic_types(self):

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)

        self._create_all_types_table()

        v1_uuid = uuid1()
        v4_uuid = uuid4()
        mydatetime = datetime(2013, 12, 31, 23, 59, 59, 999000)

        # this could use some rework tying column types to names (instead of relying on position)
        params = [
            "text",
            "ascii",
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
            mydatetime,  # timestamp
            v4_uuid,  # uuid
            v1_uuid,  # timeuuid
            u"sometext\u1234",  # varchar
            123456789123456789123456789,  # varint
        ]

        expected_vals = [
            "text",
            "ascii",
            12345678923456789,  # bigint
            True,  # boolean
            Decimal('1.234567890123456789'),  # decimal
            0.000244140625,  # double
            1.25,  # float
            "1.2.3.4",  # inet
            12345,  # int
            ['a', 'b', 'c'],  # list<text> collection
            sortedset((1, 2, 3)),  # set<int> collection
            {'a': 1, 'b': 2},  # map<text, int> collection
            mydatetime,  # timestamp
            v4_uuid,  # uuid
            v1_uuid,  # timeuuid
            u"sometext\u1234",  # varchar
            123456789123456789123456789,  # varint
        ]

        if self._cass_version >= (2, 1, 4):
            mydate = date(2015, 1, 15)
            mytime = time(16, 47, 25, 7)

            params.append(mydate)
            params.append(mytime)

            expected_vals.append(mydate)
            expected_vals.append(mytime)

        columns_string = ','.join(self._col_names)
        placeholders = ', '.join(["%s"] * len(self._col_names))
        s.execute("INSERT INTO alltypes (key, %s) VALUES (0, %s)" %
                  (columns_string, placeholders), params)

        results = s.execute("SELECT %s FROM alltypes WHERE key=0" % columns_string)

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(actual, expected)

        # try the same thing with a prepared statement
        placeholders = ','.join(["?"] * len(self._col_names))
        prepared = s.prepare("INSERT INTO alltypes (key, %s) VALUES (1, %s)" %
                             (columns_string, placeholders))
        s.execute(prepared.bind(params))

        results = s.execute("SELECT %s FROM alltypes WHERE key=1" % columns_string)

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(actual, expected)

        # query with prepared statement
        prepared = s.prepare("SELECT %s FROM alltypes WHERE key=?" % columns_string)
        results = s.execute(prepared.bind((1,)))

        for expected, actual in zip(expected_vals, results[0]):
            self.assertEqual(actual, expected)

        # query with prepared statement, no explicit columns
        s.row_factory = dict_factory
        prepared = s.prepare("SELECT * FROM alltypes")
        results = s.execute(prepared.bind(()))

        row = results[0]
        for expected, name in zip(expected_vals, self._col_names):
            self.assertEqual(row[name], expected)

        s.shutdown()

    def test_empty_strings_and_nones(self):
        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)
        s.row_factory = dict_factory

        self._create_all_types_table()

        columns_string = ','.join(self._col_names)
        s.execute("INSERT INTO alltypes (key) VALUES (2)")
        results = s.execute("SELECT %s FROM alltypes WHERE key=2" % columns_string)
        self.assertTrue(all(x is None for x in results[0].values()))

        prepared = s.prepare("SELECT %s FROM alltypes WHERE key=?" % columns_string)
        results = s.execute(prepared.bind((2,)))
        self.assertTrue(all(x is None for x in results[0].values()))

        # insert empty strings for string-like fields and fetch them
        expected_values = {'text_col': '', 'ascii_col': '', 'varchar_col': '', 'listtext_col': [''], 'maptextint_col': OrderedMap({'': 3})}
        columns_string = ','.join(expected_values.keys())
        placeholders = ','.join(["%s"] * len(expected_values))
        s.execute("INSERT INTO alltypes (key, %s) VALUES (3, %s)" % (columns_string, placeholders), expected_values.values())
        self.assertEqual(expected_values,
                         s.execute("SELECT %s FROM alltypes WHERE key=3" % columns_string)[0])
        self.assertEqual(expected_values,
                         s.execute(s.prepare("SELECT %s FROM alltypes WHERE key=?" % columns_string), (3,))[0])

        # non-string types shouldn't accept empty strings
        for col in ('bigint_col', 'boolean_col', 'decimal_col', 'double_col',
                    'float_col', 'int_col', 'listtext_col', 'setint_col',
                    'maptextint_col', 'uuid_col', 'timeuuid_col', 'varint_col'):
            query = "INSERT INTO alltypes (key, %s) VALUES (4, %%s)" % col
            try:
                s.execute(query, [''])
            except InvalidRequest:
                pass
            else:
                self.fail("Expected an InvalidRequest error when inserting an "
                          "emptry string for column %s" % (col, ))

            prepared = s.prepare("INSERT INTO alltypes (key, %s) VALUES (4, ?)" % col)
            try:
                s.execute(prepared, [''])
            except TypeError:
                pass
            else:
                self.fail("Expected an InvalidRequest error when inserting an "
                          "emptry string for column %s with a prepared statement" % (col, ))

        # insert values for all columns
        values = ['text', 'ascii', 1, True, Decimal('1.0'), 0.1, 0.1,
                  "1.2.3.4", 1, ['a'], set([1]), {'a': 1},
                  datetime.now(), uuid4(), uuid1(), 'a', 1]
        if self._cass_version >= (2, 1, 4):
            values.append('2014-01-01')
            values.append('01:02:03.456789012')

        columns_string = ','.join(self._col_names)
        placeholders = ','.join(["%s"] * len(self._col_names))
        insert = "INSERT INTO alltypes (key, %s) VALUES (5, %s)" % (columns_string, placeholders)
        s.execute(insert, values)

        # then insert None, which should null them out
        null_values = [None] * len(self._col_names)
        s.execute(insert, null_values)

        select = "SELECT %s FROM alltypes WHERE key=5" % columns_string
        results = s.execute(select)
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        prepared = s.prepare(select)
        results = s.execute(prepared.bind(()))
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        # do the same thing again, but use a prepared statement to insert the nulls
        s.execute(insert, values)

        placeholders = ','.join(["?"] * len(self._col_names))
        prepared = s.prepare("INSERT INTO alltypes (key, %s) VALUES (5, %s)" % (columns_string, placeholders))
        s.execute(prepared, null_values)

        results = s.execute(select)
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        prepared = s.prepare(select)
        results = s.execute(prepared.bind(()))
        self.assertEqual([], [(name, val) for (name, val) in results[0].items() if val is not None])

        s.shutdown()

    def test_empty_values(self):
        s = self._session
        s.execute("CREATE TABLE empty_values (a text PRIMARY KEY, b int)")
        s.execute("INSERT INTO empty_values (a, b) VALUES ('a', blobAsInt(0x))")
        try:
            Int32Type.support_empty_values = True
            results = s.execute("SELECT b FROM empty_values WHERE a='a'")[0]
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

        s = self._session

        s.execute("CREATE TABLE tz_aware (a ascii PRIMARY KEY, b timestamp)")

        # test non-prepared statement
        s.execute("INSERT INTO tz_aware (a, b) VALUES ('key1', %s)", parameters=(dt,))
        result = s.execute("SELECT b FROM tz_aware WHERE a='key1'")[0].b
        self.assertEqual(dt.utctimetuple(), result.utctimetuple())

        # test prepared statement
        prepared = s.prepare("INSERT INTO tz_aware (a, b) VALUES ('key2', ?)")
        s.execute(prepared, parameters=(dt,))
        result = s.execute("SELECT b FROM tz_aware WHERE a='key2'")[0].b
        self.assertEqual(dt.utctimetuple(), result.utctimetuple())

    def test_tuple_type(self):
        """
        Basic test of tuple functionality
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)

        # use this encoder in order to insert tuples
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("CREATE TABLE tuple_type  (a int PRIMARY KEY, b frozen<tuple<ascii, int, boolean>>)")

        # test non-prepared statement
        complete = ('foo', 123, True)
        s.execute("INSERT INTO tuple_type (a, b) VALUES (0, %s)", parameters=(complete,))
        result = s.execute("SELECT b FROM tuple_type WHERE a=0")[0]
        self.assertEqual(complete, result.b)

        partial = ('bar', 456)
        partial_result = partial + (None,)
        s.execute("INSERT INTO tuple_type (a, b) VALUES (1, %s)", parameters=(partial,))
        result = s.execute("SELECT b FROM tuple_type WHERE a=1")[0]
        self.assertEqual(partial_result, result.b)

        # test single value tuples
        subpartial = ('zoo',)
        subpartial_result = subpartial + (None, None)
        s.execute("INSERT INTO tuple_type (a, b) VALUES (2, %s)", parameters=(subpartial,))
        result = s.execute("SELECT b FROM tuple_type WHERE a=2")[0]
        self.assertEqual(subpartial_result, result.b)

        # test prepared statement
        prepared = s.prepare("INSERT INTO tuple_type (a, b) VALUES (?, ?)")
        s.execute(prepared, parameters=(3, complete))
        s.execute(prepared, parameters=(4, partial))
        s.execute(prepared, parameters=(5, subpartial))

        # extra items in the tuple should result in an error
        self.assertRaises(ValueError, s.execute, prepared, parameters=(0, (1, 2, 3, 4, 5, 6)))

        prepared = s.prepare("SELECT b FROM tuple_type WHERE a=?")
        self.assertEqual(complete, s.execute(prepared, (3,))[0].b)
        self.assertEqual(partial_result, s.execute(prepared, (4,))[0].b)
        self.assertEqual(subpartial_result, s.execute(prepared, (5,))[0].b)

        s.shutdown()

    def test_tuple_type_varying_lengths(self):
        """
        Test tuple types of lengths of 1, 2, 3, and 384 to ensure edge cases work
        as expected.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)

        # set the row_factory to dict_factory for programmatic access
        # set the encoder for tuples for the ability to write tuples
        s.row_factory = dict_factory
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        # programmatically create the table with tuples of said sizes
        lengths = (1, 2, 3, 384)
        value_schema = []
        for i in lengths:
            value_schema += [' v_%s frozen<tuple<%s>>' % (i, ', '.join(['int'] * i))]
        s.execute("CREATE TABLE tuple_lengths (k int PRIMARY KEY, %s)" % (', '.join(value_schema),))

        # insert tuples into same key using different columns
        # and verify the results
        for i in lengths:
            # ensure tuples of larger sizes throw an error
            created_tuple = tuple(range(0, i + 1))
            self.assertRaises(InvalidRequest, s.execute, "INSERT INTO tuple_lengths (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            # ensure tuples of proper sizes are written and read correctly
            created_tuple = tuple(range(0, i))

            s.execute("INSERT INTO tuple_lengths (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_lengths WHERE k=0", (i,))[0]
            self.assertEqual(tuple(created_tuple), result['v_%s' % i])
        s.shutdown()

    def test_tuple_primitive_subtypes(self):
        """
        Ensure tuple subtypes are appropriately handled.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("CREATE TABLE tuple_primitive ("
                  "k int PRIMARY KEY, "
                  "v frozen<tuple<%s>>)" % ','.join(DATA_TYPE_PRIMITIVES))

        for i in range(len(DATA_TYPE_PRIMITIVES)):
            # create tuples to be written and ensure they match with the expected response
            # responses have trailing None values for every element that has not been written
            created_tuple = [get_sample(DATA_TYPE_PRIMITIVES[j]) for j in range(i + 1)]
            response_tuple = tuple(created_tuple + [None for j in range(len(DATA_TYPE_PRIMITIVES) - i - 1)])
            written_tuple = tuple(created_tuple)

            s.execute("INSERT INTO tuple_primitive (k, v) VALUES (%s, %s)", (i, written_tuple))

            result = s.execute("SELECT v FROM tuple_primitive WHERE k=%s", (i,))[0]
            self.assertEqual(response_tuple, result.v)
        s.shutdown()

    def test_tuple_non_primitive_subtypes(self):
        """
        Ensure tuple subtypes are appropriately handled for maps, sets, and lists.
        """

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)

        # set the row_factory to dict_factory for programmatic access
        # set the encoder for tuples for the ability to write tuples
        s.row_factory = dict_factory
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

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
        s.execute("CREATE TABLE tuple_non_primative ("
                  "k int PRIMARY KEY, "
                  "%s)" % ', '.join(values))

        i = 0
        # test tuple<list<datatype>>
        for datatype in DATA_TYPE_PRIMITIVES:
            created_tuple = tuple([[get_sample(datatype)]])
            s.execute("INSERT INTO tuple_non_primative (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_non_primative WHERE k=0", (i,))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
            i += 1

        # test tuple<set<datatype>>
        for datatype in DATA_TYPE_PRIMITIVES:
            created_tuple = tuple([sortedset([get_sample(datatype)])])
            s.execute("INSERT INTO tuple_non_primative (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_non_primative WHERE k=0", (i,))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
            i += 1

        # test tuple<map<datatype, datatype>>
        for datatype in DATA_TYPE_PRIMITIVES:
            if datatype == 'blob':
                # unhashable type: 'bytearray'
                created_tuple = tuple([{get_sample('ascii'): get_sample(datatype)}])
            else:
                created_tuple = tuple([{get_sample(datatype): get_sample(datatype)}])

            s.execute("INSERT INTO tuple_non_primative (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_non_primative WHERE k=0", (i,))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
            i += 1
        s.shutdown()

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

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)

        # set the row_factory to dict_factory for programmatic access
        # set the encoder for tuples for the ability to write tuples
        s.row_factory = dict_factory
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        # create a table with multiple sizes of nested tuples
        s.execute("CREATE TABLE nested_tuples ("
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
            s.execute("INSERT INTO nested_tuples (k, v_%s) VALUES (%s, %s)", (i, i, created_tuple))

            # verify tuple was written and read correctly
            result = s.execute("SELECT v_%s FROM nested_tuples WHERE k=%s", (i, i))[0]
            self.assertEqual(created_tuple, result['v_%s' % i])
        s.shutdown()

    def test_tuples_with_nulls(self):
        """
        Test tuples with null and empty string fields.
        """
        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        s = self._session

        s.execute("CREATE TABLE tuples_nulls (k int PRIMARY KEY, t frozen<tuple<text, int, uuid, blob>>)")

        insert = s.prepare("INSERT INTO tuples_nulls (k, t) VALUES (0, ?)")
        s.execute(insert, [(None, None, None, None)])

        result = s.execute("SELECT * FROM tuples_nulls WHERE k=0")
        self.assertEqual((None, None, None, None), result[0].t)

        read = s.prepare("SELECT * FROM tuples_nulls WHERE k=0")
        self.assertEqual((None, None, None, None), s.execute(read)[0].t)

        # also test empty strings where compatible
        s.execute(insert, [('', None, None, b'')])
        result = s.execute("SELECT * FROM tuples_nulls WHERE k=0")
        self.assertEqual(('', None, None, b''), result[0].t)
        self.assertEqual(('', None, None, b''), s.execute(read)[0].t)

    def test_unicode_query_string(self):
        s = self._session

        query = u"SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = 'ef\u2052ef' AND columnfamily_name = %s"
        s.execute(query, (u"fe\u2051fe",))

    def insert_select_column(self, session, table_name, column_name, value):
        insert = session.prepare("INSERT INTO %s (k, %s) VALUES (?, ?)" % (table_name, column_name))
        session.execute(insert, (0, value))
        result = session.execute("SELECT %s FROM %s WHERE k=%%s" % (column_name, table_name), (0,))[0][0]
        self.assertEqual(result, value)

    def test_nested_collections(self):

        if self._cass_version < (2, 1, 3):
            raise unittest.SkipTest("Support for nested collections was introduced in Cassandra 2.1.3")

        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest("Protocol version > 3 required for nested collections")

        name = self._testMethodName

        s = self._session.cluster.connect()
        s.set_keyspace(self._session.keyspace)
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("""
            CREATE TYPE %s (
                m frozen<map<int,text>>,
                t tuple<int,text>,
                l frozen<list<int>>,
                s frozen<set<int>>
            )""" % name)
        s.execute("""
            CREATE TYPE %s_nested (
                m frozen<map<int,text>>,
                t tuple<int,text>,
                l frozen<list<int>>,
                s frozen<set<int>>,
                u frozen<%s>
            )""" % (name, name))
        s.execute("""
            CREATE TABLE %s (
                k int PRIMARY KEY,
                map_map map<frozen<map<int,int>>, frozen<map<int,int>>>,
                map_set map<frozen<set<int>>, frozen<set<int>>>,
                map_list map<frozen<list<int>>, frozen<list<int>>>,
                map_tuple map<frozen<tuple<int, int>>, frozen<tuple<int>>>,
                map_udt map<frozen<%s_nested>, frozen<%s>>,
            )""" % (name, name, name))

        validate = partial(self.insert_select_column, s, name)
        validate('map_map', OrderedMap([({1: 1, 2: 2}, {3: 3, 4: 4}), ({5: 5, 6: 6}, {7: 7, 8: 8})]))
        validate('map_set', OrderedMap([(set((1, 2)), set((3, 4))), (set((5, 6)), set((7, 8)))]))
        validate('map_list', OrderedMap([([1, 2], [3, 4]), ([5, 6], [7, 8])]))
        validate('map_tuple', OrderedMap([((1, 2), (3,)), ((4, 5), (6,))]))

        value = nested_collection_udt({1: 'v1', 2: 'v2'}, (3, 'v3'), [4, 5, 6, 7], set((8, 9, 10)))
        key = nested_collection_udt_nested(value.m, value.t, value.l, value.s, value)
        key2 = nested_collection_udt_nested({3: 'v3'}, value.t, value.l, value.s, value)
        validate('map_udt', OrderedMap([(key, value), (key2, value)]))

        s.execute("DROP TABLE %s" % (name))
        s.execute("DROP TYPE %s_nested" % (name))
        s.execute("DROP TYPE %s" % (name))
        s.shutdown()

    def test_reading_composite_type(self):
        s = self._session
        s.execute("""
            CREATE TABLE composites (
                a int PRIMARY KEY,
                b 'org.apache.cassandra.db.marshal.CompositeType(AsciiType, Int32Type)'
            )""")

        # CompositeType string literals are split on ':' chars
        s.execute("INSERT INTO composites (a, b) VALUES (0, 'abc:123')")
        result = s.execute("SELECT * FROM composites WHERE a = 0")[0]
        self.assertEqual(0, result.a)
        self.assertEqual(('abc', 123), result.b)

        # CompositeType values can omit elements at the end
        s.execute("INSERT INTO composites (a, b) VALUES (0, 'abc')")
        result = s.execute("SELECT * FROM composites WHERE a = 0")[0]
        self.assertEqual(0, result.a)
        self.assertEqual(('abc',), result.b)
