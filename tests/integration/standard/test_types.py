# Copyright DataStax, Inc.
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

import unittest

import ipaddress
import math
import random
import string
import socket
import uuid

from datetime import datetime, date, time, timedelta
from decimal import Decimal
from functools import partial

from packaging.version import Version

import cassandra
from cassandra import InvalidRequest
from cassandra import util
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.cqltypes import Int32Type, EMPTY
from cassandra.query import dict_factory, ordered_dict_factory
from cassandra.util import sortedset, Duration, OrderedMap
from tests.unit.cython.utils import cythontest

from tests.integration import use_singledc, execute_until_pass, notprotocolv1, \
    BasicSharedKeyspaceUnitTestCase, greaterthancass21, lessthancass30, \
    greaterthanorequalcass3_10, TestCluster, requires_composite_type, greaterthanorequalcass50
from tests.integration.datatype_utils import update_datatypes, PRIMITIVE_DATATYPES, COLLECTION_TYPES, PRIMITIVE_DATATYPES_KEYS, \
    get_sample, get_all_samples, get_collection_sample


def setup_module():
    use_singledc()
    update_datatypes()


class TypeTests(BasicSharedKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        # cls._cass_version, cls. = get_server_versions()
        super(TypeTests, cls).setUpClass()
        cls.session.set_keyspace(cls.ks_name)

    def test_can_insert_blob_type_as_string(self):
        """
        Tests that byte strings in Python maps to blob type in Cassandra
        """
        s = self.session

        s.execute("CREATE TABLE blobstring (a ascii PRIMARY KEY, b blob)")

        params = ['key1', b'blobbyblob']
        query = "INSERT INTO blobstring (a, b) VALUES (%s, %s)"

        s.execute(query, params)

        results = s.execute("SELECT * FROM blobstring").one()
        for expected, actual in zip(params, results):
            assert expected == actual

    def test_can_insert_blob_type_as_bytearray(self):
        """
        Tests that blob type in Cassandra maps to bytearray in Python
        """
        s = self.session

        s.execute("CREATE TABLE blobbytes (a ascii PRIMARY KEY, b blob)")

        params = ['key1', bytearray(b'blob1')]
        s.execute("INSERT INTO blobbytes (a, b) VALUES (%s, %s)", params)

        results = s.execute("SELECT * FROM blobbytes").one()
        for expected, actual in zip(params, results):
            assert expected == actual

    @unittest.skipIf(not hasattr(cassandra, 'deserializers'), "Cython required for to test DesBytesTypeArray deserializer")
    def test_des_bytes_type_array(self):
        """
        Simple test to ensure the DesBytesTypeByteArray deserializer functionally works

        @since 3.1
        @jira_ticket PYTHON-503
        @expected_result byte array should be deserialized appropriately.

        @test_category queries:custom_payload
        """
        original = None
        try:

            original = cassandra.deserializers.DesBytesType
            cassandra.deserializers.DesBytesType = cassandra.deserializers.DesBytesTypeByteArray
            s = self.session

            s.execute("CREATE TABLE blobbytes2 (a ascii PRIMARY KEY, b blob)")

            params = ['key1', bytearray(b'blob1')]
            s.execute("INSERT INTO blobbytes2 (a, b) VALUES (%s, %s)", params)

            results = s.execute("SELECT * FROM blobbytes2").one()
            for expected, actual in zip(params, results):
                assert expected == actual
        finally:
            if original is not None:
                cassandra.deserializers.DesBytesType=original

    def test_can_insert_primitive_datatypes(self):
        """
        Test insertion of all datatype primitives
        """
        c = TestCluster()
        s = c.connect(self.keyspace_name)

        # create table
        alpha_type_list = ["zz int PRIMARY KEY"]
        col_names = ["zz"]
        start_index = ord('a')
        for i, datatype in enumerate(PRIMITIVE_DATATYPES):
            alpha_type_list.append("{0} {1}".format(chr(start_index + i), datatype))
            col_names.append(chr(start_index + i))

        s.execute("CREATE TABLE alltypes ({0})".format(', '.join(alpha_type_list)))

        # create the input
        params = [0]
        for datatype in PRIMITIVE_DATATYPES:
            params.append((get_sample(datatype)))

        # insert into table as a simple statement
        columns_string = ', '.join(col_names)
        placeholders = ', '.join(["%s"] * len(col_names))
        s.execute("INSERT INTO alltypes ({0}) VALUES ({1})".format(columns_string, placeholders), params)

        # verify data
        results = s.execute("SELECT {0} FROM alltypes WHERE zz=0".format(columns_string)).one()
        for expected, actual in zip(params, results):
            assert actual == expected

        # try the same thing sending one insert at the time
        s.execute("TRUNCATE alltypes;")
        for i, datatype in enumerate(PRIMITIVE_DATATYPES):
            single_col_name = chr(start_index + i)
            single_col_names = ["zz", single_col_name]
            placeholders = ','.join(["%s"] * len(single_col_names))
            single_columns_string = ', '.join(single_col_names)
            for j, data_sample in enumerate(get_all_samples(datatype)):
                key = i + 1000 * j
                single_params = (key, data_sample)
                s.execute("INSERT INTO alltypes ({0}) VALUES ({1})".format(single_columns_string, placeholders),
                          single_params)
                # verify data
                result = s.execute("SELECT {0} FROM alltypes WHERE zz=%s".format(single_columns_string), (key,)).one()[1]
                compare_value = data_sample

                if isinstance(data_sample, ipaddress.IPv4Address) or isinstance(data_sample, ipaddress.IPv6Address):
                    compare_value = str(data_sample)
                assert result == compare_value

        # try the same thing with a prepared statement
        placeholders = ','.join(["?"] * len(col_names))
        s.execute("TRUNCATE alltypes;")
        insert = s.prepare("INSERT INTO alltypes ({0}) VALUES ({1})".format(columns_string, placeholders))
        s.execute(insert.bind(params))

        # verify data
        results = s.execute("SELECT {0} FROM alltypes WHERE zz=0".format(columns_string)).one()
        for expected, actual in zip(params, results):
            assert actual == expected

        # verify data with prepared statement query
        select = s.prepare("SELECT {0} FROM alltypes WHERE zz=?".format(columns_string))
        results = s.execute(select.bind([0])).one()
        for expected, actual in zip(params, results):
            assert actual == expected

        # verify data with with prepared statement, use dictionary with no explicit columns
        select = s.prepare("SELECT * FROM alltypes")
        results = s.execute(select,
                            execution_profile=s.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, row_factory=ordered_dict_factory)).one()

        for expected, actual in zip(params, results.values()):
            assert actual == expected

        c.shutdown()

    def test_can_insert_collection_datatypes(self):
        """
        Test insertion of all collection types
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name)
        # use tuple encoding, to convert native python tuple into raw CQL
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        # create table
        alpha_type_list = ["zz int PRIMARY KEY"]
        col_names = ["zz"]
        start_index = ord('a')
        for i, collection_type in enumerate(COLLECTION_TYPES):
            for j, datatype in enumerate(PRIMITIVE_DATATYPES_KEYS):
                if collection_type == "map":
                    type_string = "{0}_{1} {2}<{3}, {3}>".format(chr(start_index + i), chr(start_index + j),
                                                                     collection_type, datatype)
                elif collection_type == "tuple":
                    type_string = "{0}_{1} frozen<{2}<{3}>>".format(chr(start_index + i), chr(start_index + j),
                                                            collection_type, datatype)
                else:
                    type_string = "{0}_{1} {2}<{3}>".format(chr(start_index + i), chr(start_index + j),
                                                            collection_type, datatype)
                alpha_type_list.append(type_string)
                col_names.append("{0}_{1}".format(chr(start_index + i), chr(start_index + j)))

        s.execute("CREATE TABLE allcoltypes ({0})".format(', '.join(alpha_type_list)))
        columns_string = ', '.join(col_names)

        # create the input for simple statement
        params = [0]
        for collection_type in COLLECTION_TYPES:
            for datatype in PRIMITIVE_DATATYPES_KEYS:
                params.append((get_collection_sample(collection_type, datatype)))

        # insert into table as a simple statement
        placeholders = ', '.join(["%s"] * len(col_names))
        s.execute("INSERT INTO allcoltypes ({0}) VALUES ({1})".format(columns_string, placeholders), params)

        # verify data
        results = s.execute("SELECT {0} FROM allcoltypes WHERE zz=0".format(columns_string)).one()
        for expected, actual in zip(params, results):
            assert actual == expected

        # create the input for prepared statement
        params = [0]
        for collection_type in COLLECTION_TYPES:
            for datatype in PRIMITIVE_DATATYPES_KEYS:
                params.append((get_collection_sample(collection_type, datatype)))

        # try the same thing with a prepared statement
        placeholders = ','.join(["?"] * len(col_names))
        insert = s.prepare("INSERT INTO allcoltypes ({0}) VALUES ({1})".format(columns_string, placeholders))
        s.execute(insert.bind(params))

        # verify data
        results = s.execute("SELECT {0} FROM allcoltypes WHERE zz=0".format(columns_string)).one()
        for expected, actual in zip(params, results):
            assert actual == expected

        # verify data with prepared statement query
        select = s.prepare("SELECT {0} FROM allcoltypes WHERE zz=?".format(columns_string))
        results = s.execute(select.bind([0])).one()
        for expected, actual in zip(params, results):
            assert actual == expected

        # verify data with with prepared statement, use dictionary with no explicit columns
        select = s.prepare("SELECT * FROM allcoltypes")
        results = s.execute(select,
                            execution_profile=s.execution_profile_clone_update(EXEC_PROFILE_DEFAULT,
                                                                               row_factory=ordered_dict_factory)).one()

        for expected, actual in zip(params, results.values()):
            assert actual == expected

        c.shutdown()

    def test_can_insert_empty_strings_and_nulls(self):
        """
        Test insertion of empty strings and null values
        """
        s = self.session

        # create table
        alpha_type_list = ["zz int PRIMARY KEY"]
        col_names = []
        string_types = set(('ascii', 'text', 'varchar'))
        string_columns = set((''))
        # this is just a list of types to try with empty strings
        non_string_types = PRIMITIVE_DATATYPES - string_types - set(('blob', 'date', 'inet', 'time', 'timestamp'))
        non_string_columns = set()
        start_index = ord('a')
        for i, datatype in enumerate(PRIMITIVE_DATATYPES):
            col_name = chr(start_index + i)
            alpha_type_list.append("{0} {1}".format(col_name, datatype))
            col_names.append(col_name)
            if datatype in non_string_types:
                non_string_columns.add(col_name)
            if datatype in string_types:
                string_columns.add(col_name)

        execute_until_pass(s, "CREATE TABLE all_empty ({0})".format(', '.join(alpha_type_list)))

        # verify all types initially null with simple statement
        columns_string = ','.join(col_names)
        s.execute("INSERT INTO all_empty (zz) VALUES (2)")
        results = s.execute("SELECT {0} FROM all_empty WHERE zz=2".format(columns_string)).one()
        assert all(x is None for x in results)

        # verify all types initially null with prepared statement
        select = s.prepare("SELECT {0} FROM all_empty WHERE zz=?".format(columns_string))
        results = s.execute(select.bind([2])).one()
        assert all(x is None for x in results)

        # insert empty strings for string-like fields
        expected_values = dict((col, '') for col in string_columns)
        columns_string = ','.join(string_columns)
        placeholders = ','.join(["%s"] * len(string_columns))
        s.execute("INSERT INTO all_empty (zz, {0}) VALUES (3, {1})".format(columns_string, placeholders), expected_values.values())

        # verify string types empty with simple statement
        results = s.execute("SELECT {0} FROM all_empty WHERE zz=3".format(columns_string)).one()
        for expected, actual in zip(expected_values.values(), results):
            assert actual == expected

        # verify string types empty with prepared statement
        results = s.execute(s.prepare("SELECT {0} FROM all_empty WHERE zz=?".format(columns_string)), [3]).one()
        for expected, actual in zip(expected_values.values(), results):
            assert actual == expected

        # non-string types shouldn't accept empty strings
        for col in non_string_columns:
            query = "INSERT INTO all_empty (zz, {0}) VALUES (4, %s)".format(col)
            with self.assertRaises(InvalidRequest):
                s.execute(query, [''])

            insert = s.prepare("INSERT INTO all_empty (zz, {0}) VALUES (4, ?)".format(col))
            with self.assertRaises(TypeError):
                s.execute(insert, [''])

        # verify that Nones can be inserted and overwrites existing data
        # create the input
        params = []
        for datatype in PRIMITIVE_DATATYPES:
            params.append((get_sample(datatype)))

        # insert the data
        columns_string = ','.join(col_names)
        placeholders = ','.join(["%s"] * len(col_names))
        simple_insert = "INSERT INTO all_empty (zz, {0}) VALUES (5, {1})".format(columns_string, placeholders)
        s.execute(simple_insert, params)

        # then insert None, which should null them out
        null_values = [None] * len(col_names)
        s.execute(simple_insert, null_values)

        # check via simple statement
        query = "SELECT {0} FROM all_empty WHERE zz=5".format(columns_string)
        results = s.execute(query).one()
        for col in results:
            assert None == col

        # check via prepared statement
        select = s.prepare("SELECT {0} FROM all_empty WHERE zz=?".format(columns_string))
        results = s.execute(select.bind([5])).one()
        for col in results:
            assert None == col

        # do the same thing again, but use a prepared statement to insert the nulls
        s.execute(simple_insert, params)

        placeholders = ','.join(["?"] * len(col_names))
        insert = s.prepare("INSERT INTO all_empty (zz, {0}) VALUES (5, {1})".format(columns_string, placeholders))
        s.execute(insert, null_values)

        results = s.execute(query).one()
        for col in results:
            assert None == col

        results = s.execute(select.bind([5])).one()
        for col in results:
            assert None == col

    def test_can_insert_empty_values_for_int32(self):
        """
        Ensure Int32Type supports empty values
        """
        s = self.session

        execute_until_pass(s, "CREATE TABLE empty_values (a text PRIMARY KEY, b int)")
        execute_until_pass(s, "INSERT INTO empty_values (a, b) VALUES ('a', blobAsInt(0x))")
        try:
            Int32Type.support_empty_values = True
            results = execute_until_pass(s, "SELECT b FROM empty_values WHERE a='a'").one()
            self.assertIs(EMPTY, results.b)
        finally:
            Int32Type.support_empty_values = False

    def test_timezone_aware_datetimes_are_timestamps(self):
        """
        Ensure timezone-aware datetimes are converted to timestamps correctly
        """

        try:
            import pytz
        except ImportError as exc:
            raise unittest.SkipTest('pytz is not available: %r' % (exc,))

        dt = datetime(1997, 8, 29, 11, 14)
        eastern_tz = pytz.timezone('US/Eastern')
        eastern_tz.localize(dt)

        s = self.session

        s.execute("CREATE TABLE tz_aware (a ascii PRIMARY KEY, b timestamp)")

        # test non-prepared statement
        s.execute("INSERT INTO tz_aware (a, b) VALUES ('key1', %s)", [dt])
        result = s.execute("SELECT b FROM tz_aware WHERE a='key1'").one().b
        assert dt.utctimetuple() == result.utctimetuple()

        # test prepared statement
        insert = s.prepare("INSERT INTO tz_aware (a, b) VALUES ('key2', ?)")
        s.execute(insert.bind([dt]))
        result = s.execute("SELECT b FROM tz_aware WHERE a='key2'").one().b
        assert dt.utctimetuple() == result.utctimetuple()

    def test_can_insert_tuples(self):
        """
        Basic test of tuple functionality
        """

        if self.cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = TestCluster()
        s = c.connect(self.keyspace_name)

        # use this encoder in order to insert tuples
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("CREATE TABLE tuple_type  (a int PRIMARY KEY, b frozen<tuple<ascii, int, boolean>>)")

        # test non-prepared statement
        complete = ('foo', 123, True)
        s.execute("INSERT INTO tuple_type (a, b) VALUES (0, %s)", parameters=(complete,))
        result = s.execute("SELECT b FROM tuple_type WHERE a=0").one()
        assert complete == result.b

        partial = ('bar', 456)
        partial_result = partial + (None,)
        s.execute("INSERT INTO tuple_type (a, b) VALUES (1, %s)", parameters=(partial,))
        result = s.execute("SELECT b FROM tuple_type WHERE a=1").one()
        assert partial_result == result.b

        # test single value tuples
        subpartial = ('zoo',)
        subpartial_result = subpartial + (None, None)
        s.execute("INSERT INTO tuple_type (a, b) VALUES (2, %s)", parameters=(subpartial,))
        result = s.execute("SELECT b FROM tuple_type WHERE a=2").one()
        assert subpartial_result == result.b

        # test prepared statement
        prepared = s.prepare("INSERT INTO tuple_type (a, b) VALUES (?, ?)")
        s.execute(prepared, parameters=(3, complete))
        s.execute(prepared, parameters=(4, partial))
        s.execute(prepared, parameters=(5, subpartial))

        # extra items in the tuple should result in an error
        self.assertRaises(ValueError, s.execute, prepared, parameters=(0, (1, 2, 3, 4, 5, 6)))

        prepared = s.prepare("SELECT b FROM tuple_type WHERE a=?")
        assert complete == s.execute(prepared, (3,)).one().b
        assert partial_result == s.execute(prepared, (4,)).one().b
        assert subpartial_result == s.execute(prepared, (5,)).one().b

        c.shutdown()

    def test_can_insert_tuples_with_varying_lengths(self):
        """
        Test tuple types of lengths of 1, 2, 3, and 384 to ensure edge cases work
        as expected.
        """

        if self.cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)}
        )
        s = c.connect(self.keyspace_name)

        # set the encoder for tuples for the ability to write tuples
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

            result = s.execute("SELECT v_%s FROM tuple_lengths WHERE k=0", (i,)).one()
            assert tuple(created_tuple) == result['v_%s' % i]
        c.shutdown()

    def test_can_insert_tuples_all_primitive_datatypes(self):
        """
        Ensure tuple subtypes are appropriately handled.
        """

        if self.cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = TestCluster()
        s = c.connect(self.keyspace_name)
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        s.execute("CREATE TABLE tuple_primitive ("
                  "k int PRIMARY KEY, "
                  "v frozen<tuple<%s>>)" % ','.join(PRIMITIVE_DATATYPES))

        values = []
        type_count = len(PRIMITIVE_DATATYPES)
        for i, data_type in enumerate(PRIMITIVE_DATATYPES):
            # create tuples to be written and ensure they match with the expected response
            # responses have trailing None values for every element that has not been written
            values.append(get_sample(data_type))
            expected = tuple(values + [None] * (type_count - len(values)))
            s.execute("INSERT INTO tuple_primitive (k, v) VALUES (%s, %s)", (i, tuple(values)))
            result = s.execute("SELECT v FROM tuple_primitive WHERE k=%s", (i,)).one()
            assert result.v == expected
        c.shutdown()

    def test_can_insert_tuples_all_collection_datatypes(self):
        """
        Ensure tuple subtypes are appropriately handled for maps, sets, and lists.
        """

        if self.cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)}
        )
        s = c.connect(self.keyspace_name)

        # set the encoder for tuples for the ability to write tuples
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        values = []

        # create list values
        for datatype in PRIMITIVE_DATATYPES_KEYS:
            values.append('v_{0} frozen<tuple<list<{1}>>>'.format(len(values), datatype))

        # create set values
        for datatype in PRIMITIVE_DATATYPES_KEYS:
            values.append('v_{0} frozen<tuple<set<{1}>>>'.format(len(values), datatype))

        # create map values
        for datatype in PRIMITIVE_DATATYPES_KEYS:
            datatype_1 = datatype_2 = datatype
            if datatype == 'blob':
                # unhashable type: 'bytearray'
                datatype_1 = 'ascii'
            values.append('v_{0} frozen<tuple<map<{1}, {2}>>>'.format(len(values), datatype_1, datatype_2))

        # make sure we're testing all non primitive data types in the future
        if set(COLLECTION_TYPES) != set(['tuple', 'list', 'map', 'set']):
            raise NotImplemented('Missing datatype not implemented: {}'.format(
                set(COLLECTION_TYPES) - set(['tuple', 'list', 'map', 'set'])
            ))

        # create table
        s.execute("CREATE TABLE tuple_non_primative ("
                  "k int PRIMARY KEY, "
                  "%s)" % ', '.join(values))

        i = 0
        # test tuple<list<datatype>>
        for datatype in PRIMITIVE_DATATYPES_KEYS:
            created_tuple = tuple([[get_sample(datatype)]])
            s.execute("INSERT INTO tuple_non_primative (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_non_primative WHERE k=0", (i,)).one()
            assert created_tuple == result['v_%s' % i]
            i += 1

        # test tuple<set<datatype>>
        for datatype in PRIMITIVE_DATATYPES_KEYS:
            created_tuple = tuple([sortedset([get_sample(datatype)])])
            s.execute("INSERT INTO tuple_non_primative (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_non_primative WHERE k=0", (i,)).one()
            assert created_tuple == result['v_%s' % i]
            i += 1

        # test tuple<map<datatype, datatype>>
        for datatype in PRIMITIVE_DATATYPES_KEYS:
            if datatype == 'blob':
                # unhashable type: 'bytearray'
                created_tuple = tuple([{get_sample('ascii'): get_sample(datatype)}])
            else:
                created_tuple = tuple([{get_sample(datatype): get_sample(datatype)}])

            s.execute("INSERT INTO tuple_non_primative (k, v_%s) VALUES (0, %s)", (i, created_tuple))

            result = s.execute("SELECT v_%s FROM tuple_non_primative WHERE k=0", (i,)).one()
            assert created_tuple == result['v_%s' % i]
            i += 1
        c.shutdown()

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

    def test_can_insert_nested_tuples(self):
        """
        Ensure nested are appropriately handled.
        """

        if self.cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        c = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)}
        )
        s = c.connect(self.keyspace_name)

        # set the encoder for tuples for the ability to write tuples
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        # create a table with multiple sizes of nested tuples
        s.execute("CREATE TABLE nested_tuples ("
                  "k int PRIMARY KEY, "
                  "v_1 frozen<%s>,"
                  "v_2 frozen<%s>,"
                  "v_3 frozen<%s>,"
                  "v_32 frozen<%s>"
                  ")" % (self.nested_tuples_schema_helper(1),
                         self.nested_tuples_schema_helper(2),
                         self.nested_tuples_schema_helper(3),
                         self.nested_tuples_schema_helper(32)))

        for i in (1, 2, 3, 32):
            # create tuple
            created_tuple = self.nested_tuples_creator_helper(i)

            # write tuple
            s.execute("INSERT INTO nested_tuples (k, v_%s) VALUES (%s, %s)", (i, i, created_tuple))

            # verify tuple was written and read correctly
            result = s.execute("SELECT v_%s FROM nested_tuples WHERE k=%s", (i, i)).one()
            assert created_tuple == result['v_%s' % i]
        c.shutdown()

    def test_can_insert_tuples_with_nulls(self):
        """
        Test tuples with null and empty string fields.
        """

        if self.cass_version < (2, 1, 0):
            raise unittest.SkipTest("The tuple type was introduced in Cassandra 2.1")

        s = self.session

        s.execute("CREATE TABLE tuples_nulls (k int PRIMARY KEY, t frozen<tuple<text, int, uuid, blob>>)")

        insert = s.prepare("INSERT INTO tuples_nulls (k, t) VALUES (0, ?)")
        s.execute(insert, [(None, None, None, None)])

        result = s.execute("SELECT * FROM tuples_nulls WHERE k=0")
        assert (None, None, None, None) == result.one().t

        read = s.prepare("SELECT * FROM tuples_nulls WHERE k=0")
        assert (None, None, None, None) == s.execute(read).one().t

        # also test empty strings where compatible
        s.execute(insert, [('', None, None, b'')])
        result = s.execute("SELECT * FROM tuples_nulls WHERE k=0")
        assert ('', None, None, b'') == result.one().t
        assert ('', None, None, b'') == s.execute(read).one().t

    def test_insert_collection_with_null_fails(self):
        """
        NULLs in list / sets / maps are forbidden.
        This is a regression test - there was a bug that serialized None values
        in collections as empty values instead of nulls.
        """
        s = self.session
        columns = []
        for collection_type in ['list', 'set']:
            for simple_type in PRIMITIVE_DATATYPES_KEYS:
                columns.append(f'{collection_type}_{simple_type} {collection_type}<{simple_type}>')
        for simple_type in PRIMITIVE_DATATYPES_KEYS:
            columns.append(f'map_k_{simple_type} map<{simple_type}, ascii>')
            columns.append(f'map_v_{simple_type} map<ascii, {simple_type}>')
        s.execute(f'CREATE TABLE collection_nulls (k int PRIMARY KEY, {", ".join(columns)})')

        def raises_simple_and_prepared(exc_type, query_str, args):
            self.assertRaises(exc_type, lambda: s.execute(query_str, args))
            p = s.prepare(query_str.replace('%s', '?'))
            self.assertRaises(exc_type, lambda: s.execute(p, args))

        i = 0
        for simple_type in PRIMITIVE_DATATYPES_KEYS:
            query_str = f'INSERT INTO collection_nulls (k, set_{simple_type}) VALUES (%s, %s)'
            args = [i, sortedset([None, get_sample(simple_type)])]
            raises_simple_and_prepared(InvalidRequest, query_str, args)
            i += 1
        for simple_type in PRIMITIVE_DATATYPES_KEYS:
            query_str = f'INSERT INTO collection_nulls (k, list_{simple_type}) VALUES (%s, %s)'
            args = [i, [None, get_sample(simple_type)]]
            raises_simple_and_prepared(InvalidRequest, query_str, args)
            i += 1
        for simple_type in PRIMITIVE_DATATYPES_KEYS:
            query_str = f'INSERT INTO collection_nulls (k, map_k_{simple_type}) VALUES (%s, %s)'
            args = [i, OrderedMap([(get_sample(simple_type), 'abc'), (None, 'def')])]
            raises_simple_and_prepared(InvalidRequest, query_str, args)
            i += 1
        for simple_type in PRIMITIVE_DATATYPES_KEYS:
            query_str = f'INSERT INTO collection_nulls (k, map_v_{simple_type}) VALUES (%s, %s)'
            args = [i, OrderedMap([('abc', None), ('def', get_sample(simple_type))])]
            raises_simple_and_prepared(InvalidRequest, query_str, args)
            i += 1



    def test_can_insert_unicode_query_string(self):
        """
        Test to ensure unicode strings can be used in a query
        """
        s = self.session
        s.execute(u"SELECT * FROM system.local WHERE key = 'ef\u2052ef'")
        s.execute(u"SELECT * FROM system.local WHERE key = %s", (u"fe\u2051fe",))

    @requires_composite_type
    def test_can_read_composite_type(self):
        """
        Test to ensure that CompositeTypes can be used in a query
        """
        s = self.session

        s.execute("""
            CREATE TABLE composites (
                a int PRIMARY KEY,
                b 'org.apache.cassandra.db.marshal.CompositeType(AsciiType, Int32Type)'
            )""")

        # CompositeType string literals are split on ':' chars
        s.execute("INSERT INTO composites (a, b) VALUES (0, 'abc:123')")
        result = s.execute("SELECT * FROM composites WHERE a = 0").one()
        assert 0 == result.a
        assert ('abc', 123) == result.b

        # CompositeType values can omit elements at the end
        s.execute("INSERT INTO composites (a, b) VALUES (0, 'abc')")
        result = s.execute("SELECT * FROM composites WHERE a = 0").one()
        assert 0 == result.a
        assert ('abc',) == result.b

    @notprotocolv1
    def test_special_float_cql_encoding(self):
        """
        Test to insure that Infinity -Infinity and NaN are supported by the python driver.

        @since 3.0.0
        @jira_ticket PYTHON-282
        @expected_result nan, inf and -inf can be inserted and selected correctly.

        @test_category data_types
        """
        s = self.session

        s.execute("""
            CREATE TABLE float_cql_encoding (
                f float PRIMARY KEY,
                d double
            )""")
        items = (float('nan'), float('inf'), float('-inf'))

        def verify_insert_select(ins_statement, sel_statement):
            execute_concurrent_with_args(s, ins_statement, ((f, f) for f in items))
            for f in items:
                row = s.execute(sel_statement, (f,)).one()
                if math.isnan(f):
                    assert math.isnan(row.f)
                    assert math.isnan(row.d)
                else:
                    assert row.f == f
                    assert row.d == f

        # cql encoding
        verify_insert_select('INSERT INTO float_cql_encoding (f, d) VALUES (%s, %s)',
                             'SELECT * FROM float_cql_encoding WHERE f=%s')

        s.execute("TRUNCATE float_cql_encoding")

        # prepared binding
        verify_insert_select(s.prepare('INSERT INTO float_cql_encoding (f, d) VALUES (?, ?)'),
                             s.prepare('SELECT * FROM float_cql_encoding WHERE f=?'))

    @cythontest
    def test_cython_decimal(self):
        """
        Test to validate that decimal deserialization works correctly in with our cython extensions

        @since 3.0.0
        @jira_ticket PYTHON-212
        @expected_result no exceptions are thrown, decimal is decoded correctly

        @test_category data_types serialization
        """

        self.session.execute("CREATE TABLE {0} (dc decimal PRIMARY KEY)".format(self.function_table_name))
        try:
            self.session.execute("INSERT INTO {0} (dc) VALUES (-1.08430792318105707)".format(self.function_table_name))
            results = self.session.execute("SELECT * FROM {0}".format(self.function_table_name))
            assert str(results.one().dc) == '-1.08430792318105707'
        finally:
            self.session.execute("DROP TABLE {0}".format(self.function_table_name))

    @greaterthanorequalcass3_10
    def test_smoke_duration_values(self):
        """
        Test to write several Duration values to the database and verify
        they can be read correctly. The verify than an exception is arisen
        if the value is too big

        @since 3.10
        @jira_ticket PYTHON-747
        @expected_result the read value in C* matches the written one

        @test_category data_types serialization
        """
        self.session.execute("""
            CREATE TABLE duration_smoke (k int primary key, v duration)
            """)
        self.addCleanup(self.session.execute, "DROP TABLE duration_smoke")

        prepared = self.session.prepare("""
            INSERT INTO duration_smoke (k, v)
            VALUES (?, ?)
            """)

        nanosecond_smoke_values = [0, -1, 1, 100, 1000, 1000000, 1000000000,
                        10000000000000,-9223372036854775807, 9223372036854775807,
                        int("7FFFFFFFFFFFFFFF", 16), int("-7FFFFFFFFFFFFFFF", 16)]
        month_day_smoke_values = [0, -1, 1, 100, 1000, 1000000, 1000000000,
                                  int("7FFFFFFF", 16), int("-7FFFFFFF", 16)]

        for nanosecond_value in nanosecond_smoke_values:
            for month_day_value in month_day_smoke_values:

                # Must have the same sign
                if (month_day_value <= 0) != (nanosecond_value <= 0):
                    continue

                self.session.execute(prepared, (1, Duration(month_day_value, month_day_value, nanosecond_value)))
                results = self.session.execute("SELECT * FROM duration_smoke")

                v = results.one()[1]
                assert Duration(month_day_value, month_day_value, nanosecond_value) == v, "Error encoding value {0},{0},{1}".format(month_day_value, nanosecond_value)

        self.assertRaises(ValueError, self.session.execute, prepared,
                          (1, Duration(0, 0, int("8FFFFFFFFFFFFFF0", 16))))
        self.assertRaises(ValueError, self.session.execute, prepared,
                          (1, Duration(0, int("8FFFFFFFFFFFFFF0", 16), 0)))
        self.assertRaises(ValueError, self.session.execute, prepared,
                          (1, Duration(int("8FFFFFFFFFFFFFF0", 16), 0, 0)))

class TypeTestsProtocol(BasicSharedKeyspaceUnitTestCase):

    @greaterthancass21
    @lessthancass30
    def test_nested_types_with_protocol_version(self):
        """
        Test to validate that nested type serialization works on various protocol versions. Provided
        the version of cassandra is greater the 2.1.3 we would expect to nested to types to work at all protocol versions.

        @since 3.0.0
        @jira_ticket PYTHON-215
        @expected_result no exceptions are thrown

        @test_category data_types serialization
        """
        ddl = '''CREATE TABLE {0}.t (
                k int PRIMARY KEY,
                v list<frozen<set<int>>>)'''.format(self.keyspace_name)

        self.session.execute(ddl)
        ddl = '''CREATE TABLE {0}.u (
                k int PRIMARY KEY,
                v set<frozen<list<int>>>)'''.format(self.keyspace_name)
        self.session.execute(ddl)
        ddl = '''CREATE TABLE {0}.v (
                k int PRIMARY KEY,
                v map<frozen<set<int>>, frozen<list<int>>>,
                v1 frozen<tuple<int, text>>)'''.format(self.keyspace_name)
        self.session.execute(ddl)

        self.session.execute("CREATE TYPE {0}.typ (v0 frozen<map<int, frozen<list<int>>>>, v1 frozen<list<int>>)".format(self.keyspace_name))

        ddl = '''CREATE TABLE {0}.w (
                k int PRIMARY KEY,
                v frozen<typ>)'''.format(self.keyspace_name)

        self.session.execute(ddl)

        for pvi in range(3, 5):
            self.run_inserts_at_version(pvi)
            for pvr in range(3, 5):
                self.read_inserts_at_level(pvr)

    def read_inserts_at_level(self, proto_ver):
        session = TestCluster(protocol_version=proto_ver).connect(self.keyspace_name)
        try:
            results = session.execute('select * from t').one()
            assert "[SortedSet([1, 2]), SortedSet([3, 5])]" == str(results.v)

            results = session.execute('select * from u').one()
            assert "SortedSet([[1, 2], [3, 5]])" == str(results.v)

            results = session.execute('select * from v').one()
            assert "{SortedSet([1, 2]): [1, 2, 3], SortedSet([3, 5]): [4, 5, 6]}" == str(results.v)

            results = session.execute('select * from w').one()
            assert "typ(v0=OrderedMapSerializedKey([(1, [1, 2, 3]), (2, [4, 5, 6])]), v1=[7, 8, 9])" == str(results.v)

        finally:
            session.cluster.shutdown()

    def run_inserts_at_version(self, proto_ver):
        session = TestCluster(protocol_version=proto_ver).connect(self.keyspace_name)
        try:
            p = session.prepare('insert into t (k, v) values (?, ?)')
            session.execute(p, (0, [{1, 2}, {3, 5}]))

            p = session.prepare('insert into u (k, v) values (?, ?)')
            session.execute(p, (0, {(1, 2), (3, 5)}))

            p = session.prepare('insert into v (k, v, v1) values (?, ?, ?)')
            session.execute(p, (0, {(1, 2): [1, 2, 3], (3, 5): [4, 5, 6]}, (123, 'four')))

            p = session.prepare('insert into w (k, v) values (?, ?)')
            session.execute(p, (0, ({1: [1, 2, 3], 2: [4, 5, 6]}, [7, 8, 9])))

        finally:
            session.cluster.shutdown()

@greaterthanorequalcass50
class TypeTestsVector(BasicSharedKeyspaceUnitTestCase):

    def _get_first_j(self, rs):
        rows = rs.all()
        assert len(rows) == 1
        return rows[0].j

    def _get_row_simple(self, idx, table_name):
        rs = self.session.execute("select j from {0}.{1} where i = {2}".format(self.keyspace_name, table_name, idx))
        return self._get_first_j(rs)

    def _get_row_prepared(self, idx, table_name):
        cql = "select j from {0}.{1} where i = ?".format(self.keyspace_name, table_name)
        ps = self.session.prepare(cql)
        rs = self.session.execute(ps, [idx])
        return self._get_first_j(rs)

    def _round_trip_test(self, subtype, subtype_fn, test_fn, use_positional_parameters=True):

        table_name = subtype.replace("<","A").replace(">", "B").replace(",", "C") + "isH"

        def random_subtype_vector():
            return [subtype_fn() for _ in range(3)]

        ddl = """CREATE TABLE {0}.{1} (
                    i int PRIMARY KEY,
                    j vector<{2}, 3>)""".format(self.keyspace_name, table_name, subtype)
        self.session.execute(ddl)

        if use_positional_parameters:
            cql = "insert into {0}.{1} (i,j) values (%s,%s)".format(self.keyspace_name, table_name)
            expected1 = random_subtype_vector()
            data1 = {1:random_subtype_vector(), 2:expected1, 3:random_subtype_vector()}
            for k,v in data1.items():
                # Attempt a set of inserts using the driver's support for positional params
                self.session.execute(cql, (k,v))

        cql = "insert into {0}.{1} (i,j) values (?,?)".format(self.keyspace_name, table_name)
        expected2 = random_subtype_vector()
        ps = self.session.prepare(cql)
        data2 = {4:random_subtype_vector(), 5:expected2, 6:random_subtype_vector()}
        for k,v in data2.items():
            # Add some additional rows via prepared statements
            self.session.execute(ps, [k,v])

        # Use prepared queries to gather data from the rows we added via simple queries and vice versa
        if use_positional_parameters:
            observed1 = self._get_row_prepared(2, table_name)
            for idx in range(0, 3):
                test_fn(observed1[idx], expected1[idx])

        observed2 = self._get_row_simple(5, table_name)
        for idx in range(0, 3):
            test_fn(observed2[idx], expected2[idx])

    def test_round_trip_integers(self):
        self._round_trip_test("int", partial(random.randint, 0, 2 ** 31), self.assertEqual)
        self._round_trip_test("bigint", partial(random.randint, 0, 2 ** 63), self.assertEqual)
        self._round_trip_test("smallint", partial(random.randint, 0, 2 ** 15), self.assertEqual)
        self._round_trip_test("tinyint", partial(random.randint, 0, (2 ** 7) - 1), self.assertEqual)
        self._round_trip_test("varint", partial(random.randint, 0, 2 ** 63), self.assertEqual)

    def test_round_trip_floating_point(self):
        _almost_equal_test_fn = partial(self.assertAlmostEqual, places=5)
        def _random_decimal():
            return Decimal(random.uniform(0.0, 100.0))

        # Max value here isn't really connected to max value for floating point nums in IEEE 754... it's used here
        # mainly as a convenient benchmark
        self._round_trip_test("float", partial(random.uniform, 0.0, 100.0), _almost_equal_test_fn)
        self._round_trip_test("double", partial(random.uniform, 0.0, 100.0), _almost_equal_test_fn)
        self._round_trip_test("decimal", _random_decimal, _almost_equal_test_fn)

    def test_round_trip_text(self):
        def _random_string():
            return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(24))

        self._round_trip_test("ascii", _random_string, self.assertEqual)
        self._round_trip_test("text", _random_string, self.assertEqual)

    def test_round_trip_date_and_time(self):
        _almost_equal_test_fn = partial(self.assertAlmostEqual, delta=timedelta(seconds=1))
        def _random_datetime():
            return datetime.today() - timedelta(hours=random.randint(0,18), days=random.randint(1,1000))
        def _random_date():
            return _random_datetime().date()
        def _random_time():
            return _random_datetime().time()

        self._round_trip_test("date", _random_date, self.assertEqual)
        self._round_trip_test("time", _random_time, self.assertEqual)
        self._round_trip_test("timestamp", _random_datetime, _almost_equal_test_fn)

    def test_round_trip_uuid(self):
        self._round_trip_test("uuid", uuid.uuid1, self.assertEqual)
        self._round_trip_test("timeuuid", uuid.uuid1, self.assertEqual)

    def test_round_trip_miscellany(self):
        def _random_bytes():
            return random.getrandbits(32).to_bytes(4,'big')
        def _random_boolean():
            return random.choice([True, False])
        def _random_duration():
            return Duration(random.randint(0,11), random.randint(0,11), random.randint(0,10000))
        def _random_inet():
            return socket.inet_ntoa(_random_bytes())

        self._round_trip_test("boolean", _random_boolean, self.assertEqual)
        self._round_trip_test("duration", _random_duration, self.assertEqual)
        self._round_trip_test("inet", _random_inet, self.assertEqual)
        self._round_trip_test("blob", _random_bytes, self.assertEqual)

    def test_round_trip_collections(self):
        def _random_seq():
            return [random.randint(0,100000) for _ in range(8)]
        def _random_set():
            return set(_random_seq())
        def _random_map():
            return {k:v for (k,v) in zip(_random_seq(), _random_seq())}

        # Goal here is to test collections of both fixed and variable size subtypes
        self._round_trip_test("list<int>", _random_seq, self.assertEqual)
        self._round_trip_test("list<varint>", _random_seq, self.assertEqual)
        self._round_trip_test("set<int>", _random_set, self.assertEqual)
        self._round_trip_test("set<varint>", _random_set, self.assertEqual)
        self._round_trip_test("map<int,int>", _random_map, self.assertEqual)
        self._round_trip_test("map<int,varint>", _random_map, self.assertEqual)
        self._round_trip_test("map<varint,int>", _random_map, self.assertEqual)
        self._round_trip_test("map<varint,varint>", _random_map, self.assertEqual)

    def test_round_trip_vector_of_vectors(self):
        def _random_vector():
            return [random.randint(0,100000) for _ in range(2)]

        self._round_trip_test("vector<int,2>", _random_vector, self.assertEqual)
        self._round_trip_test("vector<varint,2>", _random_vector, self.assertEqual)

    def test_round_trip_tuples(self):
        def _random_tuple():
            return (random.randint(0,100000),random.randint(0,100000))

        # Unfortunately we can't use positional parameters when inserting tuples because the driver will try to encode
        # them as lists before sending them to the server... and that confuses the parsing logic.
        self._round_trip_test("tuple<int,int>", _random_tuple, self.assertEqual, use_positional_parameters=False)
        self._round_trip_test("tuple<int,varint>", _random_tuple, self.assertEqual, use_positional_parameters=False)
        self._round_trip_test("tuple<varint,int>", _random_tuple, self.assertEqual, use_positional_parameters=False)
        self._round_trip_test("tuple<varint,varint>", _random_tuple, self.assertEqual, use_positional_parameters=False)

    def test_round_trip_udts(self):
        def _udt_equal_test_fn(udt1, udt2):
            assert udt1.a == udt2.a
            assert udt1.b == udt2.b

        self.session.execute("create type {}.fixed_type (a int, b int)".format(self.keyspace_name))
        self.session.execute("create type {}.mixed_type_one (a int, b varint)".format(self.keyspace_name))
        self.session.execute("create type {}.mixed_type_two (a varint, b int)".format(self.keyspace_name))
        self.session.execute("create type {}.var_type (a varint, b varint)".format(self.keyspace_name))

        class GeneralUDT:
            def __init__(self, a, b):
                self.a = a
                self.b = b

        self.cluster.register_user_type(self.keyspace_name,'fixed_type', GeneralUDT)
        self.cluster.register_user_type(self.keyspace_name,'mixed_type_one', GeneralUDT)
        self.cluster.register_user_type(self.keyspace_name,'mixed_type_two', GeneralUDT)
        self.cluster.register_user_type(self.keyspace_name,'var_type', GeneralUDT)

        def _random_udt():
            return GeneralUDT(random.randint(0,100000),random.randint(0,100000))

        self._round_trip_test("fixed_type", _random_udt, _udt_equal_test_fn)
        self._round_trip_test("mixed_type_one", _random_udt, _udt_equal_test_fn)
        self._round_trip_test("mixed_type_two", _random_udt, _udt_equal_test_fn)
        self._round_trip_test("var_type", _random_udt, _udt_equal_test_fn)
