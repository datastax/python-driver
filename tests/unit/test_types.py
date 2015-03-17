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
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from binascii import unhexlify
import datetime
import tempfile
import six
import time

import cassandra
from cassandra.cqltypes import (BooleanType, lookup_casstype_simple, lookup_casstype,
                                LongType, DecimalType, SetType, cql_typename,
                                CassandraType, UTF8Type, parse_casstype_args,
                                SimpleDateType, TimeType,
                                EmptyValue, _CassandraType, DateType, int64_pack)
from cassandra.encoder import cql_quote
from cassandra.protocol import (write_string, read_longstring, write_stringmap,
                                read_stringmap, read_inet, write_inet,
                                read_string, write_longstring)
from cassandra.query import named_tuple_factory


class TypeTests(unittest.TestCase):

    def test_lookup_casstype_simple(self):
        """
        Ensure lookup_casstype_simple returns the correct classes
        """

        self.assertEqual(lookup_casstype_simple('AsciiType'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype_simple('LongType'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_casstype_simple('BytesType'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype_simple('BooleanType'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype_simple('CounterColumnType'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype_simple('DecimalType'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype_simple('DoubleType'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype_simple('FloatType'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype_simple('InetAddressType'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype_simple('Int32Type'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype_simple('UTF8Type'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype_simple('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype_simple('SimpleDateType'), cassandra.cqltypes.SimpleDateType)
        self.assertEqual(lookup_casstype_simple('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype_simple('TimeType'), cassandra.cqltypes.TimeType)
        self.assertEqual(lookup_casstype_simple('UUIDType'), cassandra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype_simple('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype_simple('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype_simple('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype_simple('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype_simple('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype_simple('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype_simple('ReversedType'), cassandra.cqltypes.ReversedType)

        self.assertEqual(str(lookup_casstype_simple('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

    def test_lookup_casstype(self):
        """
        Ensure lookup_casstype returns the correct classes
        """

        self.assertEqual(lookup_casstype('AsciiType'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype('LongType'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_casstype('BytesType'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype('BooleanType'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype('CounterColumnType'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('DecimalType'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype('DoubleType'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype('FloatType'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype('InetAddressType'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype('Int32Type'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype('UTF8Type'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('TimeType'), cassandra.cqltypes.TimeType)
        self.assertEqual(lookup_casstype('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype('UUIDType'), cassandra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype('ReversedType'), cassandra.cqltypes.ReversedType)

        self.assertEqual(str(lookup_casstype('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

        self.assertRaises(ValueError, lookup_casstype, 'AsciiType~')

        # TODO: Do a few more tests
        # "I would say some parameterized and nested types would be good to test,
        # like "MapType(AsciiType, IntegerType)" and "ReversedType(AsciiType)"
        self.assertEqual(str(lookup_casstype(BooleanType(True))), str(BooleanType(True)))

    def test_casstype_parameterized(self):
        self.assertEqual(LongType.cass_parameterized_type_with(()), 'LongType')
        self.assertEqual(LongType.cass_parameterized_type_with((), full=True), 'org.apache.cassandra.db.marshal.LongType')
        self.assertEqual(SetType.cass_parameterized_type_with([DecimalType], full=True), 'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.DecimalType)')

        self.assertEqual(LongType.cql_parameterized_type(), 'bigint')

        subtypes = (cassandra.cqltypes.UTF8Type, cassandra.cqltypes.UTF8Type)
        self.assertEqual('map<text, text>',
                         cassandra.cqltypes.MapType.apply_parameters(subtypes).cql_parameterized_type())

    def test_datetype_from_string(self):
        # Ensure all formats can be parsed, without exception
        for format in cassandra.cqltypes.cql_timestamp_formats:
            date_string = str(datetime.datetime.now().strftime(format))
            cassandra.cqltypes.DateType(date_string)

    def test_simpledate(self):
        """
        Test cassandra.cqltypes.SimpleDateType() construction
        """
        Date = cassandra.util.Date
        # from datetime
        expected_dt = datetime.datetime(1492, 10, 12, 1, 1)
        expected_date = Date(expected_dt)
        self.assertEqual(str(expected_date), '1492-10-12')

        # from string
        sd = SimpleDateType('1492-10-12')
        self.assertEqual(sd.val, expected_date)
        sd = SimpleDateType('+1492-10-12')
        self.assertEqual(sd.val, expected_date)

        # Date
        sd = SimpleDateType(expected_date)
        self.assertEqual(sd.val, expected_date)

        # date
        sd = SimpleDateType(datetime.date(expected_dt.year, expected_dt.month, expected_dt.day))
        self.assertEqual(sd.val, expected_date)

        # days
        sd = SimpleDateType(0)
        self.assertEqual(sd.val, Date(datetime.date(1970, 1, 1)))
        sd = SimpleDateType(-1)
        self.assertEqual(sd.val, Date(datetime.date(1969, 12, 31)))
        sd = SimpleDateType(1)
        self.assertEqual(sd.val, Date(datetime.date(1970, 1, 2)))
        # limits
        min_builtin = Date(datetime.date(1, 1, 1))
        max_builtin = Date(datetime.date(9999, 12, 31))
        self.assertEqual(SimpleDateType(min_builtin.days_from_epoch).val, min_builtin)
        self.assertEqual(SimpleDateType(max_builtin.days_from_epoch).val, max_builtin)
        # just proving we can construct with on offset outside buildin range
        self.assertEqual(SimpleDateType(min_builtin.days_from_epoch - 1).val.days_from_epoch,
                         min_builtin.days_from_epoch - 1)
        self.assertEqual(SimpleDateType(max_builtin.days_from_epoch + 1).val.days_from_epoch,
                         max_builtin.days_from_epoch + 1)

        # no contruct
        self.assertRaises(ValueError, SimpleDateType, '-1999-10-10')
        self.assertRaises(TypeError, SimpleDateType, 1.234)

        # str
        date_str = '2015-03-16'
        self.assertEqual(str(Date(date_str)), date_str)
        # out of range
        self.assertEqual(str(Date(2932897)), '2932897')
        self.assertEqual(repr(Date(1)), 'Date(1)')

        # eq other types
        self.assertEqual(Date(1234), 1234)
        self.assertEqual(Date(1), datetime.date(1970, 1, 2))
        self.assertFalse(Date(2932897) == datetime.date(9999, 12, 31))  # date can't represent year > 9999
        self.assertEqual(Date(2932897), 2932897)

    def test_time(self):
        """
        Test cassandra.cqltypes.TimeType() construction
        """
        Time = cassandra.util.Time
        one_micro = 1000
        one_milli = 1000 * one_micro
        one_second = 1000 * one_milli
        one_minute = 60 * one_second
        one_hour = 60 * one_minute

        # from strings
        tt = TimeType('00:00:00.000000001')
        self.assertEqual(tt.val, 1)
        tt = TimeType('00:00:00.000001')
        self.assertEqual(tt.val, one_micro)
        tt = TimeType('00:00:00.001')
        self.assertEqual(tt.val, one_milli)
        tt = TimeType('00:00:01')
        self.assertEqual(tt.val, one_second)
        tt = TimeType('00:01:00')
        self.assertEqual(tt.val, one_minute)
        tt = TimeType('01:00:00')
        self.assertEqual(tt.val, one_hour)
        tt = TimeType('01:00:00.')
        self.assertEqual(tt.val, one_hour)

        tt = TimeType('23:59:59.1')
        tt = TimeType('23:59:59.12')
        tt = TimeType('23:59:59.123')
        tt = TimeType('23:59:59.1234')
        tt = TimeType('23:59:59.12345')

        tt = TimeType('23:59:59.123456')
        self.assertEqual(tt.val, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro)

        tt = TimeType('23:59:59.1234567')
        self.assertEqual(tt.val, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 700)

        tt = TimeType('23:59:59.12345678')
        self.assertEqual(tt.val, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 780)

        tt = TimeType('23:59:59.123456789')
        self.assertEqual(tt.val, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 789)

        # from int
        tt = TimeType(12345678)
        self.assertEqual(tt.val, 12345678)

        # from time
        expected_time = datetime.time(12, 1, 2, 3)
        tt = TimeType(expected_time)
        self.assertEqual(tt.val, expected_time)

        # util.Time self equality
        self.assertEqual(Time(1234), Time(1234))

        # str
        time_str = '12:13:14.123456789'
        self.assertEqual(str(Time(time_str)), time_str)
        self.assertEqual(repr(Time(1)), 'Time(1)')

        # no construct
        self.assertRaises(ValueError, TimeType, '1999-10-10 11:11:11.1234')
        self.assertRaises(TypeError, TimeType, 1.234)
        self.assertRaises(ValueError, TimeType, 123456789000000)
        self.assertRaises(TypeError, TimeType, datetime.datetime(2004, 12, 23, 11, 11, 1))

    def test_cql_typename(self):
        """
        Smoke test cql_typename
        """

        self.assertEqual(cql_typename('DateType'), 'timestamp')
        self.assertEqual(cql_typename('org.apache.cassandra.db.marshal.ListType(IntegerType)'), 'list<varint>')

    def test_named_tuple_colname_substitution(self):
        colnames = ("func(abc)", "[applied]", "func(func(abc))", "foo_bar", "foo_bar_")
        rows = [(1, 2, 3, 4, 5)]
        result = named_tuple_factory(colnames, rows)[0]
        self.assertEqual(result[0], result.func_abc)
        self.assertEqual(result[1], result.applied)
        self.assertEqual(result[2], result.func_func_abc)
        self.assertEqual(result[3], result.foo_bar)
        self.assertEqual(result[4], result.foo_bar_)

    def test_parse_casstype_args(self):
        class FooType(CassandraType):
            typename = 'org.apache.cassandra.db.marshal.FooType'

            def __init__(self, subtypes, names):
                self.subtypes = subtypes
                self.names = names

            @classmethod
            def apply_parameters(cls, subtypes, names):
                return cls(subtypes, [unhexlify(six.b(name)) if name is not None else name for name in names])

        class BarType(FooType):
            typename = 'org.apache.cassandra.db.marshal.BarType'

        ctype = parse_casstype_args(''.join((
            'org.apache.cassandra.db.marshal.FooType(',
                '63697479:org.apache.cassandra.db.marshal.UTF8Type,',
                'BarType(61646472657373:org.apache.cassandra.db.marshal.UTF8Type),',
                '7a6970:org.apache.cassandra.db.marshal.UTF8Type',
            ')')))

        self.assertEqual(FooType, ctype.__class__)

        self.assertEqual(UTF8Type, ctype.subtypes[0])

        # middle subtype should be a BarType instance with its own subtypes and names
        self.assertIsInstance(ctype.subtypes[1], BarType)
        self.assertEqual([UTF8Type], ctype.subtypes[1].subtypes)
        self.assertEqual([b"address"], ctype.subtypes[1].names)

        self.assertEqual(UTF8Type, ctype.subtypes[2])
        self.assertEqual([b'city', None, b'zip'], ctype.names)

    def test_empty_value(self):
        self.assertEqual(str(EmptyValue()), 'EMPTY')

    def test_cassandratype_base(self):
        cassandra_type = _CassandraType('randomvaluetocheck')
        self.assertEqual(cassandra_type.val, 'randomvaluetocheck')
        self.assertEqual(cassandra_type.validate('randomvaluetocheck2'), 'randomvaluetocheck2')
        self.assertEqual(cassandra_type.val, 'randomvaluetocheck')

    def test_datetype(self):
        now_time_seconds = time.time()
        now_datetime = datetime.datetime.utcfromtimestamp(now_time_seconds)

        # Cassandra timestamps in millis
        now_timestamp = now_time_seconds * 1e3

        # same results serialized
        self.assertEqual(DateType.serialize(now_datetime, 0), DateType.serialize(now_timestamp, 0))

        # from timestamp
        date_type = DateType(now_timestamp)
        self.assertEqual(date_type.my_timestamp(), now_timestamp)

        # from datetime object
        date_type = DateType(now_datetime)
        self.assertEqual(date_type.my_timestamp(), now_datetime)

        # deserialize
        # epoc
        expected = 0
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime.utcfromtimestamp(expected))

        # beyond 32b
        expected = 2 ** 33
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime.utcfromtimestamp(expected))

        # less than epoc (PYTHON-119)
        expected = -770172256
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(1945, 8, 5, 23, 15, 44))

        self.assertRaises(ValueError, date_type.interpret_datestring, 'fakestring')

        # work around rounding difference among Python versions (PYTHON-230)
        expected = 1424817268.274
        self.assertEqual(DateType.deserialize(int64_pack(int(1000 * expected)), 0), datetime.datetime(2015, 2, 24, 22, 34, 28, 274000))

    def test_write_read_string(self):
        with tempfile.TemporaryFile() as f:
            value = u'test'
            write_string(f, value)
            f.seek(0)
            self.assertEqual(read_string(f), value)

    def test_write_read_longstring(self):
        with tempfile.TemporaryFile() as f:
            value = u'test'
            write_longstring(f, value)
            f.seek(0)
            self.assertEqual(read_longstring(f), value)

    def test_write_read_stringmap(self):
        with tempfile.TemporaryFile() as f:
            value = {'key': 'value'}
            write_stringmap(f, value)
            f.seek(0)
            self.assertEqual(read_stringmap(f), value)

    def test_write_read_inet(self):
        with tempfile.TemporaryFile() as f:
            value = ('192.168.1.1', 9042)
            write_inet(f, value)
            f.seek(0)
            self.assertEqual(read_inet(f), value)

        with tempfile.TemporaryFile() as f:
            value = ('2001:db8:0:f101::1', 9042)
            write_inet(f, value)
            f.seek(0)
            self.assertEqual(read_inet(f), value)

    def test_cql_quote(self):
        self.assertEqual(cql_quote(u'test'), "'test'")
        self.assertEqual(cql_quote('test'), "'test'")
        self.assertEqual(cql_quote(0), '0')
