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
import datetime
import tempfile
import six
import time

import cassandra
from cassandra.cqltypes import (BooleanType, lookup_casstype_simple, lookup_casstype,
                                LongType, DecimalType, SetType, cql_typename,
                                CassandraType, UTF8Type, parse_casstype_args,
                                SimpleDateType, TimeType, ByteType, ShortType,
                                EmptyValue, _CassandraType, DateType, int64_pack)
from cassandra.encoder import cql_quote
from cassandra.protocol import (write_string, read_longstring, write_stringmap,
                                read_stringmap, read_inet, write_inet,
                                read_string, write_longstring)
from cassandra.query import named_tuple_factory
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy, ConvictionPolicy
from cassandra.util import Date, Time
from cassandra.metadata import Token


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
        self.assertEqual(lookup_casstype_simple('ByteType'), cassandra.cqltypes.ByteType)
        self.assertEqual(lookup_casstype_simple('ShortType'), cassandra.cqltypes.ShortType)
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
        self.assertEqual(lookup_casstype_simple('DurationType'), cassandra.cqltypes.DurationType)

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
        self.assertEqual(lookup_casstype('ByteType'), cassandra.cqltypes.ByteType)
        self.assertEqual(lookup_casstype('ShortType'), cassandra.cqltypes.ShortType)
        self.assertEqual(lookup_casstype('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype('UUIDType'), cassandra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype('ReversedType'), cassandra.cqltypes.ReversedType)
        self.assertEqual(lookup_casstype('DurationType'), cassandra.cqltypes.DurationType)

        self.assertEqual(str(lookup_casstype('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

        self.assertRaises(ValueError, lookup_casstype, 'AsciiType~')

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
            cassandra.cqltypes.DateType.interpret_datestring(date_string)

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

    def test_datetype(self):
        now_time_seconds = time.time()
        now_datetime = datetime.datetime.utcfromtimestamp(now_time_seconds)

        # Cassandra timestamps in millis
        now_timestamp = now_time_seconds * 1e3

        # same results serialized
        self.assertEqual(DateType.serialize(now_datetime, 0), DateType.serialize(now_timestamp, 0))

        # deserialize
        # epoc
        expected = 0
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime.utcfromtimestamp(expected))

        # beyond 32b
        expected = 2 ** 33
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(2242, 3, 16, 12, 56, 32))

        # less than epoc (PYTHON-119)
        expected = -770172256
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(1945, 8, 5, 23, 15, 44))

        # work around rounding difference among Python versions (PYTHON-230)
        expected = 1424817268.274
        self.assertEqual(DateType.deserialize(int64_pack(int(1000 * expected)), 0), datetime.datetime(2015, 2, 24, 22, 34, 28, 274000))

        # Large date overflow (PYTHON-452)
        expected = 2177403010.123
        self.assertEqual(DateType.deserialize(int64_pack(int(1000 * expected)), 0), datetime.datetime(2038, 12, 31, 10, 10, 10, 123000))

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


class TestOrdering(unittest.TestCase):
    def _check_order_consistency(self, smaller, bigger, equal=False):
        self.assertLessEqual(smaller, bigger)
        self.assertGreaterEqual(bigger, smaller)
        if equal:
            self.assertEqual(smaller, bigger)
        else:
            self.assertNotEqual(smaller, bigger)
            self.assertLess(smaller, bigger)
            self.assertGreater(bigger, smaller)

    def _shuffle_lists(self, *args):
        return [item for sublist in zip(*args) for item in sublist]

    def _check_sequence_consistency(self, ordered_sequence, equal=False):
        for i, el in enumerate(ordered_sequence):
            for previous in ordered_sequence[:i]:
                self._check_order_consistency(previous, el, equal)
            for posterior in ordered_sequence[i + 1:]:
                self._check_order_consistency(el, posterior, equal)

    def test_host_order(self):
        """
        Test Host class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the hosts are ordered correctly

        @test_category data_types
        """
        hosts = [Host(addr, SimpleConvictionPolicy) for addr in
                 ("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4")]
        hosts_equal = [Host(addr, SimpleConvictionPolicy) for addr in
                       ("127.0.0.1", "127.0.0.1")]
        hosts_equal_conviction = [Host("127.0.0.1", SimpleConvictionPolicy), Host("127.0.0.1", ConvictionPolicy)]
        self._check_sequence_consistency(hosts)
        self._check_sequence_consistency(hosts_equal, equal=True)
        self._check_sequence_consistency(hosts_equal_conviction, equal=True)

    def test_date_order(self):
        """
        Test Date class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the dates are ordered correctly

        @test_category data_types
        """
        dates_from_string = [Date("2017-01-01"), Date("2017-01-05"), Date("2017-01-09"), Date("2017-01-13")]
        dates_from_string_equal = [Date("2017-01-01"), Date("2017-01-01")]
        self._check_sequence_consistency(dates_from_string)
        self._check_sequence_consistency(dates_from_string_equal, equal=True)

        date_format = "%Y-%m-%d"

        dates_from_value = [
            Date((datetime.datetime.strptime(dtstr, date_format) -
                  datetime.datetime(1970, 1, 1)).days)
            for dtstr in ("2017-01-02", "2017-01-06", "2017-01-10", "2017-01-14")
        ]
        dates_from_value_equal = [Date(1), Date(1)]
        self._check_sequence_consistency(dates_from_value)
        self._check_sequence_consistency(dates_from_value_equal, equal=True)

        dates_from_datetime = [Date(datetime.datetime.strptime(dtstr, date_format))
                               for dtstr in ("2017-01-03", "2017-01-07", "2017-01-11", "2017-01-15")]
        dates_from_datetime_equal = [Date(datetime.datetime.strptime("2017-01-01", date_format)),
                               Date(datetime.datetime.strptime("2017-01-01", date_format))]
        self._check_sequence_consistency(dates_from_datetime)
        self._check_sequence_consistency(dates_from_datetime_equal, equal=True)

        dates_from_date = [
            Date(datetime.datetime.strptime(dtstr, date_format).date()) for dtstr in
            ("2017-01-04", "2017-01-08", "2017-01-12", "2017-01-16")
        ]
        dates_from_date_equal = [datetime.datetime.strptime(dtstr, date_format) for dtstr in
                                 ("2017-01-09", "2017-01-9")]

        self._check_sequence_consistency(dates_from_date)
        self._check_sequence_consistency(dates_from_date_equal, equal=True)

        self._check_sequence_consistency(self._shuffle_lists(dates_from_string, dates_from_value,
                                                             dates_from_datetime, dates_from_date))

    def test_timer_order(self):
        """
        Test Time class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the times are ordered correctly

        @test_category data_types
        """
        time_from_int = [Time(1000), Time(4000), Time(7000), Time(10000)]
        time_from_int_equal = [Time(1), Time(1)]
        self._check_sequence_consistency(time_from_int)
        self._check_sequence_consistency(time_from_int_equal, equal=True)

        time_from_datetime = [Time(datetime.time(hour=0, minute=0, second=0, microsecond=us))
                              for us in (2, 5, 8, 11)]
        time_from_datetime_equal = [Time(datetime.time(hour=0, minute=0, second=0, microsecond=us))
                                    for us in (1, 1)]
        self._check_sequence_consistency(time_from_datetime)
        self._check_sequence_consistency(time_from_datetime_equal, equal=True)

        time_from_string = [Time("00:00:00.000003000"), Time("00:00:00.000006000"),
                            Time("00:00:00.000009000"), Time("00:00:00.000012000")]
        time_from_string_equal = [Time("00:00:00.000004000"), Time("00:00:00.000004000")]
        self._check_sequence_consistency(time_from_string)
        self._check_sequence_consistency(time_from_string_equal, equal=True)

        self._check_sequence_consistency(self._shuffle_lists(time_from_int, time_from_datetime, time_from_string))

    def test_token_order(self):
        """
        Test Token class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the tokens are ordered correctly

        @test_category data_types
        """
        tokens = [Token(1), Token(2), Token(3), Token(4)]
        tokens_equal = [Token(1), Token(1)]
        self._check_sequence_consistency(tokens)
        self._check_sequence_consistency(tokens_equal, equal=True)
