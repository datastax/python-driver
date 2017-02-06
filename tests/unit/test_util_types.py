# Copyright 2013-2016 DataStax, Inc.
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

import datetime

from cassandra.util import Date, Time, Duration


class DateTests(unittest.TestCase):

    def test_from_datetime(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        self.assertEqual(str(d), str(expected_date))

    def test_from_string(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        sd = Date('1492-10-12')
        self.assertEqual(sd, d)
        sd = Date('+1492-10-12')
        self.assertEqual(sd, d)

    def test_from_date(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        self.assertEqual(d.date(), expected_date)

    def test_from_days(self):
        sd = Date(0)
        self.assertEqual(sd, Date(datetime.date(1970, 1, 1)))
        sd = Date(-1)
        self.assertEqual(sd, Date(datetime.date(1969, 12, 31)))
        sd = Date(1)
        self.assertEqual(sd, Date(datetime.date(1970, 1, 2)))

    def test_limits(self):
        min_builtin = Date(datetime.date(1, 1, 1))
        max_builtin = Date(datetime.date(9999, 12, 31))
        self.assertEqual(Date(min_builtin.days_from_epoch), min_builtin)
        self.assertEqual(Date(max_builtin.days_from_epoch), max_builtin)
        # just proving we can construct with on offset outside buildin range
        self.assertEqual(Date(min_builtin.days_from_epoch - 1).days_from_epoch,
                         min_builtin.days_from_epoch - 1)
        self.assertEqual(Date(max_builtin.days_from_epoch + 1).days_from_epoch,
                         max_builtin.days_from_epoch + 1)

    def test_invalid_init(self):
        self.assertRaises(ValueError, Date, '-1999-10-10')
        self.assertRaises(TypeError, Date, 1.234)

    def test_str(self):
        date_str = '2015-03-16'
        self.assertEqual(str(Date(date_str)), date_str)

    def test_out_of_range(self):
        self.assertEqual(str(Date(2932897)), '2932897')
        self.assertEqual(repr(Date(1)), 'Date(1)')

    def test_equals(self):
        self.assertEqual(Date(1234), 1234)
        self.assertEqual(Date(1), datetime.date(1970, 1, 2))
        self.assertFalse(Date(2932897) == datetime.date(9999, 12, 31))  # date can't represent year > 9999
        self.assertEqual(Date(2932897), 2932897)


class TimeTests(unittest.TestCase):

    def test_units_from_string(self):
        one_micro = 1000
        one_milli = 1000 * one_micro
        one_second = 1000 * one_milli
        one_minute = 60 * one_second
        one_hour = 60 * one_minute

        tt = Time('00:00:00.000000001')
        self.assertEqual(tt.nanosecond_time, 1)
        tt = Time('00:00:00.000001')
        self.assertEqual(tt.nanosecond_time, one_micro)
        tt = Time('00:00:00.001')
        self.assertEqual(tt.nanosecond_time, one_milli)
        tt = Time('00:00:01')
        self.assertEqual(tt.nanosecond_time, one_second)
        tt = Time('00:01:00')
        self.assertEqual(tt.nanosecond_time, one_minute)
        tt = Time('01:00:00')
        self.assertEqual(tt.nanosecond_time, one_hour)
        tt = Time('01:00:00.')
        self.assertEqual(tt.nanosecond_time, one_hour)

        tt = Time('23:59:59.123456')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro)

        tt = Time('23:59:59.1234567')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 700)

        tt = Time('23:59:59.12345678')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 780)

        tt = Time('23:59:59.123456789')
        self.assertEqual(tt.nanosecond_time, 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 789)

    def test_micro_precision(self):
        Time('23:59:59.1')
        Time('23:59:59.12')
        Time('23:59:59.123')
        Time('23:59:59.1234')
        Time('23:59:59.12345')

    def test_from_int(self):
        tt = Time(12345678)
        self.assertEqual(tt.nanosecond_time, 12345678)

    def test_from_time(self):
        expected_time = datetime.time(12, 1, 2, 3)
        tt = Time(expected_time)
        self.assertEqual(tt, expected_time)

    def test_as_time(self):
        expected_time = datetime.time(12, 1, 2, 3)
        tt = Time(expected_time)
        self.assertEqual(tt.time(), expected_time)

    def test_equals(self):
        # util.Time self equality
        self.assertEqual(Time(1234), Time(1234))

    def test_str_repr(self):
        time_str = '12:13:14.123456789'
        self.assertEqual(str(Time(time_str)), time_str)
        self.assertEqual(repr(Time(1)), 'Time(1)')

    def test_invalid_init(self):
        self.assertRaises(ValueError, Time, '1999-10-10 11:11:11.1234')
        self.assertRaises(TypeError, Time, 1.234)
        self.assertRaises(ValueError, Time, 123456789000000)
        self.assertRaises(TypeError, Time, datetime.datetime(2004, 12, 23, 11, 11, 1))


class DurationTests(unittest.TestCase):

    def test_valid_format(self):

        valid = Duration(1, 1, 1)
        self.assertEqual(valid.months, 1)
        self.assertEqual(valid.days, 1)
        self.assertEqual(valid.nanoseconds, 1)

        valid = Duration(nanoseconds=100000)
        self.assertEqual(valid.months, 0)
        self.assertEqual(valid.days, 0)
        self.assertEqual(valid.nanoseconds, 100000)

        valid = Duration()
        self.assertEqual(valid.months, 0)
        self.assertEqual(valid.days, 0)
        self.assertEqual(valid.nanoseconds, 0)

        valid = Duration(-10, -21, -1000)
        self.assertEqual(valid.months, -10)
        self.assertEqual(valid.days, -21)
        self.assertEqual(valid.nanoseconds, -1000)

    def test_equality(self):

        first = Duration(1, 1, 1)
        second = Duration(-1, 1, 1)
        self.assertNotEqual(first, second)

        first = Duration(1, 1, 1)
        second = Duration(1, 1, 1)
        self.assertEqual(first, second)

        first = Duration()
        second = Duration(0, 0, 0)
        self.assertEqual(first, second)

        first = Duration(1000, 10000, 2345345)
        second = Duration(1000, 10000, 2345345)
        self.assertEqual(first, second)

        first = Duration(12, 0 , 100)
        second = Duration(nanoseconds=100, months=12)
        self.assertEqual(first, second)

    def test_str(self):

        self.assertEqual(str(Duration(1, 1, 1)), "1mo1d1ns")
        self.assertEqual(str(Duration(1, 1, -1)), "-1mo1d1ns")
        self.assertEqual(str(Duration(1, 1, 1000000000000000)), "1mo1d1000000000000000ns")
        self.assertEqual(str(Duration(52, 23, 564564)), "52mo23d564564ns")



