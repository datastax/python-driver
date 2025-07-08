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

import datetime

from cassandra.util import Date, Time, Duration, Version, maybe_add_timeout_to_query


class DateTests(unittest.TestCase):

    def test_from_datetime(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        assert str(d) == str(expected_date)

    def test_from_string(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        sd = Date('1492-10-12')
        assert sd == d
        sd = Date('+1492-10-12')
        assert sd == d

    def test_from_date(self):
        expected_date = datetime.date(1492, 10, 12)
        d = Date(expected_date)
        assert d.date() == expected_date

    def test_from_days(self):
        sd = Date(0)
        assert sd == Date(datetime.date(1970, 1, 1))
        sd = Date(-1)
        assert sd == Date(datetime.date(1969, 12, 31))
        sd = Date(1)
        assert sd == Date(datetime.date(1970, 1, 2))

    def test_limits(self):
        min_builtin = Date(datetime.date(1, 1, 1))
        max_builtin = Date(datetime.date(9999, 12, 31))
        assert Date(min_builtin.days_from_epoch) == min_builtin
        assert Date(max_builtin.days_from_epoch) == max_builtin
        # just proving we can construct with on offset outside buildin range
        assert Date(min_builtin.days_from_epoch - 1).days_from_epoch == min_builtin.days_from_epoch - 1
        assert Date(max_builtin.days_from_epoch + 1).days_from_epoch == max_builtin.days_from_epoch + 1

    def test_invalid_init(self):
        self.assertRaises(ValueError, Date, '-1999-10-10')
        self.assertRaises(TypeError, Date, 1.234)

    def test_str(self):
        date_str = '2015-03-16'
        assert str(Date(date_str)) == date_str

    def test_out_of_range(self):
        assert str(Date(2932897)) == '2932897'
        assert repr(Date(1)) == 'Date(1)'

    def test_equals(self):
        assert Date(1234) == 1234
        assert Date(1) == datetime.date(1970, 1, 2)
        self.assertFalse(Date(2932897) == datetime.date(9999, 12, 31))  # date can't represent year > 9999
        assert Date(2932897) == 2932897


class TimeTests(unittest.TestCase):

    def test_units_from_string(self):
        one_micro = 1000
        one_milli = 1000 * one_micro
        one_second = 1000 * one_milli
        one_minute = 60 * one_second
        one_hour = 60 * one_minute

        tt = Time('00:00:00.000000001')
        assert tt.nanosecond_time == 1
        tt = Time('00:00:00.000001')
        assert tt.nanosecond_time == one_micro
        tt = Time('00:00:00.001')
        assert tt.nanosecond_time == one_milli
        tt = Time('00:00:01')
        assert tt.nanosecond_time == one_second
        tt = Time('00:01:00')
        assert tt.nanosecond_time == one_minute
        tt = Time('01:00:00')
        assert tt.nanosecond_time == one_hour
        tt = Time('01:00:00.')
        assert tt.nanosecond_time == one_hour

        tt = Time('23:59:59.123456')
        assert tt.nanosecond_time == 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro

        tt = Time('23:59:59.1234567')
        assert tt.nanosecond_time == 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 700

        tt = Time('23:59:59.12345678')
        assert tt.nanosecond_time == 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 780

        tt = Time('23:59:59.123456789')
        assert tt.nanosecond_time == 23 * one_hour + 59 * one_minute + 59 * one_second + 123 * one_milli + 456 * one_micro + 789

    def test_micro_precision(self):
        Time('23:59:59.1')
        Time('23:59:59.12')
        Time('23:59:59.123')
        Time('23:59:59.1234')
        Time('23:59:59.12345')

    def test_from_int(self):
        tt = Time(12345678)
        assert tt.nanosecond_time == 12345678

    def test_from_time(self):
        expected_time = datetime.time(12, 1, 2, 3)
        tt = Time(expected_time)
        assert tt == expected_time

    def test_as_time(self):
        expected_time = datetime.time(12, 1, 2, 3)
        tt = Time(expected_time)
        assert tt.time() == expected_time

    def test_equals(self):
        # util.Time self equality
        assert Time(1234) == Time(1234)

    def test_str_repr(self):
        time_str = '12:13:14.123456789'
        assert str(Time(time_str)) == time_str
        assert repr(Time(1)) == 'Time(1)'

    def test_invalid_init(self):
        self.assertRaises(ValueError, Time, '1999-10-10 11:11:11.1234')
        self.assertRaises(TypeError, Time, 1.234)
        self.assertRaises(ValueError, Time, 123456789000000)
        self.assertRaises(TypeError, Time, datetime.datetime(2004, 12, 23, 11, 11, 1))


class DurationTests(unittest.TestCase):

    def test_valid_format(self):

        valid = Duration(1, 1, 1)
        assert valid.months == 1
        assert valid.days == 1
        assert valid.nanoseconds == 1

        valid = Duration(nanoseconds=100000)
        assert valid.months == 0
        assert valid.days == 0
        assert valid.nanoseconds == 100000

        valid = Duration()
        assert valid.months == 0
        assert valid.days == 0
        assert valid.nanoseconds == 0

        valid = Duration(-10, -21, -1000)
        assert valid.months == -10
        assert valid.days == -21
        assert valid.nanoseconds == -1000

    def test_equality(self):

        first = Duration(1, 1, 1)
        second = Duration(-1, 1, 1)
        self.assertNotEqual(first, second)

        first = Duration(1, 1, 1)
        second = Duration(1, 1, 1)
        assert first == second

        first = Duration()
        second = Duration(0, 0, 0)
        assert first == second

        first = Duration(1000, 10000, 2345345)
        second = Duration(1000, 10000, 2345345)
        assert first == second

        first = Duration(12, 0 , 100)
        second = Duration(nanoseconds=100, months=12)
        assert first == second

    def test_str(self):

        assert str(Duration(1, 1, 1)) == "1mo1d1ns"
        assert str(Duration(1, 1, -1)) == "-1mo1d1ns"
        assert str(Duration(1, 1, 1000000000000000)) == "1mo1d1000000000000000ns"
        assert str(Duration(52, 23, 564564)) == "52mo23d564564ns"


class VersionTests(unittest.TestCase):

    def test_version_parsing(self):
        versions = [
            ('2.0.0', (2, 0, 0, 0, 0)),
            ('3.1.0', (3, 1, 0, 0, 0)),
            ('2.4.54', (2, 4, 54, 0, 0)),
            ('3.1.1.12', (3, 1, 1, 12, 0)),
            ('3.55.1.build12', (3, 55, 1, 'build12', 0)),
            ('3.55.1.20190429-TEST', (3, 55, 1, 20190429, 'TEST')),
            ('4.0-SNAPSHOT', (4, 0, 0, 0, 'SNAPSHOT')),
            ('1.0.5.4.3', (1, 0, 5, 4, 0)),
            ('1-SNAPSHOT', (1, 0, 0, 0, 'SNAPSHOT')),
            ('4.0.1.2.3.4.5-ABC-123-SNAP-TEST.blah', (4, 0, 1, 2, 'ABC-123-SNAP-TEST.blah')),
            ('2.1.hello', (2, 1, 0, 0, 0)),
            ('2.test.1', (2, 0, 0, 0, 0)),
        ]

        for str_version, expected_result in versions:
            v = Version(str_version)
            assert str_version == str(v)
            assert v.major == expected_result[0]
            assert v.minor == expected_result[1]
            assert v.patch == expected_result[2]
            assert v.build == expected_result[3]
            assert v.prerelease == expected_result[4]

        # not supported version formats
        with self.assertRaises(ValueError):
            Version('test.1.0')

    def test_version_compare(self):
        # just tests a bunch of versions

        # major wins
        self.assertTrue(Version('3.3.0') > Version('2.5.0'))
        self.assertTrue(Version('3.3.0') > Version('2.5.0.66'))
        self.assertTrue(Version('3.3.0') > Version('2.5.21'))

        # minor wins
        self.assertTrue(Version('2.3.0') > Version('2.2.0'))
        self.assertTrue(Version('2.3.0') > Version('2.2.7'))
        self.assertTrue(Version('2.3.0') > Version('2.2.7.9'))

        # patch wins
        self.assertTrue(Version('2.3.1') > Version('2.3.0'))
        self.assertTrue(Version('2.3.1') > Version('2.3.0.4post0'))
        self.assertTrue(Version('2.3.1') > Version('2.3.0.44'))

        # various
        self.assertTrue(Version('2.3.0.1') > Version('2.3.0.0'))
        self.assertTrue(Version('2.3.0.680') > Version('2.3.0.670'))
        self.assertTrue(Version('2.3.0.681') > Version('2.3.0.680'))
        self.assertTrue(Version('2.3.0.1build0') > Version('2.3.0.1'))  # 4th part fallback to str cmp
        self.assertTrue(Version('2.3.0.build0') > Version('2.3.0.1'))  # 4th part fallback to str cmp
        self.assertTrue(Version('2.3.0') < Version('2.3.0.build'))

        self.assertTrue(Version('4-a') <= Version('4.0.0'))
        self.assertTrue(Version('4-a') <= Version('4.0-alpha1'))
        self.assertTrue(Version('4-a') <= Version('4.0-beta1'))
        self.assertTrue(Version('4.0.0') >= Version('4.0.0'))
        self.assertTrue(Version('4.0.0.421') >= Version('4.0.0'))
        self.assertTrue(Version('4.0.1') >= Version('4.0.0'))
        self.assertTrue(Version('2.3.0') == Version('2.3.0'))
        self.assertTrue(Version('2.3.32') == Version('2.3.32'))
        self.assertTrue(Version('2.3.32') == Version('2.3.32.0'))
        self.assertTrue(Version('2.3.0.build') == Version('2.3.0.build'))

        self.assertTrue(Version('4') == Version('4.0.0'))
        self.assertTrue(Version('4.0') == Version('4.0.0.0'))
        self.assertTrue(Version('4.0') > Version('3.9.3'))

        self.assertTrue(Version('4.0') > Version('4.0-SNAPSHOT'))
        self.assertTrue(Version('4.0-SNAPSHOT') == Version('4.0-SNAPSHOT'))
        self.assertTrue(Version('4.0.0-SNAPSHOT') == Version('4.0-SNAPSHOT'))
        self.assertTrue(Version('4.0.0-SNAPSHOT') == Version('4.0.0-SNAPSHOT'))
        self.assertTrue(Version('4.0.0.build5-SNAPSHOT') == Version('4.0.0.build5-SNAPSHOT'))
        self.assertTrue(Version('4.1-SNAPSHOT') > Version('4.0-SNAPSHOT'))
        self.assertTrue(Version('4.0.1-SNAPSHOT') > Version('4.0.0-SNAPSHOT'))
        self.assertTrue(Version('4.0.0.build6-SNAPSHOT') > Version('4.0.0.build5-SNAPSHOT'))
        self.assertTrue(Version('4.0-SNAPSHOT2') > Version('4.0-SNAPSHOT1'))
        self.assertTrue(Version('4.0-SNAPSHOT2') > Version('4.0.0-SNAPSHOT1'))

        self.assertTrue(Version('4.0.0-alpha1-SNAPSHOT') > Version('4.0.0-SNAPSHOT'))


class FunctionTests(unittest.TestCase):
    def test_maybe_add_timeout_to_query(self):
        assert "SELECT * FROM HOSTS" == maybe_add_timeout_to_query("SELECT * FROM HOSTS", None)
        assert "SELECT * FROM HOSTS USING TIMEOUT 1000ms" == maybe_add_timeout_to_query("SELECT * FROM HOSTS", datetime.timedelta(seconds=1))
