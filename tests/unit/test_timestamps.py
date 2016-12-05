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

import mock

from cassandra import timestamps


class _TimestampTestMixin(object):

    @mock.patch('cassandra.timestamps.time')
    def _call_and_check_results(self,
                                patched_time_module,
                                system_time_expected_stamp_pairs,
                                timestamp_generator=None):
        """
        For each element in an iterable of (system_time, expected_timestamp)
        pairs, call a :class:`cassandra.timestamps.MonotonicTimestampGenerator`
        with system_times as the underlying time.time() result, then assert
        that the result is expected_timestamp. Skips the check if
        expected_timestamp is None.
        """
        patched_time_module.time = mock.Mock()
        system_times, expected_timestamps = zip(*system_time_expected_stamp_pairs)

        patched_time_module.time.side_effect = system_times
        tsg = timestamp_generator or timestamps.MonotonicTimestampGenerator()

        for expected in expected_timestamps:
            actual = tsg()
            if expected is not None:
                self.assertEqual(actual, expected)

        # assert we patched timestamps.time.time correctly
        with self.assertRaises(StopIteration):
            tsg()


class TestTimestampGeneratorOutput(unittest.TestCase, _TimestampTestMixin):
    """
    Mock time.time and test the output of MonotonicTimestampGenerator.__call__
    given different patterns of changing results.
    """

    def test_timestamps_during_and_after_same_system_time(self):
        """
        Timestamps should increase monotonically over repeated system time.

        Test that MonotonicTimestampGenerator's output increases by 1 when the
        underlying system time is the same, then returns to normal when the
        system time increases again.
        """
        self._call_and_check_results(
            system_time_expected_stamp_pairs=(
                (15.0, 15 * 1e6),
                (15.0, 15 * 1e6 + 1),
                (15.0, 15 * 1e6 + 2),
                (15.01, 15.01 * 1e6))
        )

    def test_timestamps_during_and_after_backwards_system_time(self):
        """
        Timestamps should increase monotonically over system time going backwards.

        Test that MonotonicTimestampGenerator's output increases by 1 when the
        underlying system time goes backward, then returns to normal when the
        system time increases again.
        """
        self._call_and_check_results(
            system_time_expected_stamp_pairs=(
                (15.0, 15 * 1e6),
                (13.0, 15 * 1e6 + 1),
                (14.0, 15 * 1e6 + 2),
                (13.5, 15 * 1e6 + 3),
                (15.01, 15.01 * 1e6))
        )


class TestTimestampGeneratorLogging(unittest.TestCase, _TimestampTestMixin):

    def setUp(self):
        self.log_patcher = mock.patch('cassandra.timestamps.log')
        self.addCleanup(self.log_patcher.stop)
        self.patched_timestamp_log = self.log_patcher.start()

    def assertLastCallArgRegex(self, call, pattern):
        last_warn_args, last_warn_kwargs = call
        self.assertEqual(len(last_warn_args), 1)
        self.assertEqual(len(last_warn_kwargs), 0)
        self.assertRegexpMatches(
            last_warn_args[0],
            pattern,
        )

    def test_basic_log_content(self):
        tsg = timestamps.MonotonicTimestampGenerator()
        tsg._last_warn = 12

        tsg._next_timestamp(20, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 0)
        tsg._next_timestamp(16, tsg.last)

        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 1)
        self.assertLastCallArgRegex(
            self.patched_timestamp_log.warn.call_args,
            r'Clock skew detected:.*\b16\b.*\b4\b.*\b20\b'
        )

    def test_disable_logging(self):
        no_warn_tsg = timestamps.MonotonicTimestampGenerator(warn_on_drift=False)

        no_warn_tsg.last = 100
        no_warn_tsg._next_timestamp(99, no_warn_tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 0)

    def test_warning_threshold_respected_no_logging(self):
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_threshold=2,
        )
        tsg.last, tsg._last_warn = 100, 97
        tsg._next_timestamp(98, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 0)

    def test_warning_threshold_respected_logs(self):
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_threshold=1
        )
        tsg.last, tsg._last_warn = 100, 97
        tsg._next_timestamp(98, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 1)

    def test_warning_interval_respected_no_logging(self):
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_interval=2
        )
        tsg.last = 100
        tsg._next_timestamp(70, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 1)

        tsg._next_timestamp(71, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 1)

    def test_warning_interval_respected_logs(self):
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_interval=1
        )
        tsg.last = 100
        tsg._next_timestamp(70, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 1)

        tsg._next_timestamp(72, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warn.call_args_list), 2)
