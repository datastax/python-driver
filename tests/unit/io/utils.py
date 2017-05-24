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
    import unittest # noqa

import time


class TimerCallback(object):

    invoked = False
    created_time = 0
    invoked_time = 0
    expected_wait = 0

    def __init__(self, expected_wait):
        self.invoked = False
        self.created_time = time.time()
        self.expected_wait = expected_wait

    def invoke(self):
        self.invoked_time = time.time()
        self.invoked = True

    def was_invoked(self):
        return self.invoked

    def get_wait_time(self):
        elapsed_time = self.invoked_time - self.created_time
        return elapsed_time

    def wait_match_excepted(self):
        if self.expected_wait - .01 <= self.get_wait_time() <= self.expected_wait + .01:
            return True
        return False


def get_timeout(gross_time, start, end, precision, split_range):
    """
    A way to generate varying timeouts based on ranges
    :param gross_time: Some integer between start and end
    :param start: the start value of the range
    :param end: the end value of the range
    :param precision: the precision to use to generate the timeout.
    :param split_range: generate values from both ends
    :return: a timeout value to use
    """
    if split_range:
            top_num = float(end) / precision
            bottom_num = float(start) / precision
            if gross_time % 2 == 0:
                timeout = top_num - float(gross_time) / precision
            else:
                timeout = bottom_num + float(gross_time) / precision

    else:
        timeout = float(gross_time) / precision

    return timeout


def submit_and_wait_for_completion(unit_test, connection, start, end, increment, precision, split_range=False):
    """
   This will submit a number of timers to the provided connection. It will then ensure that the corresponding
   callback is invoked in the appropriate amount of time.
   :param unit_test:  Invoking unit tests
   :param connection: Connection to create the timer on.
   :param start: Lower bound of range.
   :param end: Upper bound of the time range
   :param increment: +1, or -1
   :param precision: 100 for centisecond, 1000 for milliseconds
   :param split_range: True to split the range between incrementing and decrementing.
   """

    # Various lists for tracking callback as completed or pending
    pending_callbacks = []
    completed_callbacks = []

    # submit timers with various timeouts
    for gross_time in range(start, end, increment):
        timeout = get_timeout(gross_time, start, end, precision, split_range)
        callback = TimerCallback(timeout)
        connection.create_timer(timeout, callback.invoke)
        pending_callbacks.append(callback)

    # wait for all the callbacks associated with the timers to be invoked
    while len(pending_callbacks) is not 0:
        for callback in pending_callbacks:
            if callback.was_invoked():
                pending_callbacks.remove(callback)
                completed_callbacks.append(callback)
        time.sleep(.1)

    # ensure they are all called back in a timely fashion
    for callback in completed_callbacks:
        unit_test.assertAlmostEqual(callback.expected_wait, callback.get_wait_time(), delta=.15)


class TimerConnectionTests(object):
    def test_multi_timer_validation(self):
        """
        Verify that timer timeouts are honored appropriately
        """
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, self.connection_class, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, self.connection_class, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, self.connection_class, 0, 100, 1, 100, True),

    def test_timer_cancellation(self):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = self.connection_class.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        timer_manager = self.connection_class._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())
