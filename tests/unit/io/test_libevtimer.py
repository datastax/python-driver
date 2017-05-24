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


from mock import patch, Mock

import time

from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback
from tests.unit.io.utils import TimerConnectionTests
from tests import is_monkey_patched


try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None  # noqa


@patch('socket.socket')
@patch('cassandra.io.libevwrapper.IO')
class LibevTimerTest(unittest.TestCase):

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

    def make_connection(self):
        c = LibevConnection('1.2.3.4', cql_version='3.0.1')
        c._socket = Mock()
        c._socket.send.side_effect = lambda x: len(x)
        return c

    def test_multi_timer_validation(self, *args):
        """
        Verify that timer timeouts are honored appropriately
        """
        c = self.make_connection()
        c.initialize_reactor()
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, c, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, c, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, c, 0, 100, 1, 100, True)

    def test_timer_cancellation(self, *args):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        connection = self.make_connection()
        timeout = .1
        callback = TimerCallback(timeout)
        timer = connection.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        timer_manager = connection._libevloop._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())

