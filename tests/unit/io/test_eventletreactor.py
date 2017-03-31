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
    import unittest # noqa

from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback
from tests import is_eventlet_time_monkey_patched, is_gevent_time_monkey_patched
from tests.unit.io.eventlet_utils import restore_saved_module
import time
from eventlet import monkey_patch

try:
    from cassandra.io.eventletreactor import EventletConnection
except ImportError:
    EventletConnection = None  # noqa


class EventletTimerTest(unittest.TestCase):
    need_unpatch = False

    @classmethod
    def setUpClass(cls):
        if is_eventlet_time_monkey_patched():
            return  # no dynamic patching if we have eventlet applied
        if EventletConnection is not None:
            if not is_gevent_time_monkey_patched():
                cls.need_unpatch = True
                monkey_patch(time=True)

    @classmethod
    def tearDownClass(cls):
        if cls.need_unpatch:
            restore_saved_module(time)

    def setUp(self):
        if not is_eventlet_time_monkey_patched():
            raise unittest.SkipTest("Can't test gevent without monkey patching")
        EventletConnection.initialize_reactor()

    def test_multi_timer_validation(self, *args):
        """
        Verify that timer timeouts are honored appropriately
        """
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, EventletConnection, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, EventletConnection, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, EventletConnection, 0, 100, 1, 100, True)

    def test_timer_cancellation(self):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = EventletConnection.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        timer_manager = EventletConnection._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())
