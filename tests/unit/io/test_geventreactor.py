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

import time
from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback
from tests import is_gevent_monkey_patched, is_eventlet_monkey_patched

try:
    from cassandra.io.geventreactor import GeventConnection
    import gevent.monkey
    from gevent_utils import gevent_un_patch_all
except ImportError:
    GeventConnection = None  # noqa


class GeventTimerTest(unittest.TestCase):

    need_unpatch = False

    @classmethod
    def setUpClass(cls):
        if is_eventlet_monkey_patched():
            return  # no dynamic patching if we have eventlet applied
        if GeventConnection is not None:
            if not is_gevent_monkey_patched():
                cls.need_unpatch = True
                gevent.monkey.patch_all()

    @classmethod
    def tearDownClass(cls):
        if cls.need_unpatch:
            gevent_un_patch_all()

    def setUp(self):
        if not is_gevent_monkey_patched():
            raise unittest.SkipTest("Can't test gevent without monkey patching")
        GeventConnection.initialize_reactor()

    def test_multi_timer_validation(self):
        """
        Verify that timer timeouts are honored appropriately
        """

        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, GeventConnection, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, GeventConnection, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, GeventConnection, 0, 100, 1, 100, True),

    def test_timer_cancellation(self):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = GeventConnection.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        timer_manager = GeventConnection._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())
