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
try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

import time
from mock import patch
import socket
from cassandra.io.asyncorereactor import AsyncoreConnection
from tests import is_monkey_patched
from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback, ReactorTestMixin


class AsyncoreConnectionTest(unittest.TestCase, ReactorTestMixin):

    connection_class = AsyncoreConnection
    socket_attr_name = 'socket'

    @classmethod
    def setUpClass(cls):
        if is_monkey_patched():
            return
        AsyncoreConnection.initialize_reactor()
        cls.socket_patcher = patch('socket.socket', spec=socket.socket)
        cls.mock_socket = cls.socket_patcher.start()
        cls.mock_socket().connect_ex.return_value = 0
        cls.mock_socket().getsockopt.return_value = 0
        cls.mock_socket().fileno.return_value = 100

        AsyncoreConnection.add_channel = lambda *args, **kwargs: None

    @classmethod
    def tearDownClass(cls):
        if is_monkey_patched():
            return
        cls.socket_patcher.stop()

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test asyncore with monkey patching")

    def test_multi_timer_validation(self):
        """
        Verify that timer timeouts are honored appropriately
        """
        self.make_connection()
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, AsyncoreConnection, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, AsyncoreConnection, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, AsyncoreConnection, 0, 100, 1, 100, True)

    def test_timer_cancellation(self):
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
        timer_manager = connection._loop._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())
