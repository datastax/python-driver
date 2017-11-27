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
from mock import patch
import socket
from cassandra.io.asyncorereactor import AsyncoreConnection
from tests import is_monkey_patched
from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback, ReactorTestMixin, TimerTestMixin


def patch_socket_and_set_up_connection(cls):
    AsyncoreConnection.initialize_reactor()

    cls.socket_patcher = patch('socket.socket', spec=socket.socket)
    cls.mock_socket = cls.socket_patcher.start()
    cls.mock_socket().connect_ex.return_value = 0
    cls.mock_socket().getsockopt.return_value = 0
    cls.mock_socket().fileno.return_value = 100

    AsyncoreConnection.add_channel = lambda *args, **kwargs: None


class AsyncoreConnectionTest(unittest.TestCase, ReactorTestMixin):

    connection_class = AsyncoreConnection
    socket_attr_name = 'socket'
    loop_attr_name = '_loop'

    @classmethod
    def setUpClass(cls):
        if is_monkey_patched():
            return
        patch_socket_and_set_up_connection(cls)

    @classmethod
    def tearDownClass(cls):
        if is_monkey_patched():
            return
        cls.socket_patcher.stop()

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test asyncore with monkey patching")


class TestAsyncoreTimer(TimerTestMixin, unittest.TestCase):
    connection_class = AsyncoreConnection

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._loop._timers

    @classmethod
    def setUpClass(cls):
        if is_monkey_patched():
            return
        patch_socket_and_set_up_connection(cls)

    @classmethod
    def tearDownClass(cls):
        if is_monkey_patched():
            return
        cls.socket_patcher.stop()

    def setUp(self):
        self.connection = self.connection_class(connect_timeout=5)
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test asyncore with monkey patching")
