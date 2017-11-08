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
import six

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

import errno
import math
import time
from mock import patch
import socket
from socket import error as socket_error
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.protocol import SupportedMessage, ReadyMessage
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

    def test_blocking_on_write(self):
        c = self.make_connection()

        # make the OptionsMessage write block
        c.socket.send.side_effect = socket_error(errno.EAGAIN, "socket busy")
        c.handle_write()

        self.assertFalse(c.is_defunct)

        # try again with normal behavior
        c.socket.send.side_effect = lambda x: len(x)
        c.handle_write()
        self.assertFalse(c.is_defunct)
        self.assertTrue(c.socket.send.call_args is not None)

    def test_partial_send(self):
        c = self.make_connection()

        # only write the first four bytes of the OptionsMessage
        write_size = 4
        c.socket.send.side_effect = None
        c.socket.send.return_value = write_size
        c.handle_write()

        msg_size = 9  # v3+ frame header
        expected_writes = int(math.ceil(float(msg_size) / write_size))
        size_mod = msg_size % write_size
        last_write_size = size_mod if size_mod else write_size
        self.assertFalse(c.is_defunct)
        self.assertEqual(expected_writes, c.socket.send.call_count)
        self.assertEqual(last_write_size, len(c.socket.send.call_args[0][0]))

    def test_socket_error_on_read(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write()

        # read in a SupportedMessage response
        c.socket.recv.side_effect = socket_error(errno.EIO, "busy socket")
        c.handle_read()

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_partial_header_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        c.socket.recv.return_value = message[0:1]
        c.handle_read()
        self.assertEqual(c._iobuf.getvalue(), message[0:1])

        c.socket.recv.return_value = message[1:]
        c.handle_read()
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write()

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c.socket.recv.return_value = self.make_msg(header)
        c.handle_read()

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_partial_message_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first nine bytes
        c.socket.recv.return_value = message[:9]
        c.handle_read()
        self.assertEqual(c._iobuf.getvalue(), message[:9])

        # ... then read in the rest
        c.socket.recv.return_value = message[9:]
        c.handle_read()
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write()

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c.socket.recv.return_value = self.make_msg(header)
        c.handle_read()

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

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
