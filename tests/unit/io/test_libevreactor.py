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

import errno
import math
from mock import patch
import os
import weakref
import six
from socket import error as socket_error

from cassandra.connection import ConnectionException, ProtocolError
from cassandra.protocol import (SupportedMessage, ReadyMessage, ServerError)
from cassandra.marshal import int32_pack

from tests import is_monkey_patched
from tests.unit.io.utils import ReactorTestMixin


try:
    from cassandra.io.libevreactor import _cleanup as libev__cleanup
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None  # noqa


class LibevConnectionTest(unittest.TestCase, ReactorTestMixin):

    connection_class = LibevConnection
    socket_attr_name = '_socket'
    null_handle_function_args = None, 0

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

        # we patch here rather than as a decorator so that the Mixin can avoid
        # specifying patch args to test methods
        patchers = [patch(obj) for obj in
                    ('socket.socket',
                     'cassandra.io.libevwrapper.IO',
                     'cassandra.io.libevwrapper.Prepare',
                     'cassandra.io.libevwrapper.Async',
                     'cassandra.io.libevreactor.LibevLoop.maybe_start')]
        for p in patchers:
            self.addCleanup(p.stop)
        for p in patchers:
            p.start()

    def test_egain_on_buffer_size(self):
        # get a connection that's already fully started
        c = self.test_successful_connection()

        header = six.b('\x00\x00\x00\x00') + int32_pack(20000)
        responses = [
            header + (six.b('a') * (4096 - len(header))),
            six.b('a') * 4096,
            socket_error(errno.EAGAIN),
            six.b('a') * 100,
            socket_error(errno.EAGAIN)]

        def side_effect(*args):
            response = responses.pop(0)
            if isinstance(response, socket_error):
                raise response
            else:
                return response

        c._socket.recv.side_effect = side_effect
        c.handle_read(None, 0)
        self.assertEqual(c._current_frame.end_pos, 20000 + len(header))
        # the EAGAIN prevents it from reading the last 100 bytes
        c._iobuf.seek(0, os.SEEK_END)
        pos = c._iobuf.tell()
        self.assertEqual(pos, 4096 + 4096)

        # now tell it to read the last 100 bytes
        c.handle_read(None, 0)
        c._iobuf.seek(0, os.SEEK_END)
        pos = c._iobuf.tell()
        self.assertEqual(pos, 4096 + 4096 + 100)

    def test_protocol_error(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, 0)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0xa4)
        options = self.make_options_body()
        c._socket.recv.return_value = self.make_msg(header, options)
        c.handle_read(None, 0)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(c.connected_event.is_set())
        self.assertIsInstance(c.last_error, ProtocolError)

    def test_error_message_on_startup(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, 0)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        c._socket.recv.return_value = self.make_msg(header, options)
        c.handle_read(None, 0)

        # let it write out a StartupMessage
        c.handle_write(None, 0)

        header = self.make_header_prefix(ServerError, stream_id=1)
        body = self.make_error_body(ServerError.error_code, ServerError.summary)
        c._socket.recv.return_value = self.make_msg(header, body)
        c.handle_read(None, 0)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, ConnectionException)
        self.assertTrue(c.connected_event.is_set())

    def test_socket_error_on_write(self):
        c = self.make_connection()

        # make the OptionsMessage write fail
        c._socket.send.side_effect = socket_error(errno.EIO, "bad stuff!")
        c.handle_write(None, 0)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_blocking_on_write(self):
        c = self.make_connection()

        # make the OptionsMessage write block
        c._socket.send.side_effect = socket_error(errno.EAGAIN, "socket busy")
        c.handle_write(None, 0)

        self.assertFalse(c.is_defunct)

        # try again with normal behavior
        c._socket.send.side_effect = lambda x: len(x)
        c.handle_write(None, 0)
        self.assertFalse(c.is_defunct)
        self.assertTrue(c._socket.send.call_args is not None)

    def test_partial_send(self):
        c = self.make_connection()

        # only write the first four bytes of the OptionsMessage
        write_size = 4
        c._socket.send.side_effect = None
        c._socket.send.return_value = write_size
        c.handle_write(None, 0)

        msg_size = 9  # v3+ frame header
        expected_writes = int(math.ceil(float(msg_size) / write_size))
        size_mod = msg_size % write_size
        last_write_size = size_mod if size_mod else write_size
        self.assertFalse(c.is_defunct)
        self.assertEqual(expected_writes, c._socket.send.call_count)
        self.assertEqual(last_write_size, len(c._socket.send.call_args[0][0]))

    def test_socket_error_on_read(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, 0)

        # read in a SupportedMessage response
        c._socket.recv.side_effect = socket_error(errno.EIO, "busy socket")
        c.handle_read(None, 0)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_partial_header_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first byte
        c._socket.recv.return_value = message[0:1]
        c.handle_read(None, 0)
        self.assertEqual(c._iobuf.getvalue(), message[0:1])

        c._socket.recv.return_value = message[1:]
        c.handle_read(None, 0)
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write(None, 0)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, 0)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_partial_message_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first nine bytes
        c._socket.recv.return_value = message[:9]
        c.handle_read(None, 0)
        self.assertEqual(c._iobuf.getvalue(), message[:9])

        # ... then read in the rest
        c._socket.recv.return_value = message[9:]
        c.handle_read(None, 0)
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write(None, 0)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, 0)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_watchers_are_finished(self):
        """
        Test for asserting that watchers are closed in LibevConnection

        This test simulates a process termination without calling cluster.shutdown(), which would trigger
        LibevConnection._libevloop._cleanup. It will check the watchers have been closed
        Finally it will restore the LibevConnection reactor so it doesn't affect
        the rest of the tests

        @since 3.10
        @jira_ticket PYTHON-747
        @expected_result the watchers are closed

        @test_category connection
        """
        with patch.object(LibevConnection._libevloop, "_thread"), \
             patch.object(LibevConnection._libevloop, "notify"):

            self.make_connection()

            # We have to make a copy because the connections shouldn't
            # be alive when we verify them
            live_connections = set(LibevConnection._libevloop._live_conns)

            # This simulates the process ending without cluster.shutdown()
            # being called, then with atexit _cleanup for libevreactor would
            # be called
            libev__cleanup(weakref.ref(LibevConnection._libevloop))
            for conn in live_connections:
                for watcher in (conn._write_watcher, conn._read_watcher):
                    self.assertTrue(watcher.stop.mock_calls)

        LibevConnection._libevloop._shutdown = False
