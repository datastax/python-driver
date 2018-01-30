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

from cassandra.connection import (ConnectionException, ProtocolError,
                                  HEADER_DIRECTION_TO_CLIENT)
from cassandra.marshal import int32_pack, uint8_pack, uint32_pack
from cassandra.protocol import (write_stringmultimap, write_int, write_string,
                                SupportedMessage, ReadyMessage, ServerError)
from tests import is_monkey_patched

from functools import wraps
import six
from six import binary_type, BytesIO
from mock import Mock

import errno
import math
import os
from socket import error as socket_error

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


def submit_and_wait_for_completion(unit_test, create_timer, start, end, increment, precision, split_range=False):
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
        create_timer(timeout, callback.invoke)
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


def noop_if_monkey_patched(f):
    if is_monkey_patched():
        @wraps(f)
        def noop(*args, **kwargs):
            return
        return noop

    return f


class TimerTestMixin(object):

    connection_class = connection = None
    # replace with property returning the connection's create_timer and _timers
    create_timer = _timers = None

    def setUp(self):
        self.connection = self.connection_class(
            connect_timeout=5
        )

    def tearDown(self):
        self.connection.close()

    def test_multi_timer_validation(self):
        """
        Verify that timer timeouts are honored appropriately
        """
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, self.create_timer, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, self.create_timer, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, self.create_timer, 0, 100, 1, 100, True),

    def test_timer_cancellation(self):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = self.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        timer_manager = self._timers
        # Assert that the cancellation was honored
        self.assertFalse(timer_manager._queue)
        self.assertFalse(timer_manager._new_timers)
        self.assertFalse(callback.was_invoked())


class ReactorTestMixin(object):

    connection_class = socket_attr_name = None
    null_handle_function_args = ()

    def get_socket(self, connection):
        return getattr(connection, self.socket_attr_name)

    def set_socket(self, connection, obj):
        return setattr(connection, self.socket_attr_name, obj)

    def make_header_prefix(self, message_class, version=2, stream_id=0):
        return binary_type().join(map(uint8_pack, [
            0xff & (HEADER_DIRECTION_TO_CLIENT | version),
            0,  # flags (compression)
            stream_id,
            message_class.opcode  # opcode
        ]))

    def make_connection(self):
        c = self.connection_class('1.2.3.4', cql_version='3.0.1', connect_timeout=5)
        mocket = Mock()
        mocket.send.side_effect = lambda x: len(x)
        self.set_socket(c, mocket)
        return c

    def make_options_body(self):
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.1'],
            'COMPRESSION': []
        })
        return options_buf.getvalue()

    def make_error_body(self, code, msg):
        buf = BytesIO()
        write_int(buf, code)
        write_string(buf, msg)
        return buf.getvalue()

    def make_msg(self, header, body=binary_type()):
        return header + uint32_pack(len(body)) + body

    def test_successful_connection(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(*self.null_handle_function_args)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        self.get_socket(c).recv.return_value = self.make_msg(header, options)
        c.handle_read(*self.null_handle_function_args)

        # let it write out a StartupMessage
        c.handle_write(*self.null_handle_function_args)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        self.get_socket(c).recv.return_value = self.make_msg(header)
        c.handle_read(*self.null_handle_function_args)

        self.assertTrue(c.connected_event.is_set())
        return c

    def test_eagain_on_buffer_size(self):
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

        self.get_socket(c).recv.side_effect = side_effect
        c.handle_read(*self.null_handle_function_args)
        self.assertEqual(c._current_frame.end_pos, 20000 + len(header))
        # the EAGAIN prevents it from reading the last 100 bytes
        c._iobuf.seek(0, os.SEEK_END)
        pos = c._iobuf.tell()
        self.assertEqual(pos, 4096 + 4096)

        # now tell it to read the last 100 bytes
        c.handle_read(*self.null_handle_function_args)
        c._iobuf.seek(0, os.SEEK_END)
        pos = c._iobuf.tell()
        self.assertEqual(pos, 4096 + 4096 + 100)

    def test_protocol_error(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(*self.null_handle_function_args)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0xa4)
        options = self.make_options_body()
        self.get_socket(c).recv.return_value = self.make_msg(header, options)
        c.handle_read(*self.null_handle_function_args)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(c.connected_event.is_set())
        self.assertIsInstance(c.last_error, ProtocolError)

    def test_error_message_on_startup(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(*self.null_handle_function_args)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        self.get_socket(c).recv.return_value = self.make_msg(header, options)
        c.handle_read(*self.null_handle_function_args)

        # let it write out a StartupMessage
        c.handle_write(*self.null_handle_function_args)

        header = self.make_header_prefix(ServerError, stream_id=1)
        body = self.make_error_body(ServerError.error_code, ServerError.summary)
        self.get_socket(c).recv.return_value = self.make_msg(header, body)
        c.handle_read(*self.null_handle_function_args)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, ConnectionException)
        self.assertTrue(c.connected_event.is_set())

    def test_socket_error_on_write(self):
        c = self.make_connection()

        # make the OptionsMessage write fail
        self.get_socket(c).send.side_effect = socket_error(errno.EIO, "bad stuff!")
        c.handle_write(*self.null_handle_function_args)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_blocking_on_write(self):
        c = self.make_connection()

        # make the OptionsMessage write block
        self.get_socket(c).send.side_effect = socket_error(errno.EAGAIN,
                                                           "socket busy")
        c.handle_write(*self.null_handle_function_args)

        self.assertFalse(c.is_defunct)

        # try again with normal behavior
        self.get_socket(c).send.side_effect = lambda x: len(x)
        c.handle_write(*self.null_handle_function_args)
        self.assertFalse(c.is_defunct)
        self.assertTrue(self.get_socket(c).send.call_args is not None)

    def test_partial_send(self):
        c = self.make_connection()

        # only write the first four bytes of the OptionsMessage
        write_size = 4
        self.get_socket(c).send.side_effect = None
        self.get_socket(c).send.return_value = write_size
        c.handle_write(*self.null_handle_function_args)

        msg_size = 9  # v3+ frame header
        expected_writes = int(math.ceil(float(msg_size) / write_size))
        size_mod = msg_size % write_size
        last_write_size = size_mod if size_mod else write_size
        self.assertFalse(c.is_defunct)
        self.assertEqual(expected_writes, self.get_socket(c).send.call_count)
        self.assertEqual(last_write_size,
                         len(self.get_socket(c).send.call_args[0][0]))

    def test_socket_error_on_read(self):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(*self.null_handle_function_args)

        # read in a SupportedMessage response
        self.get_socket(c).recv.side_effect = socket_error(errno.EIO,
                                                           "busy socket")
        c.handle_read(*self.null_handle_function_args)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_partial_header_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        self.get_socket(c).recv.return_value = message[0:1]
        c.handle_read(*self.null_handle_function_args)
        self.assertEqual(c._iobuf.getvalue(), message[0:1])

        self.get_socket(c).recv.return_value = message[1:]
        c.handle_read(*self.null_handle_function_args)
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write(*self.null_handle_function_args)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        self.get_socket(c).recv.return_value = self.make_msg(header)
        c.handle_read(*self.null_handle_function_args)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_partial_message_read(self):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first nine bytes
        self.get_socket(c).recv.return_value = message[:9]
        c.handle_read(*self.null_handle_function_args)
        self.assertEqual(c._iobuf.getvalue(), message[:9])

        # ... then read in the rest
        self.get_socket(c).recv.return_value = message[9:]
        c.handle_read(*self.null_handle_function_args)
        self.assertEqual(six.binary_type(), c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write(*self.null_handle_function_args)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        self.get_socket(c).recv.return_value = self.make_msg(header)
        c.handle_read(*self.null_handle_function_args)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)
