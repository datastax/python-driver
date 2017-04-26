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
    import unittest
from mock import Mock, patch
import time

try:
    from twisted.test import proto_helpers
    from twisted.python.failure import Failure
    from cassandra.io import twistedreactor
except ImportError:
    twistedreactor = None  # NOQA

from cassandra.connection import _Frame
from tests.unit.io.utils import submit_and_wait_for_completion, TimerCallback


class TestTwistedTimer(unittest.TestCase):
    """
    Simple test class that is used to validate that the TimerManager, and timer
    classes function appropriately with the twisted infrastructure
    """

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()

    def test_multi_timer_validation(self):
        """
        Verify that the timers are called in the correct order
        """
        twistedreactor.TwistedConnection.initialize_reactor()
        connection = twistedreactor.TwistedConnection('1.2.3.4',
                                                       cql_version='3.0.1')
        # Tests timers submitted in order at various timeouts
        submit_and_wait_for_completion(self, connection, 0, 100, 1, 100)
        # Tests timers submitted in reverse order at various timeouts
        submit_and_wait_for_completion(self, connection, 100, 0, -1, 100)
        # Tests timers submitted in varying order at various timeouts
        submit_and_wait_for_completion(self, connection, 0, 100, 1, 100, True)

    def test_timer_cancellation(self, *args):
        """
        Verify that timer cancellation is honored
        """

        # Various lists for tracking callback stage
        connection = twistedreactor.TwistedConnection('1.2.3.4',
                                                       cql_version='3.0.1')
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


class TestTwistedProtocol(unittest.TestCase):

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.tr = proto_helpers.StringTransportWithDisconnection()
        self.tr.connector = Mock()
        self.mock_connection = Mock()
        self.tr.connector.factory = twistedreactor.TwistedConnectionClientFactory(
            self.mock_connection)
        self.obj_ut = twistedreactor.TwistedConnectionProtocol()
        self.tr.protocol = self.obj_ut

    def tearDown(self):
        pass

    def test_makeConnection(self):
        """
        Verify that the protocol class notifies the connection
        object that a successful connection was made.
        """
        self.obj_ut.makeConnection(self.tr)
        self.assertTrue(self.mock_connection.client_connection_made.called)

    def test_receiving_data(self):
        """
        Verify that the dataReceived() callback writes the data to
        the connection object's buffer and calls handle_read().
        """
        self.obj_ut.makeConnection(self.tr)
        self.obj_ut.dataReceived('foobar')
        self.assertTrue(self.mock_connection.handle_read.called)
        self.mock_connection._iobuf.write.assert_called_with("foobar")


class TestTwistedClientFactory(unittest.TestCase):
    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.mock_connection = Mock()
        self.obj_ut = twistedreactor.TwistedConnectionClientFactory(
            self.mock_connection)

    def test_client_connection_failed(self):
        """
        Verify that connection failed causes the connection object to close.
        """
        exc = Exception('a test')
        self.obj_ut.clientConnectionFailed(None, Failure(exc))
        self.mock_connection.defunct.assert_called_with(exc)

    def test_client_connection_lost(self):
        """
        Verify that connection lost causes the connection object to close.
        """
        exc = Exception('a test')
        self.obj_ut.clientConnectionLost(None, Failure(exc))
        self.mock_connection.defunct.assert_called_with(exc)


class TestTwistedConnection(unittest.TestCase):
    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.reactor_cft_patcher = patch(
            'twisted.internet.reactor.callFromThread')
        self.reactor_run_patcher = patch('twisted.internet.reactor.run')
        self.mock_reactor_cft = self.reactor_cft_patcher.start()
        self.mock_reactor_run = self.reactor_run_patcher.start()
        self.obj_ut = twistedreactor.TwistedConnection('1.2.3.4',
                                                       cql_version='3.0.1')

    def tearDown(self):
        self.reactor_cft_patcher.stop()
        self.reactor_run_patcher.stop()

    def test_connection_initialization(self):
        """
        Verify that __init__() works correctly.
        """
        self.mock_reactor_cft.assert_called_with(self.obj_ut.add_connection)
        self.obj_ut._loop._cleanup()
        self.mock_reactor_run.assert_called_with(installSignalHandlers=False)

    @patch('twisted.internet.reactor.connectTCP')
    def test_add_connection(self, mock_connectTCP):
        """
        Verify that add_connection() gives us a valid twisted connector.
        """
        self.obj_ut.add_connection()
        self.assertTrue(self.obj_ut.connector is not None)
        self.assertTrue(mock_connectTCP.called)

    def test_client_connection_made(self):
        """
        Verifiy that _send_options_message() is called in
        client_connection_made()
        """
        self.obj_ut._send_options_message = Mock()
        self.obj_ut.client_connection_made()
        self.obj_ut._send_options_message.assert_called_with()

    @patch('twisted.internet.reactor.connectTCP')
    def test_close(self, mock_connectTCP):
        """
        Verify that close() disconnects the connector and errors callbacks.
        """
        self.obj_ut.error_all_requests = Mock()
        self.obj_ut.add_connection()
        self.obj_ut.is_closed = False
        self.obj_ut.close()
        self.obj_ut.connector.disconnect.assert_called_with()
        self.assertTrue(self.obj_ut.connected_event.is_set())
        self.assertTrue(self.obj_ut.error_all_requests.called)

    def test_handle_read__incomplete(self):
        """
        Verify that handle_read() processes incomplete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), b'')  # buf starts empty
        # incomplete header
        self.obj_ut._iobuf.write(b'\x84\x00\x00\x00\x00')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), b'\x84\x00\x00\x00\x00')

        # full header, but incomplete body
        self.obj_ut._iobuf.write(b'\x00\x00\x00\x15')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._iobuf.getvalue(),
                         b'\x84\x00\x00\x00\x00\x00\x00\x00\x15')
        self.assertEqual(self.obj_ut._current_frame.end_pos, 30)

        # verify we never attempted to process the incomplete message
        self.assertFalse(self.obj_ut.process_msg.called)

    def test_handle_read__fullmessage(self):
        """
        Verify that handle_read() processes complete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), b'')  # buf starts empty

        # write a complete message, plus 'NEXT' (to simulate next message)
        # assumes protocol v3+ as default Connection.protocol_version
        body = b'this is the drum roll'
        extra = b'NEXT'
        self.obj_ut._iobuf.write(
            b'\x84\x01\x00\x02\x03\x00\x00\x00\x15' + body + extra)
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), extra)
        self.obj_ut.process_msg.assert_called_with(
            _Frame(version=4, flags=1, stream=2, opcode=3, body_offset=9, end_pos= 9 + len(body)), body)

    @patch('twisted.internet.reactor.connectTCP')
    def test_push(self, mock_connectTCP):
        """
        Verifiy that push() calls transport.write(data).
        """
        self.obj_ut.add_connection()
        self.obj_ut.push('123 pickup')
        self.mock_reactor_cft.assert_called_with(
            self.obj_ut.connector.transport.write, '123 pickup')

