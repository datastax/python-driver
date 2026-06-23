AsyncioConnection, ASYNCIO_AVAILABLE = None, False
try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    ASYNCIO_AVAILABLE = True
except (ImportError, SyntaxError):
    AsyncioConnection = None
    ASYNCIO_AVAILABLE = False

from tests import is_monkey_patched, connection_class
from tests.unit.io.utils import TimerCallback, TimerTestMixin, submit_and_wait_for_completion

from unittest.mock import patch, MagicMock, Mock, AsyncMock

import asyncio
import socket as stdlib_socket
import unittest
import time
import threading

skip_me = (is_monkey_patched() or
           (not ASYNCIO_AVAILABLE) or
           (connection_class is not AsyncioConnection))


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioTimerTests(TimerTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if skip_me:
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    @classmethod
    def tearDownClass(cls):
        if skip_me:
            return
        if ASYNCIO_AVAILABLE and AsyncioConnection._loop:
            AsyncioConnection._loop.stop()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        raise RuntimeError('no TimerManager for AsyncioConnection')

    def setUp(self):
        if skip_me:
            return
        socket_patcher = patch('socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        old_selector = AsyncioConnection._loop._selector
        AsyncioConnection._loop._selector = MagicMock()

        def reset_selector():
            AsyncioConnection._loop._selector = old_selector

        self.addCleanup(reset_selector)

        super(AsyncioTimerTests, self).setUp()

    def test_multi_timer_validation(self):
        """
        Override with a wider tolerance for asyncio's thread-based scheduling,
        which has inherently more jitter than libev's native event loop.
        """
        from tests.unit.io.utils import get_timeout
        pending_callbacks = []
        completed_callbacks = []

        for gross_time in range(0, 100, 1):
            timeout = get_timeout(gross_time, 0, 100, 100, False)
            callback = TimerCallback(timeout)
            self.create_timer(timeout, callback.invoke)
            pending_callbacks.append(callback)

        while len(pending_callbacks) != 0:
            for callback in pending_callbacks:
                if callback.was_invoked():
                    pending_callbacks.remove(callback)
                    completed_callbacks.append(callback)
            time.sleep(.1)

        for callback in completed_callbacks:
            self.assertAlmostEqual(callback.expected_wait, callback.get_wait_time(), delta=.25)

    def test_timer_cancellation(self):
        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = self.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        # Assert that the cancellation was honored
        self.assertFalse(callback.was_invoked())


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioConnectionTest(unittest.TestCase):
    """
    Tests for AsyncioConnection covering write, read, close, and error
    handling at the reactor level.  Unlike the ReactorTestMixin used by
    asyncore/libev, these tests exercise the public interface (push/close)
    because handle_read/handle_write are async coroutines running inside
    the event loop thread.
    """

    @classmethod
    def setUpClass(cls):
        if skip_me:
            return
        # Force a fresh reactor so we aren't affected by a previous test
        # class that may have stopped the shared event loop.
        if AsyncioConnection._loop is not None:
            try:
                AsyncioConnection._loop.call_soon_threadsafe(
                    AsyncioConnection._loop.stop)
            except RuntimeError:
                pass
            if AsyncioConnection._loop_thread:
                AsyncioConnection._loop_thread.join(timeout=1.0)
        AsyncioConnection._loop = None
        AsyncioConnection._loop_thread = None
        AsyncioConnection.initialize_reactor()
        cls._loop = AsyncioConnection._loop
        # Save original loop methods so we can restore after each test
        cls._orig_sock_recv = cls._loop.sock_recv
        cls._orig_sock_sendall = cls._loop.sock_sendall

    @classmethod
    def tearDownClass(cls):
        if skip_me:
            return
        cls._loop.sock_recv = cls._orig_sock_recv
        cls._loop.sock_sendall = cls._orig_sock_sendall

    def _make_connection(self):
        """
        Create an AsyncioConnection with mocked socket and _connect_socket.
        Loop socket methods are pre-mocked so that the handle_read/handle_write
        coroutines started in __init__ don't hit real I/O.
        """
        mock_socket = MagicMock(spec=stdlib_socket.socket)
        mock_socket.fileno.return_value = 99
        mock_socket.setblocking = MagicMock()
        mock_socket.connect.return_value = None
        mock_socket.getsockopt.return_value = 0
        mock_socket.send.side_effect = lambda x: len(x)

        def fake_connect_socket(self_inner):
            self_inner._socket = mock_socket

        with patch.object(AsyncioConnection, '_connect_socket', fake_connect_socket):
            conn = AsyncioConnection(
                host='127.0.0.1',
                cql_version='3.0.1',
                connect_timeout=5,
            )
        return conn

    def setUp(self):
        if skip_me:
            return

        loop = self._loop

        # Pre-mock sock_recv to block indefinitely (read loop won't spin)
        self._recv_unblock = threading.Event()

        async def blocking_recv(sock, bufsize):
            while not self._recv_unblock.is_set():
                await asyncio.sleep(0.01)
            raise asyncio.CancelledError()

        # Pre-mock sock_sendall to silently consume data (options message, etc.)
        self._sent_data = []

        async def capturing_sendall(sock, data):
            self._sent_data.append(bytes(data))

        loop.sock_recv = blocking_recv
        loop.sock_sendall = capturing_sendall

        self.conn = self._make_connection()
        # Give the loop a moment to process __init__ tasks (options message)
        time.sleep(0.1)
        # Clear any data sent during init (options message)
        self._sent_data.clear()

    def tearDown(self):
        if skip_me:
            return
        # Unblock the recv so the read loop can exit
        self._recv_unblock.set()
        try:
            self.conn.close()
        except Exception:
            pass
        time.sleep(0.05)
        # Restore default mocks for next test
        self._loop.sock_recv = self._orig_sock_recv
        self._loop.sock_sendall = self._orig_sock_sendall

    def test_push_sends_data(self):
        """
        Verify that push() enqueues data and the write loop sends it
        via sock_sendall on the event loop.
        """
        test_data = b'hello world'
        self.conn.push(test_data)

        # Wait for the event loop to drain the write queue
        time.sleep(0.2)

        self.assertTrue(len(self._sent_data) > 0)
        self.assertEqual(b''.join(self._sent_data), test_data)

    def test_push_chunking(self):
        """
        Verify that data larger than out_buffer_size is chunked
        into multiple pieces before being sent.
        """
        buf_size = self.conn.out_buffer_size
        # Send data that is 2.5x the buffer size
        test_data = b'x' * int(buf_size * 2.5)
        self.conn.push(test_data)

        time.sleep(0.2)

        # Should have been broken into at least 3 chunks
        self.assertGreaterEqual(len(self._sent_data), 3)
        self.assertEqual(b''.join(self._sent_data), test_data)

    def test_write_error_defuncts_connection(self):
        """
        Verify that a socket error during write causes the
        connection to become defunct.
        """
        loop = self._loop

        async def error_sendall(sock, data):
            raise stdlib_socket.error(32, "Broken pipe")

        loop.sock_sendall = error_sendall

        self.conn.push(b'trigger error')
        time.sleep(0.2)

        self.assertTrue(self.conn.is_defunct)
        self.assertIsInstance(self.conn.last_error, stdlib_socket.error)

    def test_read_eof_closes_connection(self):
        """
        Verify that receiving an empty buffer (EOF / server close)
        causes the connection to close.
        """
        loop = self._loop

        # Cancel the existing read watcher so we can start a new one
        if self.conn._read_watcher:
            self.conn._read_watcher.cancel()
        time.sleep(0.05)

        call_count = 0
        async def eof_recv(sock, bufsize):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return b''  # EOF
            raise asyncio.CancelledError()

        loop.sock_recv = eof_recv

        self.conn._read_watcher = asyncio.run_coroutine_threadsafe(
            self.conn.handle_read(), loop=loop
        )

        time.sleep(0.2)
        self.assertTrue(self.conn.is_closed)

    def test_read_error_defuncts_connection(self):
        """
        Verify that a socket error during read causes the
        connection to become defunct.
        """
        loop = self._loop

        if self.conn._read_watcher:
            self.conn._read_watcher.cancel()
        time.sleep(0.05)

        async def error_recv(sock, bufsize):
            raise stdlib_socket.error(104, "Connection reset by peer")

        loop.sock_recv = error_recv

        self.conn._read_watcher = asyncio.run_coroutine_threadsafe(
            self.conn.handle_read(), loop=loop
        )

        time.sleep(0.2)
        self.assertTrue(self.conn.is_defunct)
        self.assertIsInstance(self.conn.last_error, stdlib_socket.error)

    def test_read_processes_data(self):
        """
        Verify that data received via sock_recv is written to the
        IO buffer and process_io_buffer is called.
        """
        loop = self._loop

        if self.conn._read_watcher:
            self.conn._read_watcher.cancel()
        time.sleep(0.05)

        call_count = 0
        async def data_then_eof_recv(sock, bufsize):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return b'some data from server'
            return b''

        loop.sock_recv = data_then_eof_recv

        with patch.object(self.conn, 'process_io_buffer') as mock_process:
            self.conn._read_watcher = asyncio.run_coroutine_threadsafe(
                self.conn.handle_read(), loop=loop
            )
            time.sleep(0.2)
            mock_process.assert_called()

    def test_close_cancels_watchers(self):
        """
        Verify that closing the connection cancels both the
        read and write watchers.
        """
        read_watcher = self.conn._read_watcher
        write_watcher = self.conn._write_watcher

        self.conn.close()
        time.sleep(0.2)

        self.assertTrue(self.conn.is_closed)
        # The watchers should have been cancelled
        if read_watcher:
            self.assertTrue(read_watcher.cancelled() or read_watcher.done())
        if write_watcher:
            self.assertTrue(write_watcher.cancelled() or write_watcher.done())


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioForkTest(unittest.TestCase):
    """
    Test that handle_fork() properly resets reactor state.
    """

    def test_handle_fork_resets_state(self):
        """
        Verify handle_fork() clears loop, thread, and updates pid.
        """
        AsyncioConnection.initialize_reactor()
        self.assertIsNotNone(AsyncioConnection._loop)
        self.assertIsNotNone(AsyncioConnection._loop_thread)

        old_loop = AsyncioConnection._loop
        old_thread = AsyncioConnection._loop_thread

        AsyncioConnection.handle_fork()

        self.assertIsNone(AsyncioConnection._loop)
        self.assertIsNone(AsyncioConnection._loop_thread)

        # Re-initialize for other tests
        AsyncioConnection.initialize_reactor()
        self.assertIsNotNone(AsyncioConnection._loop)
        self.assertIsNotNone(AsyncioConnection._loop_thread)
        # Should be a new loop and thread
        self.assertIsNot(AsyncioConnection._loop, old_loop)
