AsyncioConnection, ASYNCIO_AVAILABLE = None, False
try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    ASYNCIO_AVAILABLE = True
except (ImportError, SyntaxError):
    AsyncioConnection = None
    ASYNCIO_AVAILABLE = False

from tests import is_monkey_patched, connection_class
from tests.unit.io.utils import TimerCallback, TimerTestMixin, submit_and_wait_for_completion

from unittest.mock import patch, MagicMock

import unittest
import time

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
