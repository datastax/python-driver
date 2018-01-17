from cassandra.io.asyncioreactor import AsyncioConnection
from tests import is_monkey_patched
from tests.unit.io.utils import TimerCallback, TimerTestMixin

from mock import patch

import unittest
import time


class AsyncioTimerTests(TimerTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if is_monkey_patched():
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    @classmethod
    def tearDownClass(cls):
        AsyncioConnection._loop.stop()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        raise RuntimeError('no TimerManager for AsyncioConnection')

    def setUp(self):
        socket_patcher = patch('socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        super(AsyncioTimerTests, self).setUp()

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
