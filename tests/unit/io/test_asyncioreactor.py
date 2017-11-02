from cassandra.io.asyncioreactor import AsyncioConnection
from tests import is_monkey_patched
from tests.unit.io.utils import TimerConnectionTests

import unittest


class AsyncioTimerConnectionTests(unittest.TestCase, TimerConnectionTests):

    @classmethod
    def setUpClass(cls):
        if is_monkey_patched():
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    def test_timer_cancellation(self):
        pass
    test_timer_cancellation.__test__ = False
