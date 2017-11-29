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


from mock import patch, Mock
import socket


from tests.unit.io.utils import TimerTestMixin
from tests import is_monkey_patched


try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None  # noqa


class LibevTimerPatcher(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.patchers = [
            patch('socket.socket', spec=socket.socket),
            patch('cassandra.io.libevwrapper.IO')
        ]
        for p in cls.patchers:
            p.start()

    @classmethod
    def tearDownClass(cls):
        for p in cls.patchers:
            try:
                p.stop()
            except:
                pass


class LibevTimerTest(LibevTimerPatcher, TimerTestMixin):

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._libevloop._timers

    def make_connection(self):
        c = LibevConnection('1.2.3.4', cql_version='3.0.1')
        c._socket_impl = Mock()
        c._socket.return_value.send.side_effect = lambda x: len(x)
        return c

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

        self.connection = self.make_connection()
        LibevConnection.initialize_reactor()
