# Copyright 2013-2016 DataStax, Inc.
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


from tests.unit.io.utils import TimerConnectionTests
from tests import notmonkeypatch
try:
    from cassandra.io.geventreactor import GeventConnection
    import gevent.monkey
    from tests.unit.io.gevent_utils import restore_saved_module
except ImportError:
    GeventConnection = None  # noqa

@unittest.skipUnless(GeventConnection is not None, "Skpping the gevent tests because it's not installed")
@notmonkeypatch
class GeventTimerTest(unittest.TestCase, TimerConnectionTests):

    def setUp(self):
        self.connection_class = GeventConnection
        #We only to patch the time module
        gevent.monkey.patch_time()
        GeventConnection.initialize_reactor()

    def tearDown(self):
        restore_saved_module("time")
        GeventConnection._timers = None