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


from tests.unit.io.utils import TimerConnectionTests
from tests import MONKEY_PATCH_LOOP, notmonkeypatch
try:
    from cassandra.io.geventreactor import GeventConnection
    import gevent.monkey
except ImportError:
    GeventConnection = None  # noqa


@unittest.skipIf(GeventConnection is None, "Skpping the gevent tests because it's not installed")
@notmonkeypatch
class GeventTimerTest(unittest.TestCase, TimerConnectionTests):
    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if not MONKEY_PATCH_LOOP:
            return
        gevent.monkey.patch_all()
        cls.connection_class = GeventConnection
        GeventConnection.initialize_reactor()

    # There is no unpatching because there is not a clear way
    # of doing it reliably
