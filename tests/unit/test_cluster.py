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
    import unittest  # noqa

from mock import patch, Mock

from cassandra import ConsistencyLevel
from cassandra.cluster import _Scheduler, Session
from cassandra.query import SimpleStatement


class SchedulerTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a ticket

    @patch('time.time', return_value=3)  # always queue at same time
    @patch('cassandra.cluster._Scheduler.run')  # don't actually run the thread
    def test_event_delay_timing(self, *args):
        """
        Schedule something with a time collision to make sure the heap comparison works

        PYTHON-473
        """
        sched = _Scheduler(None)
        sched.schedule(0, lambda: None)
        sched.schedule(0, lambda: None)  # pre-473: "TypeError: unorderable types: function() < function()"t


class SessionTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a PR

    @patch('cassandra.cluster.ResponseFuture._make_query_plan')
    def test_default_serial_consistency_level(self, *args):
        """
        Make sure default_serial_consistency_level passes through to a query message.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        s = Session(Mock(protocol_version=4), [])

        # default is None
        self.assertIsNone(s.default_serial_consistency_level)

        sentinel = 1001
        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL, sentinel):
            s.default_serial_consistency_level = cl

            # default is passed through
            f = s._create_response_future(query='', parameters=[], trace=False, custom_payload={}, timeout=100)
            self.assertEqual(f.message.serial_consistency_level, cl)

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s._create_response_future(SimpleStatement(query_string='', serial_consistency_level=cl_override), parameters=[], trace=False, custom_payload={}, timeout=100)
                self.assertEqual(s.default_serial_consistency_level, cl)
                self.assertEqual(f.message.serial_consistency_level, cl_override)
