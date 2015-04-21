# Copyright 2013-2015 DataStax, Inc.
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

from functools import partial
from six.moves import range
from threading import Thread, Event

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cluster import NoHostAvailable
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.protocol import QueryMessage

from tests import is_monkey_patched
from tests.integration import use_singledc, PROTOCOL_VERSION

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None


def setup_module():
    use_singledc()


class ConnectionTests(object):

    klass = None

    def setUp(self):
        self.klass.initialize_reactor()

    def get_connection(self):
        """
        Helper method to solve automated testing issues within Jenkins.
        Officially patched under the 2.0 branch through
        17998ef72a2fe2e67d27dd602b6ced33a58ad8ef, but left as is for the
        1.0 branch due to possible regressions for fixing an
        automated testing edge-case.
        """
        conn = None
        e = None
        for i in range(5):
            try:
                conn = self.klass.factory(host='127.0.0.1', timeout=5, protocol_version=PROTOCOL_VERSION)
                break
            except (OperationTimedOut, NoHostAvailable) as e:
                continue

        if conn:
            return conn
        else:
            raise e

    def test_single_connection(self):
        """
        Test a single connection with sequential requests.
        """
        conn = self.get_connection()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"
        event = Event()

        def cb(count, *args, **kwargs):
            count += 1
            if count >= 10:
                conn.close()
                event.set()
            else:
                conn.send_msg(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                    request_id=0,
                    cb=partial(cb, count))

        conn.send_msg(
            QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
            request_id=0,
            cb=partial(cb, 0))
        event.wait()

    def test_single_connection_pipelined_requests(self):
        """
        Test a single connection with pipelined requests.
        """
        conn = self.get_connection()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"
        responses = [False] * 100
        event = Event()

        def cb(response_list, request_num, *args, **kwargs):
            response_list[request_num] = True
            if all(response_list):
                conn.close()
                event.set()

        for i in range(100):
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                request_id=i,
                cb=partial(cb, responses, i))

        event.wait()

    def test_multiple_connections(self):
        """
        Test multiple connections with pipelined requests.
        """
        conns = [self.get_connection() for i in range(5)]
        events = [Event() for i in range(5)]
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(event, conn, count, *args, **kwargs):
            count += 1
            if count >= 10:
                conn.close()
                event.set()
            else:
                conn.send_msg(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                    request_id=count,
                    cb=partial(cb, event, conn, count))

        for event, conn in zip(events, conns):
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                request_id=0,
                cb=partial(cb, event, conn, 0))

        for event in events:
            event.wait()

    def test_multiple_threads_shared_connection(self):
        """
        Test sharing a single connections across multiple threads,
        which will result in pipelined requests.
        """
        num_requests_per_conn = 25
        num_threads = 5
        event = Event()

        conn = self.get_connection()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(all_responses, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(map(all, all_responses)):
                conn.close()
                event.set()

        def send_msgs(all_responses, thread_responses):
            for i in range(num_requests_per_conn):
                qmsg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
                with conn.lock:
                    request_id = conn.get_request_id()
                conn.send_msg(qmsg, request_id, cb=partial(cb, all_responses, thread_responses, i))

        all_responses = []
        threads = []
        for i in range(num_threads):
            thread_responses = [False] * num_requests_per_conn
            all_responses.append(thread_responses)
            t = Thread(target=send_msgs, args=(all_responses, thread_responses))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        event.wait()

    def test_multiple_threads_multiple_connections(self):
        """
        Test several threads, each with their own Connection and pipelined
        requests.
        """
        num_requests_per_conn = 25
        num_conns = 5
        events = [Event() for i in range(5)]

        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(conn, event, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(thread_responses):
                conn.close()
                event.set()

        def send_msgs(conn, event):
            thread_responses = [False] * num_requests_per_conn
            for i in range(num_requests_per_conn):
                qmsg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
                with conn.lock:
                    request_id = conn.get_request_id()
                conn.send_msg(qmsg, request_id, cb=partial(cb, conn, event, thread_responses, i))

            event.wait()

        threads = []
        for i in range(num_conns):
            conn = self.get_connection()
            t = Thread(target=send_msgs, args=(conn, events[i]))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()


class AsyncoreConnectionTests(ConnectionTests, unittest.TestCase):

    klass = AsyncoreConnection

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test asyncore with monkey patching")
        ConnectionTests.setUp(self)


class LibevConnectionTests(ConnectionTests, unittest.TestCase):

    klass = LibevConnection

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest(
                'libev does not appear to be installed properly')
        ConnectionTests.setUp(self)
