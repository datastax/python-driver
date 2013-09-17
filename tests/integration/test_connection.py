try:
    import unittest2 as unittest
except ImportError:
    import unittest

from functools import partial
from threading import Thread, Event

from cassandra import ConsistencyLevel
from cassandra.decoder import QueryMessage
from cassandra.io.asyncorereactor import AsyncoreConnection

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None

class ConnectionTest(object):

    klass = None

    def test_single_connection(self):
        """
        Test a single connection with sequential requests.
        """
        conn = self.klass.factory()
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
                    cb=partial(cb, count))

        conn.send_msg(
            QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
            cb=partial(cb, 0))
        event.wait()

    def test_single_connection_pipelined_requests(self):
        """
        Test a single connection with pipelined requests.
        """
        conn = self.klass.factory()
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
                cb=partial(cb, responses, i))

        event.wait()

    def test_multiple_connections(self):
        """
        Test multiple connections with pipelined requests.
        """
        conns = [self.klass.factory() for i in range(5)]
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
                    cb=partial(cb, event, conn, count))

        for event, conn in zip(events, conns):
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
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

        conn = self.klass.factory()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(all_responses, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(map(all, all_responses)):
                conn.close()
                event.set()

        def send_msgs(all_responses, thread_responses):
            for i in range(num_requests_per_conn):
                qmsg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
                conn.send_msg(qmsg, cb=partial(cb, all_responses, thread_responses, i))

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
                conn.send_msg(qmsg, cb=partial(cb, conn, event, thread_responses, i))

            event.wait()

        threads = []
        for i in range(num_conns):
            conn = self.klass.factory()
            t = Thread(target=send_msgs, args=(conn, events[i]))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()


class AsyncoreConnectionTest(ConnectionTest, unittest.TestCase):

    klass = AsyncoreConnection


class LibevConnectionTest(ConnectionTest, unittest.TestCase):

    klass = LibevConnection

    @classmethod
    def setup_class(cls):
        if LibevConnection is None:
            raise unittest.SkipTest('pyev does not appear to be installed properly')
