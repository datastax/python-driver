import unittest
from functools import partial
from threading import Thread

from cassandra.decoder import QueryMessage, ConsistencyLevel
from cassandra.connection import Connection

class ConnectionTest(unittest.TestCase):

    def test_single_connection(self):
        """
        Test a single connection with sequential requests.
        """
        conn = Connection.factory()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(count, *args, **kwargs):
            count += 1
            if count >= 10:
                conn.close()
            else:
                conn.send_msg(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                    cb=partial(cb, count))

        conn.send_msg(
            QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
            cb=partial(cb, 0))

    def test_single_connection_pipelined_requests(self):
        """
        Test a single connection with pipelined requests.
        """
        conn = Connection.factory()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"
        responses = [False] * 100

        def cb(response_list, request_num, *args, **kwargs):
            response_list[request_num] = True
            if all(response_list):
                conn.close()

        for i in range(100):
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                cb=partial(cb, responses, i))

    def test_multiple_connections(self):
        """
        Test multiple connections with pipelined requests.
        """
        conns = [Connection.factory() for i in range(5)]
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(conn, count, *args, **kwargs):
            count += 1
            if count >= 10:
                conn.close()
            else:
                conn.send_msg(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                    cb=partial(cb, conn, count))

        for conn in conns:
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                cb=partial(cb, conn, 0))

    def test_multiple_threads_shared_connection(self):
        """
        Test sharing a single connections across multiple threads,
        which will result in pipelined requests.
        """
        num_requests_per_conn = 25
        num_threads = 5

        conn = Connection.factory()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(all_responses, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(map(all, all_responses)):
                conn.close()

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

    def test_multiple_threads_multiple_connections(self):
        """
        Test several threads, each with their own Connection and pipelined
        requests.
        """
        num_requests_per_conn = 25
        num_conns = 5

        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(conn, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(thread_responses):
                conn.close()

        def send_msgs(conn):
            thread_responses = [False] * num_requests_per_conn
            for i in range(num_requests_per_conn):
                qmsg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
                conn.send_msg(qmsg, cb=partial(cb, conn, thread_responses, i))

        threads = []
        for i in range(num_conns):
            conn = Connection.factory()
            t = Thread(target=send_msgs, args=(conn,))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()
