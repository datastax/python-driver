from tests.integration import PROTOCOL_VERSION

import logging
log = logging.getLogger(__name__)

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from itertools import cycle, count
from threading import Event

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent
from cassandra.policies import HostDistance
from cassandra.query import SimpleStatement


class QueryPagingTests(unittest.TestCase):

    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2.0+ is required for BATCH operations, currently testing against %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect()
        self.session.execute("TRUNCATE test3rf.test")

    def test_paging(self):
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        execute_concurrent(self.session, statements_and_params)

        prepared = self.session.prepare("SELECT * FROM test3rf.test")

        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
            self.session.default_fetch_size = fetch_size
            self.assertEqual(100, len(list(self.session.execute("SELECT * FROM test3rf.test"))))

            statement = SimpleStatement("SELECT * FROM test3rf.test")
            self.assertEqual(100, len(list(self.session.execute(statement))))

            self.assertEqual(100, len(list(self.session.execute(prepared))))

    def test_paging_verify(self):
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        execute_concurrent(self.session, statements_and_params)

        prepared = self.session.prepare("SELECT * FROM test3rf.test")

        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
            self.session.default_fetch_size = fetch_size
            results = self.session.execute("SELECT * FROM test3rf.test")
            result_array = []
            result_set = set()
            for result in results:
                result_array.append(result.k)
                result_set.add(result.v)
            result_array.sort()

            self.assertEqual(range(100), result_array)
            self.assertEqual(set([0]), result_set)

            statement = SimpleStatement("SELECT * FROM test3rf.test")
            results = self.session.execute(statement)
            result_array = []
            result_set = set()
            for result in results:
                result_array.append(result.k)
                result_set.add(result.v)
            result_array.sort()

            self.assertEqual(range(100), result_array)
            self.assertEqual(set([0]), result_set)

            results = self.session.execute(prepared)
            result_array = []
            result_set = set()
            for result in results:
                result_array.append(result.k)
                result_set.add(result.v)
            result_array.sort()

            self.assertEqual(range(100), result_array)
            self.assertEqual(set([0]), result_set)

    def test_paging_verify_2(self):
        ddl = '''
            CREATE TABLE test3rf.test_paging_verify_2 (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY(k1, k2)
            )'''
        self.session.execute(ddl)

        statements_and_params = zip(cycle(["INSERT INTO test3rf.test_paging_verify_2 "
                                           "(k1, k2, v) VALUES (0, %s, %s)"]),
                                    [(i, i + 1) for i in range(100)])
        execute_concurrent(self.session, statements_and_params)

        prepared = self.session.prepare("SELECT * FROM test3rf.test_paging_verify_2")

        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
            self.session.default_fetch_size = fetch_size
            results = self.session.execute("SELECT * FROM test3rf.test_paging_verify_2")
            result_array = []
            value_array = []
            for result in results:
                result_array.append(result.k2)
                value_array.append(result.v)

            self.assertEqual(range(100), result_array)
            self.assertEqual(range(1, 101), value_array)

            statement = SimpleStatement("SELECT * FROM test3rf.test_paging_verify_2")
            results = self.session.execute(statement)
            result_array = []
            value_array = []
            for result in results:
                result_array.append(result.k2)
                value_array.append(result.v)

            self.assertEqual(range(100), result_array)
            self.assertEqual(range(1, 101), value_array)

            results = self.session.execute(prepared)
            result_array = []
            value_array = []
            for result in results:
                result_array.append(result.k2)
                value_array.append(result.v)

            self.assertEqual(range(100), result_array)
            self.assertEqual(range(1, 101), value_array)

    def test_async_paging(self):
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        execute_concurrent(self.session, statements_and_params)

        prepared = self.session.prepare("SELECT * FROM test3rf.test")

        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
            self.session.default_fetch_size = fetch_size
            self.assertEqual(100, len(list(self.session.execute_async("SELECT * FROM test3rf.test").result())))

            statement = SimpleStatement("SELECT * FROM test3rf.test")
            self.assertEqual(100, len(list(self.session.execute_async(statement).result())))

            self.assertEqual(100, len(list(self.session.execute_async(prepared).result())))

    def test_async_paging_verify(self):
        ddl = '''
            CREATE TABLE test3rf.test_async_paging_verify (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY(k1, k2)
            )'''
        self.session.execute(ddl)

        statements_and_params = zip(cycle(["INSERT INTO test3rf.test_async_paging_verify "
                                           "(k1, k2, v) VALUES (0, %s, %s)"]),
                                    [(i, i + 1) for i in range(100)])
        execute_concurrent(self.session, statements_and_params)

        prepared = self.session.prepare("SELECT * FROM test3rf.test_async_paging_verify")

        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
            self.session.default_fetch_size = fetch_size
            results = self.session.execute_async("SELECT * FROM test3rf.test_async_paging_verify").result()
            result_array = []
            value_array = []
            for result in results:
                result_array.append(result.k2)
                value_array.append(result.v)

            self.assertEqual(range(100), result_array)
            self.assertEqual(range(1, 101), value_array)

            statement = SimpleStatement("SELECT * FROM test3rf.test_async_paging_verify")
            results = self.session.execute_async(statement).result()
            result_array = []
            value_array = []
            for result in results:
                result_array.append(result.k2)
                value_array.append(result.v)

            self.assertEqual(range(100), result_array)
            self.assertEqual(range(1, 101), value_array)

            results = self.session.execute_async(prepared).result()
            result_array = []
            value_array = []
            for result in results:
                result_array.append(result.k2)
                value_array.append(result.v)

            self.assertEqual(range(100), result_array)
            self.assertEqual(range(1, 101), value_array)

    def test_paging_callbacks(self):
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        execute_concurrent(self.session, statements_and_params)

        prepared = self.session.prepare("SELECT * FROM test3rf.test")

        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
            self.session.default_fetch_size = fetch_size
            future = self.session.execute_async("SELECT * FROM test3rf.test")

            event = Event()
            counter = count()

            def handle_page(rows, future, counter):
                for row in rows:
                    counter.next()

                if future.has_more_pages:
                    future.start_fetching_next_page()
                else:
                    event.set()

            def handle_error(err):
                event.set()
                self.fail(err)

            future.add_callbacks(callback=handle_page, callback_args=(future, counter), errback=handle_error)
            event.wait()
            self.assertEquals(counter.next(), 100)

            # simple statement
            future = self.session.execute_async(SimpleStatement("SELECT * FROM test3rf.test"))
            event.clear()
            counter = count()

            future.add_callbacks(callback=handle_page, callback_args=(future, counter), errback=handle_error)
            event.wait()
            self.assertEquals(counter.next(), 100)

            # prepared statement
            future = self.session.execute_async(prepared)
            event.clear()
            counter = count()

            future.add_callbacks(callback=handle_page, callback_args=(future, counter), errback=handle_error)
            event.wait()
            self.assertEquals(counter.next(), 100)
