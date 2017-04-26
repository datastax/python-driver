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

from itertools import cycle
from six import next
import sys, logging, traceback

from cassandra import InvalidRequest, ConsistencyLevel, ReadTimeout, WriteTimeout, OperationTimedOut, \
    ReadFailure, WriteFailure
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args, ExecutionResult
from cassandra.policies import HostDistance
from cassandra.query import tuple_factory, SimpleStatement

from tests.integration import use_singledc, PROTOCOL_VERSION

from six import next

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class ClusterTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        if PROTOCOL_VERSION < 3:
            cls.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        cls.session = cls.cluster.connect()
        cls.session.row_factory = tuple_factory

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()

    def execute_concurrent_helper(self, session, query, results_generator=False):
        count = 0
        while count < 100:
            try:
                return execute_concurrent(session, query, results_generator=False)
            except (ReadTimeout, WriteTimeout, OperationTimedOut, ReadFailure, WriteFailure):
                ex_type, ex, tb = sys.exc_info()
                log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                count += 1

        raise RuntimeError("Failed to execute query after 100 attempts: {0}".format(query))

    def execute_concurrent_args_helper(self, session, query, params, results_generator=False):
        count = 0
        while count < 100:
            try:
                return execute_concurrent_with_args(session, query, params, results_generator=results_generator)
            except (ReadTimeout, WriteTimeout, OperationTimedOut, ReadFailure, WriteFailure):
                ex_type, ex, tb = sys.exc_info()
                log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb

        raise RuntimeError("Failed to execute query after 100 attempts: {0}".format(query))

    def test_execute_concurrent(self):
        for num_statements in (0, 1, 2, 7, 10, 99, 100, 101, 199, 200, 201):
            # write
            statement = SimpleStatement(
                "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
                consistency_level=ConsistencyLevel.QUORUM)
            statements = cycle((statement, ))
            parameters = [(i, i) for i in range(num_statements)]

            results = self.execute_concurrent_helper(self.session, list(zip(statements, parameters)))
            self.assertEqual(num_statements, len(results))
            for success, result in results:
                self.assertTrue(success)
                self.assertFalse(result)

            # read
            statement = SimpleStatement(
                "SELECT v FROM test3rf.test WHERE k=%s",
                consistency_level=ConsistencyLevel.QUORUM)
            statements = cycle((statement, ))
            parameters = [(i, ) for i in range(num_statements)]

            results = self.execute_concurrent_helper(self.session, list(zip(statements, parameters)))
            self.assertEqual(num_statements, len(results))
            self.assertEqual([(True, [(i,)]) for i in range(num_statements)], results)

    def test_execute_concurrent_with_args(self):
        for num_statements in (0, 1, 2, 7, 10, 99, 100, 101, 199, 200, 201):
            statement = SimpleStatement(
                "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
                consistency_level=ConsistencyLevel.QUORUM)
            parameters = [(i, i) for i in range(num_statements)]

            results = self.execute_concurrent_args_helper(self.session, statement, parameters)
            self.assertEqual(num_statements, len(results))
            for success, result in results:
                self.assertTrue(success)
                self.assertFalse(result)

            # read
            statement = SimpleStatement(
                "SELECT v FROM test3rf.test WHERE k=%s",
                consistency_level=ConsistencyLevel.QUORUM)
            parameters = [(i, ) for i in range(num_statements)]

            results = self.execute_concurrent_args_helper(self.session, statement, parameters)
            self.assertEqual(num_statements, len(results))
            self.assertEqual([(True, [(i,)]) for i in range(num_statements)], results)

    def test_execute_concurrent_with_args_generator(self):
        """
        Test to validate that generator based results are surfaced correctly

        Repeatedly inserts data into a a table and attempts to query it. It then validates that the
        results are returned in the order expected

        @since 2.7.0
        @jira_ticket PYTHON-123
        @expected_result all data should be returned in order.

        @test_category queries:async
        """
        for num_statements in (0, 1, 2, 7, 10, 99, 100, 101, 199, 200, 201):
            statement = SimpleStatement(
                "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
                consistency_level=ConsistencyLevel.QUORUM)
            parameters = [(i, i) for i in range(num_statements)]

            results = self.execute_concurrent_args_helper(self.session, statement, parameters, results_generator=True)
            for success, result in results:
                self.assertTrue(success)
                self.assertFalse(result)

            results = self.execute_concurrent_args_helper(self.session, statement, parameters, results_generator=True)
            for result in results:
                self.assertTrue(isinstance(result, ExecutionResult))
                self.assertTrue(result.success)
                self.assertFalse(result.result_or_exc)

            # read
            statement = SimpleStatement(
                "SELECT v FROM test3rf.test WHERE k=%s",
                consistency_level=ConsistencyLevel.QUORUM)
            parameters = [(i, ) for i in range(num_statements)]

            results = self.execute_concurrent_args_helper(self.session, statement, parameters, results_generator=True)

            for i in range(num_statements):
                result = next(results)
                self.assertEqual((True, [(i,)]), result)
            self.assertRaises(StopIteration, next, results)

    def test_execute_concurrent_paged_result(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2+ is required for Paging, currently testing against %r"
                % (PROTOCOL_VERSION,))

        num_statements = 201
        statement = SimpleStatement(
            "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.QUORUM)
        parameters = [(i, i) for i in range(num_statements)]

        results = self.execute_concurrent_args_helper(self.session, statement, parameters)
        self.assertEqual(num_statements, len(results))
        for success, result in results:
            self.assertTrue(success)
            self.assertFalse(result)

        # read
        statement = SimpleStatement(
            "SELECT * FROM test3rf.test LIMIT %s",
            consistency_level=ConsistencyLevel.QUORUM,
            fetch_size=int(num_statements / 2))

        results = self.execute_concurrent_args_helper(self.session, statement, [(num_statements,)])
        self.assertEqual(1, len(results))
        self.assertTrue(results[0][0])
        result = results[0][1]
        self.assertTrue(result.has_more_pages)
        self.assertEqual(num_statements, sum(1 for _ in result))

    def test_execute_concurrent_paged_result_generator(self):
        """
        Test to validate that generator based results are surfaced correctly when paging is used

        Inserts data into a a table and attempts to query it. It then validates that the
        results are returned as expected (no order specified)

        @since 2.7.0
        @jira_ticket PYTHON-123
        @expected_result all data should be returned in order.

        @test_category paging
        """
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest(
                "Protocol 2+ is required for Paging, currently testing against %r"
                % (PROTOCOL_VERSION,))

        num_statements = 201
        statement = SimpleStatement(
            "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.QUORUM)
        parameters = [(i, i) for i in range(num_statements)]

        results = self.execute_concurrent_args_helper(self.session, statement, parameters, results_generator=True)
        self.assertEqual(num_statements, sum(1 for _ in results))

        # read
        statement = SimpleStatement(
            "SELECT * FROM test3rf.test LIMIT %s",
            consistency_level=ConsistencyLevel.QUORUM,
            fetch_size=int(num_statements / 2))

        paged_results_gen = self.execute_concurrent_args_helper(self.session, statement, [(num_statements,)], results_generator=True)

        # iterate over all the result and make sure we find the correct number.
        found_results = 0
        for result_tuple in paged_results_gen:
            paged_result = result_tuple[1]
            for _ in paged_result:
                found_results += 1

        self.assertEqual(found_results, num_statements)

    def test_first_failure(self):
        statements = cycle(("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", ))
        parameters = [(i, i) for i in range(100)]

        # we'll get an error back from the server
        parameters[57] = ('efefef', 'awefawefawef')

        self.assertRaises(
            InvalidRequest,
            execute_concurrent, self.session, list(zip(statements, parameters)), raise_on_first_error=True)

    def test_first_failure_client_side(self):
        statement = SimpleStatement(
            "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.QUORUM)
        statements = cycle((statement, ))
        parameters = [(i, i) for i in range(100)]

        # the driver will raise an error when binding the params
        parameters[57] = 1

        self.assertRaises(
            TypeError,
            execute_concurrent, self.session, list(zip(statements, parameters)), raise_on_first_error=True)

    def test_no_raise_on_first_failure(self):
        statement = SimpleStatement(
            "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.QUORUM)
        statements = cycle((statement, ))
        parameters = [(i, i) for i in range(100)]

        # we'll get an error back from the server
        parameters[57] = ('efefef', 'awefawefawef')

        results = execute_concurrent(self.session, list(zip(statements, parameters)), raise_on_first_error=False)
        for i, (success, result) in enumerate(results):
            if i == 57:
                self.assertFalse(success)
                self.assertIsInstance(result, InvalidRequest)
            else:
                self.assertTrue(success)
                self.assertFalse(result)

    def test_no_raise_on_first_failure_client_side(self):
        statement = SimpleStatement(
            "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.QUORUM)
        statements = cycle((statement, ))
        parameters = [(i, i) for i in range(100)]

        # the driver will raise an error when binding the params
        parameters[57] = 1

        results = execute_concurrent(self.session, list(zip(statements, parameters)), raise_on_first_error=False)
        for i, (success, result) in enumerate(results):
            if i == 57:
                self.assertFalse(success)
                self.assertIsInstance(result, TypeError)
            else:
                self.assertTrue(success)
                self.assertFalse(result)
