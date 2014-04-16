# Copyright 2013-2014 DataStax, Inc.
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

from tests.integration import PROTOCOL_VERSION

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from itertools import cycle

from cassandra import InvalidRequest
from cassandra.cluster import Cluster
from cassandra.concurrent import (execute_concurrent,
                                  execute_concurrent_with_args)
from cassandra.policies import HostDistance
from cassandra.query import tuple_factory


class ClusterTests(unittest.TestCase):

    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        self.session = self.cluster.connect()
        self.session.row_factory = tuple_factory

    def test_execute_concurrent(self):
        for num_statements in (0, 1, 2, 7, 10, 99, 100, 101, 199, 200, 201):
            # write
            statements = cycle(("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", ))
            parameters = [(i, i) for i in range(num_statements)]

            results = execute_concurrent(self.session, zip(statements, parameters))
            self.assertEqual(num_statements, len(results))
            self.assertEqual([(True, None)] * num_statements, results)

            # read
            statements = cycle(("SELECT v FROM test3rf.test WHERE k=%s", ))
            parameters = [(i, ) for i in range(num_statements)]

            results = execute_concurrent(self.session, zip(statements, parameters))
            self.assertEqual(num_statements, len(results))
            self.assertEqual([(True, [(i,)]) for i in range(num_statements)], results)

    def test_execute_concurrent_with_args(self):
        for num_statements in (0, 1, 2, 7, 10, 99, 100, 101, 199, 200, 201):
            statement = "INSERT INTO test3rf.test (k, v) VALUES (%s, %s)"
            parameters = [(i, i) for i in range(num_statements)]

            results = execute_concurrent_with_args(self.session, statement, parameters)
            self.assertEqual(num_statements, len(results))
            self.assertEqual([(True, None)] * num_statements, results)

            # read
            statement = "SELECT v FROM test3rf.test WHERE k=%s"
            parameters = [(i, ) for i in range(num_statements)]

            results = execute_concurrent_with_args(self.session, statement, parameters)
            self.assertEqual(num_statements, len(results))
            self.assertEqual([(True, [(i,)]) for i in range(num_statements)], results)

    def test_first_failure(self):
        statements = cycle(("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", ))
        parameters = [(i, i) for i in range(100)]

        # we'll get an error back from the server
        parameters[57] = ('efefef', 'awefawefawef')

        self.assertRaises(
            InvalidRequest,
            execute_concurrent, self.session, zip(statements, parameters), raise_on_first_error=True)

    def test_first_failure_client_side(self):
        statements = cycle(("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", ))
        parameters = [(i, i) for i in range(100)]

        # the driver will raise an error when binding the params
        parameters[57] = 1

        self.assertRaises(
            TypeError,
            execute_concurrent, self.session, zip(statements, parameters), raise_on_first_error=True)

    def test_no_raise_on_first_failure(self):
        statements = cycle(("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", ))
        parameters = [(i, i) for i in range(100)]

        # we'll get an error back from the server
        parameters[57] = ('efefef', 'awefawefawef')

        results = execute_concurrent(self.session, zip(statements, parameters), raise_on_first_error=False)
        for i, (success, result) in enumerate(results):
            if i == 57:
                self.assertFalse(success)
                self.assertIsInstance(result, InvalidRequest)
            else:
                self.assertTrue(success)
                self.assertEqual(None, result)

    def test_no_raise_on_first_failure_client_side(self):
        statements = cycle(("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)", ))
        parameters = [(i, i) for i in range(100)]

        # the driver will raise an error when binding the params
        parameters[57] = i

        results = execute_concurrent(self.session, zip(statements, parameters), raise_on_first_error=False)
        for i, (success, result) in enumerate(results):
            if i == 57:
                self.assertFalse(success)
                self.assertIsInstance(result, TypeError)
            else:
                self.assertTrue(success)
                self.assertEqual(None, result)
