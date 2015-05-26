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


from tests.integration import use_singledc, PROTOCOL_VERSION

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa
from cassandra import InvalidRequest

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement, UNSET_VALUE


def setup_module():
    use_singledc()


class PreparedStatementTests(unittest.TestCase):

    def test_basic(self):
        """
        Test basic PreparedStatement usage
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE preparedtests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        session.set_keyspace("preparedtests")
        session.execute(
            """
            CREATE TABLE cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)

        prepared = session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind(('a', 'b', 'c'))

        session.execute(bound)

        prepared = session.prepare(
            """
            SELECT * FROM cf0 WHERE a=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind(('a'))
        results = session.execute(bound)
        self.assertEqual(results, [('a', 'b', 'c')])

        # test with new dict binding
        prepared = session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind({
            'a': 'x',
            'b': 'y',
            'c': 'z'
        })

        session.execute(bound)

        prepared = session.prepare(
            """
            SELECT * FROM cf0 WHERE a=?
            """)

        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind({'a': 'x'})
        results = session.execute(bound)
        self.assertEqual(results, [('x', 'y', 'z')])

        cluster.shutdown()

    def test_missing_primary_key(self):
        """
        Ensure an InvalidRequest is thrown
        when prepared statements are missing the primary key
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (v) VALUES  (?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1,))
        self.assertRaises(InvalidRequest, session.execute, bound)

        cluster.shutdown()

    def test_missing_primary_key_dicts(self):
        """
        Ensure an InvalidRequest is thrown
        when prepared statements are missing the primary key
        with dict bindings
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (v) VALUES  (?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind({'v': 1})
        self.assertRaises(InvalidRequest, session.execute, bound)

        cluster.shutdown()

    def test_too_many_bind_values(self):
        """
        Ensure a ValueError is thrown when attempting to bind too many variables
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (v) VALUES  (?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        self.assertRaises(ValueError, prepared.bind, (1, 2))

        cluster.shutdown()

    def test_too_many_bind_values_dicts(self):
        """
        Ensure an error is thrown when attempting to bind the wrong values
        with dict bindings
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)

        # too many values
        self.assertRaises(ValueError, prepared.bind, {'k': 1, 'v': 2, 'v2': 3})

        # right number, but one does not belong
        if PROTOCOL_VERSION < 4:
            # pre v4, the driver bails with key error when 'v' is found missing
            self.assertRaises(KeyError, prepared.bind, {'k': 1, 'v2': 3})
        else:
            # post v4, the driver uses UNSET_VALUE for 'v' and bails when 'v2' is unbound
            self.assertRaises(ValueError, prepared.bind, {'k': 1, 'v2': 3})

        # also catch too few variables with dicts
        self.assertIsInstance(prepared, PreparedStatement)
        if PROTOCOL_VERSION < 4:
            self.assertRaises(KeyError, prepared.bind, {})
        else:
            # post v4, the driver attempts to use UNSET_VALUE for unspecified keys
            self.assertRaises(ValueError, prepared.bind, {})

        cluster.shutdown()

    def test_none_values(self):
        """
        Ensure binding None is handled correctly
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        session.execute(bound)

        prepared = session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind((1,))
        results = session.execute(bound)
        self.assertEqual(results[0].v, None)

        cluster.shutdown()

    def test_unset_values(self):
        """
        Test to validate that UNSET_VALUEs are bound, and have the expected effect

        Prepare a statement and insert all values. Then follow with execute excluding
        parameters. Verify that the original values are unaffected.

        @since 2.6.0

        @jira_ticket PYTHON-317
        @expected_result UNSET_VALUE is implicitly added to bind parameters, and properly encoded, leving unset values unaffected.

        @test_category prepared_statements:binding
        """
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Binding UNSET values is not supported in protocol version < 4")

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        # table with at least two values so one can be used as a marker
        session.execute("CREATE TABLE IF NOT EXISTS test1rf.test_unset_values (k int PRIMARY KEY, v0 int, v1 int)")
        insert = session.prepare("INSERT INTO test1rf.test_unset_values (k, v0, v1) VALUES  (?, ?, ?)")
        select = session.prepare("SELECT * FROM test1rf.test_unset_values WHERE k=?")

        bind_expected = [
            # initial condition
            ((0, 0, 0),                            (0, 0, 0)),
            # unset implicit
            ((0, 1,),                              (0, 1, 0)),
            ({'k': 0, 'v0': 2},                    (0, 2, 0)),
            ({'k': 0, 'v1': 1},                    (0, 2, 1)),
            # unset explicit
            ((0, 3, UNSET_VALUE),                  (0, 3, 1)),
            ((0, UNSET_VALUE, 2),                  (0, 3, 2)),
            ({'k': 0, 'v0': 4, 'v1': UNSET_VALUE}, (0, 4, 2)),
            ({'k': 0, 'v0': UNSET_VALUE, 'v1': 3}, (0, 4, 3)),
            # nulls still work
            ((0, None, None),                      (0, None, None)),
        ]

        for params, expected in bind_expected:
            session.execute(insert, params)
            results = session.execute(select, (0,))
            self.assertEqual(results[0], expected)

        self.assertRaises(ValueError, session.execute, select, (UNSET_VALUE, 0, 0))

        cluster.shutdown()

    def test_no_meta(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (0, 0)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind(None)
        session.execute(bound)

        prepared = session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=0
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind(None)
        results = session.execute(bound)
        self.assertEqual(results[0].v, 0)

        cluster.shutdown()

    def test_none_values_dicts(self):
        """
        Ensure binding None is handled correctly with dict bindings
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        # test with new dict binding
        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind({'k': 1, 'v': None})
        session.execute(bound)

        prepared = session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind({'k': 1})
        results = session.execute(bound)
        self.assertEqual(results[0].v, None)

        cluster.shutdown()

    def test_async_binding(self):
        """
        Ensure None binding over async queries
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        future = session.execute_async(prepared, (873, None))
        future.result()

        prepared = session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        future = session.execute_async(prepared, (873,))
        results = future.result()
        self.assertEqual(results[0].v, None)

        cluster.shutdown()

    def test_async_binding_dicts(self):
        """
        Ensure None binding over async queries with dict bindings
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        prepared = session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        future = session.execute_async(prepared, {'k': 873, 'v': None})
        future.result()

        prepared = session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        future = session.execute_async(prepared, {'k': 873})
        results = future.result()
        self.assertEqual(results[0].v, None)

        cluster.shutdown()
