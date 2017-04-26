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

from tests.integration import get_server_versions, use_singledc, PROTOCOL_VERSION, BasicSharedKeyspaceUnitTestCaseWFunctionTable, BasicSharedKeyspaceUnitTestCase, execute_until_pass

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from cassandra.cluster import Cluster, ResultSet
from cassandra.query import tuple_factory, named_tuple_factory, dict_factory, ordered_dict_factory
from cassandra.util import OrderedDict


def setup_module():
    use_singledc()


class NameTupleFactory(BasicSharedKeyspaceUnitTestCase):

    def setUp(self):
        super(NameTupleFactory, self).setUp()
        self.session.row_factory = named_tuple_factory
        ddl = '''
                CREATE TABLE {0}.{1} (
                    k int PRIMARY KEY,
                    v1 text,
                    v2 text,
                    v3 text)'''.format(self.ks_name, self.function_table_name)
        self.session.execute(ddl)
        execute_until_pass(self.session, ddl)

    def test_sanitizing(self):
        """
        Test to ensure that same named results are surfaced in the NamedTupleFactory

        Creates a table with a few different text fields. Inserts a few values in that table.
        It then fetches the values and confirms that despite all be being selected as the same name
        they are propagated in the result set differently.

        @since 3.3
        @jira_ticket PYTHON-467
        @expected_result duplicate named results have unique row names.

        @test_category queries
        """

        for x in range(5):
            insert1 = '''
                INSERT INTO {0}.{1}
                    ( k , v1, v2, v3 )
                VALUES
                    ( 1 , 'v1{2}', 'v2{2}','v3{2}' )
          '''.format(self.keyspace_name, self.function_table_name, str(x))
            self.session.execute(insert1)

        query = "SELECT v1 AS duplicate, v2 AS duplicate, v3 AS duplicate from {0}.{1}".format(self.ks_name, self.function_table_name)
        rs = self.session.execute(query)
        row = rs[0]
        self.assertTrue(hasattr(row, 'duplicate'))
        self.assertTrue(hasattr(row, 'duplicate_'))
        self.assertTrue(hasattr(row, 'duplicate__'))


class RowFactoryTests(BasicSharedKeyspaceUnitTestCaseWFunctionTable):
    """
    Test different row_factories and access code
    """
    def setUp(self):
        super(RowFactoryTests, self).setUp()
        self.insert1 = '''
            INSERT INTO {0}.{1}
                ( k , v )
            VALUES
                ( 1 , 1 )
        '''.format(self.keyspace_name, self.function_table_name)

        self.insert2 = '''
            INSERT INTO {0}.{1}
                ( k , v )
            VALUES
                ( 2 , 2 )
        '''.format(self.keyspace_name, self.function_table_name)

        self.select = '''
            SELECT * FROM {0}.{1}
        '''.format(self.keyspace_name, self.function_table_name)

    def tearDown(self):
        self.drop_function_table()

    def test_tuple_factory(self):
        session = self.session
        session.row_factory = tuple_factory

        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, ResultSet)
        self.assertIsInstance(result[0], tuple)

        for row in result:
            self.assertEqual(row[0], row[1])

        self.assertEqual(result[0][0], result[0][1])
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[1][0], result[1][1])
        self.assertEqual(result[1][0], 2)

    def test_named_tuple_factory(self):
        session = self.session
        session.row_factory = named_tuple_factory

        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, ResultSet)
        result = list(result)

        for row in result:
            self.assertEqual(row.k, row.v)

        self.assertEqual(result[0].k, result[0].v)
        self.assertEqual(result[0].k, 1)
        self.assertEqual(result[1].k, result[1].v)
        self.assertEqual(result[1].k, 2)

    def test_dict_factory(self):
        session = self.session
        session.row_factory = dict_factory

        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, ResultSet)
        self.assertIsInstance(result[0], dict)

        for row in result:
            self.assertEqual(row['k'], row['v'])

        self.assertEqual(result[0]['k'], result[0]['v'])
        self.assertEqual(result[0]['k'], 1)
        self.assertEqual(result[1]['k'], result[1]['v'])
        self.assertEqual(result[1]['k'], 2)

    def test_ordered_dict_factory(self):
        session = self.session
        session.row_factory = ordered_dict_factory

        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, ResultSet)
        self.assertIsInstance(result[0], OrderedDict)

        for row in result:
            self.assertEqual(row['k'], row['v'])

        self.assertEqual(result[0]['k'], result[0]['v'])
        self.assertEqual(result[0]['k'], 1)
        self.assertEqual(result[1]['k'], result[1]['v'])
        self.assertEqual(result[1]['k'], 2)


class NamedTupleFactoryAndNumericColNamesTests(unittest.TestCase):
    """
    Test for PYTHON-122: Improve Error Handling/Reporting for named_tuple_factory and Numeric Column Names
    """
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls._cass_version, cls._cql_version = get_server_versions()
        ddl = '''
            CREATE TABLE test1rf.table_num_col ( key blob PRIMARY KEY, "626972746864617465" blob )
            WITH COMPACT STORAGE'''
        cls.session.execute(ddl)

    @classmethod
    def teardown_class(cls):
        cls.session.execute("DROP TABLE test1rf.table_num_col")
        cls.cluster.shutdown()

    def test_no_exception_on_select(self):
        """
        no exception on SELECT for numeric column name
        """
        try:
            self.session.execute('SELECT * FROM test1rf.table_num_col')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)

    def test_can_select_using_alias(self):
        """
        can SELECT "<numeric col name>" AS aliases
        """
        if self._cass_version < (2, 0, 0):
            raise unittest.SkipTest("Alias in SELECT not supported before 2.0")

        try:
            self.session.execute('SELECT key, "626972746864617465" AS my_col from test1rf.table_num_col')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)

    def test_can_select_with_dict_factory(self):
        """
        can SELECT numeric column  using  dict_factory
        """
        self.session.row_factory = dict_factory
        try:
            self.session.execute('SELECT * FROM test1rf.table_num_col')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)
