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

from cassandra.cluster import Cluster
from cassandra.query import tuple_factory, named_tuple_factory, dict_factory, ordered_dict_factory
from cassandra.util import OrderedDict


class RowFactoryTests(unittest.TestCase):
    """
    Test different row_factories and access code
    """

    truncate = '''
        TRUNCATE test3rf.test
    '''

    insert1 = '''
        INSERT INTO test3rf.test
            ( k , v )
        VALUES
            ( 1 , 1 )
    '''

    insert2 = '''
        INSERT INTO test3rf.test
            ( k , v )
        VALUES
            ( 2 , 2 )
    '''

    select = '''
        SELECT * FROM test3rf.test
    '''

    def test_tuple_factory(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        session.row_factory = tuple_factory

        session.execute(self.truncate)
        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], tuple)

        for row in result:
            self.assertEqual(row[0], row[1])

        self.assertEqual(result[0][0], result[0][1])
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[1][0], result[1][1])
        self.assertEqual(result[1][0], 2)

    def test_named_tuple_factoryy(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        session.row_factory = named_tuple_factory

        session.execute(self.truncate)
        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, list)

        for row in result:
            self.assertEqual(row.k, row.v)

        self.assertEqual(result[0].k, result[0].v)
        self.assertEqual(result[0].k, 1)
        self.assertEqual(result[1].k, result[1].v)
        self.assertEqual(result[1].k, 2)

    def test_dict_factory(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        session.row_factory = dict_factory

        session.execute(self.truncate)
        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)

        for row in result:
            self.assertEqual(row['k'], row['v'])

        self.assertEqual(result[0]['k'], result[0]['v'])
        self.assertEqual(result[0]['k'], 1)
        self.assertEqual(result[1]['k'], result[1]['v'])
        self.assertEqual(result[1]['k'], 2)

    def test_ordered_dict_factory(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        session.row_factory = ordered_dict_factory

        session.execute(self.truncate)
        session.execute(self.insert1)
        session.execute(self.insert2)

        result = session.execute(self.select)

        self.assertIsInstance(result, list)
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
    def setUp(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()

        ddl = '''
            CREATE TABLE test3rf.ToBeIndexed ( key blob PRIMARY KEY, "626972746864617465" blob )
            WITH COMPACT STORAGE'''

        self.session.execute(ddl)

    def tearDown(self):
        """
        Shutdown cluster
        """
        self.session.execute("DROP TABLE test3rf.ToBeIndexed")
        self.cluster.shutdown()

    def test_no_exception_on_select(self):
        """
        no exception on SELECT for numeric column name
        """
        try:
            self.session.execute('SELECT * FROM test3rf.ToBeIndexed')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)

    def test_can_select_using_alias(self):
        """
        can SELECT "<numeric col name>" AS aliases
        """
        try:
            self.session.execute('SELECT key, "626972746864617465" AS my_col from test3rf.ToBeIndexed')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)

    def test_can_select_with_dict_factory(self):
        """
        can SELECT numeric column  using  dict_factory
        """
        self.session.row_factory = dict_factory
        try:
            self.session.execute('SELECT * FROM test3rf.ToBeIndexed')
        except ValueError as e:
            self.fail("Unexpected ValueError exception: %s" % e.message)
