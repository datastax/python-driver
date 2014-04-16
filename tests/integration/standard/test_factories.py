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


class TestFactories(unittest.TestCase):
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
