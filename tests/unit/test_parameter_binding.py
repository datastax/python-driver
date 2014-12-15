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

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from cassandra.encoder import Encoder
from cassandra.query import bind_params, ValueSequence
from cassandra.query import PreparedStatement, BoundStatement
from cassandra.cqltypes import Int32Type
from cassandra.util import OrderedDict

from six.moves import xrange


class ParamBindingTest(unittest.TestCase):

    def test_bind_sequence(self):
        result = bind_params("%s %s %s", (1, "a", 2.0), Encoder())
        self.assertEqual(result, "1 'a' 2.0")

    def test_bind_map(self):
        result = bind_params("%(a)s %(b)s %(c)s", dict(a=1, b="a", c=2.0), Encoder())
        self.assertEqual(result, "1 'a' 2.0")

    def test_sequence_param(self):
        result = bind_params("%s", (ValueSequence((1, "a", 2.0)),), Encoder())
        self.assertEqual(result, "( 1 , 'a' , 2.0 )")

    def test_generator_param(self):
        result = bind_params("%s", ((i for i in xrange(3)),), Encoder())
        self.assertEqual(result, "[ 0 , 1 , 2 ]")

    def test_none_param(self):
        result = bind_params("%s", (None,), Encoder())
        self.assertEqual(result, "NULL")

    def test_list_collection(self):
        result = bind_params("%s", (['a', 'b', 'c'],), Encoder())
        self.assertEqual(result, "[ 'a' , 'b' , 'c' ]")

    def test_set_collection(self):
        result = bind_params("%s", (set(['a', 'b']),), Encoder())
        self.assertIn(result, ("{ 'a' , 'b' }", "{ 'b' , 'a' }"))

    def test_map_collection(self):
        vals = OrderedDict()
        vals['a'] = 'a'
        vals['b'] = 'b'
        vals['c'] = 'c'
        result = bind_params("%s", (vals,), Encoder())
        self.assertEqual(result, "{ 'a' : 'a' , 'b' : 'b' , 'c' : 'c' }")

    def test_quote_escaping(self):
        result = bind_params("%s", ("""'ef''ef"ef""ef'""",), Encoder())
        self.assertEqual(result, """'''ef''''ef"ef""ef'''""")


class BoundStatementTestCase(unittest.TestCase):

    def test_invalid_argument_type(self):
        keyspace = 'keyspace1'
        column_family = 'cf1'

        column_metadata = [
            (keyspace, column_family, 'foo1', Int32Type),
            (keyspace, column_family, 'foo2', Int32Type)
        ]

        prepared_statement = PreparedStatement(column_metadata=column_metadata,
                                               query_id=None,
                                               routing_key_indexes=[],
                                               query=None,
                                               keyspace=keyspace,
                                               protocol_version=2)
        bound_statement = BoundStatement(prepared_statement=prepared_statement)

        values = ['nonint', 1]

        try:
            bound_statement.bind(values)
        except TypeError as e:
            self.assertIn('foo1', str(e))
            self.assertIn('Int32Type', str(e))
            self.assertIn('str', str(e))
        else:
            self.fail('Passed invalid type but exception was not thrown')

        values = [1, ['1', '2']]

        try:
            bound_statement.bind(values)
        except TypeError as e:
            self.assertIn('foo2', str(e))
            self.assertIn('Int32Type', str(e))
            self.assertIn('list', str(e))
        else:
            self.fail('Passed invalid type but exception was not thrown')

    def test_inherit_fetch_size(self):
        keyspace = 'keyspace1'
        column_family = 'cf1'

        column_metadata = [
            (keyspace, column_family, 'foo1', Int32Type),
            (keyspace, column_family, 'foo2', Int32Type)
        ]

        prepared_statement = PreparedStatement(column_metadata=column_metadata,
                                               query_id=None,
                                               routing_key_indexes=[],
                                               query=None,
                                               keyspace=keyspace,
                                               protocol_version=2,
                                               fetch_size=1234)
        bound_statement = BoundStatement(prepared_statement=prepared_statement)
        self.assertEqual(1234, bound_statement.fetch_size)

    def test_too_few_parameters_for_key(self):
        keyspace = 'keyspace1'
        column_family = 'cf1'

        column_metadata = [
            (keyspace, column_family, 'foo1', Int32Type),
            (keyspace, column_family, 'foo2', Int32Type)
        ]

        prepared_statement = PreparedStatement(column_metadata=column_metadata,
                                               query_id=None,
                                               routing_key_indexes=[0, 1],
                                               query=None,
                                               keyspace=keyspace,
                                               protocol_version=2,
                                               fetch_size=1234)

        self.assertRaises(ValueError, prepared_statement.bind, (1,))

        bound = prepared_statement.bind((1,2))
        self.assertEqual(bound.keyspace, keyspace)
