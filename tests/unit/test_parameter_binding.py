try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from cassandra.query import bind_params, ValueSequence
from cassandra.query import PreparedStatement, BoundStatement
from cassandra.query import InvalidParameterTypeError
from cassandra.cqltypes import Int32Type

try:
    from collections import OrderedDict
except ImportError:  # Python <2.7
    from cassandra.util import OrderedDict # NOQA


class ParamBindingTest(unittest.TestCase):

    def test_bind_sequence(self):
        result = bind_params("%s %s %s", (1, "a", 2.0))
        self.assertEquals(result, "1 'a' 2.0")

    def test_bind_map(self):
        result = bind_params("%(a)s %(b)s %(c)s", dict(a=1, b="a", c=2.0))
        self.assertEquals(result, "1 'a' 2.0")

    def test_sequence_param(self):
        result = bind_params("%s", (ValueSequence((1, "a", 2.0)),))
        self.assertEquals(result, "( 1 , 'a' , 2.0 )")

    def test_generator_param(self):
        result = bind_params("%s", ((i for i in xrange(3)),))
        self.assertEquals(result, "[ 0 , 1 , 2 ]")

    def test_none_param(self):
        result = bind_params("%s", (None,))
        self.assertEquals(result, "NULL")

    def test_list_collection(self):
        result = bind_params("%s", (['a', 'b', 'c'],))
        self.assertEquals(result, "[ 'a' , 'b' , 'c' ]")

    def test_set_collection(self):
        result = bind_params("%s", (set(['a', 'b']),))
        self.assertIn(result, ("{ 'a' , 'b' }", "{ 'b' , 'a' }"))

    def test_map_collection(self):
        vals = OrderedDict()
        vals['a'] = 'a'
        vals['b'] = 'b'
        vals['c'] = 'c'
        result = bind_params("%s", (vals,))
        self.assertEquals(result, "{ 'a' : 'a' , 'b' : 'b' , 'c' : 'c' }")

    def test_quote_escaping(self):
        result = bind_params("%s", ("""'ef''ef"ef""ef'""",))
        self.assertEquals(result, """'''ef''''ef"ef""ef'''""")


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
                                               keyspace=keyspace)
        bound_statement = BoundStatement(prepared_statement=prepared_statement)

        values = ['nonint', 1]

        try:
            bound_statement.bind(values)
        except InvalidParameterTypeError as e:
            self.assertEqual(e.col_name, 'foo1')
            self.assertEqual(e.expected_type, Int32Type)
            self.assertEqual(e.actual_type, str)
        else:
            self.fail('Passed invalid type but exception was not thrown')

        try:
            bound_statement.bind(values)
        except TypeError as e:
            self.assertEqual(e.col_name, 'foo1')
            self.assertEqual(e.expected_type, Int32Type)
            self.assertEqual(e.actual_type, str)
        else:
            self.fail('Passed invalid type but exception was not thrown')

        values = [1, ['1', '2']]

        try:
            bound_statement.bind(values)
        except InvalidParameterTypeError as e:
            self.assertEqual(e.col_name, 'foo2')
            self.assertEqual(e.expected_type, Int32Type)
            self.assertEqual(e.actual_type, list)
        else:
            self.fail('Passed invalid type but exception was not thrown')
