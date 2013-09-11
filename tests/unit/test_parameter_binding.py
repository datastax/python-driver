try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra.query import bind_params, ValueSequence

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
        self.assertEquals(result, "( 0 , 1 , 2 )")

    def test_none_param(self):
        result = bind_params("%s", (None,))
        self.assertEquals(result, "NULL")

    def test_list_collection(self):
        result = bind_params("%s", (['a', 'b', 'c'],))
        self.assertEquals(result, "[ 'a' , 'b' , 'c' ]")

    def test_set_collection(self):
        result = bind_params("%s", ({'a', 'b', 'c'},))
        self.assertEquals(result, "{ 'a' , 'c' , 'b' }")

    def test_map_collection(self):
        result = bind_params("%s", ({'a': 'a', 'b': 'b'},))
        self.assertEquals(result, "{ 'a' : 'a' , 'b' : 'b' }")

    def  test_quote_escaping(self):
        result = bind_params("%s", ("""'ef''ef"ef""ef'""",))
        self.assertEquals(result, """'''ef''''ef"ef""ef'''""")
