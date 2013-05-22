import unittest

from cassandra.query import bind_params

class ParamBindingTest(unittest.TestCase):

    def test_bind_sequence(self):
        result = bind_params("%s %s %s", (1, "a", 2.0))
        self.assertEquals(result, "1 'a' 2.0")

    def test_bind_map(self):
        result = bind_params("%(a)s %(b)s %(c)s", dict(a=1, b="a", c=2.0))
        self.assertEquals(result, "1 'a' 2.0")

    def test_sequence_param(self):
        result = bind_params("%s", ((1, "a", 2.0),))
        self.assertEquals(result, "( 1 , 'a' , 2.0 )")

    def test_generator_param(self):
        result = bind_params("%s", ((i for i in xrange(3)),))
        self.assertEquals(result, "( 0 , 1 , 2 )")

    def test_none_param(self):
        result = bind_params("%s", (None,))
        self.assertEquals(result, "NULL")
