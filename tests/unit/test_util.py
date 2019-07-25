import copy
import unittest

from cassandra.util import UniqueValuesDict


class TestUniqueValuesDict(unittest.TestCase):
    def test_same_value_cannot_exist_in_multiple_instances(self):
        # via constructor iterable
        x = UniqueValuesDict({'a': 1, 'b': 2})
        with self.assertRaises(ValueError):
            y = UniqueValuesDict({'c': 1})

        # via constructor kwargs
        x = UniqueValuesDict(a=3, b=4)
        with self.assertRaises(ValueError):
            y = UniqueValuesDict(c=4)

        # via update iterable
        x = UniqueValuesDict()
        x.update({'a': 5, 'b': 6})
        y = UniqueValuesDict()
        with self.assertRaises(ValueError):
            y.update({'c': 6})

        # via update kwargs
        x = UniqueValuesDict()
        x.update(a=7, b=8)
        y = UniqueValuesDict()
        with self.assertRaises(ValueError):
            y.update(c=7)

        # via setitem
        x = UniqueValuesDict()
        x['a'] = 9
        y = UniqueValuesDict()
        with self.assertRaises(ValueError):
            y['b'] = 9

    def test_same_value_can_exist_in_same_instance(self):
        # via constructor
        x = UniqueValuesDict({'a': 1, 'b': 1})

        # via update
        x = UniqueValuesDict(a=2, b=2)

        # via setitem
        x = UniqueValuesDict()
        x['a'] = 3
        x['b'] = 3

    def test_can_add_value_after_remove(self):
        # via del
        x = UniqueValuesDict(a=1)
        y = UniqueValuesDict()
        del x['a']
        y['a'] = 1

        # via pop
        x = UniqueValuesDict(a=2)
        y = UniqueValuesDict()
        self.assertEqual(x.pop('a'), 2)
        y['a'] = 2

        # via popitem
        x = UniqueValuesDict(a=3)
        y = UniqueValuesDict()
        x.popitem()
        y['a'] = 2

    def test_updated_value_can_be_inserted(self):
        # via update
        x = UniqueValuesDict(a=1)
        x.update(a=2)
        y = UniqueValuesDict(a=1)
        self.assertDictEqual(x, {'a': 2})
        self.assertDictEqual(y, {'a': 1})

        # via setitem
        x = UniqueValuesDict(a=3)
        x['a'] = 4
        y = UniqueValuesDict(a=3)
        self.assertDictEqual(x, {'a': 4})
        self.assertDictEqual(y, {'a': 3})

    def test_deleting_instances(self):
        x = UniqueValuesDict({'a': 1, 'b': 2})
        del x
        x = UniqueValuesDict({'a': 1, 'b': 2})
        x = None
        x = UniqueValuesDict({'a': 1, 'b': 2})

    def test_copy(self):
        x = UniqueValuesDict(a=1)
        with self.assertRaises(Exception):
            x.copy()

        with self.assertRaises(ValueError):
            copy.copy(x)

        with self.assertRaises(ValueError):
            copy.deepcopy(x)

    def test_setdefault(self):
        # Same instance
        x = UniqueValuesDict()
        x.setdefault('a', 1)
        x.setdefault('b', 1)
        x.setdefault('a', 2)
        self.assertEqual(x.setdefault('a'), 1)
        self.assertDictEqual(x, {'a': 1, 'b': 1})

        # Different instance
        y = UniqueValuesDict()
        y.setdefault('a', 3)
        y.setdefault('a', 1)
        with self.assertRaises(ValueError):
            y.setdefault('b', 1)
