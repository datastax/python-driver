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
    import unittest  # noqa

from cassandra.util import OrderedMap
from cassandra.cqltypes import EMPTY
import six

class OrderedMapTest(unittest.TestCase):
    def test_init(self):
        a = OrderedMap(zip(['one', 'three', 'two'], [1, 3, 2]))
        b = OrderedMap([('one', 1), ('three', 3), ('two', 2)])
        c = OrderedMap(a)
        builtin = {'one': 1, 'two': 2, 'three': 3}
        self.assertEqual(a, b)
        self.assertEqual(a, c)
        self.assertEqual(a, builtin)
        self.assertEqual(OrderedMap([(1, 1), (1, 2)]), {1: 2})

    def test_contains(self):
        keys = ['first', 'middle', 'last']

        od = OrderedMap()

        od = OrderedMap(zip(keys, range(len(keys))))

        for k in keys:
            self.assertTrue(k in od)
            self.assertFalse(k not in od)

        self.assertTrue('notthere' not in od)
        self.assertFalse('notthere' in od)

    def test_keys(self):
        keys = ['first', 'middle', 'last']
        od = OrderedMap(zip(keys, range(len(keys))))

        self.assertListEqual(list(od.keys()), keys)

    def test_values(self):
        keys = ['first', 'middle', 'last']
        values = list(range(len(keys)))
        od = OrderedMap(zip(keys, values))

        self.assertListEqual(list(od.values()), values)

    def test_items(self):
        keys = ['first', 'middle', 'last']
        items = list(zip(keys, range(len(keys))))
        od = OrderedMap(items)

        self.assertListEqual(list(od.items()), items)

    def test_get(self):
        keys = ['first', 'middle', 'last']
        od = OrderedMap(zip(keys, range(len(keys))))

        for v, k in enumerate(keys):
            self.assertEqual(od.get(k), v)
        
        self.assertEqual(od.get('notthere', 'default'), 'default')
        self.assertIsNone(od.get('notthere'))

    def test_equal(self):
        d1 = {'one': 1}
        d12 = {'one': 1, 'two': 2}
        od1 = OrderedMap({'one': 1})
        od12 = OrderedMap([('one', 1), ('two', 2)])
        od21 = OrderedMap([('two', 2), ('one', 1)])

        self.assertEqual(od1, d1)
        self.assertEqual(od12, d12)
        self.assertEqual(od21, d12)
        self.assertNotEqual(od1, od12)
        self.assertNotEqual(od12, od1)
        self.assertNotEqual(od12, od21)
        self.assertNotEqual(od1, d12)
        self.assertNotEqual(od12, d1)
        self.assertNotEqual(od1, EMPTY)

    def test_getitem(self):
        keys = ['first', 'middle', 'last']
        od = OrderedMap(zip(keys, range(len(keys))))

        for v, k in enumerate(keys):
            self.assertEqual(od[k], v)
        
        with self.assertRaises(KeyError):
            od['notthere']

    def test_iter(self):
        keys = ['first', 'middle', 'last']
        values = list(range(len(keys)))
        items = list(zip(keys, values))
        od = OrderedMap(items)

        itr = iter(od)
        self.assertEqual(sum([1 for _ in itr]), len(keys))
        self.assertRaises(StopIteration, six.next, itr)

        self.assertEqual(list(iter(od)), keys)
        self.assertEqual(list(six.iteritems(od)), items)
        self.assertEqual(list(six.itervalues(od)), values)

    def test_len(self):
        self.assertEqual(len(OrderedMap()), 0)
        self.assertEqual(len(OrderedMap([(1, 1)])), 1)

    def test_mutable_keys(self):
        d = {'1': 1}
        s = set([1, 2, 3])
        od = OrderedMap([(d, 'dict'), (s, 'set')])
