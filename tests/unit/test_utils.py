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
import unittest
from cassandra.util import OrderedDict, WeakSet


class OrderedDictTests(unittest.TestCase):

    def test_init(self):
        traditional_dict = {1: 2, 3: 4}

        ordered_dict = OrderedDict()
        ordered_dict = OrderedDict(traditional_dict)
        self.assertEqual(traditional_dict, ordered_dict)

        self.assertRaises(TypeError, OrderedDict, traditional_dict, 'extra argument')

    def test_(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        del traditional_dict[1]
        del ordered_dict[1]
        self.assertEqual(traditional_dict, ordered_dict)

    def test_(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        traditional_dict[1] = 5
        ordered_dict[1] = 5
        self.assertEqual(traditional_dict, ordered_dict)

    def test_(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        for value in ordered_dict:
            self.assertIn(value, set([1, 3]))

    def test_reversed(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        current_order_dict = iter(ordered_dict)
        self.assertEqual(current_order_dict.next(), 1)
        self.assertEqual(current_order_dict.next(), 3)
        reversed_dict = reversed(ordered_dict)
        self.assertEqual(reversed_dict.next(), 3)
        self.assertEqual(reversed_dict.next(), 1)

        del traditional_dict[1]
        del ordered_dict[1]
        traditional_dict[1] = 5
        ordered_dict[1] = 5

        current_order_dict = iter(ordered_dict)
        self.assertEqual(current_order_dict.next(), 3)
        self.assertEqual(current_order_dict.next(), 1)
        reversed_dict = reversed(ordered_dict)
        self.assertEqual(reversed_dict.next(), 1)
        self.assertEqual(reversed_dict.next(), 3)

    def test_pop(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        self.assertEqual(ordered_dict.popitem(), (3, 4))
        self.assertEqual(ordered_dict.popitem(), (1, 2))
        self.assertRaises(KeyError, ordered_dict.popitem)


    def test_pop_first(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        self.assertEqual(ordered_dict.popitem(last=False), (1, 2))
        self.assertEqual(ordered_dict.popitem(last=False), (3, 4))
        self.assertRaises(KeyError, ordered_dict.popitem)

    def test_reduce(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        #TODO: Not sure how this is done

    def test_keys(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        self.assertEqual(traditional_dict.keys(), ordered_dict.keys())

    def test_repr(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        self.assertEqual(repr(ordered_dict), 'OrderedDict([(1, 2), (3, 4)])')

        ordered_dict = OrderedDict()
        self.assertEqual(repr(ordered_dict), 'OrderedDict()')

    def test_copy(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        copy_dict = ordered_dict.copy()
        self.assertEqual(ordered_dict, copy_dict)

        del copy_dict[1]
        self.assertNotEqual(ordered_dict, copy_dict)

        copy_dict[1] = 5
        self.assertNotEqual(ordered_dict, copy_dict)

    def test_fromkeys(self):
        traditional_dict = {1: 2, 3: 4}
        ordered_dict = OrderedDict(traditional_dict)

        self.assertEqual(ordered_dict.fromkeys([1, 2]), OrderedDict([(1, None), (2, None)]))

        self.assertEqual(ordered_dict.fromkeys([1, 2], True), OrderedDict([(1, True), (2, True)]))


class TestWeakSet(unittest.TestCase):
    class Foo(object): pass

    def test_init(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

    def test_add(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

        f3 = self.Foo()
        weak_set.add(f3)

        iterator = iter(weak_set)
        self.assertIn(iterator.next(), set([f1, f2, f3]))
        self.assertIn(iterator.next(), set([f1, f2, f3]))
        self.assertIn(iterator.next(), set([f1, f2, f3]))
        self.assertEqual(len(weak_set), 3)

    def test_del(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

        f3 = self.Foo()
        weak_set.add(f3)

        del f2
        self.assertIn(f1, weak_set)
        self.assertIn(f3, weak_set)
        self.assertEqual(len(weak_set), 2)

    def test_clear(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

        weak_set.clear()
        self.assertEqual(weak_set, WeakSet())

    def test_copy(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

        copy_set = weak_set.copy()
        self.assertEqual(weak_set, copy_set)

        del f2
        self.assertEqual(weak_set, copy_set)

        copy_set.discard(f1)
        self.assertNotEqual(weak_set, copy_set)

    def test_pop(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

        self.assertIn(weak_set.pop(), set([f1, f2]))
        self.assertIn(weak_set.pop(), set([f1, f2]))

        self.assertRaises(KeyError, weak_set.pop)

    def test_remove(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set = WeakSet([f1, f2])

        weak_set.remove(f2)
        self.assertEqual(weak_set.pop(), f1)

    def test_update(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_3 = WeakSet([f1, f2, f3, f4])

        weak_set_1.update(weak_set_2)
        self.assertEqual(weak_set_1, weak_set_3)

        weak_set_4 = weak_set_1.copy()
        weak_set_4.update([f3, f4])
        self.assertEqual(weak_set_3, weak_set_4)

    def test_ior(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_3 = WeakSet([f1, f2, f3, f4])

        weak_set_1 |= (weak_set_2)
        self.assertEqual(weak_set_1, weak_set_3)

        weak_set_4 = weak_set_1.copy()
        weak_set_4 |= ([f3, f4])
        self.assertEqual(weak_set_3, weak_set_4)

    def test_difference_update(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_1.difference_update(weak_set_2)
        self.assertEqual(weak_set_1, copy_set_1)

        weak_set_1.difference_update(copy_set_1)
        self.assertEqual(weak_set_1, WeakSet())

    def test_isub(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_1 -= weak_set_2
        self.assertEqual(weak_set_1, copy_set_1)

        weak_set_1 -= copy_set_1
        self.assertEqual(weak_set_1, WeakSet())

    def test_intersection(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        intersection = weak_set_1.intersection(weak_set_2)
        self.assertEqual(intersection, WeakSet())

        intersection = weak_set_1.intersection(copy_set_1)
        self.assertEqual(intersection, copy_set_1)

        f5 = self.Foo()
        weak_set_2.add(f5)
        weak_set_3 = WeakSet([f5])

        intersection = weak_set_3.intersection(weak_set_2)
        self.assertEqual(intersection, WeakSet([f5]))

        intersection = weak_set_1.intersection(copy_set_1)
        self.assertEqual(intersection, weak_set_1)
        self.assertEqual(intersection, copy_set_1)

    def test_intersection_update(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_1.intersection_update(weak_set_2)
        self.assertEqual(weak_set_1, WeakSet())

        weak_set_1.intersection_update(copy_set_1)
        self.assertEqual(weak_set_1, WeakSet())

        f5 = self.Foo()
        weak_set_2.add(f5)
        weak_set_3 = WeakSet([f5])

        weak_set_1 = copy_set_1.copy()

        weak_set_3.intersection_update(weak_set_2)
        self.assertEqual(weak_set_3, WeakSet([f5]))

        weak_set_1.intersection_update(copy_set_1)
        self.assertEqual(weak_set_1, copy_set_1)

    def test_issubset(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])

        weak_set_2 = WeakSet([f1])

        self.assertTrue(weak_set_2.issubset(weak_set_1))
        self.assertFalse(weak_set_1.issubset(weak_set_2))

    def test_issuperset(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])

        weak_set_2 = WeakSet([f1])

        self.assertFalse(weak_set_2.issuperset(weak_set_1))
        self.assertTrue(weak_set_1.issuperset(weak_set_2))

    def test_symmetric_difference(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        weak_set_2 = WeakSet([f1])

        symmetric_difference = weak_set_1.symmetric_difference(weak_set_2)
        self.assertEqual(symmetric_difference, WeakSet([f2]))

        weak_set_1 = copy_set_1.copy()
        symmetric_difference = weak_set_2.symmetric_difference(weak_set_1)
        self.assertEqual(symmetric_difference, WeakSet([f2]))

        weak_set_1 = copy_set_1.copy()
        symmetric_difference = weak_set_1.symmetric_difference(weak_set_1)
        self.assertEqual(symmetric_difference, WeakSet())

    def test_symmetric_difference_update(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        weak_set_2 = WeakSet([f1])

        weak_set_1.symmetric_difference_update(weak_set_2)
        self.assertEqual(weak_set_1, WeakSet([f2]))

        weak_set_1 = copy_set_1.copy()
        weak_set_2.symmetric_difference_update(weak_set_1)
        self.assertEqual(weak_set_2, WeakSet([f2]))

        weak_set_1 = copy_set_1.copy()
        weak_set_1.symmetric_difference_update(weak_set_1)
        self.assertEqual(weak_set_1, WeakSet())

    def test_union(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_3 = WeakSet([f1, f2, f3, f4])

        union = weak_set_1.union(weak_set_2)
        self.assertEqual(union, weak_set_3)

    def test_isdisjoint(self):
        f1 = self.Foo()
        f2 = self.Foo()
        weak_set_1 = WeakSet([f1, f2])
        copy_set_1 = weak_set_1.copy()

        f3 = self.Foo()
        f4 = self.Foo()
        weak_set_2 = WeakSet([f3, f4])

        weak_set_3 = WeakSet([f1, f2, f3, f4])

        isdisjoint = weak_set_1.isdisjoint(weak_set_2)
        self.assertEqual(isdisjoint, True)

        isdisjoint = weak_set_1.isdisjoint(weak_set_3)
        self.assertEqual(isdisjoint, False)

        isdisjoint = weak_set_1.isdisjoint(copy_set_1)
        self.assertEqual(isdisjoint, False)
