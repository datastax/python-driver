# Copyright DataStax, Inc.
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

from cassandra.util import sortedset
from cassandra.cqltypes import EMPTY

from datetime import datetime
from itertools import permutations

class SortedSetTest(unittest.TestCase):
    def test_init(self):
        input = [5, 4, 3, 2, 1, 1, 1]
        expected = sorted(set(input))
        ss = sortedset(input)
        assert len(ss) == len(expected)
        assert list(ss) == expected

    def test_repr(self):
        assert repr(sortedset([1, 2, 3, 4])) == "SortedSet([1, 2, 3, 4])"

    def test_contains(self):
        input = [5, 4, 3, 2, 1, 1, 1]
        expected = sorted(set(input))
        ss = sortedset(input)

        for i in expected:
            assert i in ss
            self.assertFalse(i not in ss)

        hi = max(expected)+1
        lo = min(expected)-1

        self.assertFalse(hi in ss)
        self.assertFalse(lo in ss)

    def test_mutable_contents(self):
        ba = bytearray(b'some data here')
        ss = sortedset([ba, ba])
        assert list(ss) == [ba]

    def test_clear(self):
        ss = sortedset([1, 2, 3])
        ss.clear()
        assert len(ss) == 0

    def test_equal(self):
        s1 = set([1])
        s12 = set([1, 2])
        ss1 = sortedset(s1)
        ss12 = sortedset(s12)

        assert ss1 == s1
        assert ss12 == s12
        assert ss12 == s12
        assert ss1.__eq__(None) == NotImplemented
        assert ss1 != ss12
        assert ss12 != ss1
        assert ss1 != s12
        assert ss12 != s1
        assert ss1 != EMPTY

    def test_copy(self):
        class comparable(object):
            def __lt__(self, other):
                return id(self) < id(other)

        o = comparable()
        ss = sortedset([comparable(), o])
        ss2 = ss.copy()
        assert id(ss) != id(ss2)
        assert o in ss
        assert o in ss2

    def test_isdisjoint(self):
        # set, ss
        s12 = set([1, 2])
        s2 = set([2])
        ss1 = sortedset([1])
        ss13 = sortedset([1, 3])
        ss3 = sortedset([3])
        # s ss disjoint
        assert s2.isdisjoint(ss1)
        assert s2.isdisjoint(ss13)
        # s ss not disjoint
        self.assertFalse(s12.isdisjoint(ss1))
        self.assertFalse(s12.isdisjoint(ss13))
        # ss s disjoint
        assert ss1.isdisjoint(s2)
        assert ss13.isdisjoint(s2)
        # ss s not disjoint
        self.assertFalse(ss1.isdisjoint(s12))
        self.assertFalse(ss13.isdisjoint(s12))
        # ss ss disjoint
        assert ss1.isdisjoint(ss3)
        assert ss3.isdisjoint(ss1)
        # ss ss not disjoint
        self.assertFalse(ss1.isdisjoint(ss13))
        self.assertFalse(ss13.isdisjoint(ss1))
        self.assertFalse(ss3.isdisjoint(ss13))
        self.assertFalse(ss13.isdisjoint(ss3))

    def test_issubset(self):
        s12 = set([1, 2])
        ss1 = sortedset([1])
        ss13 = sortedset([1, 3])
        ss3 = sortedset([3])

        assert ss1.issubset(s12)
        assert ss1.issubset(ss13)

        self.assertFalse(ss1.issubset(ss3))
        self.assertFalse(ss13.issubset(ss3))
        self.assertFalse(ss13.issubset(ss1))
        self.assertFalse(ss13.issubset(s12))

    def test_issuperset(self):
        s12 = set([1, 2])
        ss1 = sortedset([1])
        ss13 = sortedset([1, 3])
        ss3 = sortedset([3])

        assert s12.issuperset(ss1)
        assert ss13.issuperset(ss3)
        assert ss13.issuperset(ss13)

        self.assertFalse(s12.issuperset(ss13))
        self.assertFalse(ss1.issuperset(ss3))
        self.assertFalse(ss1.issuperset(ss13))

    def test_union(self):
        s1 = set([1])
        ss12 = sortedset([1, 2])
        ss23 = sortedset([2, 3])

        assert sortedset().union(s1) == sortedset([1])
        assert ss12.union(s1) == sortedset([1, 2])
        assert ss12.union(ss23) == sortedset([1, 2, 3])
        assert ss23.union(ss12) == sortedset([1, 2, 3])
        assert ss23.union(s1) == sortedset([1, 2, 3])

    def test_intersection(self):
        s12 = set([1, 2])
        ss23 = sortedset([2, 3])
        assert s12.intersection(ss23) == set([2])
        assert ss23.intersection(s12) == sortedset([2])
        assert ss23.intersection(s12, [2], (2,)) == sortedset([2])
        assert ss23.intersection(s12, [900], (2,)) == sortedset()

    def test_difference(self):
        s1 = set([1])
        ss12 = sortedset([1, 2])
        ss23 = sortedset([2, 3])

        assert sortedset().difference(s1) == sortedset()
        assert ss12.difference(s1) == sortedset([2])
        assert ss12.difference(ss23) == sortedset([1])
        assert ss23.difference(ss12) == sortedset([3])
        assert ss23.difference(s1) == sortedset([2, 3])

    def test_symmetric_difference(self):
        s = set([1, 3, 5])
        ss = sortedset([2, 3, 4])
        ss2 = sortedset([5, 6, 7])

        assert ss.symmetric_difference(s) == sortedset([1, 2, 4, 5])
        self.assertFalse(ss.symmetric_difference(ss))
        assert ss.symmetric_difference(s) == sortedset([1, 2, 4, 5])
        assert ss2.symmetric_difference(ss) == sortedset([2, 3, 4, 5, 6, 7])

    def test_pop(self):
        ss = sortedset([2, 1])
        assert ss.pop() == 2
        assert ss.pop() == 1
        try:
            ss.pop()
            self.fail("Error not thrown")
        except (KeyError, IndexError) as e:
            pass

    def test_remove(self):
        ss = sortedset([2, 1])
        assert len(ss) == 2
        self.assertRaises(KeyError, ss.remove, 3)
        assert len(ss) == 2
        ss.remove(1)
        assert len(ss) == 1
        ss.remove(2)
        self.assertFalse(ss)
        self.assertRaises(KeyError, ss.remove, 2)
        self.assertFalse(ss)

    def test_getitem(self):
        ss = sortedset(range(3))
        for i in range(len(ss)):
            assert ss[i] == i
        with self.assertRaises(IndexError):
            ss[len(ss)]

    def test_delitem(self):
        expected = [1,2,3,4]
        ss = sortedset(expected)
        for i in range(len(ss)):
            self.assertListEqual(list(ss), expected[i:])
            del ss[0]
        with self.assertRaises(IndexError):
            ss[0]

    def test_delslice(self):
        expected = [1, 2, 3, 4, 5]
        ss = sortedset(expected)
        del ss[1:3]
        self.assertListEqual(list(ss), [1, 4, 5])
        del ss[-1:]
        self.assertListEqual(list(ss), [1, 4])
        del ss[1:]
        self.assertListEqual(list(ss), [1])
        del ss[:]
        self.assertFalse(ss)
        with self.assertRaises(IndexError):
            del ss[0]

    def test_reversed(self):
        expected = range(10)
        self.assertListEqual(list(reversed(sortedset(expected))), list(reversed(expected)))

    def test_operators(self):

        ss1 = sortedset([1])
        ss12 = sortedset([1, 2])
        # __ne__
        self.assertFalse(ss12 != ss12)
        self.assertFalse(ss12 != sortedset([1, 2]))
        assert ss12 != sortedset()

        # __le__
        assert ss1 <= ss12
        assert ss12 <= ss12
        self.assertFalse(ss12 <= ss1)

        # __lt__
        assert ss1 < ss12
        self.assertFalse(ss12 < ss12)
        self.assertFalse(ss12 < ss1)

        # __ge__
        self.assertFalse(ss1 >= ss12)
        assert ss12 >= ss12
        assert ss12 >= ss1

        # __gt__
        self.assertFalse(ss1 > ss12)
        self.assertFalse(ss12 > ss12)
        assert ss12 > ss1

        # __and__
        assert ss1 & ss12 == ss1
        assert ss12 & ss12 == ss12
        assert ss12 & set() == sortedset()

        # __iand__
        tmp = sortedset(ss12)
        tmp &= ss1
        assert tmp == ss1
        tmp = sortedset(ss1)
        tmp &= ss12
        assert tmp == ss1
        tmp = sortedset(ss12)
        tmp &= ss12
        assert tmp == ss12
        tmp = sortedset(ss12)
        tmp &= set()
        assert tmp == sortedset()

        # __rand__
        assert set([1]) & ss12 == ss1

        # __or__
        assert ss1 | ss12 == ss12
        assert ss12 | ss12 == ss12
        assert ss12 | set() == ss12
        assert sortedset() | ss1 | ss12 == ss12

        # __ior__
        tmp = sortedset(ss1)
        tmp |= ss12
        assert tmp == ss12
        tmp = sortedset(ss12)
        tmp |= ss12
        assert tmp == ss12
        tmp = sortedset(ss12)
        tmp |= set()
        assert tmp == ss12
        tmp = sortedset()
        tmp |= ss1
        tmp |= ss12
        assert tmp == ss12

        # __ror__
        assert set([1]) | ss12 == ss12

        # __sub__
        assert ss1 - ss12 == set()
        assert ss12 - ss12 == set()
        assert ss12 - set() == ss12
        assert ss12 - ss1 == sortedset([2])

        # __isub__
        tmp = sortedset(ss1)
        tmp -= ss12
        assert tmp == set()
        tmp = sortedset(ss12)
        tmp -= ss12
        assert tmp == set()
        tmp = sortedset(ss12)
        tmp -= set()
        assert tmp == ss12
        tmp = sortedset(ss12)
        tmp -= ss1
        assert tmp == sortedset([2])

        # __rsub__
        assert set((1,2,3)) - ss12 == set((3,))

        # __xor__
        assert ss1 ^ ss12 == set([2])
        assert ss12 ^ ss1 == set([2])
        assert ss12 ^ ss12 == set()
        assert ss12 ^ set() == ss12

        # __ixor__
        tmp = sortedset(ss1)
        tmp ^= ss12
        assert tmp == set([2])
        tmp = sortedset(ss12)
        tmp ^= ss1
        assert tmp == set([2])
        tmp = sortedset(ss12)
        tmp ^= ss12
        assert tmp == set()
        tmp = sortedset(ss12)
        tmp ^= set()
        assert tmp == ss12

        # __rxor__
        assert set([1, 2]) ^ ss1 == (set([2]))

    def test_reduce_pickle(self):
        ss = sortedset((4,3,2,1))
        import pickle
        s = pickle.dumps(ss)
        assert pickle.loads(s) == ss

    def _test_uncomparable_types(self, items):
        for perm in permutations(items):
            ss = sortedset(perm)
            s = set(perm)
            assert s == ss
            assert ss == ss.union(s)
            for x in range(len(ss)):
                subset = set(s)
                for _ in range(x):
                    subset.pop()
                assert ss.difference(subset) == s.difference(subset)
                assert ss.intersection(subset) == s.intersection(subset)
            for x in ss:
                assert x in ss
                ss.remove(x)
                self.assertNotIn(x, ss)

    def test_uncomparable_types_with_tuples(self):
        # PYTHON-1087 - make set handle uncomparable types
        dt = datetime(2019, 5, 16)
        items = (('samekey', 3, 1),
                 ('samekey', None, 0),
                 ('samekey', dt),
                 ("samekey", None, 2),
                 ("samekey", None, 1),
                 ('samekey', dt),
                 ('samekey', None, 0),
                 ("samekey", datetime.now()))

        self._test_uncomparable_types(items)

    def test_uncomparable_types_with_integers(self):
        # PYTHON-1087 - make set handle uncomparable types
        items = (None, 1, 2, 6, None, None, 92)
        self._test_uncomparable_types(items)
