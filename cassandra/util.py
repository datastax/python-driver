from __future__ import with_statement

try:
    from collections import OrderedDict
except ImportError:
    # OrderedDict from Python 2.7+

    # Copyright (c) 2009 Raymond Hettinger
    #
    # Permission is hereby granted, free of charge, to any person
    # obtaining a copy of this software and associated documentation files
    # (the "Software"), to deal in the Software without restriction,
    # including without limitation the rights to use, copy, modify, merge,
    # publish, distribute, sublicense, and/or sell copies of the Software,
    # and to permit persons to whom the Software is furnished to do so,
    # subject to the following conditions:
    #
    #     The above copyright notice and this permission notice shall be
    #     included in all copies or substantial portions of the Software.
    #
    #     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    #     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    #     OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    #     NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    #     HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    #     WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    #     FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    #     OTHER DEALINGS IN THE SOFTWARE.
    from UserDict import DictMixin

    class OrderedDict(dict, DictMixin):  # noqa
        """ A dictionary which maintains the insertion order of keys. """

        def __init__(self, *args, **kwds):
            """ A dictionary which maintains the insertion order of keys. """

            if len(args) > 1:
                raise TypeError('expected at most 1 arguments, got %d' % len(args))
            try:
                self.__end
            except AttributeError:
                self.clear()
            self.update(*args, **kwds)

        def clear(self):
            self.__end = end = []
            end += [None, end, end]         # sentinel node for doubly linked list
            self.__map = {}                 # key --> [key, prev, next]
            dict.clear(self)

        def __setitem__(self, key, value):
            if key not in self:
                end = self.__end
                curr = end[1]
                curr[2] = end[1] = self.__map[key] = [key, curr, end]
            dict.__setitem__(self, key, value)

        def __delitem__(self, key):
            dict.__delitem__(self, key)
            key, prev, next = self.__map.pop(key)
            prev[2] = next
            next[1] = prev

        def __iter__(self):
            end = self.__end
            curr = end[2]
            while curr is not end:
                yield curr[0]
                curr = curr[2]

        def __reversed__(self):
            end = self.__end
            curr = end[1]
            while curr is not end:
                yield curr[0]
                curr = curr[1]

        def popitem(self, last=True):
            if not self:
                raise KeyError('dictionary is empty')
            if last:
                key = next(reversed(self))
            else:
                key = next(iter(self))
            value = self.pop(key)
            return key, value

        def __reduce__(self):
            items = [[k, self[k]] for k in self]
            tmp = self.__map, self.__end
            del self.__map, self.__end
            inst_dict = vars(self).copy()
            self.__map, self.__end = tmp
            if inst_dict:
                return (self.__class__, (items,), inst_dict)
            return self.__class__, (items,)

        def keys(self):
            return list(self)

        setdefault = DictMixin.setdefault
        update = DictMixin.update
        pop = DictMixin.pop
        values = DictMixin.values
        items = DictMixin.items
        iterkeys = DictMixin.iterkeys
        itervalues = DictMixin.itervalues
        iteritems = DictMixin.iteritems

        def __repr__(self):
            if not self:
                return '%s()' % (self.__class__.__name__,)
            return '%s(%r)' % (self.__class__.__name__, self.items())

        def copy(self):
            return self.__class__(self)

        @classmethod
        def fromkeys(cls, iterable, value=None):
            d = cls()
            for key in iterable:
                d[key] = value
            return d

        def __eq__(self, other):
            if isinstance(other, OrderedDict):
                if len(self) != len(other):
                    return False
                for p, q in zip(self.items(), other.items()):
                    if p != q:
                        return False
                return True
            return dict.__eq__(self, other)

        def __ne__(self, other):
            return not self == other


# WeakSet from Python 2.7+ (https://code.google.com/p/weakrefset)

from _weakref import ref


class _IterationGuard(object):
    # This context manager registers itself in the current iterators of the
    # weak container, such as to delay all removals until the context manager
    # exits.
    # This technique should be relatively thread-safe (since sets are).

    def __init__(self, weakcontainer):
        # Don't create cycles
        self.weakcontainer = ref(weakcontainer)

    def __enter__(self):
        w = self.weakcontainer()
        if w is not None:
            w._iterating.add(self)
        return self

    def __exit__(self, e, t, b):
        w = self.weakcontainer()
        if w is not None:
            s = w._iterating
            s.remove(self)
            if not s:
                w._commit_removals()


class WeakSet(object):
    def __init__(self, data=None):
        self.data = set()

        def _remove(item, selfref=ref(self)):
            self = selfref()
            if self is not None:
                if self._iterating:
                    self._pending_removals.append(item)
                else:
                    self.data.discard(item)

        self._remove = _remove
        # A list of keys to be removed
        self._pending_removals = []
        self._iterating = set()
        if data is not None:
            self.update(data)

    def _commit_removals(self):
        l = self._pending_removals
        discard = self.data.discard
        while l:
            discard(l.pop())

    def __iter__(self):
        with _IterationGuard(self):
            for itemref in self.data:
                item = itemref()
                if item is not None:
                    yield item

    def __len__(self):
        return sum(x() is not None for x in self.data)

    def __contains__(self, item):
        return ref(item) in self.data

    def __reduce__(self):
        return (self.__class__, (list(self),),
                getattr(self, '__dict__', None))

    __hash__ = None

    def add(self, item):
        if self._pending_removals:
            self._commit_removals()
        self.data.add(ref(item, self._remove))

    def clear(self):
        if self._pending_removals:
            self._commit_removals()
        self.data.clear()

    def copy(self):
        return self.__class__(self)

    def pop(self):
        if self._pending_removals:
            self._commit_removals()
        while True:
            try:
                itemref = self.data.pop()
            except KeyError:
                raise KeyError('pop from empty WeakSet')
            item = itemref()
            if item is not None:
                return item

    def remove(self, item):
        if self._pending_removals:
            self._commit_removals()
        self.data.remove(ref(item))

    def discard(self, item):
        if self._pending_removals:
            self._commit_removals()
        self.data.discard(ref(item))

    def update(self, other):
        if self._pending_removals:
            self._commit_removals()
        if isinstance(other, self.__class__):
            self.data.update(other.data)
        else:
            for element in other:
                self.add(element)

    def __ior__(self, other):
        self.update(other)
        return self

    # Helper functions for simple delegating methods.
    def _apply(self, other, method):
        if not isinstance(other, self.__class__):
            other = self.__class__(other)
        newdata = method(other.data)
        newset = self.__class__()
        newset.data = newdata
        return newset

    def difference(self, other):
        return self._apply(other, self.data.difference)
    __sub__ = difference

    def difference_update(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.difference_update(ref(item) for item in other)

    def __isub__(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.difference_update(ref(item) for item in other)
        return self

    def intersection(self, other):
        return self._apply(other, self.data.intersection)
    __and__ = intersection

    def intersection_update(self, other):
        if self._pending_removals:
            self._commit_removals()
        self.data.intersection_update(ref(item) for item in other)

    def __iand__(self, other):
        if self._pending_removals:
            self._commit_removals()
        self.data.intersection_update(ref(item) for item in other)
        return self

    def issubset(self, other):
        return self.data.issubset(ref(item) for item in other)
    __lt__ = issubset

    def __le__(self, other):
        return self.data <= set(ref(item) for item in other)

    def issuperset(self, other):
        return self.data.issuperset(ref(item) for item in other)
    __gt__ = issuperset

    def __ge__(self, other):
        return self.data >= set(ref(item) for item in other)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.data == set(ref(item) for item in other)

    def symmetric_difference(self, other):
        return self._apply(other, self.data.symmetric_difference)
    __xor__ = symmetric_difference

    def symmetric_difference_update(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.symmetric_difference_update(ref(item) for item in other)

    def __ixor__(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.symmetric_difference_update(ref(item) for item in other)
        return self

    def union(self, other):
        return self._apply(other, self.data.union)
    __or__ = union

    def isdisjoint(self, other):
        return len(self.intersection(other)) == 0

try:
    from blist import sortedset
except ImportError:

    import warnings

    warnings.warn(
        "The blist library is not available, so a pure python list-based set will "
        "be used in place of blist.sortedset for set collection values. "
        "You can find the blist library here: https://pypi.python.org/pypi/blist/")

    from bisect import bisect_left

    class sortedset(object):
        '''
        A sorted set based on sorted list

        This set is used in place of blist.sortedset in Python environments
        where blist module/extension is not available.

        A sorted set implementation is used in this case because it does not
        require its elements to be immutable/hashable.

        #Not implemented: update functions, inplace operators

        '''

        def __init__(self, iterable=()):
            self._items = []
            for i in iterable:
                self.add(i)

        def __len__(self):
            return len(self._items)

        def __iter__(self):
            return iter(self._items)

        def __reversed__(self):
            return reversed(self._items)

        def __repr__(self):
            return '%s(%r)' % (
                self.__class__.__name__,
                self._items)

        def __reduce__(self):
            return self.__class__, (self._items,)

        def __eq__(self, other):
            if isinstance(other, self.__class__):
                return self._items == other._items
            else:
                if not isinstance(other, set):
                    return False

                return len(other) == len(self._items) and all(item in other for item in self._items)

        def __ne__(self, other):
            if isinstance(other, self.__class__):
                return self._items != other._items
            else:
                if not isinstance(other, set):
                    return True

                return len(other) != len(self._items) or any(item not in other for item in self._items)

        def __le__(self, other):
            return self.issubset(other)

        def __lt__(self, other):
            return len(other) > len(self._items) and self.issubset(other)

        def __ge__(self, other):
            return self.issuperset(other)

        def __gt__(self, other):
            return len(self._items) > len(other) and self.issuperset(other)

        def __and__(self, other):
            return self._intersect(other)

        def __or__(self, other):
            return self.union(other)

        def __sub__(self, other):
            return self._diff(other)

        def __xor__(self, other):
            return self.symmetric_difference(other)

        def __contains__(self, item):
            i = bisect_left(self._items, item)
            return i < len(self._items) and self._items[i] == item

        def add(self, item):
            i = bisect_left(self._items, item)
            if i < len(self._items):
                if self._items[i] != item:
                    self._items.insert(i, item)
            else:
                self._items.append(item)

        def clear(self):
            del self._items[:]

        def copy(self):
            new = sortedset()
            new._items = list(self._items)
            return new

        def isdisjoint(self, other):
            return len(self._intersect(other)) == 0

        def issubset(self, other):
            return len(self._intersect(other)) == len(self._items)

        def issuperset(self, other):
            return len(self._intersect(other)) == len(other)

        def pop(self):
            if not self._items:
                raise KeyError("pop from empty set")
            return self._items.pop()

        def remove(self, item):
            i = bisect_left(self._items, item)
            if i < len(self._items):
                if self._items[i] == item:
                    self._items.pop(i)
                    return
            raise KeyError('%r' % item)

        def union(self, *others):
            union = sortedset()
            union._items = list(self._items)
            for other in others:
                if isinstance(other, self.__class__):
                    i = 0
                    for item in other._items:
                        i = bisect_left(union._items, item, i)
                        if i < len(union._items):
                            if item != union._items[i]:
                                union._items.insert(i, item)
                        else:
                            union._items.append(item)
                else:
                    for item in other:
                        union.add(item)
            return union

        def intersection(self, *others):
            isect = self.copy()
            for other in others:
                isect = isect._intersect(other)
                if not isect:
                    break
            return isect

        def difference(self, *others):
            diff = self.copy()
            for other in others:
                diff = diff._diff(other)
                if not diff:
                    break
            return diff

        def symmetric_difference(self, other):
            diff_self_other = self._diff(other)
            diff_other_self = other.difference(self)
            return diff_self_other.union(diff_other_self)

        def _diff(self, other):
            diff = sortedset()
            if isinstance(other, self.__class__):
                i = 0
                for item in self._items:
                    i = bisect_left(other._items, item, i)
                    if i < len(other._items):
                        if item != other._items[i]:
                            diff._items.append(item)
                    else:
                        diff._items.append(item)
            else:
                for item in self._items:
                    if item not in other:
                        diff.add(item)
            return diff

        def _intersect(self, other):
            isect = sortedset()
            if isinstance(other, self.__class__):
                i = 0
                for item in self._items:
                    i = bisect_left(other._items, item, i)
                    if i < len(other._items):
                        if item == other._items[i]:
                            isect._items.append(item)
                    else:
                        break
            else:
                for item in self._items:
                    if item in other:
                        isect.add(item)
            return isect
