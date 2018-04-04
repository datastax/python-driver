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

from __future__ import with_statement
import calendar
import datetime
from functools import total_ordering
import random
import six
import uuid
import sys

DATETIME_EPOC = datetime.datetime(1970, 1, 1)

assert sys.byteorder in ('little', 'big')
is_little_endian = sys.byteorder == 'little'

def datetime_from_timestamp(timestamp):
    """
    Creates a timezone-agnostic datetime from timestamp (in seconds) in a consistent manner.
    Works around a Windows issue with large negative timestamps (PYTHON-119),
    and rounding differences in Python 3.4 (PYTHON-340).

    :param timestamp: a unix timestamp, in seconds
    """
    dt = DATETIME_EPOC + datetime.timedelta(seconds=timestamp)
    return dt


def unix_time_from_uuid1(uuid_arg):
    """
    Converts a version 1 :class:`uuid.UUID` to a timestamp with the same precision
    as :meth:`time.time()` returns.  This is useful for examining the
    results of queries returning a v1 :class:`~uuid.UUID`.

    :param uuid_arg: a version 1 :class:`~uuid.UUID`
    """
    return (uuid_arg.time - 0x01B21DD213814000) / 1e7


def datetime_from_uuid1(uuid_arg):
    """
    Creates a timezone-agnostic datetime from the timestamp in the
    specified type-1 UUID.

    :param uuid_arg: a version 1 :class:`~uuid.UUID`
    """
    return datetime_from_timestamp(unix_time_from_uuid1(uuid_arg))


def min_uuid_from_time(timestamp):
    """
    Generates the minimum TimeUUID (type 1) for a given timestamp, as compared by Cassandra.

    See :func:`uuid_from_time` for argument and return types.
    """
    return uuid_from_time(timestamp, 0x808080808080, 0x80)  # Cassandra does byte-wise comparison; fill with min signed bytes (0x80 = -128)


def max_uuid_from_time(timestamp):
    """
    Generates the maximum TimeUUID (type 1) for a given timestamp, as compared by Cassandra.

    See :func:`uuid_from_time` for argument and return types.
    """
    return uuid_from_time(timestamp, 0x7f7f7f7f7f7f, 0x3f7f)  # Max signed bytes (0x7f = 127)


def uuid_from_time(time_arg, node=None, clock_seq=None):
    """
    Converts a datetime or timestamp to a type 1 :class:`uuid.UUID`.

    :param time_arg:
      The time to use for the timestamp portion of the UUID.
      This can either be a :class:`datetime` object or a timestamp
      in seconds (as returned from :meth:`time.time()`).
    :type datetime: :class:`datetime` or timestamp

    :param node:
      None integer for the UUID (up to 48 bits). If not specified, this
      field is randomized.
    :type node: long

    :param clock_seq:
      Clock sequence field for the UUID (up to 14 bits). If not specified,
      a random sequence is generated.
    :type clock_seq: int

    :rtype: :class:`uuid.UUID`

    """
    if hasattr(time_arg, 'utctimetuple'):
        seconds = int(calendar.timegm(time_arg.utctimetuple()))
        microseconds = (seconds * 1e6) + time_arg.time().microsecond
    else:
        microseconds = int(time_arg * 1e6)

    # 0x01b21dd213814000 is the number of 100-ns intervals between the
    # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
    intervals = int(microseconds * 10) + 0x01b21dd213814000

    time_low = intervals & 0xffffffff
    time_mid = (intervals >> 32) & 0xffff
    time_hi_version = (intervals >> 48) & 0x0fff

    if clock_seq is None:
        clock_seq = random.getrandbits(14)
    else:
        if clock_seq > 0x3fff:
            raise ValueError('clock_seq is out of range (need a 14-bit value)')

    clock_seq_low = clock_seq & 0xff
    clock_seq_hi_variant = 0x80 | ((clock_seq >> 8) & 0x3f)

    if node is None:
        node = random.getrandbits(48)

    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                             clock_seq_hi_variant, clock_seq_low, node), version=1)

LOWEST_TIME_UUID = uuid.UUID('00000000-0000-1000-8080-808080808080')
""" The lowest possible TimeUUID, as sorted by Cassandra. """

HIGHEST_TIME_UUID = uuid.UUID('ffffffff-ffff-1fff-bf7f-7f7f7f7f7f7f')
""" The highest possible TimeUUID, as sorted by Cassandra. """


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


from bisect import bisect_left


class SortedSet(object):
    '''
    A sorted set based on sorted list

    A sorted set implementation is used in this case because it does not
    require its elements to be immutable/hashable.

    #Not implemented: update functions, inplace operators
    '''

    def __init__(self, iterable=()):
        self._items = []
        self.update(iterable)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, i):
        return self._items[i]

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
            try:
                return len(other) == len(self._items) and all(item in self for item in other)
            except TypeError:
                return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return self._items != other._items
        else:
            try:
                return len(other) != len(self._items) or any(item not in self for item in other)
            except TypeError:
                return NotImplemented

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
    __rand__ = __and__

    def __iand__(self, other):
        isect = self._intersect(other)
        self._items = isect._items
        return self

    def __or__(self, other):
        return self.union(other)
    __ror__ = __or__

    def __ior__(self, other):
        union = self.union(other)
        self._items = union._items
        return self

    def __sub__(self, other):
        return self._diff(other)

    def __rsub__(self, other):
        return sortedset(other) - self

    def __isub__(self, other):
        diff = self._diff(other)
        self._items = diff._items
        return self

    def __xor__(self, other):
        return self.symmetric_difference(other)
    __rxor__ = __xor__

    def __ixor__(self, other):
        sym_diff = self.symmetric_difference(other)
        self._items = sym_diff._items
        return self

    def __contains__(self, item):
        i = bisect_left(self._items, item)
        return i < len(self._items) and self._items[i] == item

    def __delitem__(self, i):
        del self._items[i]

    def __delslice__(self, i, j):
        del self._items[i:j]

    def add(self, item):
        i = bisect_left(self._items, item)
        if i < len(self._items):
            if self._items[i] != item:
                self._items.insert(i, item)
        else:
            self._items.append(item)

    def update(self, iterable):
        for i in iterable:
            self.add(i)

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

sortedset = SortedSet  # backwards-compatibility


from collections import Mapping
from six.moves import cPickle


class OrderedMap(Mapping):
    '''
    An ordered map that accepts non-hashable types for keys. It also maintains the
    insertion order of items, behaving as OrderedDict in that regard. These maps
    are constructed and read just as normal mapping types, exept that they may
    contain arbitrary collections and other non-hashable items as keys::

        >>> od = OrderedMap([({'one': 1, 'two': 2}, 'value'),
        ...                  ({'three': 3, 'four': 4}, 'value2')])
        >>> list(od.keys())
        [{'two': 2, 'one': 1}, {'three': 3, 'four': 4}]
        >>> list(od.values())
        ['value', 'value2']

    These constructs are needed to support nested collections in Cassandra 2.1.3+,
    where frozen collections can be specified as parameters to others\*::

        CREATE TABLE example (
            ...
            value map<frozen<map<int, int>>, double>
            ...
        )

    This class derives from the (immutable) Mapping API. Objects in these maps
    are not intended be modified.

    \* Note: Because of the way Cassandra encodes nested types, when using the
    driver with nested collections, :attr:`~.Cluster.protocol_version` must be 3
    or higher.

    '''

    def __init__(self, *args, **kwargs):
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))

        self._items = []
        self._index = {}
        if args:
            e = args[0]
            if callable(getattr(e, 'keys', None)):
                for k in e.keys():
                    self._insert(k, e[k])
            else:
                for k, v in e:
                    self._insert(k, v)

        for k, v in six.iteritems(kwargs):
            self._insert(k, v)

    def _insert(self, key, value):
        flat_key = self._serialize_key(key)
        i = self._index.get(flat_key, -1)
        if i >= 0:
            self._items[i] = (key, value)
        else:
            self._items.append((key, value))
            self._index[flat_key] = len(self._items) - 1

    __setitem__ = _insert

    def __getitem__(self, key):
        try:
            index = self._index[self._serialize_key(key)]
            return self._items[index][1]
        except KeyError:
            raise KeyError(str(key))

    def __delitem__(self, key):
        # not efficient -- for convenience only
        try:
            index = self._index.pop(self._serialize_key(key))
            self._index = dict((k, i if i < index else i - 1) for k, i in self._index.items())
            self._items.pop(index)
        except KeyError:
            raise KeyError(str(key))

    def __iter__(self):
        for i in self._items:
            yield i[0]

    def __len__(self):
        return len(self._items)

    def __eq__(self, other):
        if isinstance(other, OrderedMap):
            return self._items == other._items
        try:
            d = dict(other)
            return len(d) == len(self._items) and all(i[1] == d[i[0]] for i in self._items)
        except KeyError:
            return False
        except TypeError:
            pass
        return NotImplemented

    def __repr__(self):
        return '%s([%s])' % (
            self.__class__.__name__,
            ', '.join("(%r, %r)" % (k, v) for k, v in self._items))

    def __str__(self):
        return '{%s}' % ', '.join("%r: %r" % (k, v) for k, v in self._items)

    def popitem(self):
        try:
            kv = self._items.pop()
            del self._index[self._serialize_key(kv[0])]
            return kv
        except IndexError:
            raise KeyError()

    def _serialize_key(self, key):
        return cPickle.dumps(key)


class OrderedMapSerializedKey(OrderedMap):

    def __init__(self, cass_type, protocol_version):
        super(OrderedMapSerializedKey, self).__init__()
        self.cass_key_type = cass_type
        self.protocol_version = protocol_version

    def _insert_unchecked(self, key, flat_key, value):
        self._items.append((key, value))
        self._index[flat_key] = len(self._items) - 1

    def _serialize_key(self, key):
        return self.cass_key_type.serialize(key, self.protocol_version)


import datetime
import time

if six.PY3:
    long = int


@total_ordering
class Time(object):
    '''
    Idealized time, independent of day.

    Up to nanosecond resolution
    '''

    MICRO = 1000
    MILLI = 1000 * MICRO
    SECOND = 1000 * MILLI
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    nanosecond_time = 0

    def __init__(self, value):
        """
        Initializer value can be:

        - integer_type: absolute nanoseconds in the day
        - datetime.time: built-in time
        - string_type: a string time of the form "HH:MM:SS[.mmmuuunnn]"
        """
        if isinstance(value, six.integer_types):
            self._from_timestamp(value)
        elif isinstance(value, datetime.time):
            self._from_time(value)
        elif isinstance(value, six.string_types):
            self._from_timestring(value)
        else:
            raise TypeError('Time arguments must be a whole number, datetime.time, or string')

    @property
    def hour(self):
        """
        The hour component of this time (0-23)
        """
        return self.nanosecond_time // Time.HOUR

    @property
    def minute(self):
        """
        The minute component of this time (0-59)
        """
        minutes = self.nanosecond_time // Time.MINUTE
        return minutes % 60

    @property
    def second(self):
        """
        The second component of this time (0-59)
        """
        seconds = self.nanosecond_time // Time.SECOND
        return seconds % 60

    @property
    def nanosecond(self):
        """
        The fractional seconds component of the time, in nanoseconds
        """
        return self.nanosecond_time % Time.SECOND

    def time(self):
        """
        Return a built-in datetime.time (nanosecond precision truncated to micros).
        """
        return datetime.time(hour=self.hour, minute=self.minute, second=self.second,
                             microsecond=self.nanosecond // Time.MICRO)

    def _from_timestamp(self, t):
        if t >= Time.DAY:
            raise ValueError("value must be less than number of nanoseconds in a day (%d)" % Time.DAY)
        self.nanosecond_time = t

    def _from_timestring(self, s):
        try:
            parts = s.split('.')
            base_time = time.strptime(parts[0], "%H:%M:%S")
            self.nanosecond_time = (base_time.tm_hour * Time.HOUR +
                                    base_time.tm_min * Time.MINUTE +
                                    base_time.tm_sec * Time.SECOND)

            if len(parts) > 1:
                # right pad to 9 digits
                nano_time_str = parts[1] + "0" * (9 - len(parts[1]))
                self.nanosecond_time += int(nano_time_str)

        except ValueError:
            raise ValueError("can't interpret %r as a time" % (s,))

    def _from_time(self, t):
        self.nanosecond_time = (t.hour * Time.HOUR +
                                t.minute * Time.MINUTE +
                                t.second * Time.SECOND +
                                t.microsecond * Time.MICRO)

    def __hash__(self):
        return self.nanosecond_time

    def __eq__(self, other):
        if isinstance(other, Time):
            return self.nanosecond_time == other.nanosecond_time

        if isinstance(other, six.integer_types):
            return self.nanosecond_time == other

        return self.nanosecond_time % Time.MICRO == 0 and \
            datetime.time(hour=self.hour, minute=self.minute, second=self.second,
                          microsecond=self.nanosecond // Time.MICRO) == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, Time):
            return NotImplemented
        return self.nanosecond_time < other.nanosecond_time

    def __repr__(self):
        return "Time(%s)" % self.nanosecond_time

    def __str__(self):
        return "%02d:%02d:%02d.%09d" % (self.hour, self.minute,
                                        self.second, self.nanosecond)


@total_ordering
class Date(object):
    '''
    Idealized date: year, month, day

    Offers wider year range than datetime.date. For Dates that cannot be represented
    as a datetime.date (because datetime.MINYEAR, datetime.MAXYEAR), this type falls back
    to printing days_from_epoch offset.
    '''

    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    date_format = "%Y-%m-%d"

    days_from_epoch = 0

    def __init__(self, value):
        """
        Initializer value can be:

        - integer_type: absolute days from epoch (1970, 1, 1). Can be negative.
        - datetime.date: built-in date
        - string_type: a string time of the form "yyyy-mm-dd"
        """
        if isinstance(value, six.integer_types):
            self.days_from_epoch = value
        elif isinstance(value, (datetime.date, datetime.datetime)):
            self._from_timetuple(value.timetuple())
        elif isinstance(value, six.string_types):
            self._from_datestring(value)
        else:
            raise TypeError('Date arguments must be a whole number, datetime.date, or string')

    @property
    def seconds(self):
        """
        Absolute seconds from epoch (can be negative)
        """
        return self.days_from_epoch * Date.DAY

    def date(self):
        """
        Return a built-in datetime.date for Dates falling in the years [datetime.MINYEAR, datetime.MAXYEAR]

        ValueError is raised for Dates outside this range.
        """
        try:
            dt = datetime_from_timestamp(self.seconds)
            return datetime.date(dt.year, dt.month, dt.day)
        except Exception:
            raise ValueError("%r exceeds ranges for built-in datetime.date" % self)

    def _from_timetuple(self, t):
        self.days_from_epoch = calendar.timegm(t) // Date.DAY

    def _from_datestring(self, s):
        if s[0] == '+':
            s = s[1:]
        dt = datetime.datetime.strptime(s, self.date_format)
        self._from_timetuple(dt.timetuple())

    def __hash__(self):
        return self.days_from_epoch

    def __eq__(self, other):
        if isinstance(other, Date):
            return self.days_from_epoch == other.days_from_epoch

        if isinstance(other, six.integer_types):
            return self.days_from_epoch == other

        try:
            return self.date() == other
        except Exception:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, Date):
            return NotImplemented
        return self.days_from_epoch < other.days_from_epoch

    def __repr__(self):
        return "Date(%s)" % self.days_from_epoch

    def __str__(self):
        try:
            dt = datetime_from_timestamp(self.seconds)
            return "%04d-%02d-%02d" % (dt.year, dt.month, dt.day)
        except:
            # If we overflow datetime.[MIN|MAX]
            return str(self.days_from_epoch)

import socket
if hasattr(socket, 'inet_pton'):
    inet_pton = socket.inet_pton
    inet_ntop = socket.inet_ntop
else:
    """
    Windows doesn't have socket.inet_pton and socket.inet_ntop until Python 3.4
    This is an alternative impl using ctypes, based on this win_inet_pton project:
    https://github.com/hickeroar/win_inet_pton
    """
    import ctypes

    class sockaddr(ctypes.Structure):
        """
        Shared struct for ipv4 and ipv6.

        https://msdn.microsoft.com/en-us/library/windows/desktop/ms740496(v=vs.85).aspx

        ``__pad1`` always covers the port.

        When being used for ``sockaddr_in6``, ``ipv4_addr`` actually covers ``sin6_flowinfo``, resulting
        in proper alignment for ``ipv6_addr``.
        """
        _fields_ = [("sa_family", ctypes.c_short),
                    ("__pad1", ctypes.c_ushort),
                    ("ipv4_addr", ctypes.c_byte * 4),
                    ("ipv6_addr", ctypes.c_byte * 16),
                    ("__pad2", ctypes.c_ulong)]

    if hasattr(ctypes, 'windll'):
        WSAStringToAddressA = ctypes.windll.ws2_32.WSAStringToAddressA
        WSAAddressToStringA = ctypes.windll.ws2_32.WSAAddressToStringA
    else:
        def not_windows(*args):
            raise OSError("IPv6 addresses cannot be handled on Windows. "
                            "Missing ctypes.windll")
        WSAStringToAddressA = not_windows
        WSAAddressToStringA = not_windows

    def inet_pton(address_family, ip_string):
        if address_family == socket.AF_INET:
            return socket.inet_aton(ip_string)

        addr = sockaddr()
        addr.sa_family = address_family
        addr_size = ctypes.c_int(ctypes.sizeof(addr))

        if WSAStringToAddressA(
                ip_string,
                address_family,
                None,
                ctypes.byref(addr),
                ctypes.byref(addr_size)
        ) != 0:
            raise socket.error(ctypes.FormatError())

        if address_family == socket.AF_INET6:
            return ctypes.string_at(addr.ipv6_addr, 16)

        raise socket.error('unknown address family')

    def inet_ntop(address_family, packed_ip):
        if address_family == socket.AF_INET:
            return socket.inet_ntoa(packed_ip)

        addr = sockaddr()
        addr.sa_family = address_family
        addr_size = ctypes.c_int(ctypes.sizeof(addr))
        ip_string = ctypes.create_string_buffer(128)
        ip_string_size = ctypes.c_int(ctypes.sizeof(ip_string))

        if address_family == socket.AF_INET6:
            if len(packed_ip) != ctypes.sizeof(addr.ipv6_addr):
                raise socket.error('packed IP wrong length for inet_ntoa')
            ctypes.memmove(addr.ipv6_addr, packed_ip, 16)
        else:
            raise socket.error('unknown address family')

        if WSAAddressToStringA(
                ctypes.byref(addr),
                addr_size,
                None,
                ip_string,
                ctypes.byref(ip_string_size)
        ) != 0:
            raise socket.error(ctypes.FormatError())

        return ip_string[:ip_string_size.value - 1]


import keyword


# similar to collections.namedtuple, reproduced here because Python 2.6 did not have the rename logic
def _positional_rename_invalid_identifiers(field_names):
    names_out = list(field_names)
    for index, name in enumerate(field_names):
        if (not all(c.isalnum() or c == '_' for c in name)
            or keyword.iskeyword(name)
            or not name
            or name[0].isdigit()
            or name.startswith('_')):
            names_out[index] = 'field_%d_' % index
    return names_out


def _sanitize_identifiers(field_names):
    names_out = _positional_rename_invalid_identifiers(field_names)
    if len(names_out) != len(set(names_out)):
        observed_names = set()
        for index, name in enumerate(names_out):
            while names_out[index] in observed_names:
                names_out[index] = "%s_" % (names_out[index],)
            observed_names.add(names_out[index])
    return names_out


class Duration(object):
    """
    Cassandra Duration Type
    """

    months = 0
    days = 0
    nanoseconds = 0

    def __init__(self, months=0, days=0, nanoseconds=0):
        self.months = months
        self.days = days
        self.nanoseconds = nanoseconds

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.months == other.months and self.days == other.days and self.nanoseconds == other.nanoseconds

    def __repr__(self):
        return "Duration({0}, {1}, {2})".format(self.months, self.days, self.nanoseconds)

    def __str__(self):
        has_negative_values = self.months < 0 or self.days < 0 or self.nanoseconds < 0
        return '%s%dmo%dd%dns' % (
            '-' if has_negative_values else '',
            abs(self.months),
            abs(self.days),
            abs(self.nanoseconds)
        )
