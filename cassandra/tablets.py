# Experimental, this interface and use may change
from threading import Lock

class Tablet(object):
    """
    Represents a single ScyllaDB tablet.
    It stores information about each replica, its host and shard, 
    and the token interval in the format (first_token, last_token].
    """
    first_token = 0
    last_token = 0
    replicas = None

    def __init__(self, first_token = 0, last_token = 0, replicas = None):
        self.first_token = first_token
        self.last_token = last_token
        self.replicas = replicas

    def __str__(self):
        return "<Tablet: first_token=%s last_token=%s replicas=%s>" \
               % (self.first_token, self.last_token, self.replicas)
    __repr__ = __str__

    @staticmethod
    def _is_valid_tablet(replicas):
        return replicas is not None and len(replicas) != 0

    @staticmethod
    def from_row(first_token, last_token, replicas):
        if Tablet._is_valid_tablet(replicas):
            tablet = Tablet(first_token, last_token,replicas)
            return tablet
        return None

# Experimental, this interface and use may change
class Tablets(object):
    _lock = None
    _tablets = {}

    def __init__(self, tablets):
        self._tablets = tablets
        self._lock = Lock()
    
    def get_tablet_for_key(self, keyspace, table, t):
        tablet = self._tablets.get((keyspace, table), [])
        if tablet == []:
            return None
        
        id = bisect_left(tablet, t.value, key = lambda tablet: tablet.last_token)
        if id < len(tablet) and t.value > tablet[id].first_token:
            return tablet[id]
        return None

    def add_tablet(self, keyspace, table, tablet):
        with self._lock:
            tablets_for_table = self._tablets.setdefault((keyspace, table), [])

            # find first overlaping range 
            start = bisect_left(tablets_for_table, tablet.first_token, key = lambda t: t.first_token)
            if start > 0 and tablets_for_table[start - 1].last_token > tablet.first_token:
                start = start - 1

            # find last overlaping range 
            end = bisect_left(tablets_for_table, tablet.last_token, key = lambda t: t.last_token)
            if end < len(tablets_for_table) and tablets_for_table[end].first_token >= tablet.last_token:
                end = end - 1

            if start <= end:
                del tablets_for_table[start:end + 1]

            tablets_for_table.insert(start, tablet)

# bisect.bisect_left implementation from Python 3.11, needed untill support for
# Python < 3.10 is dropped, it is needed to use `key` to extract last_token from
# Tablet list - better solution performance-wise than materialize list of last_tokens
def bisect_left(a, x, lo=0, hi=None, *, key=None):
    """Return the index where to insert item x in list a, assuming a is sorted.

    The return value i is such that all e in a[:i] have e < x, and all e in
    a[i:] have e >= x.  So if x already appears in the list, a.insert(i, x) will
    insert just before the leftmost x already there.

    Optional args lo (default 0) and hi (default len(a)) bound the
    slice of a to be searched.
    """

    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    # Note, the comparison uses "<" to match the
    # __lt__() logic in list.sort() and in heapq.
    if key is None:
        while lo < hi:
            mid = (lo + hi) // 2
            if a[mid] < x:
                lo = mid + 1
            else:
                hi = mid
    else:
        while lo < hi:
            mid = (lo + hi) // 2
            if key(a[mid]) < x:
                lo = mid + 1
            else:
                hi = mid
    return lo
