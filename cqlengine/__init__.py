import pkg_resources

from cassandra import ConsistencyLevel

from cqlengine.columns import *
from cqlengine.functions import *
from cqlengine.models import Model
from cqlengine.query import BatchQuery


__cqlengine_version_path__ = pkg_resources.resource_filename('cqlengine',
                                                             'VERSION')
__version__ = open(__cqlengine_version_path__, 'r').readline().strip()

# compaction
SizeTieredCompactionStrategy = "SizeTieredCompactionStrategy"
LeveledCompactionStrategy = "LeveledCompactionStrategy"

# Caching constants.
CACHING_ALL = "ALL"
CACHING_KEYS_ONLY = "KEYS_ONLY"
CACHING_ROWS_ONLY = "ROWS_ONLY"
CACHING_NONE = "NONE"

ANY = ConsistencyLevel.ANY
ONE = ConsistencyLevel.ONE
TWO = ConsistencyLevel.TWO
THREE = ConsistencyLevel.THREE
QUORUM = ConsistencyLevel.QUORUM
LOCAL_QUORUM = ConsistencyLevel.LOCAL_QUORUM
EACH_QUORUM = ConsistencyLevel.EACH_QUORUM
ALL = ConsistencyLevel.ALL
