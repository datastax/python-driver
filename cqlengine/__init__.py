import pkg_resources

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


ANY = "ANY"
ONE = "ONE"
TWO = "TWO"
THREE = "THREE"
QUORUM = "QUORUM"
LOCAL_QUORUM = "LOCAL_QUORUM"
EACH_QUORUM = "EACH_QUORUM"
ALL = "ALL"

