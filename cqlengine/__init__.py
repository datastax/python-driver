import os

from cqlengine.columns import *
from cqlengine.functions import *
from cqlengine.models import Model
from cqlengine.query import BatchQuery

__cqlengine_version_path__ = os.path.realpath(__file__ + '/../VERSION')
__version__ = open(__cqlengine_version_path__, 'r').readline().strip()

# compaction
SizeTieredCompactionStrategy = "SizeTieredCompactionStrategy"
LeveledCompactionStrategy = "LeveledCompactionStrategy"

