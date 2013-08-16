import copy
from mock import patch
from cqlengine import Model, columns, SizeTieredCompactionStrategy, LeveledCompactionStrategy
from cqlengine.exceptions import CQLEngineException
from cqlengine.management import get_compaction_options, drop_table, sync_table
from cqlengine.tests.base import BaseCassEngTestCase


class CompactionModel(Model):
    __compaction__ = None
    cid = columns.UUID(primary_key=True)
    name = columns.Text()


class BaseCompactionTest(BaseCassEngTestCase):
    def assert_option_fails(self, key):
        # key is a normal_key, converted to
        # __compaction_key__

        key = "__compaction_{}__".format(key)

        with patch.object(self.model, key, 10), \
             self.assertRaises(CQLEngineException):
            get_compaction_options(self.model)


class SizeTieredCompactionTest(BaseCompactionTest):

    def setUp(self):
        self.model = copy.deepcopy(CompactionModel)
        self.model.__compaction__ = SizeTieredCompactionStrategy

    def test_size_tiered(self):
        result = get_compaction_options(self.model)
        assert result['class'] == SizeTieredCompactionStrategy

    def test_min_threshold(self):
        self.model.__compaction_min_threshold__ = 2
        result = get_compaction_options(self.model)
        assert result['min_threshold'] == 2


class LeveledCompactionTest(BaseCompactionTest):
    def setUp(self):
        self.model = copy.deepcopy(CompactionLeveledStrategyModel)

    def test_simple_leveled(self):
        result = get_compaction_options(self.model)
        assert result['class'] == LeveledCompactionStrategy

    def test_bucket_high_fails(self):
        self.assert_option_fails('bucket_high')

    def test_bucket_low_fails(self):
        self.assert_option_fails('bucket_low')

    def test_max_threshold_fails(self):
        self.assert_option_fails('max_threshold')

    def test_min_threshold_fails(self):
        self.assert_option_fails('min_threshold')

    def test_min_sstable_size_fails(self):
        self.assert_option_fails('min_sstable_size')

    def test_sstable_size_in_mb(self):
        with patch.object(self.model, '__compaction_sstable_size_in_mb__', 32):
            result = get_compaction_options(self.model)

        assert result['sstable_size_in_mb'] == 32

    def test_create_table(self):
        class LeveledcompactionTestTable(Model):
            __compaction__ = LeveledCompactionStrategy
            __compaction_sstable_size_in_mb__ = 64
            user_id = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(LeveledcompactionTestTable)
        sync_table(LeveledcompactionTestTable)

        LeveledcompactionTestTable.__compaction__ = SizeTieredCompactionStrategy
        LeveledcompactionTestTable.__compaction_sstable_size_in_mb__ = None

        sync_table(LeveledcompactionTestTable)


class EmptyCompactionTest(BaseCassEngTestCase):
    def test_empty_compaction(self):
        self.model = copy.deepcopy(CompactionModel)
        result = get_compaction_options(self.model)
        self.assertIsNone(result)


class CompactionLeveledStrategyModel(Model):
    __compaction__ = LeveledCompactionStrategy
    cid = columns.UUID(primary_key=True)
    name = columns.Text()


class CompactionSizeTieredModel(Model):
    __compaction__ = SizeTieredCompactionStrategy
    cid = columns.UUID(primary_key=True)
    name = columns.Text()
