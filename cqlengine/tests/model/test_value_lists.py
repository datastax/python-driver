import random
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.management import sync_table
from cqlengine.management import drop_table
from cqlengine.models import Model
from cqlengine import columns


class TestModel(Model):
    __keyspace__ = 'test'
    id = columns.Integer(primary_key=True)
    clustering_key = columns.Integer(primary_key=True, clustering_order='desc')

class TestClusteringComplexModel(Model):
    __keyspace__ = 'test'
    id = columns.Integer(primary_key=True)
    clustering_key = columns.Integer(primary_key=True, clustering_order='desc')
    some_value = columns.Integer()

class TestClusteringOrder(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestClusteringOrder, cls).setUpClass()
        sync_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestClusteringOrder, cls).tearDownClass()
        drop_table(TestModel)

    def test_clustering_order(self):
        """
        Tests that models can be saved and retrieved
        """
        items = list(range(20))
        random.shuffle(items)
        for i in items:
            TestModel.create(id=1, clustering_key=i)

        values = list(TestModel.objects.values_list('clustering_key', flat=True))
        # [19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L]
        self.assertEquals(values, sorted(items, reverse=True))

    def test_clustering_order_more_complex(self):
        """
        Tests that models can be saved and retrieved
        """
        sync_table(TestClusteringComplexModel)

        items = list(range(20))
        random.shuffle(items)
        for i in items:
            TestClusteringComplexModel.create(id=1, clustering_key=i, some_value=2)

        values = list(TestClusteringComplexModel.objects.values_list('some_value', flat=True))

        self.assertEquals([2] * 20, values)
        drop_table(TestClusteringComplexModel)

