import random
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.management import sync_table
from cqlengine.management import delete_table
from cqlengine.models import Model
from cqlengine import columns

class TestModel(Model):
    id = columns.Integer(primary_key=True)
    clustering_key = columns.Integer(primary_key=True, clustering_order='desc')

class TestClusteringComplexModel(Model):
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
        delete_table(TestModel)

    def test_clustering_order(self):
        """
        Tests that models can be saved and retrieved
        """
        items = list(range(20))
        random.shuffle(items)
        for i in items:
            TestModel.create(id=1, clustering_key=i)

        values = list(TestModel.objects.values_list('clustering_key', flat=True))
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
        delete_table(TestClusteringComplexModel)
