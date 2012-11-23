from unittest import skip
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.management import create_column_family
from cqlengine.management import delete_column_family
from cqlengine.models import Model
from cqlengine.models import Model
from cqlengine import columns

class TestModel(Model):
    count   = columns.Integer()
    text    = columns.Text()
    
#class TestModel2(Model):
    
class TestModelIO(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestModelIO, cls).setUpClass()
        create_column_family(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestModelIO, cls).tearDownClass()
        delete_column_family(TestModel)

    def test_model_save_and_load(self):
        """
        Tests that models can be saved and retrieved
        """
        tm = TestModel.objects.create(count=8, text='123456789')
        tm2 = TestModel.objects.find(tm.pk)

        for cname in tm._columns.keys():
            self.assertEquals(getattr(tm, cname), getattr(tm2, cname))

    def test_model_updating_works_properly(self):
        """
        Tests that subsequent saves after initial model creation work
        """
        tm = TestModel.objects.create(count=8, text='123456789')

        tm.count = 100
        tm.save()

        tm2 = TestModel.objects.find(tm.pk)
        self.assertEquals(tm.count, tm2.count)

    def test_model_deleting_works_properly(self):
        """
        Tests that an instance's delete method deletes the instance
        """
        tm = TestModel.objects.create(count=8, text='123456789')
        tm.delete()
        tm2 = TestModel.objects.find(tm.pk)
        self.assertIsNone(tm2)

    def test_nullable_columns_are_saved_properly(self):
        """
        Tests that nullable columns save without any trouble
        """

    def test_column_deleting_works_properly(self):
        """
        """

