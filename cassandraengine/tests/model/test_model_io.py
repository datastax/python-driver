from cassandraengine.tests.base import BaseCassEngTestCase

from cassandraengine.models import Model
from cassandraengine import columns

class TestModel(Model):
    count   = columns.Integer()
    text    = columns.Text()
    
class TestModelIO(BaseCassEngTestCase):

    def setUp(self):
        super(TestModelIO, self).setUp()
        TestModel.objects._create_column_family()

    def tearDown(self):
        super(TestModelIO, self).tearDown()
        TestModel.objects._delete_column_family()

    def test_model_save_and_load(self):
        tm = TestModel.objects.create(count=8, text='123456789')
        tm2 = TestModel.objects.find(tm.pk)

        for cname in tm._columns.keys():
            self.assertEquals(getattr(tm, cname), getattr(tm2, cname))

