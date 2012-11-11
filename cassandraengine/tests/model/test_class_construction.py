from cassandraengine.tests.base import BaseCassEngTestCase

from cassandraengine.models import Model
from cassandraengine import columns

class TestModelClassFunction(BaseCassEngTestCase):

    def test_column_attributes_handled_correctly(self):
        """
        Tests that column attributes are moved to a _columns dict
        and replaced with simple value attributes
        """

        class TestModel(Model):
            text = columns.Text()

        self.assertHasAttr(TestModel, '_columns')
        self.assertNotHasAttr(TestModel, 'id')
        self.assertNotHasAttr(TestModel, 'text')

        inst = TestModel()
        self.assertHasAttr(inst, 'id')
        self.assertHasAttr(inst, 'text')
        self.assertIsNone(inst.id)
        self.assertIsNone(inst.text)


class TestModelValidation(BaseCassEngTestCase):
    pass

class TestModelSerialization(BaseCassEngTestCase):
    pass

