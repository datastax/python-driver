from unittest import TestCase
from cqlengine import connection

class BaseCassEngTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseCassEngTestCase, cls).setUpClass()
        # todo fix
        connection.setup(['localhost:9160'], default_keyspace='cqlengine_test')

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{} doesn't have attribute: {}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{} shouldn't have the attribute: {}".format(obj, attr))
