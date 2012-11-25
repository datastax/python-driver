from unittest import TestCase
from cqlengine import connection

class BaseCassEngTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseCassEngTestCase, cls).setUpClass()
        if not connection._hosts:
            connection.setup(['localhost'])

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr), 
                "{} doesn't have attribute: {}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr), 
                "{} shouldn't have the attribute: {}".format(obj, attr))
