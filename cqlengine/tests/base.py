from unittest import TestCase
import os
import sys
import six
from cqlengine import connection

CASSANDRA_VERSION = 20  #int(os.environ['CASSANDRA_VERSION'])

class BaseCassEngTestCase(TestCase):

    # @classmethod
    # def setUpClass(cls):
    #     super(BaseCassEngTestCase, cls).setUpClass()
    session = None

    @classmethod
    def setUpClass(cls):
        connection.setup(['192.168.56.103'], 'test')
        cls.session = connection.get_session()
        super(BaseCassEngTestCase, cls).setUpClass()

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{} doesn't have attribute: {}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{} shouldn't have the attribute: {}".format(obj, attr))

    if sys.version_info > (3, 0):
        def assertItemsEqual(self, first, second, msg=None):
            return self.assertCountEqual(first, second, msg)
