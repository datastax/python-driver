from unittest import TestCase
from cqlengine import connection
import os
from cqlengine.connection import get_session


class BaseCassEngTestCase(TestCase):

    # @classmethod
    # def setUpClass(cls):
    #     super(BaseCassEngTestCase, cls).setUpClass()
    session = None

    def setUp(self):
        self.session = get_session()
        super(BaseCassEngTestCase, self).setUp()

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{} doesn't have attribute: {}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{} shouldn't have the attribute: {}".format(obj, attr))
