from unittest import TestCase
from cqlengine import connection
import os
from cqlengine.connection import get_session


if os.environ.get('CASSANDRA_TEST_HOST'):
    CASSANDRA_TEST_HOST = os.environ['CASSANDRA_TEST_HOST']
else:
    CASSANDRA_TEST_HOST = 'localhost'

protocol_version = int(os.environ.get("CASSANDRA_PROTOCOL_VERSION", 2))

connection.setup([CASSANDRA_TEST_HOST], protocol_version=protocol_version, default_keyspace='cqlengine_test')

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
