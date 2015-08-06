"""Test the various Cython-based message deserializers"""

# Based on test_custom_protocol_handler.py

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra.cluster import Cluster
from cassandra.protocol import ProtocolHandler, LazyProtocolHandler, NumpyProtocolHandler
from tests.integration import use_singledc, PROTOCOL_VERSION
from tests.integration.datatype_utils import update_datatypes
from tests.integration.standard.utils import create_table_with_all_types, get_all_primitive_params

from cassandra.cython_deps import HAVE_CYTHON
if not HAVE_CYTHON:
    raise unittest.SkipTest("Skipping test, not compiled with Cython enabled")


def setup_module():
    use_singledc()
    update_datatypes()


class CustomProtocolHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.session.execute("CREATE KEYSPACE testspace WITH replication = "
                            "{ 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        cls.session.set_keyspace("testspace")
        create_table_with_all_types("test_table", cls.session)

    @classmethod
    def tearDownClass(cls):
        cls.session.execute("DROP KEYSPACE testspace")
        cls.cluster.shutdown()

    def test_cython_parser(self):
        """
        Test Cython-based parser that returns a list of tuples
        """
        self.cython_parser(ProtocolHandler)

    def test_cython_lazy_parser(self):
        """
        Test Cython-based parser that returns a list of tuples
        """
        self.cython_parser(LazyProtocolHandler)

    def cython_parser(self, protocol_handler):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect(keyspace="testspace")

        # use our custom protocol handler
        session.client_protocol_handler = protocol_handler
        # session.row_factory = tuple_factory

        # verify data
        params = get_all_primitive_params()
        [first_result] = session.execute("SELECT * FROM test_table WHERE primkey=0")
        self.assertEqual(len(params), len(first_result),
                         msg="Not the right number of columns?")
        for expected, actual in zip(params, first_result):
            self.assertEqual(actual, expected)

        session.shutdown()
