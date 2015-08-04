"""Test the various Cython-based message deserializers"""

# Based on test_custom_protocol_handler.py

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra.cluster import Cluster
from tests.integration import use_singledc, PROTOCOL_VERSION
from tests.integration.datatype_utils import update_datatypes
from tests.integration.standard.utils import create_table_with_all_types, get_all_primitive_params
from six import next

try:
    from cassandra.cython_protocol_handler import make_protocol_handler
except ImportError as e:
    raise unittest.SkipTest("Skipping test, not compiled with Cython enabled")

from cassandra.numpyparser import NumpyParser
from cassandra.objparser import ListParser, LazyParser


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
        self.cython_parser(ListParser())

    def test_cython_lazy_parser(self):
        """
        Test Cython-based parser that returns a list of tuples
        """
        self.cython_parser(LazyParser())

    def cython_parser(self, colparser):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect(keyspace="testspace")

        # use our custom protocol handler
        session.client_protocol_handler = make_protocol_handler(colparser)
        # session.row_factory = tuple_factory

        # verify data
        params = get_all_primitive_params()
        [first_result] = session.execute("SELECT * FROM test_table WHERE primkey=0")
        self.assertEqual(len(params), len(first_result),
                         msg="Not the right number of columns?")
        for expected, actual in zip(params, first_result):
            self.assertEqual(actual, expected)

        session.shutdown()
