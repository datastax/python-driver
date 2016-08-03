"""Test the various Cython-based message deserializers"""

# Based on test_custom_protocol_handler.py

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra import DriverException, Timeout, AlreadyExists
from cassandra.query import tuple_factory
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.protocol import ProtocolHandler, LazyProtocolHandler, NumpyProtocolHandler, ConfigurationException
from cassandra.cython_deps import HAVE_CYTHON, HAVE_NUMPY
from tests.integration import use_singledc, PROTOCOL_VERSION, notprotocolv1, drop_keyspace_shutdown_cluster, VERIFY_CYTHON, BasicSharedKeyspaceUnitTestCase, execute_with_retry_tolerant, greaterthancass21
from tests.integration.datatype_utils import update_datatypes
from tests.integration.standard.utils import (
    create_table_with_all_types, get_all_primitive_params, get_primitive_datatypes)

from tests.unit.cython.utils import cythontest, numpytest


def setup_module():
    use_singledc()
    update_datatypes()


class CythonProtocolHandlerTest(unittest.TestCase):

    N_ITEMS = 10

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.session.execute("CREATE KEYSPACE testspace WITH replication = "
                            "{ 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        cls.session.set_keyspace("testspace")
        cls.colnames = create_table_with_all_types("test_table", cls.session, cls.N_ITEMS)

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster("testspace", cls.session, cls.session)

    @cythontest
    def test_cython_parser(self):
        """
        Test Cython-based parser that returns a list of tuples
        """
        verify_iterator_data(self.assertEqual, get_data(ProtocolHandler))

    @cythontest
    def test_cython_lazy_parser(self):
        """
        Test Cython-based parser that returns an iterator of tuples
        """
        verify_iterator_data(self.assertEqual, get_data(LazyProtocolHandler))

    @notprotocolv1
    @numpytest
    def test_cython_lazy_results_paged(self):
        """
        Test Cython-based parser that returns an iterator, over multiple pages
        """
        # arrays = { 'a': arr1, 'b': arr2, ... }
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect(keyspace="testspace")
        session.row_factory = tuple_factory
        session.client_protocol_handler = LazyProtocolHandler
        session.default_fetch_size = 2

        self.assertLess(session.default_fetch_size, self.N_ITEMS)

        results = session.execute("SELECT * FROM test_table")

        self.assertTrue(results.has_more_pages)
        self.assertEqual(verify_iterator_data(self.assertEqual, results), self.N_ITEMS)  # make sure we see all rows

        cluster.shutdown()

    @notprotocolv1
    @numpytest
    def test_numpy_parser(self):
        """
        Test Numpy-based parser that returns a NumPy array
        """
        # arrays = { 'a': arr1, 'b': arr2, ... }
        result = get_data(NumpyProtocolHandler)
        self.assertFalse(result.has_more_pages)
        self._verify_numpy_page(result[0])

    @notprotocolv1
    @numpytest
    def test_numpy_results_paged(self):
        """
        Test Numpy-based parser that returns a NumPy array
        """
        # arrays = { 'a': arr1, 'b': arr2, ... }
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect(keyspace="testspace")
        session.row_factory = tuple_factory
        session.client_protocol_handler = NumpyProtocolHandler
        session.default_fetch_size = 2

        expected_pages = (self.N_ITEMS + session.default_fetch_size - 1) // session.default_fetch_size

        self.assertLess(session.default_fetch_size, self.N_ITEMS)

        results = session.execute("SELECT * FROM test_table")

        self.assertTrue(results.has_more_pages)
        for count, page in enumerate(results, 1):
            self.assertIsInstance(page, dict)
            for colname, arr in page.items():
                if count <= expected_pages:
                    self.assertGreater(len(arr), 0, "page count: %d" % (count,))
                    self.assertLessEqual(len(arr), session.default_fetch_size)
                else:
                    # we get one extra item out of this iteration because of the way NumpyParser returns results
                    # The last page is returned as a dict with zero-length arrays
                    self.assertEqual(len(arr), 0)
            self.assertEqual(self._verify_numpy_page(page), len(arr))
        self.assertEqual(count, expected_pages + 1)  # see note about extra 'page' above

        cluster.shutdown()

    @numpytest
    def test_cython_numpy_are_installed_valid(self):
        """
        Test to validate that cython and numpy are installed correctly
        @since 3.3.0
        @jira_ticket PYTHON-543
        @expected_result Cython and Numpy should be present

        @test_category configuration
        """
        if VERIFY_CYTHON:
            self.assertTrue(HAVE_CYTHON)
            self.assertTrue(HAVE_NUMPY)

    def _verify_numpy_page(self, page):
        colnames = self.colnames
        datatypes = get_primitive_datatypes()
        for colname, datatype in zip(colnames, datatypes):
            arr = page[colname]
            self.match_dtype(datatype, arr.dtype)

        return verify_iterator_data(self.assertEqual, arrays_to_list_of_tuples(page, colnames))

    def match_dtype(self, datatype, dtype):
        """Match a string cqltype (e.g. 'int' or 'blob') with a numpy dtype"""
        if datatype == 'smallint':
            self.match_dtype_props(dtype, 'i', 2)
        elif datatype == 'int':
            self.match_dtype_props(dtype, 'i', 4)
        elif datatype in ('bigint', 'counter'):
            self.match_dtype_props(dtype, 'i', 8)
        elif datatype == 'float':
            self.match_dtype_props(dtype, 'f', 4)
        elif datatype == 'double':
            self.match_dtype_props(dtype, 'f', 8)
        else:
            self.assertEqual(dtype.kind, 'O', msg=(dtype, datatype))

    def match_dtype_props(self, dtype, kind, size, signed=None):
        self.assertEqual(dtype.kind, kind, msg=dtype)
        self.assertEqual(dtype.itemsize, size, msg=dtype)


def arrays_to_list_of_tuples(arrays, colnames):
    """Convert a dict of arrays (as given by the numpy protocol handler) to a list of tuples"""
    first_array = arrays[colnames[0]]
    return [tuple(arrays[colname][i] for colname in colnames)
                for i in range(len(first_array))]


def get_data(protocol_handler):
    """
    Get data from the test table.
    """
    cluster = Cluster(protocol_version=PROTOCOL_VERSION)
    session = cluster.connect(keyspace="testspace")

    # use our custom protocol handler
    session.client_protocol_handler = protocol_handler
    session.row_factory = tuple_factory

    results = session.execute("SELECT * FROM test_table")
    session.shutdown()
    return results


def verify_iterator_data(assertEqual, results):
    """
    Check the result of get_data() when this is a list or
    iterator of tuples
    """
    count = 0
    for count, result in enumerate(results, 1):
        params = get_all_primitive_params(result[0])
        assertEqual(len(params), len(result),
                    msg="Not the right number of columns?")
        for expected, actual in zip(params, result):
            assertEqual(actual, expected)
    return count


class NumpyNullTest(BasicSharedKeyspaceUnitTestCase):

    # A dictionary containing table key to type.
    # Boolean dictates whether or not the type can be deserialized with null value
    NUMPY_TYPES = {"v1": ('bigint', False),
                   "v2": ('double', False),
                   "v3": ('float', False),
                   "v4": ('int', False),
                   "v5": ('smallint', False),
                   "v6": ("ascii", True),
                   "v7": ("blob", True),
                   "v8": ("boolean", True),
                   "v9": ("decimal", True),
                   "v10": ("inet", True),
                   "v11": ("text", True),
                   "v12": ("timestamp", True),
                   "v13": ("timeuuid", True),
                   "v14": ("uuid", True),
                   "v15": ("varchar", True),
                   "v16": ("varint", True),
                   }

    def setUp(self):
        self.session.client_protocol_handler = NumpyProtocolHandler
        self.session.row_factory = tuple_factory

    @numpytest
    @greaterthancass21
    def test_null_types(self):
        """
        Test to validate that the numpy protocol handler can deal with null values.
        @since 3.3.0
        @jira_ticket PYTHON-550
        @expected_result Numpy can handle non mapped types' null values.

        @test_category data_types:serialization
        """

        self.create_table_of_types()
        self.session.execute("INSERT INTO {0}.{1} (k) VALUES (1)".format(self.keyspace_name, self.function_table_name))
        self.validate_types()

    def create_table_of_types(self):
        """
        Builds a table containing all the numpy types
        """
        base_ddl = '''CREATE TABLE {0}.{1} (k int PRIMARY KEY'''.format(self.keyspace_name, self.function_table_name, type)
        for key, value in NumpyNullTest.NUMPY_TYPES.items():
            base_ddl = base_ddl+", {0} {1}".format(key, value[0])
        base_ddl = base_ddl+")"
        execute_with_retry_tolerant(self.session, base_ddl, (DriverException, NoHostAvailable, Timeout), (ConfigurationException, AlreadyExists))

    def validate_types(self):
        """
        Selects each type from the table and expects either an exception or None depending on type
        """
        for key, value in NumpyNullTest.NUMPY_TYPES.items():
            select = "SELECT {0} from {1}.{2}".format(key,self.keyspace_name, self.function_table_name)
            if value[1]:
                rs = execute_with_retry_tolerant(self.session, select, (NoHostAvailable), ())
                self.assertEqual(rs[0].get('v1'), None)
            else:
                with self.assertRaises(ValueError):
                    execute_with_retry_tolerant(self.session, select, (NoHostAvailable), ())

