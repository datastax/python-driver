"""Test the various Cython-based message deserializers"""

# Based on test_custom_protocol_handler.py

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra.query import tuple_factory
from cassandra.cluster import Cluster
from cassandra.protocol import ProtocolHandler, LazyProtocolHandler, NumpyProtocolHandler

from tests.integration import use_singledc, PROTOCOL_VERSION
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
        cls.session.execute("DROP KEYSPACE testspace")
        cls.cluster.shutdown()

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

    @numpytest
    def test_numpy_parser(self):
        """
        Test Numpy-based parser that returns a NumPy array
        """
        # arrays = { 'a': arr1, 'b': arr2, ... }
        arrays = get_data(NumpyProtocolHandler)

        colnames = self.colnames
        datatypes = get_primitive_datatypes()
        for colname, datatype in zip(colnames, datatypes):
            arr = arrays[colname]
            self.match_dtype(datatype, arr.dtype)

        verify_iterator_data(self.assertEqual, arrays_to_list_of_tuples(arrays, colnames))

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
    Get some data from the test table.

    :param key: if None, get all results (100.000 results), otherwise get only one result
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
    for result in results:
        params = get_all_primitive_params(result[0])
        assertEqual(len(params), len(result),
                    msg="Not the right number of columns?")
        for expected, actual in zip(params, result):
            assertEqual(actual, expected)
