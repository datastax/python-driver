"""
This module provider an optional protocol parser that returns
NumPy arrays.

=============================================================================
This module should not be imported by any of the main python-driver modules,
as numpy is an optional dependency.
=============================================================================
"""

include "ioutils.pyx"

from libc.stdint cimport uint64_t
from cpython.ref cimport Py_INCREF, PyObject

from cassandra.objparser cimport RowParser
from cassandra.bytesio cimport BytesIOReader
from cassandra.datatypes cimport DataType
from cassandra import cqltypes
from cassandra.util import is_little_endian

import numpy as np


cdef extern from "numpyFlags.h":
    # Include 'numpyFlags.h' into the generated C code to disable the
    # deprecated NumPy API
    pass

cdef extern from "Python.h":
    # An integer type large enough to hold a pointer
    ctypedef uint64_t Py_uintptr_t

cdef extern from "numpy/arrayobject.h":
    # Avoid using 'numpy' from Cython, as it access the 'data' attribute
    # of PyArrayObject, which is deprecated:
    #
    #     warning: #warning "Using deprecated NumPy API, disable it by
    #     #defining NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION" [-Wcpp]
    #
    ctypedef class np.ndarray [object PyArrayObject]:
        pass


# Simple array descriptor, useful to parse rows into a NumPy array
ctypedef struct ArrDesc:
    Py_uintptr_t buf_ptr
    Py_ssize_t stride
    int is_object

cdef ArrDesc[:] _dummyArray = <ArrDesc[:0]> NULL
arrDescDtype = np.array(_dummyArray).dtype


_cqltype_to_numpy = {
    cqltypes.LongType:          np.dtype('>i8'),
    cqltypes.CounterColumnType: np.dtype('>i8'),
    cqltypes.Int32Type:         np.dtype('>i4'),
    cqltypes.ShortType:         np.dtype('>i2'),
    cqltypes.FloatType:         np.dtype('>f4'),
    cqltypes.DoubleType:        np.dtype('>f8'),
}

obj_dtype = np.dtype('O')

def make_array(coltype, array_size):
    """
    Allocate a new NumPy array of the given column type and size.
    """
    dtype = _cqltype_to_numpy.get(coltype, obj_dtype)
    return np.empty((array_size,), dtype=dtype)


def make_arrays(colnames, coltypes, array_size):
    """
    Allocate arrays for each result column.

    returns a tuple of (array_descs, arrays), where
        'array_descs' describe the arrays for NativeRowParser and
        'arrays' is a dict mapping column names to arrays
            (e.g. this can be fed into pandas.DataFrame)
    """
    row_size = len(colnames)
    array_descs = np.empty((row_size,), arrDescDtype)
    arrays = {}

    for i, colname, coltype in zip(range(row_size), colnames, coltypes):
        arr = make_array(coltype, array_size)
        array_descs[i].buf_ptr = arr.ctypes.data
        array_descs[i].stride = arr.strides[0]
        array_descs[i].is_object = coltype in _cqltype_to_numpy
        arrays[colname] = arr

    return array_descs, arrays


cdef class NativeRowParser(RowParser):
    """
    This is a row parser that copies bytes into arrays (e.g. NumPy arrays)
    for types it recognizes, such as int64. Values of other types are
    converted to objects.

    NOTE: This class is stateful, in that every time unpack_row is called it
          advanced the pointer into the array by updates the buf_ptr field
          of self.arrays
    """

    cdef ArrDesc[::1] arrays
    cdef DataType[::1] datatypes
    cdef Py_ssize_t size

    def __init__(self, ArrDesc[::1] arrays, DataType[::1] datatypes):
        self.arrays = arrays
        self.datatypes = datatypes
        self.size = len(datatypes)

    cpdef unpack_row(self, BytesIOReader reader, protocol_version):
        cdef char *buf
        cdef Py_ssize_t i, bufsize, rowsize = self.size
        cdef ArrDesc arr

        for i in range(rowsize):
            buf = get_buf(reader, &bufsize)
            if buf == NULL:
                raise ValueError("Unexpected end of stream")

            arr = self.arrays[i]

            if arr.is_object:
                dt = self.datatypes[i]
                val = dt.deserialize(buf, bufsize, protocol_version)
                Py_INCREF(val)
                (<PyObject **> arr.buf_ptr)[0] = <PyObject *> val
            else:
                memcopy(buf, <char *> arr.buf_ptr, bufsize)

            # Update the pointer into the array for the next time
            self.arrays[i].buf_ptr += arr.stride


cdef inline memcopy(char *src, char *dst, Py_ssize_t size):
    """
    Our own simple memcopy which can be inlined. This is useful because our data types
    are only a few bytes.
    """
    cdef Py_ssize_t i
    for i in range(size):
        dst[i] = src[i]


def make_native_byteorder(arr):
    """
    Make sure all values have a native endian in the NumPy arrays.
    """
    if is_little_endian and not arr.dtype.kind == 'O':
        # We have arrays in big-endian order. First swap the bytes
        # into little endian order, and then update the numpy dtype
        # accordingly (e.g. from '>i8' to '<i8')
        #
        # Ignore any object arrays of dtype('O')
        return arr.byteswap().newbyteorder()
    return arr
