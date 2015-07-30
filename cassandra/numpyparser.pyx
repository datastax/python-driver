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

from cassandra.rowparser cimport RowParser
from cassandra.bytesio cimport BytesIOReader
from cassandra.datatypes cimport DataType
from cassandra import cqltypes

import numpy as np
cimport numpy as np

from cassandra.util import is_little_endian

from cpython.ref cimport Py_INCREF, PyObject

cdef extern from "Python.h":
    # An integer type large enough to hold a pointer
    ctypedef uint64_t Py_uintptr_t

# ctypedef struct TypeRepr:
#     Py_ssize_t size
#     int is_object

ctypedef struct ArrRepr:
    # TypeRepr typ
    Py_uintptr_t buf_ptr
    Py_ssize_t stride
    int is_object

_cqltype_to_numpy = {
    cqltypes.LongType:          np.dtype('>i8'),
    cqltypes.CounterColumnType: np.dtype('>i8'),
    cqltypes.Int32Type:         np.dtype('>i4'),
    cqltypes.ShortType:         np.dtype('>i2'),
    cqltypes.FloatType:         np.dtype('>f4'),
    cqltypes.DoubleType:        np.dtype('>f8'),
}


# cdef type_repr(coltype):
#     """
#     Get a low-level type representation for the cqltype
#     """
#     cdef TypeRepr res
#     if coltype in _cqltype_to_numpy:
#         dtype = _cqltype_to_numpy[coltype]
#         res.size = dtype.itemsize
#         res.is_object = False
#     else:
#         res.size = sizeof(PyObject *)
#         res.is_object = True
#     return res


cdef ArrRepr array_repr(np.ndarray arr, coltype):
    """
    Construct a low-level array representation
    """
    assert arr.ndim == 1, "Expected a one-dimensional array"

    cdef ArrRepr res
    # Get the data pointer to the underlying memory of the numpy array
    res.buf_ptr = arr.ctypes.data
    res.stride = arr.strides[0]
    res.is_object = coltype in _cqltype_to_numpy
    return res


cdef class NativeRowParser(RowParser):
    """
    This is a row parser that copies bytes into arrays (e.g. NumPy arrays)
    for types it recognizes, such as int64. Values of other types are
    converted to objects.

    NOTE: This class is stateful, in that every time unpack_row is called it
          advanced the pointer into the array by updates the buf_ptr field
          of self.arrays
    """

    # ArrRepr contains a 'buf_ptr' field, which is not supported as a memoryview dtype
    cdef ArrRepr[::1] arrays
    cdef DataType[::1] datatypes
    cdef Py_ssize_t size

    def __init__(self, ArrRepr[::1] arrays, DataType[::1] datatypes):
        self.arrays = arrays
        self.datatypes = datatypes
        self.size = len(datatypes)

    cpdef unpack_row(self, BytesIOReader reader, protocol_version):
        cdef char *buf
        cdef Py_ssize_t i, bufsize, rowsize = self.size
        cdef ArrRepr arr

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
    if is_little_endian:
        # We have arrays in big-endian order. First swap the bytes
        # into little endian order, and then update the numpy dtype
        # accordingly (e.g. from '>i8' to '<i8')
        return arr.byteswap().newbyteorder()
    return arr
