include "ioutils.pyx"

from cpython.tuple cimport (
        PyTuple_New,
        # Return value: New reference.
        # Return a new tuple object of size len, or NULL on failure.
        PyTuple_SET_ITEM,
        # Like PyTuple_SetItem(), but does no error checking, and should
        # only be used to fill in brand new tuples. Note: This function
        # ``steals'' a reference to o.
        )

from cpython.ref cimport (
        Py_INCREF
        # void Py_INCREF(object o)
        #     Increment the reference count for object o. The object must not
        #     be NULL; if you aren't sure that it isn't NULL, use
        #     Py_XINCREF().
        )

from cassandra.bytesio cimport BytesIOReader
from cassandra.datatypes cimport DataType


cdef class RowParser:
    cpdef unpack_row(self, BytesIOReader reader, protocol_version):
        """
        Unpack a single row of data in a ResultMessage.
        """
        raise NotImplementedError


cdef class TupleRowParser(RowParser):
    """
    Parse a single returned row into a tuple of objects:

        (obj1, ..., objN)

    Attributes
    ===========
    datatypes:
        this is a memoryview of N DataType objects that can deserialize bytes
        into objects
    """

    cdef DataType[::1] datatypes
    cdef Py_ssize_t size

    def __init__(self, Py_ssize_t n, DataType[::1] datatypes):
        self.datatypes = datatypes
        self.size = n

    cpdef unpack_row(self, BytesIOReader reader, protocol_version):
        cdef char *buf
        cdef Py_ssize_t i, bufsize, rowsize = self.size
        cdef DataType dt
        cdef tuple res = PyTuple_New(self.size)

        for i in range(rowsize):
            buf = get_buf(reader, &bufsize)
            if buf == NULL:
                val = None
            else:
                dt = self.datatypes[i]
                val = dt.deserialize(buf, bufsize, protocol_version)

            Py_INCREF(val)
            PyTuple_SET_ITEM(res, i, val)

        return res
