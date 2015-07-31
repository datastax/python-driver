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


cdef class ColumnParser:
    """Decode a ResultMessage into a set of columns"""
    cpdef parse_rows(self, BytesIOReader reader, DataType[::1] datatypes,
                     protocol_version):
        raise NotImplementedError


cdef class ListParser(ColumnParser):
    """Decode a ResultMessage into a list of tuples (or other objects)"""

    cpdef parse_rows(self, BytesIOReader r, DataType[::1] datatypes, ver):
        cdef Py_ssize_t i, rowcount
        rowcount = read_int(r)
        cdef RowParser rowparser = TupleRowParser(len(datatypes), datatypes)
        return [rowparser.unpack_row(r, ver) for i in range(rowcount)]


cdef class LazyParser(ColumnParser):
    """Decode a ResultMessage lazily using a generator"""

    cpdef parse_rows(self, BytesIOReader r, DataType[::1] datatypes, ver):
        # Use a little helper function as closures (generators) are not
        # supported in cpdef methods
        return parse_rows_lazy(r, self.rowparser, datatypes, ver)


def parse_rows_lazy(BytesIOReader r, DataType[::1] datatypes, ver):
    cdef Py_ssize_t i, rowcount
    rowcount = read_int(r)
    cdef RowParser rowparser = TupleRowParser(len(datatypes), datatypes)
    return (rowparser.unpack_row(r, ver) for i in range(rowcount))


cdef class RowParser:
    """Parser for a single row"""

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
