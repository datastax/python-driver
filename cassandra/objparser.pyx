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
from cassandra.deserializers cimport Deserializer
from cassandra.parsing cimport ParseDesc, ColumnParser, RowParser


cdef class ListParser(ColumnParser):
    """Decode a ResultMessage into a list of tuples (or other objects)"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        cdef Py_ssize_t i, rowcount
        rowcount = read_int(reader)
        cdef RowParser rowparser = TupleRowParser()
        return [rowparser.unpack_row(reader, desc) for i in range(rowcount)]


cdef class LazyParser(ColumnParser):
    """Decode a ResultMessage lazily using a generator"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        # Use a little helper function as closures (generators) are not
        # supported in cpdef methods
        return parse_rows_lazy(reader, desc)


def parse_rows_lazy(BytesIOReader reader, ParseDesc desc):
    cdef Py_ssize_t i, rowcount
    rowcount = read_int(reader)
    cdef RowParser rowparser = TupleRowParser()
    return (rowparser.unpack_row(reader, desc) for i in range(rowcount))


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

    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc):
        cdef Buffer buf
        cdef Py_ssize_t i, rowsize = desc.rowsize
        cdef Deserializer deserializer
        cdef tuple res = PyTuple_New(desc.rowsize)

        for i in range(rowsize):
            # Read the next few bytes
            get_buf(reader, &buf)

            # Deserialize bytes to python object
            deserializer = desc.datatypes[i]
            val = deserializer.deserialize(&buf, desc.protocol_version)

            # Insert new object into tuple
            Py_INCREF(val)
            PyTuple_SET_ITEM(res, i, val)

        return res
