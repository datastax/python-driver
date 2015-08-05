from cassandra.bytesio cimport BytesIOReader
from cassandra.deserializers cimport Deserializer

cdef class ParseDesc:
    cdef public object colnames
    cdef public object coltypes
    cdef Deserializer[::1] datatypes
    cdef public object protocol_version
    cdef Py_ssize_t rowsize

cdef class ColumnParser:
    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc)

cdef class RowParser:
    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc)

