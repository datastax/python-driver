from cassandra.bytesio cimport BytesIOReader
from cassandra.datatypes cimport DataType

cdef class ColumnParser:
    cpdef parse_rows(self, BytesIOReader reader, DataType[::1] datatypes,
                     protocol_version)


cdef class RowParser:
    cpdef unpack_row(self, BytesIOReader reader, protocol_version)

