from cassandra.bytesio cimport BytesIOReader

cdef class RowParser:
    cpdef unpack_row(self, BytesIOReader reader, protocol_version)

