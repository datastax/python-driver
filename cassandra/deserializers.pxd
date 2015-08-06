# -- cython: profile=True

from cassandra.buffer cimport Buffer

cdef class Deserializer:
    # The cqltypes._CassandraType corresponding to this deserializer
    cdef object cqltype

    # String may be empty, whereas other values may not be.
    # Other values may be NULL, in which case the integer length
    # of the binary data is negative. However, non-string types
    # may also return a zero length for legacy reasons
    # (see http://code.metager.de/source/xref/apache/cassandra/doc/native_protocol_v3.spec
    # paragraph 6)
    cdef bint empty_binary_ok

    cdef deserialize(self, Buffer *buf, int protocol_version)
    # cdef deserialize(self, CString byts, protocol_version)


cdef inline object from_binary(Deserializer deserializer,
                               Buffer *buf,
                               int protocol_version):
    if buf.size <= 0 and not deserializer.empty_binary_ok:
        return _ret_empty(deserializer, buf.size)
    return deserializer.deserialize(buf, protocol_version)

cdef _ret_empty(Deserializer deserializer, Py_ssize_t buf_size)
