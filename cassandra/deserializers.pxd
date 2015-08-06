# -- cython: profile=True

from cassandra.buffer cimport Buffer

cdef class Deserializer:
    cdef deserialize(self, Buffer *buf, int protocol_version)
    # cdef deserialize(self, CString byts, protocol_version)
