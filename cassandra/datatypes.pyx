include 'marshal.pyx'

from cassandra import cqltypes


cdef class LLDataType:
    cdef void deserialize_ptr(self, char *buf, Py_ssize_t size,
                              void *out, protocol_version):
        pass


cdef class DataType:
    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version):
        pass


cdef class LLInt64(LLDataType):
    """
    Low-level Cassandra datatype
    """

    cdef void deserialize_ptr(self, char *buf, Py_ssize_t size, void *out, protocol_version):
        cdef int64_t x = int64_unpack(buf)
        (<int64_t *> out)[0] = x


cdef class Int64(DataType):

    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version):
        cdef int64_t x = int64_unpack(buf)
        return x

    def __str__(self):
        return "int64"


cdef class GenericDataType(DataType):
    """
    Wrap a generic datatype for deserialization
    """

    cdef object cqltype

    def __init__(self, cqltype):
        self.cqltype = cqltype

    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version):
        return self.cqltype.deserialize(buf[:size], protocol_version)

    def __str__(self):
        return "GenericDataType(%s)" % (self.cqltype,)

