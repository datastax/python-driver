cdef class LLDataType:
    """
    Low-level Cassandra datatype
    """

    cdef Py_ssize_t size

    cdef void deserialize_ptr(self, char *buf, Py_ssize_t size, void *out, protocol_version)

cdef class DataType:
    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version)


cdef class Int64(DataType):
    pass

