cdef class DataType:
    cdef object cqltype
    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version)
