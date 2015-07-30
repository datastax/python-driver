include 'marshal.pyx'

cdef class DataType:
    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version):
        pass


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

