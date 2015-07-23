cdef class BytesIOReader:
    cdef bytes buf
    cdef char *buf_ptr
    cdef Py_ssize_t pos
    cdef Py_ssize_t size
    cdef char *read(self, Py_ssize_t n = ?)

