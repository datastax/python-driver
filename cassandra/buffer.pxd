cdef struct Buffer:
    char *ptr
    Py_ssize_t size

cdef inline Buffer from_bytes(bytes byts)
cdef inline bytes to_bytes(Buffer *buf)
cdef inline char *buf_ptr(Buffer *buf)
cdef inline Buffer from_ptr_and_size(char *ptr, Py_ssize_t size)