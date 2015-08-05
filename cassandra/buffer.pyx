"""
Simple buffer data structure. This buffer can be included:

    include "buffer.pyx"

or imported:

    from cassanda cimport buffer

but this prevents inlining of the functions below.
"""

from cpython.bytes cimport PyBytes_AS_STRING
    # char* PyBytes_AS_STRING(object string)
    # Macro form of PyBytes_AsString() but without error
    # checking. Only string objects are supported; no Unicode objects
    # should be passed.

from cassandra.buffer cimport Buffer

cdef struct Buffer:
    char *ptr
    Py_ssize_t size

cdef inline Buffer from_bytes(bytes byts):
    return from_ptr_and_size(PyBytes_AS_STRING(byts), len(byts))

cdef inline bytes to_bytes(Buffer *buf):
    return buf.ptr[:buf.size]

cdef inline char *buf_ptr(Buffer *buf):
    return buf.ptr

cdef inline Buffer from_ptr_and_size(char *ptr, Py_ssize_t size):
    cdef Buffer res
    res.ptr = ptr
    res.size = size
    return res