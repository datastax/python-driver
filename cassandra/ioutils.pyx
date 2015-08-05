include 'marshal.pyx'
include 'buffer.pyx'

from libc.stdint cimport int32_t
from cassandra.bytesio cimport BytesIOReader


cdef inline int get_buf(BytesIOReader reader, Buffer *buf_out) except -1:
    """
    Get a pointer into the buffer provided by BytesIOReader for the
    next data item in the stream of values.
    """
    cdef Py_ssize_t raw_val_size = read_int(reader)
    if raw_val_size < 0:
        raise ValueError("Expected positive item size")

    buf_out.ptr = reader.read(raw_val_size)
    buf_out.size = raw_val_size
    return 0

cdef inline int32_t read_int(BytesIOReader reader) except ?0xDEAD:
    return int32_unpack(reader.read(4))
