# -- cython: profile=True

include 'marshal.pyx'

from cython.view cimport array as cython_array
from cassandra.datatypes import Int64, GenericDataType
from cassandra.cqltypes import LongType

# TODO: Port cqltypes to this module

cdef class DataType:
    """
    Cython-based datatype
    """

    def __init__(self, cqltype):
        self.cqltype = cqltype

    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version):
        raise NotImplementedError


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

    cdef object deserialize(self, char *buf, Py_ssize_t size, protocol_version):
        return self.cqltype.deserialize(buf[:size], protocol_version)

    def __str__(self):
        return "GenericDataType(%s)" % (self.cqltype,)


def make_datatypes(coltypes):
    cdef DataType[::1] datatypes
    return obj_array([make_datatype(ct) for ct in coltypes])


def make_datatype(coltype):
    return Int64(coltype) if coltype == LongType else GenericDataType(coltype)


def obj_array(list objs):
    """Create a (Cython) array of objects given a list of objects"""
    cdef object[:] arr
    arr = cython_array(shape=(len(objs),), itemsize=sizeof(void *), format="O")
    # arr[:] = objs # This does not work (segmentation faults)
    for i, obj in enumerate(objs):
        arr[i] = obj
    return arr


