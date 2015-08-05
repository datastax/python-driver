# -- cython: profile=True

include 'marshal.pyx'
include 'buffer.pyx'

from cython.view cimport array as cython_array
from decimal import Decimal
from uuid import UUID

import inspect

cdef class Deserializer:
    cdef deserialize(self, Buffer *buf, protocol_version):
        raise NotImplementedError


cdef class DesLongType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return int64_unpack(buf.ptr)


# TODO: Use libmpdec: http://www.bytereef.org/mpdecimal/index.html
cdef class DesDecimalType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        scale = int32_unpack(buf.ptr)
        unscaled = varint_unpack(buf.ptr + 4)
        return Decimal('%de%d' % (unscaled, -scale))


cdef class DesUUIDType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return UUID(bytes=to_bytes(buf))


cdef class DesBooleanType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return bool(int8_unpack(buf.ptr))


cdef class DesByteType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return int8_unpack(buf.ptr)


cdef class DesAsciiType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        if six.PY2:
            return to_bytes(buf)
        return to_bytes(buf).decode('ascii')


cdef class DesFloatType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return float_unpack(buf.ptr)


cdef class DesDoubleType(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return double_unpack(buf.ptr)


cdef class DesInt32Type(Deserializer):
    cdef deserialize(self, Buffer *buf, protocol_version):
        return int32_unpack(buf.ptr)


cdef class GenericDeserializer(Deserializer):
    """
    Wrap a generic datatype for deserialization
    """

    cdef object cqltype

    def __init__(self, cqltype):
        self.cqltype = cqltype

    cdef deserialize(self, Buffer *buf, protocol_version):
        return self.cqltype.deserialize(to_bytes(buf), protocol_version)

#--------------------------------------------------------------------------

def make_deserializers(cqltypes):
    """Create an array of Deserializers for each given cqltype in cqltypes"""
    cdef Deserializer[::1] deserializers
    return obj_array([find_deserializer(ct) for ct in cqltypes])


cpdef Deserializer find_deserializer(cqltype):
    """Find a deserializer for a cqltype"""
    name = inspect.isclass(cqltype) and 'Des' + cqltype.__name__
    if name in globals():
        deserializer_cls =  globals()[name]
        deserializer_cls()
    return GenericDeserializer(cqltype)


def obj_array(list objs):
    """Create a (Cython) array of objects given a list of objects"""
    cdef object[:] arr
    arr = cython_array(shape=(len(objs),), itemsize=sizeof(void *), format="O")
    # arr[:] = objs # This does not work (segmentation faults)
    for i, obj in enumerate(objs):
        arr[i] = obj
    return arr
