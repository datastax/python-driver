# -- cython: profile=True

from libc.stdint cimport int32_t, uint16_t

include 'marshal.pyx'
include 'cython_utils.pyx'
from cassandra.buffer cimport Buffer, to_bytes

from cython.view cimport array as cython_array

import socket
import inspect
from decimal import Decimal
from uuid import UUID

from cassandra import util


cdef class Deserializer:
    cdef deserialize(self, Buffer *buf, int protocol_version):
        raise NotImplementedError


cdef class DesLongType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return int64_unpack(buf.ptr)


# TODO: Use libmpdec: http://www.bytereef.org/mpdecimal/index.html
cdef class DesDecimalType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        scale = int32_unpack(buf.ptr)
        unscaled = varint_unpack(buf.ptr + 4)
        return Decimal('%de%d' % (unscaled, -scale))


cdef class DesUUIDType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return UUID(bytes=to_bytes(buf))


cdef class DesBooleanType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return bool(int8_unpack(buf.ptr))


cdef class DesByteType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return int8_unpack(buf.ptr)


cdef class DesAsciiType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if six.PY2:
            return to_bytes(buf)
        return to_bytes(buf).decode('ascii')


cdef class DesFloatType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return float_unpack(buf.ptr)


cdef class DesDoubleType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return double_unpack(buf.ptr)


cdef class DesInt32Type(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return int32_unpack(buf.ptr)


cdef class DesIntegerType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return varint_unpack(to_bytes(buf))


cdef class DesInetAddressType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef bytes byts = to_bytes(buf)

        # TODO: optimize inet_ntop, inet_ntoa
        if len(buf.size) == 16:
            return util.inet_ntop(socket.AF_INET6, byts)
        else:
            # util.inet_pton could also handle, but this is faster
            # since we've already determined the AF
            return socket.inet_ntoa(byts)


cdef class DesCounterColumnType(DesLongType):
    pass


cdef class DesDateType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        timestamp = int64_unpack(buf.ptr) / 1000.0
        return datetime_from_timestamp(timestamp)


cdef class TimestampType(DesDateType):
    pass


cdef class TimeUUIDType(DesDateType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return UUID(bytes=to_bytes(buf))


# Values of the 'date'` type are encoded as 32-bit unsigned integers
# representing a number of days with epoch (January 1st, 1970) at the center of the
# range (2^31).
EPOCH_OFFSET_DAYS = 2 ** 31

cdef class DesSimpleDateType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        days = uint32_unpack(buf.ptr) - EPOCH_OFFSET_DAYS
        return util.Date(days)


cdef class DesShortType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return int16_unpack(buf.ptr)


cdef class DesTimeType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return util.Time(int64_unpack(to_bytes(buf)))


cdef class DesUTF8Type(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return to_bytes(buf).decode('utf8')


cdef class DesVarcharType(DesUTF8Type):
    pass


cdef class _DesParameterizedType(Deserializer):

    cdef object cqltype
    cdef object adapter
    cdef object subtypes
    cdef Deserializer[::1] deserializers

    def __init__(self, cqltype):
        assert cqltype.subtypes and len(cqltype.subtypes) == 1
        self.cqltype = cqltype
        self.adapter = cqltype.adapter
        self.subtypes = cqltype.subtypes
        self.deserializers = make_deserializers(cqltype.subtypes)


cdef class _DesSimpleParameterizedType(_DesParameterizedType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef uint16_t v2_and_below = 0
        cdef int32_t v3_and_above = 0

        if protocol_version >= 3:
            result = _deserialize_parameterized[int32_t](
                v3_and_above, self.deserializers[0], buf, protocol_version)
        else:
            result = _deserialize_parameterized[uint16_t](
                v2_and_below, self.deserializers[0], buf, protocol_version)
        return self.adapter(result)


ctypedef fused itemlen_t:
    uint16_t # protocol <= v2
    int32_t  # protocol >= v3


cdef itemlen_t _unpack(itemlen_t dummy, const char *buf):
    cdef itemlen_t result
    if itemlen_t is uint16_t:
        result = uint16_unpack(buf)
    else:
        result = int32_unpack(buf)
    return result

cdef list _deserialize_parameterized(
        itemlen_t dummy, Deserializer deserializer,
        Buffer *buf, int protocol_version):
    cdef itemlen_t itemlen
    cdef Buffer sub_buf

    cdef itemlen_t numelements = _unpack[itemlen_t](dummy, buf.ptr)
    cdef itemlen_t p = sizeof(itemlen_t)
    cdef list result = []

    for _ in range(numelements):
        itemlen = _unpack[itemlen_t](dummy, buf.ptr + p)
        p += sizeof(itemlen_t)
        sub_buf.ptr = buf.ptr + p
        sub_buf.size = itemlen
        p += itemlen
        result.append(deserializer.deserialize(&sub_buf, protocol_version))

    return result

# cdef deserialize_v3_and_above(
#         Deserializer deserializer, Buffer *buf, int protocol_version):
#     cdef Py_ssize_t itemlen
#     cdef Buffer sub_buf
#
#     cdef Py_ssize_t numelements = int32_unpack(buf.ptr)
#     cdef Py_ssize_t p = 4
#     cdef list result = []
#
#     for _ in range(numelements):
#         itemlen = int32_unpack(buf.ptr + p)
#         p += 4
#         sub_buf.ptr = buf.ptr + p
#         sub_buf.size = itemlen
#         p += itemlen
#         result.append(deserializer.deserialize(&sub_buf, protocol_version))
#
#     return result
#
#
# cdef deserialize_v2_and_below(
#         Deserializer deserializer, Buffer *buf, int protocol_version):
#     cdef Py_ssize_t itemlen
#     cdef Buffer sub_buf
#
#     cdef Py_ssize_t numelements = uint16_unpack(buf.ptr)
#     cdef Py_ssize_t p = 2
#     cdef list result = []
#
#     for _ in range(numelements):
#         itemlen = uint16_unpack(buf.ptr + p)
#         p += 2
#         sub_buf.ptr = buf.ptr + p
#         sub_buf.size = itemlen
#         p += itemlen
#         result.append(deserializer.deserialize(&sub_buf, protocol_version))
#
#     return result



cdef class GenericDeserializer(Deserializer):
    """
    Wrap a generic datatype for deserialization
    """

    cdef object cqltype

    def __init__(self, cqltype):
        self.cqltype = cqltype

    cdef deserialize(self, Buffer *buf, int protocol_version):
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
