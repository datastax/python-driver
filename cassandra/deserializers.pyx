# -- cython: profile=True

from libc.stdint cimport int32_t, uint16_t

include 'marshal.pyx'
from cassandra.buffer cimport Buffer, to_bytes
from cassandra.cython_utils cimport datetime_from_timestamp

from cython.view cimport array as cython_array
from cassandra.tuple cimport tuple_new, tuple_set

import socket
from decimal import Decimal
from uuid import UUID

from cassandra import cqltypes
from cassandra import util


cdef class Deserializer:
    """Cython-based deserializer class for a cqltype"""

    def __init__(self, cqltype):
        self.cqltype = cqltype
        self.empty_binary_ok = False

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
        if int8_unpack(buf.ptr):
            return True
        return False


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
        cdef double timestamp = int64_unpack(buf.ptr) / 1000.0
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

    cdef object subtypes
    cdef Deserializer[::1] deserializers
    cdef Py_ssize_t subtypes_len

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.subtypes = cqltype.subtypes
        self.deserializers = make_deserializers(cqltype.subtypes)


cdef class _DesSingleParamType(_DesParameterizedType):
    cdef Deserializer deserializer

    def __init__(self, cqltype):
        assert cqltype.subtypes and len(cqltype.subtypes) == 1, cqltype.subtypes
        super().__init__(cqltype)
        self.deserializer = self.deserializers[0]


#--------------------------------------------------------------------------
# List and set deserialization

cdef class DesListType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef uint16_t v2_and_below = 2
        cdef int32_t v3_and_above = 3

        if protocol_version >= 3:
            result = _deserialize_list_or_set[int32_t](
                v3_and_above, buf, protocol_version, self.deserializer)
        else:
            result = _deserialize_list_or_set[uint16_t](
                v2_and_below, buf, protocol_version, self.deserializer)

        return result

cdef class DesSetType(DesListType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return util.sortedset(DesListType.deserialize(self, buf, protocol_version))


ctypedef fused itemlen_t:
    uint16_t # protocol <= v2
    int32_t  # protocol >= v3

cdef list _deserialize_list_or_set(itemlen_t dummy_version,
                                   Buffer *buf, int protocol_version,
                                   Deserializer deserializer):
    """
    Deserialize a list or set.

    The 'dummy' parameter is needed to make fused types work, so that
    we can specialize on the protocol version.
    """
    cdef itemlen_t itemlen
    cdef Buffer sub_buf

    cdef itemlen_t numelements = _unpack[itemlen_t](dummy_version, buf.ptr)
    cdef itemlen_t p = sizeof(itemlen_t)
    cdef list result = []

    for _ in range(numelements):
        itemlen = _unpack[itemlen_t](dummy_version, buf.ptr + p)
        p += sizeof(itemlen_t)
        sub_buf.ptr = buf.ptr + p
        sub_buf.size = itemlen
        p += itemlen
        result.append(from_binary(deserializer, &sub_buf, protocol_version))

    return result

cdef itemlen_t _unpack(itemlen_t dummy_version, const char *buf):
    cdef itemlen_t result
    if itemlen_t is uint16_t:
        result = uint16_unpack(buf)
    else:
        result = int32_unpack(buf)
    return result

#--------------------------------------------------------------------------
# Map deserialization

cdef class DesMapType(_DesParameterizedType):

    cdef Deserializer key_deserializer, val_deserializer

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.key_deserializer = self.deserializers[0]
        self.val_deserializer = self.deserializers[1]

    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef uint16_t v2_and_below = 0
        cdef int32_t v3_and_above = 0
        key_type, val_type = self.cqltype.subtypes

        if protocol_version >= 3:
            result = _deserialize_map[int32_t](
                v3_and_above, buf, protocol_version,
                self.key_deserializer, self.val_deserializer,
                key_type, val_type)
        else:
            result = _deserialize_map[uint16_t](
                v2_and_below, buf, protocol_version,
                self.key_deserializer, self.val_deserializer,
                key_type, val_type)

        return result


cdef _deserialize_map(itemlen_t dummy_version,
                      Buffer *buf, int protocol_version,
                      Deserializer key_deserializer, Deserializer val_deserializer,
                      key_type, val_type):
    cdef itemlen_t itemlen, val_len, key_len
    cdef Buffer key_buf, val_buf

    cdef itemlen_t numelements = _unpack[itemlen_t](dummy_version, buf.ptr)
    cdef itemlen_t p = sizeof(itemlen_t)
    cdef list result = []

    numelements = _unpack[itemlen_t](dummy_version, buf.ptr)
    p = sizeof(itemlen_t)
    themap = util.OrderedMapSerializedKey(key_type, protocol_version)
    for _ in range(numelements):
        key_len = _unpack[itemlen_t](dummy_version, buf.ptr + p)
        p += sizeof(itemlen_t)
        # keybytes = byts[p:p + key_len]
        key_buf.ptr = buf.ptr + p
        key_buf.size = key_len
        p += key_len
        val_len = _unpack(dummy_version, buf.ptr + p)
        p += sizeof(itemlen_t)
        # valbytes = byts[p:p + val_len]
        val_buf.ptr = buf.ptr + p
        val_buf.size = val_len
        p += val_len
        key = from_binary(key_deserializer, &key_buf, protocol_version)
        val = from_binary(val_deserializer, &val_buf, protocol_version)
        themap._insert_unchecked(key, to_bytes(&key_buf), val)

    return themap

#--------------------------------------------------------------------------

cdef class DesTupleType(_DesParameterizedType):

    # TODO: Use TupleRowParser to parse these tuples

    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Py_ssize_t i, p
        cdef int32_t itemlen
        cdef tuple res = tuple_new(self.subtypes_len)
        cdef Buffer item_buf
        cdef Deserializer deserializer

        protocol_version = max(3, protocol_version)

        p = 0
        values = []
        for i in range(self.subtypes_len):
            item = None
            if p != buf.size:
                itemlen = int32_unpack(buf.ptr + p)
                p += 4
                if itemlen >= 0:
                    item_buf.ptr = buf.ptr + p
                    item_buf.size = itemlen
                    deserializer = self.deserializers[i]
                    item = from_binary(deserializer, &item_buf, protocol_version)
                    p += itemlen

            tuple_set(res, i, item)

        return res


cdef class DesUserType(DesTupleType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        typ = self.cqltype
        values = DesTupleType.deserialize(self, buf, protocol_version)
        if typ.mapped_class:
            return typ.mapped_class(**dict(zip(typ.fieldnames, values)))
        else:
            return typ.tuple_type(*values)


cdef class DesCompositeType(_DesParameterizedType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Py_ssize_t i
        cdef Buffer elem_buf
        cdef int16_t element_length
        cdef Deserializer deserializer
        cdef tuple res = tuple_new(self.subtypes_len)

        for i in range(self.subtypes_len):
            if not buf.size:
                # CompositeType can have missing elements at the end
                break

            element_length = uint16_unpack(buf.ptr)
            elem_buf.ptr = buf.ptr + 2
            elem_buf.size = element_length

            # skip element length, element, and the EOC (one byte)
            buf.ptr = buf.ptr + 2 + element_length + 1
            buf.size = buf.size - (2 + element_length + 1)
            deserializer = self.deserializers[i]
            item = from_binary(deserializer, &elem_buf, protocol_version)
            tuple_set(res, i, item)

        return res


DesDynamicCompositeType = DesCompositeType


cdef class DesReversedType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return from_binary(self.deserializer, buf, protocol_version)


cdef class DesFrozenType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return from_binary(self.deserializer, buf, protocol_version)

#--------------------------------------------------------------------------

cdef _ret_empty(Deserializer deserializer, Py_ssize_t buf_size):
    """
    Decide whether to return None or EMPTY when a buffer size is
    zero or negative. This is used by from_binary in deserializers.pxd.
    """
    if buf_size < 0:
        return None
    elif deserializer.cqltype.support_empty_values:
        return cqltypes.EMPTY
    else:
        return None

#--------------------------------------------------------------------------
# Generic deserialization

cdef class GenericDeserializer(Deserializer):
    """
    Wrap a generic datatype for deserialization
    """

    cdef deserialize(self, Buffer *buf, int protocol_version):
        return self.cqltype.deserialize(to_bytes(buf), protocol_version)

#--------------------------------------------------------------------------
# Helper utilities

def make_deserializers(cqltypes):
    """Create an array of Deserializers for each given cqltype in cqltypes"""
    cdef Deserializer[::1] deserializers
    return obj_array([find_deserializer(ct) for ct in cqltypes])


cpdef Deserializer find_deserializer(cqltype):
    """Find a deserializer for a cqltype"""
    name = 'Des' + cqltype.__name__

    if name in globals():
        cls = globals()[name]
    elif issubclass(cqltype, cqltypes.ListType):
        cls = DesListType
    elif issubclass(cqltype, cqltypes.SetType):
        cls = DesSetType
    elif issubclass(cqltype, cqltypes.MapType):
        cls = DesMapType
    elif issubclass(cqltype, cqltypes.UserType):
        # UserType is a subclass of TupleType, so should precede it
        cls = DesUserType
    elif issubclass(cqltype, cqltypes.TupleType):
        cls = DesTupleType
    elif issubclass(cqltype, cqltypes.DynamicCompositeType):
        # DynamicCompositeType is a subclass of CompositeType, so should precede it
        cls = DesDynamicCompositeType
    elif issubclass(cqltype, cqltypes.CompositeType):
        cls = DesCompositeType
    elif issubclass(cqltype, cqltypes.ReversedType):
        cls = DesReversedType
    elif issubclass(cqltype, cqltypes.FrozenType):
        cls = DesFrozenType
    else:
        cls = GenericDeserializer

    return cls(cqltype)


def obj_array(list objs):
    """Create a (Cython) array of objects given a list of objects"""
    cdef object[:] arr
    arr = cython_array(shape=(len(objs),), itemsize=sizeof(void *), format="O")
    # arr[:] = objs # This does not work (segmentation faults)
    for i, obj in enumerate(objs):
        arr[i] = obj
    return arr
