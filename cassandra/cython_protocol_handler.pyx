# ython: profile=True

from libc.stdint cimport int64_t, int32_t

# from cassandra.marshal cimport (int8_pack, int8_unpack, int16_pack, int16_unpack,
#                                 uint16_pack, uint16_unpack, uint32_pack, uint32_unpack,
#                                 int32_pack, int32_unpack, int64_pack, int64_unpack, float_pack, float_unpack, double_pack, double_unpack)

from cassandra.marshal import varint_pack, varint_unpack
from cassandra import util
from cassandra.cqltypes import EMPTY
from cassandra.protocol import ResultMessage, ProtocolHandler

from cassandra.bytesio cimport BytesIOReader
from cassandra cimport typecodes

import numpy as np

include "marshal.pyx"

class FastResultMessage(ResultMessage):
    """
    Cython version of Result Message that has a faster implementation of
    recv_results_row.
    """
    # type_codes = ResultMessage.type_codes.copy()
    code_to_type = dict((v, k) for k, v in ResultMessage.type_codes.items())

    @classmethod
    def recv_results_rows(cls, f, protocol_version, user_type_map):
        paging_state, column_metadata = cls.recv_results_metadata(f, user_type_map)

        colnames = [c[2] for c in column_metadata]
        coltypes = [c[3] for c in column_metadata]
        colcodes = np.array(
                [cls.code_to_type.get(coltype, -1) for coltype in coltypes],
                dtype=np.dtype('i'))
        parsed_rows = parse_rows(BytesIOReader(f.read()), colnames,
                                 coltypes, colcodes, protocol_version)
        return (paging_state, (colnames, parsed_rows))


cdef parse_rows(BytesIOReader reader, list colnames, list coltypes,
        int[::1] colcodes, protocol_version):
    cdef Py_ssize_t i, rowcount
    cdef char *raw_val
    cdef int32_t raw_val_size
    rowcount = read_int(reader)
    # return RowIterator(reader, coltypes, colcodes, protocol_version, rowcount)
    return [parse_row(reader, coltypes, colcodes, protocol_version)
                for i in range(rowcount)]


cdef class RowIterator:
    """
    Result iterator for a set of rows

    There seems to be an issue with generator expressions + memoryviews, so we
    have a special iterator class instead.
    """
    cdef list coltypes
    cdef int[::1] colcodes
    cdef Py_ssize_t rowcount, pos
    cdef BytesIOReader reader
    cdef object protocol_version

    def __init__(self, reader, coltypes, colcodes, protocol_version, rowcount):
        self.reader = reader
        self.coltypes = coltypes
        self.colcodes = colcodes
        self.protocol_version = protocol_version
        self.rowcount = rowcount
        self.pos = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.pos >= self.rowcount:
            raise StopIteration
        self.pos += 1
        return parse_row(self.reader, self.coltypes, self.colcodes, self.protocol_version)

    next = __next__


cdef inline parse_row(BytesIOReader reader, list coltypes, int[::1] colcodes,
                      protocol_version):
    cdef Py_ssize_t j

    row = []
    for j, ctype in enumerate(coltypes):
        raw_val_size = read_int(reader)
        if raw_val_size < 0:
            val = None
        else:
            raw_val = reader.read(raw_val_size)
            val = from_binary(ctype, colcodes[j], raw_val,
                              raw_val_size, protocol_version)
        row.append(val)

    return row


class CythonProtocolHandler(ProtocolHandler):
    """
    Use FastResultMessage to decode query result message messages.
    """
    my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
    my_opcodes[FastResultMessage.opcode] = FastResultMessage
    message_types_by_opcode = my_opcodes


cdef inline int32_t read_int(BytesIOReader reader):
    return int32_unpack(reader.read(4))


cdef inline from_binary(cqltype, int typecode, char *byts, int32_t size, protocol_version):
    """
    Deserialize a bytestring into a value. See the deserialize() method
    for more information. This method differs in that if None or the empty
    string is passed in, None may be returned.

    This method provides a fast-path deserialization routine.
    """
    if size == 0 and cqltype.empty_binary_ok:
        return empty(cqltype)
    return deserialize(cqltype, typecode, byts, size, protocol_version)


cdef empty(cqltype):
    return EMPTY if cqltype.support_empty_values else None


def to_binary(cqltype, val, protocol_version):
    """
    Serialize a value into a bytestring. See the serialize() method for
    more information. This method differs in that if None is passed in,
    the result is the empty string.
    """
    return b'' if val is None else cqltype.serialize(val, protocol_version)


cdef deserialize(cqltype, int typecode, char *byts, int32_t size, protocol_version):
    if typecode == typecodes.LongType:
        return int64_unpack(byts)
    else:
        return deserialize_generic(cqltype, typecode, byts, size, protocol_version)

cdef deserialize_generic(cqltype, int typecode, char *byts, int32_t size,
        protocol_version):
    print("deserialize", cqltype)
    return cqltype.deserialize(byts[:size], protocol_version)

