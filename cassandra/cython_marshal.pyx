# -- cython: profile=True
#
# Copyright 2013-2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import six
import sys
import struct
import math

from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)
from cassandra.buffer cimport Buffer, buf_read

cdef bint is_little_endian
from cassandra.util import is_little_endian

cdef bint PY3 = six.PY3

# cdef extern from "marshal.h":
#     cdef str c_string_to_python(char *p, Py_ssize_t len)

def _make_packer(format_string):
    packer = struct.Struct(format_string)
    pack = packer.pack
    unpack = lambda s: packer.unpack(s)[0]
    return pack, unpack


cdef inline bytes pack(char *buf, Py_ssize_t size):
    """
    Pack a buffer, given as a char *, into Python bytes in byte order.
    """
    swap_order(buf, size)
    return buf[:size]


cdef inline void swap_order(char *buf, Py_ssize_t size):
    """
    Swap the byteorder of `buf` in-place on little-endian platforms
    (reverse all the bytes).
    There are functions ntohl etc, but these may be POSIX-dependent.
    """
    cdef Py_ssize_t start, end, i
    cdef char c

    if is_little_endian:
        for i in range(div2(size)):
            end = size - i - 1
            c = buf[i]
            buf[i] = buf[end]
            buf[end] = c

cdef inline Py_ssize_t div2(Py_ssize_t x):
    return x >> 1

### Packing and unpacking of signed integers

cdef inline bytes int64_pack(int64_t x):
    return pack(<char *> &x, 8)

cdef inline int64_t int64_unpack(Buffer *buf):
    cdef int64_t x = (<int64_t *> buf_read(buf, 8))[0]
    cdef char *p = <char *> &x
    swap_order(<char *> &x, 8)
    return x

cdef inline bytes int32_pack(int32_t x):
    return pack(<char *> &x, 4)

cdef inline int32_t int32_unpack(Buffer *buf):
    cdef int32_t x = (<int32_t *> buf_read(buf, 4))[0]
    cdef char *p = <char *> &x
    swap_order(<char *> &x, 4)
    return x

cdef inline bytes int16_pack(int16_t x):
    return pack(<char *> &x, 2)

cdef inline int16_t int16_unpack(Buffer *buf):
    cdef int16_t x = (<int16_t *> buf_read(buf, 2))[0]
    swap_order(<char *> &x, 2)
    return x

cdef inline bytes int8_pack(int8_t x):
    return (<char *> &x)[:1]

cdef inline int8_t int8_unpack(Buffer *buf):
    return (<int8_t *> buf_read(buf, 1))[0]

cdef inline bytes uint64_pack(uint64_t x):
    return pack(<char *> &x, 8)

cdef inline uint64_t uint64_unpack(Buffer *buf):
    cdef uint64_t x = (<uint64_t *> buf_read(buf, 8))[0]
    swap_order(<char *> &x, 8)
    return x

cdef inline bytes uint32_pack(uint32_t x):
    return pack(<char *> &x, 4)

cdef inline uint32_t uint32_unpack(Buffer *buf):
    cdef uint32_t x = (<uint32_t *> buf_read(buf, 4))[0]
    swap_order(<char *> &x, 4)
    return x

cdef inline bytes uint16_pack(uint16_t x):
    return pack(<char *> &x, 2)

cdef inline uint16_t uint16_unpack(Buffer *buf):
    cdef uint16_t x = (<uint16_t *> buf_read(buf, 2))[0]
    swap_order(<char *> &x, 2)
    return x

cdef inline bytes uint8_pack(uint8_t x):
    return pack(<char *> &x, 1)

cdef inline uint8_t uint8_unpack(Buffer *buf):
    return (<uint8_t *> buf_read(buf, 1))[0]

cdef inline bytes double_pack(double x):
    return pack(<char *> &x, 8)

cdef inline double double_unpack(Buffer *buf):
    cdef double x = (<double *> buf_read(buf, 8))[0]
    swap_order(<char *> &x, 8)
    return x

cdef inline bytes float_pack(float x):
    return pack(<char *> &x, 4)

cdef inline float float_unpack(Buffer *buf):
    cdef float x = (<float *> buf_read(buf, 4))[0]
    swap_order(<char *> &x, 4)
    return x

# int64_pack, int64_unpack = _make_packer('>q')
# int32_pack, int32_unpack = _make_packer('>i')
# int16_pack, int16_unpack = _make_packer('>h')
# int8_pack, int8_unpack = _make_packer('>b')
# uint64_pack, uint64_unpack = _make_packer('>Q')
# uint32_pack, uint32_unpack = _make_packer('>I')
# uint16_pack, uint16_unpack = _make_packer('>H')
# uint8_pack, uint8_unpack = _make_packer('>B')
# float_pack, float_unpack = _make_packer('>f')
# double_pack, double_unpack = _make_packer('>d')

# Special case for cassandra header
header_struct = struct.Struct('>BBbB')
header_pack = header_struct.pack
header_unpack = header_struct.unpack

# in protocol version 3 and higher, the stream ID is two bytes
v3_header_struct = struct.Struct('>BBhB')
v3_header_pack = v3_header_struct.pack
v3_header_unpack = v3_header_struct.unpack


cdef varint_unpack(term):
    """Unpack a variable-sized integer"""
    if PY3:
        return varint_unpack_py3(term)
    else:
        return varint_unpack_py2(term)

# TODO: Optimize these two functions
def varint_unpack_py3(term):
    cdef int64_t one = 1L

    val = int(''.join("%02x" % i for i in term), 16)
    if (term[0] & 128) != 0:
        # There is a bug in Cython (0.20 - 0.22), where if we do
        # '1 << (len(term) * 8)' Cython generates '1' directly into the
        # C code, causing integer overflows
        val -= one << (len(term) * 8)
    return val

def varint_unpack_py2(term):  # noqa
    cdef int64_t one = 1L
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (one << (len(term) * 8))
    return val


def bitlength(n):
    # return int(math.log2(n)) + 1
    bitlen = 0
    while n > 0:
        n >>= 1
        bitlen += 1
    return bitlen


def varint_pack(big):
    pos = True
    if big == 0:
        return b'\x00'
    if big < 0:
        bytelength = bitlength(abs(big) - 1) // 8 + 1
        big = (1 << bytelength * 8) + big
        pos = False
    revbytes = bytearray()
    while big > 0:
        revbytes.append(big & 0xff)
        big >>= 8
    if pos and revbytes[-1] & 0x80:
        revbytes.append(0)
    revbytes.reverse()
    return six.binary_type(revbytes)
