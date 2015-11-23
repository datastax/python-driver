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

from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)
from cassandra.buffer cimport Buffer, buf_read, to_bytes

cdef bint is_little_endian
from cassandra.util import is_little_endian

cdef bint PY3 = six.PY3


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

### Unpacking of signed integers

cdef inline int64_t int64_unpack(Buffer *buf) except ?0xDEAD:
    cdef int64_t x = (<int64_t *> buf_read(buf, 8))[0]
    cdef char *p = <char *> &x
    swap_order(<char *> &x, 8)
    return x

cdef inline int32_t int32_unpack(Buffer *buf) except ?0xDEAD:
    cdef int32_t x = (<int32_t *> buf_read(buf, 4))[0]
    cdef char *p = <char *> &x
    swap_order(<char *> &x, 4)
    return x

cdef inline int16_t int16_unpack(Buffer *buf) except ?0xDED:
    cdef int16_t x = (<int16_t *> buf_read(buf, 2))[0]
    swap_order(<char *> &x, 2)
    return x

cdef inline int8_t int8_unpack(Buffer *buf) except ?80:
    return (<int8_t *> buf_read(buf, 1))[0]

cdef inline uint64_t uint64_unpack(Buffer *buf) except ?0xDEAD:
    cdef uint64_t x = (<uint64_t *> buf_read(buf, 8))[0]
    swap_order(<char *> &x, 8)
    return x

cdef inline uint32_t uint32_unpack(Buffer *buf) except ?0xDEAD:
    cdef uint32_t x = (<uint32_t *> buf_read(buf, 4))[0]
    swap_order(<char *> &x, 4)
    return x

cdef inline uint16_t uint16_unpack(Buffer *buf) except ?0xDEAD:
    cdef uint16_t x = (<uint16_t *> buf_read(buf, 2))[0]
    swap_order(<char *> &x, 2)
    return x

cdef inline uint8_t uint8_unpack(Buffer *buf) except ?0xff:
    return (<uint8_t *> buf_read(buf, 1))[0]

cdef inline double double_unpack(Buffer *buf) except ?1.74:
    cdef double x = (<double *> buf_read(buf, 8))[0]
    swap_order(<char *> &x, 8)
    return x

cdef inline float float_unpack(Buffer *buf) except ?1.74:
    cdef float x = (<float *> buf_read(buf, 4))[0]
    swap_order(<char *> &x, 4)
    return x


cdef varint_unpack(Buffer *term):
    """Unpack a variable-sized integer"""
    if PY3:
        return varint_unpack_py3(to_bytes(term))
    else:
        return varint_unpack_py2(to_bytes(term))

# TODO: Optimize these two functions
cdef varint_unpack_py3(bytes term):
    val = int(''.join(["%02x" % i for i in term]), 16)
    if (term[0] & 128) != 0:
        shift = len(term) * 8  # * Note below
        val -= 1 << shift
    return val

cdef varint_unpack_py2(bytes term):  # noqa
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        shift = len(term) * 8  # * Note below
        val = val - (1 << shift)
    return val

# * Note *
# '1 << (len(term) * 8)' Cython tries to do native
# integer shifts, which overflows. We need this to
# emulate Python shifting, which will expand the long
# to accommodate
