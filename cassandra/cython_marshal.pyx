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
from libc.string cimport memcpy
from cassandra.buffer cimport Buffer, buf_read, to_bytes

cdef bint is_little_endian
from cassandra.util import is_little_endian

cdef bint PY3 = six.PY3

ctypedef fused num_t:
    int64_t
    int32_t
    int16_t
    int8_t
    uint64_t
    uint32_t
    uint16_t
    uint8_t
    double
    float

cdef inline num_t copy_maybe_swap(char *buf, num_t *dummy=NULL): # dummy pointer because cython wants the fused type as an arg
    """
    Copy to aligned destination, conditionally swapping to native byte order
    """
    cdef num_t ret = 0
    cdef Py_ssize_t start, end, i
    cdef char* out = <char*> &ret

    if is_little_endian:
        for i in range(sizeof(num_t)):
            out[sizeof(num_t) - i - 1] = buf[i]
    else:
        # TODO: use inline function as in numpy_parser
        memcpy(out, buf, sizeof(num_t))

    return ret

cdef inline Py_ssize_t div2(Py_ssize_t x):
    return x >> 1

cdef inline int64_t int64_unpack(Buffer *buf) except ?0xDEAD:
    return copy_maybe_swap[int64_t](buf_read(buf, sizeof(int64_t)))

cdef inline int32_t int32_unpack(Buffer *buf) except ?0xDEAD:
    return copy_maybe_swap[int32_t](buf_read(buf, sizeof(int32_t)))

cdef inline int16_t int16_unpack(Buffer *buf) except ?0xDED:
    return copy_maybe_swap[int16_t](buf_read(buf, sizeof(int16_t)))

cdef inline int8_t int8_unpack(Buffer *buf) except ?80:
    return copy_maybe_swap[int8_t](buf_read(buf, sizeof(int8_t)))

cdef inline uint64_t uint64_unpack(Buffer *buf) except ?0xDEAD:
    return copy_maybe_swap[uint64_t](buf_read(buf, sizeof(uint64_t)))

cdef inline uint32_t uint32_unpack(Buffer *buf) except ?0xDEAD:
    return copy_maybe_swap[uint32_t](buf_read(buf, sizeof(uint32_t)))

cdef inline uint16_t uint16_unpack(Buffer *buf) except ?0xDEAD:
    return copy_maybe_swap[uint16_t](buf_read(buf, sizeof(uint16_t)))

cdef inline uint8_t uint8_unpack(Buffer *buf) except ?0xff:
    return copy_maybe_swap[uint8_t](buf_read(buf, sizeof(uint8_t)))

cdef inline double double_unpack(Buffer *buf) except ?1.74:
    return copy_maybe_swap[double](buf_read(buf, sizeof(double)))

cdef inline float float_unpack(Buffer *buf) except ?1.74:
    return copy_maybe_swap[float](buf_read(buf, sizeof(float)))

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
