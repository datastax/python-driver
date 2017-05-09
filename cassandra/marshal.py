# Copyright 2013-2017 DataStax, Inc.
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
import struct


def _make_packer(format_string):
    packer = struct.Struct(format_string)
    pack = packer.pack
    unpack = lambda s: packer.unpack(s)[0]
    return pack, unpack

int64_pack, int64_unpack = _make_packer('>q')
int32_pack, int32_unpack = _make_packer('>i')
int16_pack, int16_unpack = _make_packer('>h')
int8_pack, int8_unpack = _make_packer('>b')
uint64_pack, uint64_unpack = _make_packer('>Q')
uint32_pack, uint32_unpack = _make_packer('>I')
uint16_pack, uint16_unpack = _make_packer('>H')
uint8_pack, uint8_unpack = _make_packer('>B')
float_pack, float_unpack = _make_packer('>f')
double_pack, double_unpack = _make_packer('>d')

# Special case for cassandra header
header_struct = struct.Struct('>BBbB')
header_pack = header_struct.pack
header_unpack = header_struct.unpack

# in protocol version 3 and higher, the stream ID is two bytes
v3_header_struct = struct.Struct('>BBhB')
v3_header_pack = v3_header_struct.pack
v3_header_unpack = v3_header_struct.unpack


if six.PY3:
    def byte2int(b):
        return b


    def varint_unpack(term):
        val = int(''.join("%02x" % i for i in term), 16)
        if (term[0] & 128) != 0:
            len_term = len(term)  # pulling this out of the expression to avoid overflow in cython optimized code
            val -= 1 << (len_term * 8)
        return val
else:
    def byte2int(b):
        return ord(b)


    def varint_unpack(term):  # noqa
        val = int(term.encode('hex'), 16)
        if (ord(term[0]) & 128) != 0:
            len_term = len(term)  # pulling this out of the expression to avoid overflow in cython optimized code
            val = val - (1 << (len_term * 8))
        return val


def bit_length(n):
    if six.PY3 or isinstance(n, int):
        return int.bit_length(n)
    else:
        return long.bit_length(n)


def varint_pack(big):
    pos = True
    if big == 0:
        return b'\x00'
    if big < 0:
        bytelength = bit_length(abs(big) - 1) // 8 + 1
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


def encode_zig_zag(n):
    return (n << 1) ^ (n >> 63)


def decode_zig_zag(n):
    return (n >> 1) ^ -(n & 1)


def vints_unpack(term):  # noqa
    values = []
    n = 0
    while n < len(term):
        first_byte = byte2int(term[n])

        if (first_byte & 128) == 0:
            val = first_byte
        else:
            num_extra_bytes = 8 - (~first_byte & 0xff).bit_length()
            val = first_byte & (0xff >> num_extra_bytes)
            end = n + num_extra_bytes
            while n < end:
                n += 1
                val <<= 8
                val |= byte2int(term[n]) & 0xff

        n += 1
        values.append(decode_zig_zag(val))

    return tuple(values)


def vints_pack(values):
    revbytes = bytearray()
    values = [int(v) for v in values[::-1]]
    for value in values:
        v = encode_zig_zag(value)
        if v < 128:
            revbytes.append(v)
        else:
            num_extra_bytes = 0
            num_bits = v.bit_length()
            # We need to reserve (num_extra_bytes+1) bits in the first byte
            # ie. with 1 extra byte, the first byte needs to be something like '10XXXXXX' # 2 bits reserved
            # ie. with 8 extra bytes, the first byte needs to be '11111111'  # 8 bits reserved
            reserved_bits = num_extra_bytes + 1
            while num_bits > (8-(reserved_bits)):
                num_extra_bytes += 1
                num_bits -= 8
                reserved_bits = min(num_extra_bytes + 1, 8)
                revbytes.append(v & 0xff)
                v >>= 8

            if num_extra_bytes > 8:
                raise ValueError('Value %d is too big and cannot be encoded as vint' % value)

            # We can now store the last bits in the first byte
            n = 8 - num_extra_bytes
            v |= (0xff >> n << n)
            revbytes.append(abs(v))

    revbytes.reverse()
    return six.binary_type(revbytes)