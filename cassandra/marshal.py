# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import struct

def _make_packer(format_string):
    try:
        packer = struct.Struct(format_string) # new in Python 2.5
    except AttributeError:
        pack = lambda x: struct.pack(format_string, x)
        unpack = lambda s: struct.unpack(format_string, s)
    else:
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

def varint_unpack(term):
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (1 << (len(term) * 8))
    return val

def bitlength(n):
    bitlen = 0
    while n > 0:
        n >>= 1
        bitlen += 1
    return bitlen

def varint_pack(big):
    pos = True
    if big < 0:
        bytelength = bitlength(abs(big) - 1) / 8 + 1
        big = (1 << bytelength * 8) + big
        pos = False
    revbytes = []
    while big > 0:
        revbytes.append(chr(big & 0xff))
        big >>= 8
    if pos and ord(revbytes[-1]) & 0x80:
        revbytes.append('\x00')
    revbytes.reverse()
    return ''.join(revbytes)
