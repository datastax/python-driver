# Copyright DataStax, Inc.
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

from cassandra.bytesio cimport BytesIOReader
import pytest

def test_read1():
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    assert reader.read(2)[:2] == b'ab'
    assert reader.read(2)[:2] == b'cd'
    assert reader.read(0)[:0] == b''
    assert reader.read(2)[:2] == b'ef'

def test_read2():
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(5)
    reader.read(1)

def test_read3():
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(6)

def test_read_eof():
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(5)
    with pytest.raises(EOFError):
        reader.read(2)
    reader.read(1) # see that we can still read this
