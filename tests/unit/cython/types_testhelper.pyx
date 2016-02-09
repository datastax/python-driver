# Copyright 2013-2016 DataStax, Inc.
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


import time
import datetime

include '../../../cassandra/ioutils.pyx'

from cassandra.cqltypes import DateType
from cassandra.deserializers import find_deserializer
from cassandra.bytesio cimport BytesIOReader
from cassandra.buffer cimport Buffer
from cassandra.deserializers cimport from_binary, Deserializer


def test_datetype(assert_equal):

    cdef Deserializer des = find_deserializer(DateType)

    def deserialize(timestamp):
        """Serialize a datetime and deserialize it using the cython deserializer"""

        cdef BytesIOReader reader
        cdef Buffer buf

        dt = datetime.datetime.utcfromtimestamp(timestamp)
        reader = BytesIOReader(b'\x00\x00\x00\x08' + DateType.serialize(dt, 0))
        get_buf(reader, &buf)
        deserialized_dt = from_binary(des, &buf, 0)

        return deserialized_dt

    # deserialize
    # epoc
    expected = 0
    assert_equal(deserialize(expected), datetime.datetime.utcfromtimestamp(expected))

    # beyond 32b
    expected = 2 ** 33
    assert_equal(deserialize(expected), datetime.datetime(2242, 3, 16, 12, 56, 32))

    # less than epoc (PYTHON-119)
    expected = -770172256
    assert_equal(deserialize(expected), datetime.datetime(1945, 8, 5, 23, 15, 44))

    # work around rounding difference among Python versions (PYTHON-230)
    # This wont pass with the cython extension until we fix the microseconds alignment with CPython
    #expected = 1424817268.274
    #assert_equal(deserialize(expected), datetime.datetime(2015, 2, 24, 22, 34, 28, 274000))

    # Large date overflow (PYTHON-452)
    expected = 2177403010.123
    assert_equal(deserialize(expected), datetime.datetime(2038, 12, 31, 10, 10, 10, 123000))
