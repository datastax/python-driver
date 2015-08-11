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

"""
Duplicate module of util.py, with some accelerated functions
used for deserialization.
"""

from cpython.datetime cimport (
    timedelta_new,
        # cdef inline object timedelta_new(int days, int seconds, int useconds)
        # Create timedelta object using DateTime CAPI factory function.
        # Note, there are no range checks for any of the arguments.
    import_datetime,
        # Datetime C API initialization function.
        # You have to call it before any usage of DateTime CAPI functions.
    )

import datetime
import sys

cdef bint is_little_endian
from cassandra.util import is_little_endian

import_datetime()

DATETIME_EPOC = datetime.datetime(1970, 1, 1)


cdef datetime_from_timestamp(double timestamp):
    cdef int seconds = <int> timestamp
    cdef int microseconds = (<int64_t> (timestamp * 1000000)) % 1000000
    return DATETIME_EPOC + timedelta_new(0, seconds, microseconds)
