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

DEF DAY_IN_SECONDS = 86400

DATETIME_EPOC = datetime.datetime(1970, 1, 1)


cdef datetime_from_timestamp(double timestamp):
    cdef int days = <int> (timestamp / DAY_IN_SECONDS)
    cdef int64_t days_in_seconds = (<int64_t> days) * DAY_IN_SECONDS
    cdef int seconds = <int> (timestamp - days_in_seconds)
    cdef int microseconds = <int> ((timestamp - days_in_seconds - seconds) * 1000000)

    return DATETIME_EPOC + timedelta_new(days, seconds, microseconds)
