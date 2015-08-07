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

DATETIME_EPOC = datetime.datetime(1970, 1, 1)

assert sys.byteorder in ('little', 'big')
is_little_endian = sys.byteorder == 'little'

import_datetime()

cdef datetime_from_timestamp(timestamp):
    return DATETIME_EPOC + timedelta_new(0, timestamp, 0)
