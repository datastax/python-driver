# Copyright 2013-2014 DataStax, Inc.
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

import logging
log = logging.getLogger(__name__)

from binascii import hexlify
import calendar
import datetime
import sys
import types
from uuid import UUID
import six

from cassandra.util import OrderedDict

if six.PY3:
    long = int


def cql_quote(term):
    # The ordering of this method is important for the result of this method to
    # be a native str type (for both Python 2 and 3)

    # Handle quoting of native str and bool types
    if isinstance(term, (str, bool)):
        return "'%s'" % str(term).replace("'", "''")
    # This branch of the if statement will only be used by Python 2 to catch
    # unicode strings, text_type is used to prevent type errors with Python 3.
    elif isinstance(term, six.text_type):
        return "'%s'" % term.encode('utf8').replace("'", "''")
    else:
        return str(term)


def cql_encode_none(val):
    return 'NULL'


def cql_encode_unicode(val):
    return cql_quote(val.encode('utf-8'))


def cql_encode_str(val):
    return cql_quote(val)


if six.PY3:
    def cql_encode_bytes(val):
        return (b'0x' + hexlify(val)).decode('utf-8')
elif sys.version_info >= (2, 7):
    def cql_encode_bytes(val):  # noqa
        return b'0x' + hexlify(val)
else:
    # python 2.6 requires string or read-only buffer for hexlify
    def cql_encode_bytes(val):  # noqa
        return b'0x' + hexlify(buffer(val))


def cql_encode_object(val):
    return str(val)


def cql_encode_datetime(val):
    timestamp = calendar.timegm(val.utctimetuple())
    return str(long(timestamp * 1e3 + getattr(val, 'microsecond', 0) / 1e3))


def cql_encode_date(val):
    return "'%s'" % val.strftime('%Y-%m-%d-0000')


def cql_encode_sequence(val):
    return '( %s )' % ' , '.join(cql_encoders.get(type(v), cql_encode_object)(v)
                                 for v in val)


def cql_encode_map_collection(val):
    return '{ %s }' % ' , '.join('%s : %s' % (
        cql_encode_all_types(k),
        cql_encode_all_types(v)
    ) for k, v in six.iteritems(val))


def cql_encode_list_collection(val):
    return '[ %s ]' % ' , '.join(map(cql_encode_all_types, val))


def cql_encode_set_collection(val):
    return '{ %s }' % ' , '.join(map(cql_encode_all_types, val))


def cql_encode_all_types(val):
    return cql_encoders.get(type(val), cql_encode_object)(val)


cql_encoders = {
    float: cql_encode_object,
    bytearray: cql_encode_bytes,
    str: cql_encode_str,
    int: cql_encode_object,
    UUID: cql_encode_object,
    datetime.datetime: cql_encode_datetime,
    datetime.date: cql_encode_date,
    dict: cql_encode_map_collection,
    OrderedDict: cql_encode_map_collection,
    list: cql_encode_list_collection,
    tuple: cql_encode_list_collection,
    set: cql_encode_set_collection,
    frozenset: cql_encode_set_collection,
    types.GeneratorType: cql_encode_list_collection
}

if six.PY2:
    cql_encoders.update({
        unicode: cql_encode_unicode,
        buffer: cql_encode_bytes,
        long: cql_encode_object,
        types.NoneType: cql_encode_none,
    })
else:
    cql_encoders.update({
        memoryview: cql_encode_bytes,
        bytes: cql_encode_bytes,
        type(None): cql_encode_none,
    })

# sortedset is optional
try:
    from blist import sortedset
    cql_encoders.update({
        sortedset: cql_encode_set_collection
    })
except ImportError:
    pass
