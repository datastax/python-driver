from binascii import hexlify
import calendar
import datetime
import sys
import types
from uuid import UUID
import six

from cassandra.util import OrderedDict

# if six.PY3:
#     unicode = str
#     long = int


def cql_quote(term):
    if isinstance(term, unicode):
        return "'%s'" % term.encode('utf8').replace("'", "''")
    elif isinstance(term, (str, bool)):
        return "'%s'" % str(term).replace("'", "''")
    else:
        return str(term)


def cql_encode_none(val):
    return 'NULL'


def cql_encode_unicode(val):
    return cql_quote(val.encode('utf-8'))


def cql_encode_str(val):
    return cql_quote(val)


if sys.version_info >= (2, 7):
    def cql_encode_bytes(val):
        return '0x' + hexlify(val)
else:
    # python 2.6 requires string or read-only buffer for hexlify
    def cql_encode_bytes(val):  # noqa
        return '0x' + hexlify(buffer(val))


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
    return '{ %s }' % ' , '.join(
                                 '%s : %s' % (
                                     cql_encode_all_types(k),
                                     cql_encode_all_types(v))
                                 for k, v in val.iteritems())


def cql_encode_list_collection(val):
    return '[ %s ]' % ' , '.join(map(cql_encode_all_types, val))


def cql_encode_set_collection(val):
    return '{ %s }' % ' , '.join(map(cql_encode_all_types, val))


def cql_encode_all_types(val):
    return cql_encoders.get(type(val), cql_encode_object)(val)


cql_encoders = {
    six.binary_type: cql_encode_bytes,
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
        buffer: cql_encode_bytes,
        unicode: cql_encode_unicode,
        long: cql_encode_object,
        types.NoneType: cql_encode_none,
    })
else:
    cql_encoders.update({
        memoryview: cql_encode_bytes,
        None: cql_encode_none,
    })
