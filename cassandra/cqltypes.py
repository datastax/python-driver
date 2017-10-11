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

"""
Representation of Cassandra data types. These classes should make it simple for
the library (and caller software) to deal with Cassandra-style Java class type
names and CQL type specifiers, and convert between them cleanly. Parameterized
types are fully supported in both flavors. Once you have the right Type object
for the type you want, you can use it to serialize, deserialize, or retrieve
the corresponding CQL or Cassandra type strings.
"""

# NOTE:
# If/when the need arises for interpret types from CQL string literals in
# different ways (for https://issues.apache.org/jira/browse/CASSANDRA-3799,
# for example), these classes would be a good place to tack on
# .from_cql_literal() and .as_cql_literal() classmethods (or whatever).

from __future__ import absolute_import  # to enable import io from stdlib
from binascii import unhexlify
import calendar
from collections import namedtuple
from decimal import Decimal
import io
import logging
import re
import socket
import time
import six
from six.moves import range
import sys
from uuid import UUID
import warnings

if six.PY3:
    import ipaddress

from cassandra.marshal import (int8_pack, int8_unpack, int16_pack, int16_unpack,
                               uint16_pack, uint16_unpack, uint32_pack, uint32_unpack,
                               int32_pack, int32_unpack, int64_pack, int64_unpack,
                               float_pack, float_unpack, double_pack, double_unpack,
                               varint_pack, varint_unpack, vints_pack, vints_unpack)
from cassandra import util

apache_cassandra_type_prefix = 'org.apache.cassandra.db.marshal.'

cassandra_empty_type = 'org.apache.cassandra.db.marshal.EmptyType'
cql_empty_type = 'empty'

log = logging.getLogger(__name__)

if six.PY3:
    _number_types = frozenset((int, float))
    long = int

    def _name_from_hex_string(encoded_name):
        bin_str = unhexlify(encoded_name)
        return bin_str.decode('ascii')
else:
    _number_types = frozenset((int, long, float))
    _name_from_hex_string = unhexlify


def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


_casstypes = {}
_cqltypes = {}


cql_type_scanner = re.Scanner((
    ('frozen', None),
    (r'[a-zA-Z0-9_]+', lambda s, t: t),
    (r'[\s,<>]', None),
))


def cql_types_from_string(cql_type):
    return cql_type_scanner.scan(cql_type)[0]


class CassandraTypeType(type):
    """
    The CassandraType objects in this module will normally be used directly,
    rather than through instances of those types. They can be instantiated,
    of course, but the type information is what this driver mainly needs.

    This metaclass registers CassandraType classes in the global
    by-cassandra-typename and by-cql-typename registries, unless their class
    name starts with an underscore.
    """

    def __new__(metacls, name, bases, dct):
        dct.setdefault('cassname', name)
        cls = type.__new__(metacls, name, bases, dct)
        if not name.startswith('_'):
            _casstypes[name] = cls
            if not cls.typename.startswith(apache_cassandra_type_prefix):
                _cqltypes[cls.typename] = cls
        return cls


casstype_scanner = re.Scanner((
    (r'[()]', lambda s, t: t),
    (r'[a-zA-Z0-9_.:=>]+', lambda s, t: t),
    (r'[\s,]', None),
))


def lookup_casstype_simple(casstype):
    """
    Given a Cassandra type name (either fully distinguished or not), hand
    back the CassandraType class responsible for it. If a name is not
    recognized, a custom _UnrecognizedType subclass will be created for it.

    This function does not handle complex types (so no type parameters--
    nothing with parentheses). Use lookup_casstype() instead if you might need
    that.
    """
    shortname = trim_if_startswith(casstype, apache_cassandra_type_prefix)
    try:
        typeclass = _casstypes[shortname]
    except KeyError:
        typeclass = mkUnrecognizedType(casstype)
    return typeclass


def parse_casstype_args(typestring):
    tokens, remainder = casstype_scanner.scan(typestring)
    if remainder:
        raise ValueError("weird characters %r at end" % remainder)

    # use a stack of (types, names) lists
    args = [([], [])]
    for tok in tokens:
        if tok == '(':
            args.append(([], []))
        elif tok == ')':
            types, names = args.pop()
            prev_types, prev_names = args[-1]
            prev_types[-1] = prev_types[-1].apply_parameters(types, names)
        else:
            types, names = args[-1]
            parts = re.split(':|=>', tok)
            tok = parts.pop()
            if parts:
                names.append(parts[0])
            else:
                names.append(None)

            ctype = lookup_casstype_simple(tok)
            types.append(ctype)

    # return the first (outer) type, which will have all parameters applied
    return args[0][0][0]


def lookup_casstype(casstype):
    """
    Given a Cassandra type as a string (possibly including parameters), hand
    back the CassandraType class responsible for it. If a name is not
    recognized, a custom _UnrecognizedType subclass will be created for it.

    Example:

        >>> lookup_casstype('org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)')
        <class 'cassandra.cqltypes.MapType(UTF8Type, Int32Type)'>

    """
    if isinstance(casstype, (CassandraType, CassandraTypeType)):
        return casstype
    try:
        return parse_casstype_args(casstype)
    except (ValueError, AssertionError, IndexError) as e:
        raise ValueError("Don't know how to parse type string %r: %s" % (casstype, e))


def is_reversed_casstype(data_type):
    return issubclass(data_type, ReversedType)


class EmptyValue(object):
    """ See _CassandraType.support_empty_values """

    def __str__(self):
        return "EMPTY"
    __repr__ = __str__

EMPTY = EmptyValue()


@six.add_metaclass(CassandraTypeType)
class _CassandraType(object):
    subtypes = ()
    num_subtypes = 0
    empty_binary_ok = False

    support_empty_values = False
    """
    Back in the Thrift days, empty strings were used for "null" values of
    all types, including non-string types.  For most users, an empty
    string value in an int column is the same as being null/not present,
    so the driver normally returns None in this case.  (For string-like
    types, it *will* return an empty string by default instead of None.)

    To avoid this behavior, set this to :const:`True`. Instead of returning
    None for empty string values, the EMPTY singleton (an instance
    of EmptyValue) will be returned.
    """

    def __repr__(self):
        return '<%s( %r )>' % (self.cql_parameterized_type(), self.val)

    @classmethod
    def from_binary(cls, byts, protocol_version):
        """
        Deserialize a bytestring into a value. See the deserialize() method
        for more information. This method differs in that if None or the empty
        string is passed in, None may be returned.
        """
        if byts is None:
            return None
        elif len(byts) == 0 and not cls.empty_binary_ok:
            return EMPTY if cls.support_empty_values else None
        return cls.deserialize(byts, protocol_version)

    @classmethod
    def to_binary(cls, val, protocol_version):
        """
        Serialize a value into a bytestring. See the serialize() method for
        more information. This method differs in that if None is passed in,
        the result is the empty string.
        """
        return b'' if val is None else cls.serialize(val, protocol_version)

    @staticmethod
    def deserialize(byts, protocol_version):
        """
        Given a bytestring, deserialize into a value according to the protocol
        for this type. Note that this does not create a new instance of this
        class; it merely gives back a value that would be appropriate to go
        inside an instance of this class.
        """
        return byts

    @staticmethod
    def serialize(val, protocol_version):
        """
        Given a value appropriate for this class, serialize it according to the
        protocol for this type and return the corresponding bytestring.
        """
        return val

    @classmethod
    def cass_parameterized_type_with(cls, subtypes, full=False):
        """
        Return the name of this type as it would be expressed by Cassandra,
        optionally fully qualified. If subtypes is not None, it is expected
        to be a list of other CassandraType subclasses, and the output
        string includes the Cassandra names for those subclasses as well,
        as parameters to this one.

        Example:

            >>> LongType.cass_parameterized_type_with(())
            'LongType'
            >>> LongType.cass_parameterized_type_with((), full=True)
            'org.apache.cassandra.db.marshal.LongType'
            >>> SetType.cass_parameterized_type_with([DecimalType], full=True)
            'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.DecimalType)'
        """
        cname = cls.cassname
        if full and '.' not in cname:
            cname = apache_cassandra_type_prefix + cname
        if not subtypes:
            return cname
        sublist = ', '.join(styp.cass_parameterized_type(full=full) for styp in subtypes)
        return '%s(%s)' % (cname, sublist)

    @classmethod
    def apply_parameters(cls, subtypes, names=None):
        """
        Given a set of other CassandraTypes, create a new subtype of this type
        using them as parameters. This is how composite types are constructed.

            >>> MapType.apply_parameters([DateType, BooleanType])
            <class 'cassandra.cqltypes.MapType(DateType, BooleanType)'>

        `subtypes` will be a sequence of CassandraTypes.  If provided, `names`
        will be an equally long sequence of column names or Nones.
        """
        if cls.num_subtypes != 'UNKNOWN' and len(subtypes) != cls.num_subtypes:
            raise ValueError("%s types require %d subtypes (%d given)"
                             % (cls.typename, cls.num_subtypes, len(subtypes)))
        newname = cls.cass_parameterized_type_with(subtypes)
        if six.PY2 and isinstance(newname, unicode):
            newname = newname.encode('utf-8')
        return type(newname, (cls,), {'subtypes': subtypes, 'cassname': cls.cassname, 'fieldnames': names})

    @classmethod
    def cql_parameterized_type(cls):
        """
        Return a CQL type specifier for this type. If this type has parameters,
        they are included in standard CQL <> notation.
        """
        if not cls.subtypes:
            return cls.typename
        return '%s<%s>' % (cls.typename, ', '.join(styp.cql_parameterized_type() for styp in cls.subtypes))

    @classmethod
    def cass_parameterized_type(cls, full=False):
        """
        Return a Cassandra type specifier for this type. If this type has
        parameters, they are included in the standard () notation.
        """
        return cls.cass_parameterized_type_with(cls.subtypes, full=full)


# it's initially named with a _ to avoid registering it as a real type, but
# client programs may want to use the name still for isinstance(), etc
CassandraType = _CassandraType


class _UnrecognizedType(_CassandraType):
    num_subtypes = 'UNKNOWN'


if six.PY3:
    def mkUnrecognizedType(casstypename):
        return CassandraTypeType(casstypename,
                                 (_UnrecognizedType,),
                                 {'typename': "'%s'" % casstypename})
else:
    def mkUnrecognizedType(casstypename):  # noqa
        return CassandraTypeType(casstypename.encode('utf8'),
                                 (_UnrecognizedType,),
                                 {'typename': "'%s'" % casstypename})


class BytesType(_CassandraType):
    typename = 'blob'
    empty_binary_ok = True

    @staticmethod
    def serialize(val, protocol_version):
        return six.binary_type(val)


class DecimalType(_CassandraType):
    typename = 'decimal'

    @staticmethod
    def deserialize(byts, protocol_version):
        scale = int32_unpack(byts[:4])
        unscaled = varint_unpack(byts[4:])
        return Decimal('%de%d' % (unscaled, -scale))

    @staticmethod
    def serialize(dec, protocol_version):
        try:
            sign, digits, exponent = dec.as_tuple()
        except AttributeError:
            try:
                sign, digits, exponent = Decimal(dec).as_tuple()
            except Exception:
                raise TypeError("Invalid type for Decimal value: %r", dec)
        unscaled = int(''.join([str(digit) for digit in digits]))
        if sign:
            unscaled *= -1
        scale = int32_pack(-exponent)
        unscaled = varint_pack(unscaled)
        return scale + unscaled


class UUIDType(_CassandraType):
    typename = 'uuid'

    @staticmethod
    def deserialize(byts, protocol_version):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(uuid, protocol_version):
        try:
            return uuid.bytes
        except AttributeError:
            raise TypeError("Got a non-UUID object for a UUID value")


class BooleanType(_CassandraType):
    typename = 'boolean'

    @staticmethod
    def deserialize(byts, protocol_version):
        return bool(int8_unpack(byts))

    @staticmethod
    def serialize(truth, protocol_version):
        return int8_pack(truth)

class ByteType(_CassandraType):
    typename = 'tinyint'

    @staticmethod
    def deserialize(byts, protocol_version):
        return int8_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return int8_pack(byts)


if six.PY2:
    class AsciiType(_CassandraType):
        typename = 'ascii'
        empty_binary_ok = True
else:
    class AsciiType(_CassandraType):
        typename = 'ascii'
        empty_binary_ok = True

        @staticmethod
        def deserialize(byts, protocol_version):
            return byts.decode('ascii')

        @staticmethod
        def serialize(var, protocol_version):
            try:
                return var.encode('ascii')
            except UnicodeDecodeError:
                return var


class FloatType(_CassandraType):
    typename = 'float'

    @staticmethod
    def deserialize(byts, protocol_version):
        return float_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return float_pack(byts)


class DoubleType(_CassandraType):
    typename = 'double'

    @staticmethod
    def deserialize(byts, protocol_version):
        return double_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return double_pack(byts)


class LongType(_CassandraType):
    typename = 'bigint'

    @staticmethod
    def deserialize(byts, protocol_version):
        return int64_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return int64_pack(byts)


class Int32Type(_CassandraType):
    typename = 'int'

    @staticmethod
    def deserialize(byts, protocol_version):
        return int32_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return int32_pack(byts)


class IntegerType(_CassandraType):
    typename = 'varint'

    @staticmethod
    def deserialize(byts, protocol_version):
        return varint_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return varint_pack(byts)


class InetAddressType(_CassandraType):
    typename = 'inet'

    @staticmethod
    def deserialize(byts, protocol_version):
        if len(byts) == 16:
            return util.inet_ntop(socket.AF_INET6, byts)
        else:
            # util.inet_pton could also handle, but this is faster
            # since we've already determined the AF
            return socket.inet_ntoa(byts)

    @staticmethod
    def serialize(addr, protocol_version):
        try:
            if ':' in addr:
                return util.inet_pton(socket.AF_INET6, addr)
            else:
                # util.inet_pton could also handle, but this is faster
                # since we've already determined the AF
                return socket.inet_aton(addr)
        except:
            if six.PY3 and isinstance(addr, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
                return addr.packed
            raise ValueError("can't interpret %r as an inet address" % (addr,))


class CounterColumnType(LongType):
    typename = 'counter'

cql_timestamp_formats = (
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d'
)

_have_warned_about_timestamps = False


class DateType(_CassandraType):
    typename = 'timestamp'

    @staticmethod
    def interpret_datestring(val):
        if val[-5] in ('+', '-'):
            offset = (int(val[-4:-2]) * 3600 + int(val[-2:]) * 60) * int(val[-5] + '1')
            val = val[:-5]
        else:
            offset = -time.timezone
        for tformat in cql_timestamp_formats:
            try:
                tval = time.strptime(val, tformat)
            except ValueError:
                continue
            # scale seconds to millis for the raw value
            return (calendar.timegm(tval) + offset) * 1e3
        else:
            raise ValueError("can't interpret %r as a date" % (val,))

    @staticmethod
    def deserialize(byts, protocol_version):
        timestamp = int64_unpack(byts) / 1000.0
        return util.datetime_from_timestamp(timestamp)

    @staticmethod
    def serialize(v, protocol_version):
        try:
            # v is datetime
            timestamp_seconds = calendar.timegm(v.utctimetuple())
            timestamp = timestamp_seconds * 1e3 + getattr(v, 'microsecond', 0) / 1e3
        except AttributeError:
            try:
                timestamp = calendar.timegm(v.timetuple()) * 1e3
            except AttributeError:
                # Ints and floats are valid timestamps too
                if type(v) not in _number_types:
                    raise TypeError('DateType arguments must be a datetime, date, or timestamp')
                timestamp = v

        return int64_pack(long(timestamp))


class TimestampType(DateType):
    pass


class TimeUUIDType(DateType):
    typename = 'timeuuid'

    def my_timestamp(self):
        return util.unix_time_from_uuid1(self.val)

    @staticmethod
    def deserialize(byts, protocol_version):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(timeuuid, protocol_version):
        try:
            return timeuuid.bytes
        except AttributeError:
            raise TypeError("Got a non-UUID object for a UUID value")


class SimpleDateType(_CassandraType):
    typename = 'date'
    date_format = "%Y-%m-%d"

    # Values of the 'date'` type are encoded as 32-bit unsigned integers
    # representing a number of days with epoch (January 1st, 1970) at the center of the
    # range (2^31).
    EPOCH_OFFSET_DAYS = 2 ** 31

    @staticmethod
    def deserialize(byts, protocol_version):
        days = uint32_unpack(byts) - SimpleDateType.EPOCH_OFFSET_DAYS
        return util.Date(days)

    @staticmethod
    def serialize(val, protocol_version):
        try:
            days = val.days_from_epoch
        except AttributeError:
            if isinstance(val, six.integer_types):
                # the DB wants offset int values, but util.Date init takes days from epoch
                # here we assume int values are offset, as they would appear in CQL
                # short circuit to avoid subtracting just to add offset
                return uint32_pack(val)
            days = util.Date(val).days_from_epoch
        return uint32_pack(days + SimpleDateType.EPOCH_OFFSET_DAYS)


class ShortType(_CassandraType):
    typename = 'smallint'

    @staticmethod
    def deserialize(byts, protocol_version):
        return int16_unpack(byts)

    @staticmethod
    def serialize(byts, protocol_version):
        return int16_pack(byts)


class TimeType(_CassandraType):
    typename = 'time'

    @staticmethod
    def deserialize(byts, protocol_version):
        return util.Time(int64_unpack(byts))

    @staticmethod
    def serialize(val, protocol_version):
        try:
            nano = val.nanosecond_time
        except AttributeError:
            nano = util.Time(val).nanosecond_time
        return int64_pack(nano)


class DurationType(_CassandraType):
    typename = 'duration'

    @staticmethod
    def deserialize(byts, protocol_version):
        months, days, nanoseconds = vints_unpack(byts)
        return util.Duration(months, days, nanoseconds)

    @staticmethod
    def serialize(duration, protocol_version):
        try:
            m, d, n = duration.months, duration.days, duration.nanoseconds
        except AttributeError:
            raise TypeError('DurationType arguments must be a Duration.')
        return vints_pack([m, d, n])


class UTF8Type(_CassandraType):
    typename = 'text'
    empty_binary_ok = True

    @staticmethod
    def deserialize(byts, protocol_version):
        return byts.decode('utf8')

    @staticmethod
    def serialize(ustr, protocol_version):
        try:
            return ustr.encode('utf-8')
        except UnicodeDecodeError:
            # already utf-8
            return ustr


class VarcharType(UTF8Type):
    typename = 'varchar'


class _ParameterizedType(_CassandraType):
    num_subtypes = 'UNKNOWN'

    @classmethod
    def deserialize(cls, byts, protocol_version):
        if not cls.subtypes:
            raise NotImplementedError("can't deserialize unparameterized %s"
                                      % cls.typename)
        return cls.deserialize_safe(byts, protocol_version)

    @classmethod
    def serialize(cls, val, protocol_version):
        if not cls.subtypes:
            raise NotImplementedError("can't serialize unparameterized %s"
                                      % cls.typename)
        return cls.serialize_safe(val, protocol_version)


class _SimpleParameterizedType(_ParameterizedType):
    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        subtype, = cls.subtypes
        if protocol_version >= 3:
            unpack = int32_unpack
            length = 4
        else:
            unpack = uint16_unpack
            length = 2
        numelements = unpack(byts[:length])
        p = length
        result = []
        inner_proto = max(3, protocol_version)
        for _ in range(numelements):
            itemlen = unpack(byts[p:p + length])
            p += length
            item = byts[p:p + itemlen]
            p += itemlen
            result.append(subtype.from_binary(item, inner_proto))
        return cls.adapter(result)

    @classmethod
    def serialize_safe(cls, items, protocol_version):
        if isinstance(items, six.string_types):
            raise TypeError("Received a string for a type that expects a sequence")

        subtype, = cls.subtypes
        pack = int32_pack if protocol_version >= 3 else uint16_pack
        buf = io.BytesIO()
        buf.write(pack(len(items)))
        inner_proto = max(3, protocol_version)
        for item in items:
            itembytes = subtype.to_binary(item, inner_proto)
            buf.write(pack(len(itembytes)))
            buf.write(itembytes)
        return buf.getvalue()


class ListType(_SimpleParameterizedType):
    typename = 'list'
    num_subtypes = 1
    adapter = list


class SetType(_SimpleParameterizedType):
    typename = 'set'
    num_subtypes = 1
    adapter = util.sortedset


class MapType(_ParameterizedType):
    typename = 'map'
    num_subtypes = 2

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        key_type, value_type = cls.subtypes
        if protocol_version >= 3:
            unpack = int32_unpack
            length = 4
        else:
            unpack = uint16_unpack
            length = 2
        numelements = unpack(byts[:length])
        p = length
        themap = util.OrderedMapSerializedKey(key_type, protocol_version)
        inner_proto = max(3, protocol_version)
        for _ in range(numelements):
            key_len = unpack(byts[p:p + length])
            p += length
            keybytes = byts[p:p + key_len]
            p += key_len
            val_len = unpack(byts[p:p + length])
            p += length
            valbytes = byts[p:p + val_len]
            p += val_len
            key = key_type.from_binary(keybytes, inner_proto)
            val = value_type.from_binary(valbytes, inner_proto)
            themap._insert_unchecked(key, keybytes, val)
        return themap

    @classmethod
    def serialize_safe(cls, themap, protocol_version):
        key_type, value_type = cls.subtypes
        pack = int32_pack if protocol_version >= 3 else uint16_pack
        buf = io.BytesIO()
        buf.write(pack(len(themap)))
        try:
            items = six.iteritems(themap)
        except AttributeError:
            raise TypeError("Got a non-map object for a map value")
        inner_proto = max(3, protocol_version)
        for key, val in items:
            keybytes = key_type.to_binary(key, inner_proto)
            valbytes = value_type.to_binary(val, inner_proto)
            buf.write(pack(len(keybytes)))
            buf.write(keybytes)
            buf.write(pack(len(valbytes)))
            buf.write(valbytes)
        return buf.getvalue()


class TupleType(_ParameterizedType):
    typename = 'tuple'

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        proto_version = max(3, protocol_version)
        p = 0
        values = []
        for col_type in cls.subtypes:
            if p == len(byts):
                break
            itemlen = int32_unpack(byts[p:p + 4])
            p += 4
            if itemlen >= 0:
                item = byts[p:p + itemlen]
                p += itemlen
            else:
                item = None
            # collections inside UDTs are always encoded with at least the
            # version 3 format
            values.append(col_type.from_binary(item, proto_version))

        if len(values) < len(cls.subtypes):
            nones = [None] * (len(cls.subtypes) - len(values))
            values = values + nones

        return tuple(values)

    @classmethod
    def serialize_safe(cls, val, protocol_version):
        if len(val) > len(cls.subtypes):
            raise ValueError("Expected %d items in a tuple, but got %d: %s" %
                             (len(cls.subtypes), len(val), val))

        proto_version = max(3, protocol_version)
        buf = io.BytesIO()
        for item, subtype in zip(val, cls.subtypes):
            if item is not None:
                packed_item = subtype.to_binary(item, proto_version)
                buf.write(int32_pack(len(packed_item)))
                buf.write(packed_item)
            else:
                buf.write(int32_pack(-1))
        return buf.getvalue()

    @classmethod
    def cql_parameterized_type(cls):
        subtypes_string = ', '.join(sub.cql_parameterized_type() for sub in cls.subtypes)
        return 'frozen<tuple<%s>>' % (subtypes_string,)


class UserType(TupleType):
    typename = "org.apache.cassandra.db.marshal.UserType"

    _cache = {}
    _module = sys.modules[__name__]

    @classmethod
    def make_udt_class(cls, keyspace, udt_name, field_names, field_types):
        assert len(field_names) == len(field_types)

        if six.PY2 and isinstance(udt_name, unicode):
            udt_name = udt_name.encode('utf-8')

        instance = cls._cache.get((keyspace, udt_name))
        if not instance or instance.fieldnames != field_names or instance.subtypes != field_types:
            instance = type(udt_name, (cls,), {'subtypes': field_types,
                                               'cassname': cls.cassname,
                                               'typename': udt_name,
                                               'fieldnames': field_names,
                                               'keyspace': keyspace,
                                               'mapped_class': None,
                                               'tuple_type': cls._make_registered_udt_namedtuple(keyspace, udt_name, field_names)})
            cls._cache[(keyspace, udt_name)] = instance
        return instance

    @classmethod
    def evict_udt_class(cls, keyspace, udt_name):
        if six.PY2 and isinstance(udt_name, unicode):
            udt_name = udt_name.encode('utf-8')
        try:
            del cls._cache[(keyspace, udt_name)]
        except KeyError:
            pass

    @classmethod
    def apply_parameters(cls, subtypes, names):
        keyspace = subtypes[0].cass_parameterized_type()  # when parsed from cassandra type, the keyspace is created as an unrecognized cass type; This gets the name back
        udt_name = _name_from_hex_string(subtypes[1].cassname)
        field_names = tuple(_name_from_hex_string(encoded_name) for encoded_name in names[2:])  # using tuple here to match what comes into make_udt_class from other sources (for caching equality test)
        return cls.make_udt_class(keyspace, udt_name, field_names, tuple(subtypes[2:]))

    @classmethod
    def cql_parameterized_type(cls):
        return "frozen<%s>" % (cls.typename,)

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        values = super(UserType, cls).deserialize_safe(byts, protocol_version)
        if cls.mapped_class:
            return cls.mapped_class(**dict(zip(cls.fieldnames, values)))
        elif cls.tuple_type:
            return cls.tuple_type(*values)
        else:
            return tuple(values)

    @classmethod
    def serialize_safe(cls, val, protocol_version):
        proto_version = max(3, protocol_version)
        buf = io.BytesIO()
        for i, (fieldname, subtype) in enumerate(zip(cls.fieldnames, cls.subtypes)):
            # first treat as a tuple, else by custom type
            try:
                item = val[i]
            except TypeError:
                item = getattr(val, fieldname)

            if item is not None:
                packed_item = subtype.to_binary(item, proto_version)
                buf.write(int32_pack(len(packed_item)))
                buf.write(packed_item)
            else:
                buf.write(int32_pack(-1))
        return buf.getvalue()

    @classmethod
    def _make_registered_udt_namedtuple(cls, keyspace, name, field_names):
        # this is required to make the type resolvable via this module...
        # required when unregistered udts are pickled for use as keys in
        # util.OrderedMap
        t = cls._make_udt_tuple_type(name, field_names)
        if t:
            qualified_name = "%s_%s" % (keyspace, name)
            setattr(cls._module, qualified_name, t)
        return t

    @classmethod
    def _make_udt_tuple_type(cls, name, field_names):
        # fallback to positional named, then unnamed tuples
        # for CQL identifiers that aren't valid in Python,
        try:
            t = namedtuple(name, field_names)
        except ValueError:
            try:
                t = namedtuple(name, util._positional_rename_invalid_identifiers(field_names))
                log.warn("could not create a namedtuple for '%s' because one or more field names are not valid Python identifiers (%s); " \
                         "returning positionally-named fields" % (name, field_names))
            except ValueError:
                t = None
                log.warn("could not create a namedtuple for '%s' because the name is not a valid Python identifier; " \
                         "will return tuples in its place" % (name,))
        return t


class CompositeType(_ParameterizedType):
    typename = "org.apache.cassandra.db.marshal.CompositeType"

    @classmethod
    def cql_parameterized_type(cls):
        """
        There is no CQL notation for Composites, so we override this.
        """
        typestring = cls.cass_parameterized_type(full=True)
        return "'%s'" % (typestring,)

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        result = []
        for subtype in cls.subtypes:
            if not byts:
                # CompositeType can have missing elements at the end
                break

            element_length = uint16_unpack(byts[:2])
            element = byts[2:2 + element_length]

            # skip element length, element, and the EOC (one byte)
            byts = byts[2 + element_length + 1:]
            result.append(subtype.from_binary(element, protocol_version))

        return tuple(result)


class DynamicCompositeType(_ParameterizedType):
    typename = "org.apache.cassandra.db.marshal.DynamicCompositeType"

    @classmethod
    def cql_parameterized_type(cls):
        sublist = ', '.join('%s=>%s' % (alias, typ.cass_parameterized_type(full=True)) for alias, typ in zip(cls.fieldnames, cls.subtypes))
        return "'%s(%s)'" % (cls.typename, sublist)


class ColumnToCollectionType(_ParameterizedType):
    """
    This class only really exists so that we can cleanly evaluate types when
    Cassandra includes this. We don't actually need or want the extra
    information.
    """
    typename = "org.apache.cassandra.db.marshal.ColumnToCollectionType"


class ReversedType(_ParameterizedType):
    typename = "org.apache.cassandra.db.marshal.ReversedType"
    num_subtypes = 1

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        subtype, = cls.subtypes
        return subtype.from_binary(byts, protocol_version)

    @classmethod
    def serialize_safe(cls, val, protocol_version):
        subtype, = cls.subtypes
        return subtype.to_binary(val, protocol_version)


class FrozenType(_ParameterizedType):
    typename = "frozen"
    num_subtypes = 1

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        subtype, = cls.subtypes
        return subtype.from_binary(byts, protocol_version)

    @classmethod
    def serialize_safe(cls, val, protocol_version):
        subtype, = cls.subtypes
        return subtype.to_binary(val, protocol_version)


def is_counter_type(t):
    if isinstance(t, six.string_types):
        t = lookup_casstype(t)
    return issubclass(t, CounterColumnType)


def cql_typename(casstypename):
    """
    Translate a Cassandra-style type specifier (optionally-fully-distinguished
    Java class names for data types, along with optional parameters) into a
    CQL-style type specifier.

        >>> cql_typename('DateType')
        'timestamp'
        >>> cql_typename('org.apache.cassandra.db.marshal.ListType(IntegerType)')
        'list<varint>'
    """
    return lookup_casstype(casstypename).cql_parameterized_type()
