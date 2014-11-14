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
import re
import socket
import time
from datetime import datetime
from uuid import UUID
import warnings

import six
from six.moves import range

from cassandra.marshal import (int8_pack, int8_unpack, uint16_pack, uint16_unpack,
                               int32_pack, int32_unpack, int64_pack, int64_unpack,
                               float_pack, float_unpack, double_pack, double_unpack,
                               varint_pack, varint_unpack)
from cassandra.util import OrderedDict, sortedset

apache_cassandra_type_prefix = 'org.apache.cassandra.db.marshal.'


if six.PY3:
    _number_types = frozenset((int, float))
    long = int
else:
    _number_types = frozenset((int, long, float))


def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def unix_time_from_uuid1(u):
    return (u.time - 0x01B21DD213814000) / 10000000.0


_casstypes = {}


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
            if ':' in tok:
                name, tok = tok.rsplit(':', 1)
                names.append(name)
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
        <class 'cassandra.types.MapType(UTF8Type, Int32Type)'>

    """
    if isinstance(casstype, (CassandraType, CassandraTypeType)):
        return casstype
    try:
        return parse_casstype_args(casstype)
    except (ValueError, AssertionError, IndexError) as e:
        raise ValueError("Don't know how to parse type string %r: %s" % (casstype, e))


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

    def __init__(self, val):
        self.val = self.validate(val)

    def __repr__(self):
        return '<%s( %r )>' % (self.cql_parameterized_type(), self.val)

    @staticmethod
    def validate(val):
        """
        Called to transform an input value into one of a suitable type
        for this class. As an example, the BooleanType class uses this
        to convert an incoming value to True or False.
        """
        return val

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

            >>> MapType.apply_parameters(DateType, BooleanType)
            <class 'cassandra.types.MapType(DateType, BooleanType)'>

        `subtypes` will be a sequence of CassandraTypes.  If provided, `names`
        will be an equally long sequence of column names or Nones.
        """
        if cls.num_subtypes != 'UNKNOWN' and len(subtypes) != cls.num_subtypes:
            raise ValueError("%s types require %d subtypes (%d given)"
                             % (cls.typename, cls.num_subtypes, len(subtypes)))
        newname = cls.cass_parameterized_type_with(subtypes)
        if six.PY2 and isinstance(newname, unicode):
            newname = newname.encode('utf-8')
        return type(newname, (cls,), {'subtypes': subtypes, 'cassname': cls.cassname})

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
    def validate(val):
        return bytearray(val)

    @staticmethod
    def serialize(val, protocol_version):
        return six.binary_type(val)


class DecimalType(_CassandraType):
    typename = 'decimal'

    @staticmethod
    def validate(val):
        return Decimal(val)

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
            raise TypeError("Non-Decimal type received for Decimal value")
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
    def validate(val):
        return bool(val)

    @staticmethod
    def deserialize(byts, protocol_version):
        return bool(int8_unpack(byts))

    @staticmethod
    def serialize(truth, protocol_version):
        return int8_pack(truth)


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


have_ipv6_packing = hasattr(socket, 'inet_ntop')


class InetAddressType(_CassandraType):
    typename = 'inet'

    # TODO: implement basic ipv6 support for Windows?
    # inet_ntop and inet_pton aren't available on Windows

    @staticmethod
    def deserialize(byts, protocol_version):
        if len(byts) == 16:
            if not have_ipv6_packing:
                raise Exception(
                    "IPv6 addresses cannot currently be handled on Windows")
            return socket.inet_ntop(socket.AF_INET6, byts)
        else:
            return socket.inet_ntoa(byts)

    @staticmethod
    def serialize(addr, protocol_version):
        if ':' in addr:
            fam = socket.AF_INET6
            if not have_ipv6_packing:
                raise Exception(
                    "IPv6 addresses cannot currently be handled on Windows")
            return socket.inet_pton(fam, addr)
        else:
            fam = socket.AF_INET
            return socket.inet_aton(addr)


class CounterColumnType(LongType):
    typename = 'counter'


cql_time_formats = (
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d'
)

_have_warned_about_timestamps = False


class DateType(_CassandraType):
    typename = 'timestamp'

    @classmethod
    def validate(cls, date):
        if isinstance(date, six.string_types):
            date = cls.interpret_datestring(date)
        return date

    @staticmethod
    def interpret_datestring(date):
        if date[-5] in ('+', '-'):
            offset = (int(date[-4:-2]) * 3600 + int(date[-2:]) * 60) * int(date[-5] + '1')
            date = date[:-5]
        else:
            offset = -time.timezone
        for tformat in cql_time_formats:
            try:
                tval = time.strptime(date, tformat)
            except ValueError:
                continue
            return calendar.timegm(tval) + offset
        else:
            raise ValueError("can't interpret %r as a date" % (date,))

    def my_timestamp(self):
        return self.val

    @staticmethod
    def deserialize(byts, protocol_version):
        return datetime.utcfromtimestamp(int64_unpack(byts) / 1000.0)

    @staticmethod
    def serialize(v, protocol_version):
        global _have_warned_about_timestamps
        try:
            converted = calendar.timegm(v.utctimetuple())
            converted = converted * 1e3 + getattr(v, 'microsecond', 0) / 1e3
        except AttributeError:
            # Ints and floats are valid timestamps too
            if type(v) not in _number_types:
                raise TypeError('DateType arguments must be a datetime or timestamp')

            if not _have_warned_about_timestamps:
                _have_warned_about_timestamps = True
                warnings.warn(
                    "timestamp columns in Cassandra hold a number of "
                    "milliseconds since the unix epoch.  Currently, when executing "
                    "prepared statements, this driver multiplies timestamp "
                    "values by 1000 so that the result of time.time() "
                    "can be used directly.  However, the driver cannot "
                    "match this behavior for non-prepared statements, "
                    "so the 2.0 version of the driver will no longer multiply "
                    "timestamps by 1000.  It is suggested that you simply use "
                    "datetime.datetime objects for 'timestamp' values to avoid "
                    "any ambiguity and to guarantee a smooth upgrade of the "
                    "driver.")
            converted = v * 1e3

        return int64_pack(long(converted))


class TimestampType(DateType):
    pass


class TimeUUIDType(DateType):
    typename = 'timeuuid'

    def my_timestamp(self):
        return unix_time_from_uuid1(self.val)

    @staticmethod
    def deserialize(byts, protocol_version):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(timeuuid, protocol_version):
        try:
            return timeuuid.bytes
        except AttributeError:
            raise TypeError("Got a non-UUID object for a UUID value")


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
    def __init__(self, val):
        if not self.subtypes:
            raise ValueError("%s type with no parameters can't be instantiated" % (self.typename,))
        _CassandraType.__init__(self, val)

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
    def validate(cls, val):
        subtype, = cls.subtypes
        return cls.adapter([subtype.validate(subval) for subval in val])

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
        for _ in range(numelements):
            itemlen = unpack(byts[p:p + length])
            p += length
            item = byts[p:p + itemlen]
            p += itemlen
            result.append(subtype.from_binary(item, protocol_version))
        return cls.adapter(result)

    @classmethod
    def serialize_safe(cls, items, protocol_version):
        if isinstance(items, six.string_types):
            raise TypeError("Received a string for a type that expects a sequence")

        subtype, = cls.subtypes
        pack = int32_pack if protocol_version >= 3 else uint16_pack
        buf = io.BytesIO()
        buf.write(pack(len(items)))
        for item in items:
            itembytes = subtype.to_binary(item, protocol_version)
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
    adapter = sortedset


class MapType(_ParameterizedType):
    typename = 'map'
    num_subtypes = 2

    @classmethod
    def validate(cls, val):
        subkeytype, subvaltype = cls.subtypes
        return dict((subkeytype.validate(k), subvaltype.validate(v)) for (k, v) in six.iteritems(val))

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        subkeytype, subvaltype = cls.subtypes
        if protocol_version >= 3:
            unpack = int32_unpack
            length = 4
        else:
            unpack = uint16_unpack
            length = 2
        numelements = unpack(byts[:length])
        p = length
        themap = OrderedDict()
        for _ in range(numelements):
            key_len = unpack(byts[p:p + length])
            p += length
            keybytes = byts[p:p + key_len]
            p += key_len
            val_len = unpack(byts[p:p + length])
            p += length
            valbytes = byts[p:p + val_len]
            p += val_len
            key = subkeytype.from_binary(keybytes, protocol_version)
            val = subvaltype.from_binary(valbytes, protocol_version)
            themap[key] = val
        return themap

    @classmethod
    def serialize_safe(cls, themap, protocol_version):
        subkeytype, subvaltype = cls.subtypes
        pack = int32_pack if protocol_version >= 3 else uint16_pack
        buf = io.BytesIO()
        buf.write(pack(len(themap)))
        try:
            items = six.iteritems(themap)
        except AttributeError:
            raise TypeError("Got a non-map object for a map value")
        for key, val in items:
            keybytes = subkeytype.to_binary(key, protocol_version)
            valbytes = subvaltype.to_binary(val, protocol_version)
            buf.write(pack(len(keybytes)))
            buf.write(keybytes)
            buf.write(pack(len(valbytes)))
            buf.write(valbytes)
        return buf.getvalue()


class TupleType(_ParameterizedType):
    typename = 'tuple'
    num_subtypes = 'UNKNOWN'

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
    typename = "'org.apache.cassandra.db.marshal.UserType'"

    _cache = {}

    @classmethod
    def make_udt_class(cls, keyspace, udt_name, names_and_types, mapped_class):
        if six.PY2 and isinstance(udt_name, unicode):
            udt_name = udt_name.encode('utf-8')

        try:
            return cls._cache[(keyspace, udt_name)]
        except KeyError:
            fieldnames, types = zip(*names_and_types)
            instance = type(udt_name, (cls,), {'subtypes': types,
                                               'cassname': cls.cassname,
                                               'typename': udt_name,
                                               'fieldnames': fieldnames,
                                               'keyspace': keyspace,
                                               'mapped_class': mapped_class})
            cls._cache[(keyspace, udt_name)] = instance
            return instance

    @classmethod
    def apply_parameters(cls, subtypes, names):
        keyspace = subtypes[0]
        udt_name = unhexlify(subtypes[1].cassname)
        field_names = [unhexlify(encoded_name) for encoded_name in names[2:]]
        assert len(field_names) == len(subtypes[2:])
        return type(udt_name, (cls,), {'subtypes': subtypes[2:],
                                       'cassname': cls.cassname,
                                       'typename': udt_name,
                                       'fieldnames': field_names,
                                       'keyspace': keyspace,
                                       'mapped_class': None})

    @classmethod
    def cql_parameterized_type(cls):
        return "frozen<%s>" % (cls.typename,)

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

        if cls.mapped_class:
            return cls.mapped_class(**dict(zip(cls.fieldnames, values)))
        else:
            Result = namedtuple(cls.typename, cls.fieldnames)
            return Result(*values)

    @classmethod
    def serialize_safe(cls, val, protocol_version):
        proto_version = max(3, protocol_version)
        buf = io.BytesIO()
        for fieldname, subtype in zip(cls.fieldnames, cls.subtypes):
            item = getattr(val, fieldname)
            if item is not None:
                packed_item = subtype.to_binary(getattr(val, fieldname), proto_version)
                buf.write(int32_pack(len(packed_item)))
                buf.write(packed_item)
            else:
                buf.write(int32_pack(-1))
        return buf.getvalue()


class CompositeType(_ParameterizedType):
    typename = "'org.apache.cassandra.db.marshal.CompositeType'"
    num_subtypes = 'UNKNOWN'

    @classmethod
    def cql_parameterized_type(cls):
        """
        There is no CQL notation for Composites, so we override this.
        """
        typestring = cls.cass_parameterized_type(full=True)
        return "'%s'" % (typestring,)


class DynamicCompositeType(CompositeType):
    typename = "'org.apache.cassandra.db.marshal.DynamicCompositeType'"


class ColumnToCollectionType(_ParameterizedType):
    """
    This class only really exists so that we can cleanly evaluate types when
    Cassandra includes this. We don't actually need or want the extra
    information.
    """
    typename = "'org.apache.cassandra.db.marshal.ColumnToCollectionType'"
    num_subtypes = 'UNKNOWN'


class ReversedType(_ParameterizedType):
    typename = "'org.apache.cassandra.db.marshal.ReversedType'"
    num_subtypes = 1

    @classmethod
    def deserialize_safe(cls, byts, protocol_version):
        subtype, = cls.subtypes
        return subtype.from_binary(byts)

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
