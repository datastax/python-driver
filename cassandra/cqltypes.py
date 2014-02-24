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

import calendar
from decimal import Decimal
import re
import socket
import time
from datetime import datetime
from uuid import UUID
import warnings

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # NOQA

from cassandra.marshal import (int8_pack, int8_unpack, uint16_pack, uint16_unpack,
                               int32_pack, int32_unpack, int64_pack, int64_unpack,
                               float_pack, float_unpack, double_pack, double_unpack,
                               varint_pack, varint_unpack)

apache_cassandra_type_prefix = 'org.apache.cassandra.db.marshal.'

_number_types = frozenset((int, long, float))

try:
    from blist import sortedset
except ImportError:
    warnings.warn(
        "The blist library is not available, so a normal set will "
        "be used in place of blist.sortedset for set collection values. "
        "You can find the blist library here: https://pypi.python.org/pypi/blist/")

    sortedset = set

try:
    from collections import OrderedDict
except ImportError:  # Python <2.7
    from cassandra.util import OrderedDict # NOQA


def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def unix_time_from_uuid1(u):
    return (u.get_time() - 0x01B21DD213814000) / 10000000.0

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
    args = [[]]
    for tok in tokens:
        if tok == '(':
            args.append([])
        elif tok == ')':
            arglist = args.pop()
            ctype = args[-1].pop()
            paramized = ctype.apply_parameters(*arglist)
            args[-1].append(paramized)
        else:
            if ':' in tok:
                # ignore those column name hex encoding bit; we have the
                # proper column name from elsewhere
                tok = tok.rsplit(':', 1)[-1]
            ctype = lookup_casstype_simple(tok)
            args[-1].append(ctype)

    return args[0][0]


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


class _CassandraType(object):
    __metaclass__ = CassandraTypeType
    subtypes = ()
    num_subtypes = 0
    empty_binary_ok = False

    def __init__(self, val):
        self.val = self.validate(val)

    def __str__(self):
        return '<%s( %r )>' % (self.cql_parameterized_type(), self.val)
    __repr__ = __str__

    @staticmethod
    def validate(val):
        """
        Called to transform an input value into one of a suitable type
        for this class. As an example, the BooleanType class uses this
        to convert an incoming value to True or False.
        """
        return val

    @classmethod
    def from_binary(cls, byts):
        """
        Deserialize a bytestring into a value. See the deserialize() method
        for more information. This method differs in that if None or the empty
        string is passed in, None may be returned.
        """
        if byts is None or (byts == '' and not cls.empty_binary_ok):
            return None
        return cls.deserialize(byts)

    @classmethod
    def to_binary(cls, val):
        """
        Serialize a value into a bytestring. See the serialize() method for
        more information. This method differs in that if None is passed in,
        the result is the empty string.
        """
        return '' if val is None else cls.serialize(val)

    @staticmethod
    def deserialize(byts):
        """
        Given a bytestring, deserialize into a value according to the protocol
        for this type. Note that this does not create a new instance of this
        class; it merely gives back a value that would be appropriate to go
        inside an instance of this class.
        """
        return byts

    @staticmethod
    def serialize(val):
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
    def apply_parameters(cls, *subtypes):
        """
        Given a set of other CassandraTypes, create a new subtype of this type
        using them as parameters. This is how composite types are constructed.

            >>> MapType.apply_parameters(DateType, BooleanType)
            <class 'cassandra.types.MapType(DateType, BooleanType)'>
        """
        if cls.num_subtypes != 'UNKNOWN' and len(subtypes) != cls.num_subtypes:
            raise ValueError("%s types require %d subtypes (%d given)"
                             % (cls.typename, cls.num_subtypes, len(subtypes)))
        newname = cls.cass_parameterized_type_with(subtypes).encode('utf8')
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


def mkUnrecognizedType(casstypename):
    return CassandraTypeType(casstypename.encode('utf8'),
                             (_UnrecognizedType,),
                             {'typename': "'%s'" % casstypename})


class BytesType(_CassandraType):
    typename = 'blob'
    empty_binary_ok = True

    @staticmethod
    def validate(val):
        return buffer(val)

    @staticmethod
    def serialize(val):
        return str(val)


class DecimalType(_CassandraType):
    typename = 'decimal'

    @staticmethod
    def validate(val):
        return Decimal(val)

    @staticmethod
    def deserialize(byts):
        scale = int32_unpack(byts[:4])
        unscaled = varint_unpack(byts[4:])
        return Decimal('%de%d' % (unscaled, -scale))

    @staticmethod
    def serialize(dec):
        sign, digits, exponent = dec.as_tuple()
        unscaled = int(''.join([str(digit) for digit in digits]))
        if sign:
            unscaled *= -1
        scale = int32_pack(-exponent)
        unscaled = varint_pack(unscaled)
        return scale + unscaled


class UUIDType(_CassandraType):
    typename = 'uuid'

    @staticmethod
    def deserialize(byts):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(uuid):
        return uuid.bytes


class BooleanType(_CassandraType):
    typename = 'boolean'

    @staticmethod
    def validate(val):
        return bool(val)

    @staticmethod
    def deserialize(byts):
        return bool(int8_unpack(byts))

    @staticmethod
    def serialize(truth):
        return int8_pack(bool(truth))


class AsciiType(_CassandraType):
    typename = 'ascii'
    empty_binary_ok = True


class FloatType(_CassandraType):
    typename = 'float'

    deserialize = staticmethod(float_unpack)
    serialize = staticmethod(float_pack)


class DoubleType(_CassandraType):
    typename = 'double'

    deserialize = staticmethod(double_unpack)
    serialize = staticmethod(double_pack)


class LongType(_CassandraType):
    typename = 'bigint'

    deserialize = staticmethod(int64_unpack)
    serialize = staticmethod(int64_pack)


class Int32Type(_CassandraType):
    typename = 'int'

    deserialize = staticmethod(int32_unpack)
    serialize = staticmethod(int32_pack)


class IntegerType(_CassandraType):
    typename = 'varint'

    deserialize = staticmethod(varint_unpack)
    serialize = staticmethod(varint_pack)


have_ipv6_packing = hasattr(socket, 'inet_ntop')


class InetAddressType(_CassandraType):
    typename = 'inet'

    # TODO: implement basic ipv6 support for Windows?
    # inet_ntop and inet_pton aren't available on Windows

    @staticmethod
    def deserialize(byts):
        if len(byts) == 16:
            if not have_ipv6_packing:
                raise Exception(
                    "IPv6 addresses cannot currently be handled on Windows")
            return socket.inet_ntop(socket.AF_INET6, byts)
        else:
            return socket.inet_ntoa(byts)

    @staticmethod
    def serialize(addr):
        if ':' in addr:
            fam = socket.AF_INET6
            if not have_ipv6_packing:
                raise Exception(
                    "IPv6 addresses cannot currently be handled on Windows")
            return socket.inet_pton(fam, addr)
        else:
            fam = socket.AF_INET
            return socket.inet_aton(addr)


class CounterColumnType(_CassandraType):
    typename = 'counter'

    deserialize = staticmethod(int64_unpack)
    serialize = staticmethod(int64_pack)


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
        if isinstance(date, basestring):
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
    def deserialize(byts):
        return datetime.utcfromtimestamp(int64_unpack(byts) / 1000.0)

    @staticmethod
    def serialize(v):
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
                warnings.warn("timestamp columns in Cassandra hold a number of "
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
    def deserialize(byts):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(timeuuid):
        return timeuuid.bytes


class UTF8Type(_CassandraType):
    typename = 'text'
    empty_binary_ok = True

    @staticmethod
    def deserialize(byts):
        return byts.decode('utf8')

    @staticmethod
    def serialize(ustr):
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
    def deserialize(cls, byts):
        if not cls.subtypes:
            raise NotImplementedError("can't deserialize unparameterized %s"
                                      % cls.typename)
        return cls.deserialize_safe(byts)

    @classmethod
    def serialize(cls, val):
        if not cls.subtypes:
            raise NotImplementedError("can't serialize unparameterized %s"
                                      % cls.typename)
        return cls.serialize_safe(val)


class _SimpleParameterizedType(_ParameterizedType):
    @classmethod
    def validate(cls, val):
        subtype, = cls.subtypes
        return cls.adapter([subtype.validate(subval) for subval in val])

    @classmethod
    def deserialize_safe(cls, byts):
        subtype, = cls.subtypes
        numelements = uint16_unpack(byts[:2])
        p = 2
        result = []
        for n in xrange(numelements):
            itemlen = uint16_unpack(byts[p:p + 2])
            p += 2
            item = byts[p:p + itemlen]
            p += itemlen
            result.append(subtype.from_binary(item))
        return cls.adapter(result)

    @classmethod
    def serialize_safe(cls, items):
        subtype, = cls.subtypes
        buf = StringIO()
        buf.write(uint16_pack(len(items)))
        for item in items:
            itembytes = subtype.to_binary(item)
            buf.write(uint16_pack(len(itembytes)))
            buf.write(itembytes)
        return buf.getvalue()


class ListType(_SimpleParameterizedType):
    typename = 'list'
    num_subtypes = 1
    adapter = tuple


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
        return dict((subkeytype.validate(k), subvaltype.validate(v)) for (k, v) in val.iteritems())

    @classmethod
    def deserialize_safe(cls, byts):
        subkeytype, subvaltype = cls.subtypes
        numelements = uint16_unpack(byts[:2])
        p = 2
        themap = OrderedDict()
        for n in xrange(numelements):
            key_len = uint16_unpack(byts[p:p + 2])
            p += 2
            keybytes = byts[p:p + key_len]
            p += key_len
            val_len = uint16_unpack(byts[p:p + 2])
            p += 2
            valbytes = byts[p:p + val_len]
            p += val_len
            key = subkeytype.from_binary(keybytes)
            val = subvaltype.from_binary(valbytes)
            themap[key] = val
        return themap

    @classmethod
    def serialize_safe(cls, themap):
        subkeytype, subvaltype = cls.subtypes
        buf = StringIO()
        buf.write(uint16_pack(len(themap)))
        for key, val in themap.iteritems():
            keybytes = subkeytype.to_binary(key)
            valbytes = subvaltype.to_binary(val)
            buf.write(uint16_pack(len(keybytes)))
            buf.write(keybytes)
            buf.write(uint16_pack(len(valbytes)))
            buf.write(valbytes)
        return buf.getvalue()


class CompositeType(_ParameterizedType):
    typename = "'org.apache.cassandra.db.marshal.CompositeType'"
    num_subtypes = 'UNKNOWN'


class DynamicCompositeType(_ParameterizedType):
    typename = "'org.apache.cassandra.db.marshal.DynamicCompositeType'"
    num_subtypes = 'UNKNOWN'


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
    def deserialize_safe(cls, byts):
        subtype, = cls.subtypes
        return subtype.from_binary(byts)

    @classmethod
    def serialize_safe(cls, val):
        subtype, = cls.subtypes
        return subtype.to_binary(val)


def is_counter_type(t):
    if isinstance(t, basestring):
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
