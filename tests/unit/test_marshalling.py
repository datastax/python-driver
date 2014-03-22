try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import platform
from datetime import datetime
from decimal import Decimal
from uuid import UUID

try:
    from blist import sortedset
except ImportError:
    sortedset = set

from cassandra.cqltypes import lookup_casstype
from cassandra.util import OrderedDict

marshalled_value_pairs = (
    # binary form, type, python native type
    (b'lorem ipsum dolor sit amet', 'AsciiType', 'lorem ipsum dolor sit amet'),
    (b'', 'AsciiType', ''),
    (b'\x01', 'BooleanType', True),
    (b'\x00', 'BooleanType', False),
    (b'', 'BooleanType', None),
    (b'\xff\xfe\xfd\xfc\xfb', 'BytesType', b'\xff\xfe\xfd\xfc\xfb'),
    (b'', 'BytesType', b''),
    (b'\x7f\xff\xff\xff\xff\xff\xff\xff', 'CounterColumnType', 9223372036854775807),
    (b'\x80\x00\x00\x00\x00\x00\x00\x00', 'CounterColumnType', -9223372036854775808),
    (b'', 'CounterColumnType', None),
    (b'\x00\x00\x013\x7fb\xeey', 'DateType', datetime(2011, 11, 7, 18, 55, 49, 881000)),
    (b'', 'DateType', None),
    (b'\x00\x00\x00\r\nJ\x04"^\x91\x04\x8a\xb1\x18\xfe', 'DecimalType', Decimal('1243878957943.1234124191998')),
    (b'\x00\x00\x00\x06\xe5\xde]\x98Y', 'DecimalType', Decimal('-112233.441191')),
    (b'\x00\x00\x00\x14\x00\xfa\xce', 'DecimalType', Decimal('0.00000000000000064206')),
    (b'\x00\x00\x00\x14\xff\x052', 'DecimalType', Decimal('-0.00000000000000064206')),
    (b'\xff\xff\xff\x9c\x00\xfa\xce', 'DecimalType', Decimal('64206e100')),
    (b'', 'DecimalType', None),
    (b'@\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', 19432.125),
    (b'\xc0\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', -19432.125),
    (b'\x7f\xef\x00\x00\x00\x00\x00\x00', 'DoubleType', 1.7415152243978685e+308),
    (b'', 'DoubleType', None),
    (b'F\x97\xd0@', 'FloatType', 19432.125),
    (b'\xc6\x97\xd0@', 'FloatType', -19432.125),
    (b'\xc6\x97\xd0@', 'FloatType', -19432.125),
    (b'\x7f\x7f\x00\x00', 'FloatType', 338953138925153547590470800371487866880.0),
    (b'', 'FloatType', None),
    (b'\x7f\x50\x00\x00', 'Int32Type', 2135949312),
    (b'\xff\xfd\xcb\x91', 'Int32Type', -144495),
    (b'', 'Int32Type', None),
    (b'f\x1e\xfd\xf2\xe3\xb1\x9f|\x04_\x15', 'IntegerType', 123456789123456789123456789),
    (b'', 'IntegerType', None),
    (b'\x7f\xff\xff\xff\xff\xff\xff\xff', 'LongType', 9223372036854775807),
    (b'\x80\x00\x00\x00\x00\x00\x00\x00', 'LongType', -9223372036854775808),
    (b'', 'LongType', None),
    (b'', 'InetAddressType', None),
    (b'A46\xa9', 'InetAddressType', '65.52.54.169'),
    (b'*\x00\x13(\xe1\x02\xcc\xc0\x00\x00\x00\x00\x00\x00\x01"', 'InetAddressType', '2a00:1328:e102:ccc0::122'),
    (b'\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6', 'UTF8Type', u'\u307e\u3057\u3066'),
    (b'\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6' * 1000, 'UTF8Type', u'\u307e\u3057\u3066' * 1000),
    (b'', 'UTF8Type', u''),
    (b'\xff' * 16, 'UUIDType', UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')),
    (b'I\x15~\xfc\xef<\x9d\xe3\x16\x98\xaf\x80\x1f\xb4\x0b*', 'UUIDType', UUID('49157efc-ef3c-9de3-1698-af801fb40b2a')),
    (b'', 'UUIDType', None),
    (b'', 'MapType(AsciiType, BooleanType)', None),
    (b'', 'ListType(FloatType)', None),
    (b'', 'SetType(LongType)', None),
    (b'\x00\x00', 'MapType(DecimalType, BooleanType)', OrderedDict()),
    (b'\x00\x00', 'ListType(FloatType)', ()),
    (b'\x00\x00', 'SetType(IntegerType)', sortedset()),
    (b'\x00\x01\x00\x10\xafYC\xa3\xea<\x11\xe1\xabc\xc4,\x03"y\xf0', 'ListType(TimeUUIDType)', (UUID(bytes=b'\xafYC\xa3\xea<\x11\xe1\xabc\xc4,\x03"y\xf0'),)),
)

ordered_dict_value = OrderedDict()
ordered_dict_value[u'\u307fbob'] = 199
ordered_dict_value[u''] = -1
ordered_dict_value[u'\\'] = 0

# these following entries work for me right now, but they're dependent on
# vagaries of internal python ordering for unordered types
marshalled_value_pairs_unsafe = (
    (b'\x00\x03\x00\x06\xe3\x81\xbfbob\x00\x04\x00\x00\x00\xc7\x00\x00\x00\x04\xff\xff\xff\xff\x00\x01\\\x00\x04\x00\x00\x00\x00', 'MapType(UTF8Type, Int32Type)', ordered_dict_value),
    (b'\x00\x02\x00\x08@\x01\x99\x99\x99\x99\x99\x9a\x00\x08@\x14\x00\x00\x00\x00\x00\x00', 'SetType(DoubleType)', sortedset([2.2, 5.0])),
    (b'\x00', 'IntegerType', 0),
)

if platform.python_implementation() == 'CPython':
    # Only run tests for entries which depend on internal python ordering under
    # CPython
    marshalled_value_pairs += marshalled_value_pairs_unsafe


class TestUnmarshal(unittest.TestCase):
    def test_unmarshalling(self):
        for serializedval, valtype, nativeval in marshalled_value_pairs:
            unmarshaller = lookup_casstype(valtype)
            whatwegot = unmarshaller.from_binary(serializedval)
            self.assertEqual(whatwegot, nativeval,
                             msg='Unmarshaller for %s (%s) failed: unmarshal(%r) got %r instead of %r'
                                 % (valtype, unmarshaller, serializedval, whatwegot, nativeval))
            self.assertEqual(type(whatwegot), type(nativeval),
                             msg='Unmarshaller for %s (%s) gave wrong type (%s instead of %s)'
                                 % (valtype, unmarshaller, type(whatwegot), type(nativeval)))

    def test_marshalling(self):
        for serializedval, valtype, nativeval in marshalled_value_pairs:
            marshaller = lookup_casstype(valtype)
            whatwegot = marshaller.to_binary(nativeval)
            self.assertEqual(whatwegot, serializedval,
                             msg='Marshaller for %s (%s) failed: marshal(%r) got %r instead of %r'
                                 % (valtype, marshaller, nativeval, whatwegot, serializedval))
            self.assertEqual(type(whatwegot), type(serializedval),
                             msg='Marshaller for %s (%s) gave wrong type (%s instead of %s)'
                                 % (valtype, marshaller, type(whatwegot), type(serializedval)))
