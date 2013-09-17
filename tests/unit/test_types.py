import unittest
import datetime
import cassandra
from cassandra.cqltypes import lookup_cqltype, CassandraType, BooleanType, lookup_casstype_simple, lookup_casstype, \
    AsciiType, LongType, DecimalType, DoubleType, FloatType, Int32Type, UTF8Type, IntegerType, SetType, cql_typename

from cassandra.cluster import Cluster


class TypeTests(unittest.TestCase):

    def test_lookup_casstype_simple(self):
        """
        Ensure lookup_casstype_simple returns the correct classes
        """

        self.assertEqual(lookup_casstype_simple('AsciiType'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype_simple('LongType'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_casstype_simple('BytesType'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype_simple('BooleanType'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype_simple('CounterColumnType'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype_simple('DecimalType'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype_simple('DoubleType'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype_simple('FloatType'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype_simple('InetAddressType'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype_simple('Int32Type'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype_simple('UTF8Type'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype_simple('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype_simple('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype_simple('UUIDType'), cassandra.cqltypes.UUIDType)

        # DISCUSS: varchar is not valid?
        # self.assertEqual(lookup_casstype_simple('varchar'), cassandra.cqltypes.UTF8Type)

        self.assertEqual(lookup_casstype_simple('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype_simple('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype_simple('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype_simple('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype_simple('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype_simple('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype_simple('ReversedType'), cassandra.cqltypes.ReversedType)

        self.assertEqual(str(lookup_casstype_simple('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

    def test_lookup_casstype(self):
        """
        Ensure lookup_casstype returns the correct classes
        """

        self.assertEqual(lookup_casstype('AsciiType'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype('LongType'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_casstype('BytesType'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype('BooleanType'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype('CounterColumnType'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype('DecimalType'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype('DoubleType'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype('FloatType'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype('InetAddressType'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype('Int32Type'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype('UTF8Type'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype('UUIDType'), cassandra.cqltypes.UUIDType)

        # DISCUSS: varchar is not valid?
        # self.assertEqual(lookup_casstype('varchar'), cassandra.cqltypes.UTF8Type)

        self.assertEqual(lookup_casstype('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype('ReversedType'), cassandra.cqltypes.ReversedType)

        self.assertEqual(str(lookup_casstype('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

        self.assertRaises(ValueError, lookup_casstype, 'AsciiType~')

        # DISCUSS: Figure out if other tests are needed
        self.assertEqual(str(lookup_casstype(BooleanType(True))), str(BooleanType(True)))


    def test_lookup_cqltype(self):
        """
        Ensure lookup_cqltype returns the correct class
        """

        self.assertEqual(lookup_cqltype('ascii'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_cqltype('bigint'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_cqltype('blob'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_cqltype('boolean'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_cqltype('counter'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_cqltype('decimal'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_cqltype('double'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_cqltype('float'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_cqltype('inet'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_cqltype('int'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_cqltype('text'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_cqltype('timestamp'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_cqltype('timeuuid'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_cqltype('uuid'), cassandra.cqltypes.UUIDType)
        self.assertEqual(lookup_cqltype('varchar'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_cqltype('varint'), cassandra.cqltypes.IntegerType)


        self.assertEqual(str(lookup_cqltype('list<ascii>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.AsciiType)))
        self.assertEqual(str(lookup_cqltype('list<bigint>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.LongType)))
        self.assertEqual(str(lookup_cqltype('list<blob>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.BytesType)))
        self.assertEqual(str(lookup_cqltype('list<boolean>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.BooleanType)))
        self.assertEqual(str(lookup_cqltype('list<counter>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.CounterColumnType)))
        self.assertEqual(str(lookup_cqltype('list<decimal>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.DecimalType)))
        self.assertEqual(str(lookup_cqltype('list<double>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.DoubleType)))
        self.assertEqual(str(lookup_cqltype('list<float>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.FloatType)))
        self.assertEqual(str(lookup_cqltype('list<inet>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.InetAddressType)))
        self.assertEqual(str(lookup_cqltype('list<int>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.Int32Type)))
        self.assertEqual(str(lookup_cqltype('list<text>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.UTF8Type)))
        self.assertEqual(str(lookup_cqltype('list<timestamp>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.DateType)))
        self.assertEqual(str(lookup_cqltype('list<timeuuid>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.TimeUUIDType)))
        self.assertEqual(str(lookup_cqltype('list<uuid>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.UUIDType)))
        self.assertEqual(str(lookup_cqltype('list<varchar>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.UTF8Type)))
        self.assertEqual(str(lookup_cqltype('list<varint>')),
                         str(cassandra.cqltypes.ListType.apply_parameters(cassandra.cqltypes.IntegerType)))


        self.assertEqual(str(lookup_cqltype('set<ascii>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.AsciiType)))
        self.assertEqual(str(lookup_cqltype('set<bigint>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.LongType)))
        self.assertEqual(str(lookup_cqltype('set<blob>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.BytesType)))
        self.assertEqual(str(lookup_cqltype('set<boolean>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.BooleanType)))
        self.assertEqual(str(lookup_cqltype('set<counter>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.CounterColumnType)))
        self.assertEqual(str(lookup_cqltype('set<decimal>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.DecimalType)))
        self.assertEqual(str(lookup_cqltype('set<double>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.DoubleType)))
        self.assertEqual(str(lookup_cqltype('set<float>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.FloatType)))
        self.assertEqual(str(lookup_cqltype('set<inet>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.InetAddressType)))
        self.assertEqual(str(lookup_cqltype('set<int>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.Int32Type)))
        self.assertEqual(str(lookup_cqltype('set<text>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.UTF8Type)))
        self.assertEqual(str(lookup_cqltype('set<timestamp>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.DateType)))
        self.assertEqual(str(lookup_cqltype('set<timeuuid>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.TimeUUIDType)))
        self.assertEqual(str(lookup_cqltype('set<uuid>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.UUIDType)))
        self.assertEqual(str(lookup_cqltype('set<varchar>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.UTF8Type)))
        self.assertEqual(str(lookup_cqltype('set<varint>')),
                         str(cassandra.cqltypes.SetType.apply_parameters(cassandra.cqltypes.IntegerType)))


        self.assertEqual(str(lookup_cqltype('map<ascii, ascii>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.AsciiType,
                                                                         cassandra.cqltypes.AsciiType)))
        self.assertEqual(str(lookup_cqltype('map<bigint, bigint>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.LongType,
                                                                         cassandra.cqltypes.LongType)))
        self.assertEqual(str(lookup_cqltype('map<blob, blob>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.BytesType,
                                                                         cassandra.cqltypes.BytesType)))
        self.assertEqual(str(lookup_cqltype('map<boolean, boolean>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.BooleanType,
                                                                         cassandra.cqltypes.BooleanType)))
        self.assertEqual(str(lookup_cqltype('map<counter, counter>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.CounterColumnType,
                                                                         cassandra.cqltypes.CounterColumnType)))
        self.assertEqual(str(lookup_cqltype('map<decimal, decimal>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.DecimalType,
                                                                         cassandra.cqltypes.DecimalType)))
        self.assertEqual(str(lookup_cqltype('map<double, double>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.DoubleType,
                                                                         cassandra.cqltypes.DoubleType)))
        self.assertEqual(str(lookup_cqltype('map<float, float>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.FloatType,
                                                                         cassandra.cqltypes.FloatType)))
        self.assertEqual(str(lookup_cqltype('map<inet, inet>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.InetAddressType,
                                                                         cassandra.cqltypes.InetAddressType)))
        self.assertEqual(str(lookup_cqltype('map<int, int>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.Int32Type,
                                                                         cassandra.cqltypes.Int32Type)))
        self.assertEqual(str(lookup_cqltype('map<text, text>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.UTF8Type,
                                                                         cassandra.cqltypes.UTF8Type)))
        self.assertEqual(str(lookup_cqltype('map<timestamp, timestamp>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.DateType,
                                                                         cassandra.cqltypes.DateType)))
        self.assertEqual(str(lookup_cqltype('map<timeuuid, timeuuid>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.TimeUUIDType,
                                                                         cassandra.cqltypes.TimeUUIDType)))
        self.assertEqual(str(lookup_cqltype('map<uuid, uuid>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.UUIDType,
                                                                         cassandra.cqltypes.UUIDType)))
        self.assertEqual(str(lookup_cqltype('map<varchar, varchar>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.UTF8Type,
                                                                         cassandra.cqltypes.UTF8Type)))
        self.assertEqual(str(lookup_cqltype('map<varint, varint>')),
                         str(cassandra.cqltypes.MapType.apply_parameters(cassandra.cqltypes.IntegerType,
                                                                         cassandra.cqltypes.IntegerType)))

        # DISCUSS: Figure out if other tests are needed, and how to test them
        # self.assertEqual(str(lookup_cqltype(AsciiType(CassandraType('asdf')))), str(AsciiType(CassandraType('asdf'))))
        # self.assertEqual(str(lookup_cqltype(LongType(CassandraType(1234)))), str(LongType(CassandraType(1234))))
        # self.assertEqual(str(lookup_cqltype(BytesType(CassandraType(True)))), str(BytesType(CassandraType(True))))
        self.assertEqual(str(lookup_cqltype(BooleanType(CassandraType(True)))), str(BooleanType(CassandraType(True))))
        # self.assertEqual(str(lookup_cqltype(CounterColumnType(CassandraType(True)))), str(CounterColumnType(CassandraType(True))))
        # self.assertEqual(str(lookup_cqltype(DecimalType(CassandraType(1234.1234)))), str(DecimalType(CassandraType(1234.1234))))
        # self.assertEqual(str(lookup_cqltype(DoubleType(CassandraType(1234.1234)))), str(DoubleType(CassandraType(1234.1234))))
        # self.assertEqual(str(lookup_cqltype(FloatType(CassandraType(1234.1234)))), str(FloatType(CassandraType(1234.1234))))
        # self.assertEqual(str(lookup_cqltype(InetAddressType(CassandraType(True)))), str(InetAddressType(CassandraType(True))))
        # self.assertEqual(str(lookup_cqltype(Int32Type(CassandraType(1234)))), str(Int32Type(CassandraType(1234))))
        # self.assertEqual(str(lookup_cqltype(UTF8Type(CassandraType('asdf')))), str(UTF8Type(CassandraType('asdf'))))
        # self.assertEqual(str(lookup_cqltype(DateType(CassandraType(True)))), str(DateType(CassandraType(True))))
        # self.assertEqual(str(lookup_cqltype(TimeUUIDType(CassandraType(True)))), str(TimeUUIDType(CassandraType(True))))
        # self.assertEqual(str(lookup_cqltype(UUIDType(CassandraType(True)))), str(UUIDType(CassandraType(True))))
        # self.assertEqual(str(lookup_cqltype(IntegerType(CassandraType(1234)))), str(IntegerType(CassandraType(1234))))

        # DISCUSS: Check if typo in code, or misunderstanding
        # self.assertEqual(lookup_cqltype("'ascii'"), cassandra.cqltypes.AsciiType)
        # self.assertEqual(lookup_cqltype("'bigint'"), cassandra.cqltypes.LongType)
        # self.assertEqual(lookup_cqltype("'blob'"), cassandra.cqltypes.BytesType)
        # self.assertEqual(lookup_cqltype("'boolean'"), cassandra.cqltypes.BooleanType)
        # self.assertEqual(lookup_cqltype("'counter'"), cassandra.cqltypes.CounterColumnType)
        # self.assertEqual(lookup_cqltype("'decimal'"), cassandra.cqltypes.DecimalType)
        # self.assertEqual(lookup_cqltype("'float'"), cassandra.cqltypes.FloatType)
        # self.assertEqual(lookup_cqltype("'inet'"), cassandra.cqltypes.InetAddressType)
        # self.assertEqual(lookup_cqltype("'int'"), cassandra.cqltypes.Int32Type)
        # self.assertEqual(lookup_cqltype("'text'"), cassandra.cqltypes.UTF8Type)
        # self.assertEqual(lookup_cqltype("'timestamp'"), cassandra.cqltypes.DateType)
        # self.assertEqual(lookup_cqltype("'timeuuid'"), cassandra.cqltypes.TimeUUIDType)
        # self.assertEqual(lookup_cqltype("'uuid'"), cassandra.cqltypes.UUIDType)
        # self.assertEqual(lookup_cqltype("'varchar'"), cassandra.cqltypes.UTF8Type)
        # self.assertEqual(lookup_cqltype("'varint'"), cassandra.cqltypes.IntegerType)

    def test_cassandratype(self):
        """
        Smoke test cass_parameterized_type_with
        """

        self.assertEqual(LongType.cass_parameterized_type_with(()), 'LongType')
        self.assertEqual(LongType.cass_parameterized_type_with((), full=True), 'org.apache.cassandra.db.marshal.LongType')
        self.assertEqual(SetType.cass_parameterized_type_with([DecimalType], full=True), 'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.DecimalType)')

        self.assertEqual(LongType.cql_parameterized_type(), 'bigint')
        self.assertEqual(cassandra.cqltypes.MapType.apply_parameters(
                         cassandra.cqltypes.UTF8Type, cassandra.cqltypes.UTF8Type).cql_parameterized_type(),
                         'map<text, text>')

    def test_datetype(self):
        """
        Test cassandra.cqltypes.DateType() construction
        """

        pass
        # TODO: Figure out the required format here
        # date_string = str(datetime.datetime.now().strftime('%s.%f'))
        # print date_string
        # print cassandra.cqltypes.DateType(date_string)

    def test_cql_typename(self):
        """
        Smoke test cql_typename
        """

        self.assertEqual(cql_typename('DateType'), 'timestamp')
        self.assertEqual(cql_typename('org.apache.cassandra.db.marshal.ListType(IntegerType)'), 'list<varint>')
