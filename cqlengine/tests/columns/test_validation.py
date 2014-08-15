#tests the behavior of the column classes
from datetime import datetime, timedelta
from datetime import date
from datetime import tzinfo
from decimal import Decimal as D
from unittest import TestCase
from uuid import uuid4, uuid1
from cassandra import InvalidRequest
import six
from cqlengine import ValidationError
from cqlengine.connection import execute

from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.columns import Column, TimeUUID
from cqlengine.columns import Bytes
from cqlengine.columns import Ascii
from cqlengine.columns import Text
from cqlengine.columns import Integer
from cqlengine.columns import BigInt
from cqlengine.columns import VarInt
from cqlengine.columns import DateTime
from cqlengine.columns import Date
from cqlengine.columns import UUID
from cqlengine.columns import Boolean
from cqlengine.columns import Float
from cqlengine.columns import Decimal
from cqlengine.columns import Inet

from cqlengine.management import sync_table, drop_table
from cqlengine.models import Model

import sys


class TestDatetime(BaseCassEngTestCase):
    class DatetimeTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        created_at = DateTime()

    @classmethod
    def setUpClass(cls):
        super(TestDatetime, cls).setUpClass()
        sync_table(cls.DatetimeTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDatetime, cls).tearDownClass()
        drop_table(cls.DatetimeTest)

    def test_datetime_io(self):
        now = datetime.now()
        dt = self.DatetimeTest.objects.create(test_id=0, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=0).first()
        assert dt2.created_at.timetuple()[:6] == now.timetuple()[:6]

    def test_datetime_tzinfo_io(self):
        class TZ(tzinfo):
            def utcoffset(self, date_time):
                return timedelta(hours=-1)
            def dst(self, date_time):
                return None

        now = datetime(1982, 1, 1, tzinfo=TZ())
        dt = self.DatetimeTest.objects.create(test_id=0, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=0).first()
        assert dt2.created_at.timetuple()[:6] == (now + timedelta(hours=1)).timetuple()[:6]

    def test_datetime_date_support(self):
        today = date.today()
        self.DatetimeTest.objects.create(test_id=0, created_at=today)
        dt2 = self.DatetimeTest.objects(test_id=0).first()
        assert dt2.created_at.isoformat() == datetime(today.year, today.month, today.day).isoformat()

    def test_datetime_none(self):
        dt = self.DatetimeTest.objects.create(test_id=1, created_at=None)
        dt2 = self.DatetimeTest.objects(test_id=1).first()
        assert dt2.created_at is None

        dts = self.DatetimeTest.objects.filter(test_id=1).values_list('created_at')
        assert dts[0][0] is None


class TestBoolDefault(BaseCassEngTestCase):
    class BoolDefaultValueTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        stuff = Boolean(default=True)

    @classmethod
    def setUpClass(cls):
        super(TestBoolDefault, cls).setUpClass()
        sync_table(cls.BoolDefaultValueTest)

    def test_default_is_set(self):
        tmp = self.BoolDefaultValueTest.create(test_id=1)
        self.assertEqual(True, tmp.stuff)
        tmp2 = self.BoolDefaultValueTest.get(test_id=1)
        self.assertEqual(True, tmp2.stuff)



class TestVarInt(BaseCassEngTestCase):
    class VarIntTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        bignum = VarInt(primary_key=True)

    @classmethod
    def setUpClass(cls):
        super(TestVarInt, cls).setUpClass()
        sync_table(cls.VarIntTest)

    @classmethod
    def tearDownClass(cls):
        super(TestVarInt, cls).tearDownClass()
        sync_table(cls.VarIntTest)

    def test_varint_io(self):
        # TODO: this is a weird test.  i changed the number from sys.maxint (which doesn't exist in python 3)
        # to the giant number below and it broken between runs.
        long_int = 92834902384092834092384028340283048239048203480234823048230482304820348239
        int1 = self.VarIntTest.objects.create(test_id=0, bignum=long_int)
        int2 = self.VarIntTest.objects(test_id=0).first()
        self.assertEqual(int1.bignum, int2.bignum)


class TestDate(BaseCassEngTestCase):
    class DateTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        created_at = Date()

    @classmethod
    def setUpClass(cls):
        super(TestDate, cls).setUpClass()
        sync_table(cls.DateTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDate, cls).tearDownClass()
        drop_table(cls.DateTest)

    def test_date_io(self):
        today = date.today()
        self.DateTest.objects.create(test_id=0, created_at=today)
        dt2 = self.DateTest.objects(test_id=0).first()
        assert dt2.created_at.isoformat() == today.isoformat()

    def test_date_io_using_datetime(self):
        now = datetime.utcnow()
        self.DateTest.objects.create(test_id=0, created_at=now)
        dt2 = self.DateTest.objects(test_id=0).first()
        assert not isinstance(dt2.created_at, datetime)
        assert isinstance(dt2.created_at, date)
        assert dt2.created_at.isoformat() == now.date().isoformat()

    def test_date_none(self):
        self.DateTest.objects.create(test_id=1, created_at=None)
        dt2 = self.DateTest.objects(test_id=1).first()
        assert dt2.created_at is None

        dts = self.DateTest.objects(test_id=1).values_list('created_at')
        assert dts[0][0] is None


class TestDecimal(BaseCassEngTestCase):
    class DecimalTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        dec_val = Decimal()

    @classmethod
    def setUpClass(cls):
        super(TestDecimal, cls).setUpClass()
        sync_table(cls.DecimalTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDecimal, cls).tearDownClass()
        drop_table(cls.DecimalTest)

    def test_decimal_io(self):
        dt = self.DecimalTest.objects.create(test_id=0, dec_val=D('0.00'))
        dt2 = self.DecimalTest.objects(test_id=0).first()
        assert dt2.dec_val == dt.dec_val

        dt = self.DecimalTest.objects.create(test_id=0, dec_val=5)
        dt2 = self.DecimalTest.objects(test_id=0).first()
        assert dt2.dec_val == D('5')

class TestUUID(BaseCassEngTestCase):
    class UUIDTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        a_uuid = UUID(default=uuid4())

    @classmethod
    def setUpClass(cls):
        super(TestUUID, cls).setUpClass()
        sync_table(cls.UUIDTest)

    @classmethod
    def tearDownClass(cls):
        super(TestUUID, cls).tearDownClass()
        drop_table(cls.UUIDTest)

    def test_uuid_str_with_dashes(self):
        a_uuid = uuid4()
        t0 = self.UUIDTest.create(test_id=0, a_uuid=str(a_uuid))
        t1 = self.UUIDTest.get(test_id=0)
        assert a_uuid == t1.a_uuid

    def test_uuid_str_no_dashes(self):
        a_uuid = uuid4()
        t0 = self.UUIDTest.create(test_id=1, a_uuid=a_uuid.hex)
        t1 = self.UUIDTest.get(test_id=1)
        assert a_uuid == t1.a_uuid

class TestTimeUUID(BaseCassEngTestCase):
    class TimeUUIDTest(Model):
        __keyspace__ = 'test'
        test_id = Integer(primary_key=True)
        timeuuid = TimeUUID(default=uuid1())

    @classmethod
    def setUpClass(cls):
        super(TestTimeUUID, cls).setUpClass()
        sync_table(cls.TimeUUIDTest)

    @classmethod
    def tearDownClass(cls):
        super(TestTimeUUID, cls).tearDownClass()
        drop_table(cls.TimeUUIDTest)

    def test_timeuuid_io(self):
        """
        ensures that
        :return:
        """
        t0 = self.TimeUUIDTest.create(test_id=0)
        t1 = self.TimeUUIDTest.get(test_id=0)

        assert t1.timeuuid.time == t1.timeuuid.time

class TestInteger(BaseCassEngTestCase):
    class IntegerTest(Model):
        __keyspace__ = 'test'
        test_id = UUID(primary_key=True, default=lambda:uuid4())
        value   = Integer(default=0, required=True)

    def test_default_zero_fields_validate(self):
        """ Tests that integer columns with a default value of 0 validate """
        it = self.IntegerTest()
        it.validate()

class TestBigInt(BaseCassEngTestCase):
    class BigIntTest(Model):
        __keyspace__ = 'test'
        test_id = UUID(primary_key=True, default=lambda:uuid4())
        value   = BigInt(default=0, required=True)

    def test_default_zero_fields_validate(self):
        """ Tests that bigint columns with a default value of 0 validate """
        it = self.BigIntTest()
        it.validate()

class TestText(BaseCassEngTestCase):

    def test_min_length(self):
        #min len defaults to 1
        col = Text()
        col.validate('')

        col.validate('b')

        #test not required defaults to 0
        Text(required=False).validate('')

        #test arbitrary lengths
        Text(min_length=0).validate('')
        Text(min_length=5).validate('blake')
        Text(min_length=5).validate('blaketastic')
        with self.assertRaises(ValidationError):
            Text(min_length=6).validate('blake')

    def test_max_length(self):

        Text(max_length=5).validate('blake')
        with self.assertRaises(ValidationError):
            Text(max_length=5).validate('blaketastic')

    def test_type_checking(self):
        Text().validate('string')
        Text().validate(u'unicode')
        Text().validate(bytearray('bytearray', encoding='ascii'))

        with self.assertRaises(ValidationError):
            Text(required=True).validate(None)

        with self.assertRaises(ValidationError):
            Text().validate(5)

        with self.assertRaises(ValidationError):
            Text().validate(True)

    def test_non_required_validation(self):
        """ Tests that validation is ok on none and blank values if required is False """
        Text().validate('')
        Text().validate(None)




class TestExtraFieldsRaiseException(BaseCassEngTestCase):
    class TestModel(Model):
        __keyspace__ = 'test'
        id = UUID(primary_key=True, default=uuid4)

    def test_extra_field(self):
        with self.assertRaises(ValidationError):
            self.TestModel.create(bacon=5000)

class TestPythonDoesntDieWhenExtraFieldIsInCassandra(BaseCassEngTestCase):
    class TestModel(Model):
        __keyspace__ = 'test'
        __table_name__ = 'alter_doesnt_break_running_app'
        id = UUID(primary_key=True, default=uuid4)

    def test_extra_field(self):
        drop_table(self.TestModel)
        sync_table(self.TestModel)
        self.TestModel.create()
        execute("ALTER TABLE {} add blah int".format(self.TestModel.column_family_name(include_keyspace=True)))
        self.TestModel.objects().all()

class TestTimeUUIDFromDatetime(TestCase):
    def test_conversion_specific_date(self):
        dt = datetime(1981, 7, 11, microsecond=555000)

        uuid = TimeUUID.from_datetime(dt)

        from uuid import UUID
        assert isinstance(uuid, UUID)

        ts = (uuid.time - 0x01b21dd213814000) / 1e7 # back to a timestamp
        new_dt = datetime.utcfromtimestamp(ts)

        # checks that we created a UUID1 with the proper timestamp
        assert new_dt == dt

class TestInet(BaseCassEngTestCase):

    class InetTestModel(Model):
        id = UUID(primary_key=True, default=uuid4)
        address = Inet()

    def setUp(self):
        drop_table(self.InetTestModel)
        sync_table(self.InetTestModel)

    def test_inet_saves(self):
        tmp = self.InetTestModel.create(address="192.168.1.1")

        m = self.InetTestModel.get(id=tmp.id)

        assert m.address == "192.168.1.1"

    def test_non_address_fails(self):
        with self.assertRaises(InvalidRequest):
            self.InetTestModel.create(address="ham sandwich")

