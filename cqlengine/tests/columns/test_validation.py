#tests the behavior of the column classes
from datetime import datetime
from decimal import Decimal as D
from cqlengine import ValidationError

from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.columns import Column, TimeUUID
from cqlengine.columns import Bytes
from cqlengine.columns import Ascii
from cqlengine.columns import Text
from cqlengine.columns import Integer
from cqlengine.columns import DateTime
from cqlengine.columns import UUID
from cqlengine.columns import Boolean
from cqlengine.columns import Float
from cqlengine.columns import Decimal

from cqlengine.management import create_table, delete_table
from cqlengine.models import Model

class TestDatetime(BaseCassEngTestCase):
    class DatetimeTest(Model):
        test_id = Integer(primary_key=True)
        created_at = DateTime()

    @classmethod
    def setUpClass(cls):
        super(TestDatetime, cls).setUpClass()
        create_table(cls.DatetimeTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDatetime, cls).tearDownClass()
        delete_table(cls.DatetimeTest)

    def test_datetime_io(self):
        now = datetime.now()
        dt = self.DatetimeTest.objects.create(test_id=0, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=0).first()
        assert dt2.created_at.timetuple()[:6] == now.timetuple()[:6]

class TestDecimal(BaseCassEngTestCase):
    class DecimalTest(Model):
        test_id = Integer(primary_key=True)
        dec_val = Decimal()

    @classmethod
    def setUpClass(cls):
        super(TestDecimal, cls).setUpClass()
        create_table(cls.DecimalTest)

    @classmethod
    def tearDownClass(cls):
        super(TestDecimal, cls).tearDownClass()
        delete_table(cls.DecimalTest)

    def test_datetime_io(self):
        dt = self.DecimalTest.objects.create(test_id=0, dec_val=D('0.00'))
        dt2 = self.DecimalTest.objects(test_id=0).first()
        assert dt2.dec_val == dt.dec_val

        dt = self.DecimalTest.objects.create(test_id=0, dec_val=5)
        dt2 = self.DecimalTest.objects(test_id=0).first()
        assert dt2.dec_val == D('5')
        
class TestTimeUUID(BaseCassEngTestCase):
    class TimeUUIDTest(Model):
        test_id = Integer(primary_key=True)
        timeuuid = TimeUUID()
        
    @classmethod
    def setUpClass(cls):
        super(TestTimeUUID, cls).setUpClass()
        create_table(cls.TimeUUIDTest)
        
    @classmethod
    def tearDownClass(cls):
        super(TestTimeUUID, cls).tearDownClass()
        delete_table(cls.TimeUUIDTest)
        
    def test_timeuuid_io(self):
        t0 = self.TimeUUIDTest.create(test_id=0)
        t1 = self.TimeUUIDTest.get(test_id=0)
        
        assert t1.timeuuid.time == t1.timeuuid.time

class TestInteger(BaseCassEngTestCase):
    class IntegerTest(Model):
        test_id = UUID(primary_key=True)
        value   = Integer(default=0)

    def test_default_zero_fields_validate(self):
        """ Tests that integer columns with a default value of 0 validate """
        it = self.IntegerTest()
        it.validate()

class TestText(BaseCassEngTestCase):

    def test_min_length(self):
        #min len defaults to 1
        col = Text()

        with self.assertRaises(ValidationError):
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
        Text().validate(bytearray('bytearray'))

        with self.assertRaises(ValidationError):
            Text().validate(None)

        with self.assertRaises(ValidationError):
            Text().validate(5)

        with self.assertRaises(ValidationError):
            Text().validate(True)













