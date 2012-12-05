#tests the behavior of the column classes
from datetime import datetime
from decimal import Decimal as D

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













