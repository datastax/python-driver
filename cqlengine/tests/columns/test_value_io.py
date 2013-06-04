from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid1, uuid4, UUID
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.management import create_table
from cqlengine.management import delete_table
from cqlengine.models import Model
from cqlengine import columns

class BaseColumnIOTest(BaseCassEngTestCase):
    """ Tests that values are come out of cassandra in the format we expect """

    # The generated test model is assigned here
    _test_model = None

    # the column we want to test
    TEST_COLUMN = None

    # the values we want to test against, you can
    # use a single value, or multiple comma separated values
    PKEY_VAL = None
    DATA_VAL = None

    @classmethod
    def setUpClass(cls):
        super(BaseColumnIOTest, cls).setUpClass()

        #if the test column hasn't been defined, bail out
        if not cls.TEST_COLUMN: return

        # create a table with the given column
        class IOTestModel(Model):
            table_name = cls.TEST_COLUMN.db_type + "_io_test_model_{}".format(uuid4().hex[:8])
            pkey = cls.TEST_COLUMN(primary_key=True)
            data = cls.TEST_COLUMN()
        cls._test_model = IOTestModel
        create_table(cls._test_model)

        #tupleify the tested values
        if not isinstance(cls.PKEY_VAL, tuple):
            cls.PKEY_VAL = cls.PKEY_VAL,
        if not isinstance(cls.DATA_VAL, tuple):
            cls.DATA_VAL = cls.DATA_VAL,

    @classmethod
    def tearDownClass(cls):
        super(BaseColumnIOTest, cls).tearDownClass()
        if not cls.TEST_COLUMN: return
        delete_table(cls._test_model)

    def comparator_converter(self, val):
        """ If you want to convert the original value used to compare the model vales """
        return val

    def test_column_io(self):
        """ Tests the given models class creates and retrieves values as expected """
        if not self.TEST_COLUMN: return
        for pkey, data in zip(self.PKEY_VAL, self.DATA_VAL):
            #create
            m1 = self._test_model.create(pkey=pkey, data=data)

            #get
            m2 = self._test_model.get(pkey=pkey)
            assert m1.pkey == m2.pkey == self.comparator_converter(pkey), self.TEST_COLUMN
            assert m1.data == m2.data == self.comparator_converter(data), self.TEST_COLUMN

            #delete
            self._test_model.filter(pkey=pkey).delete()

class TestTextIO(BaseColumnIOTest):

    TEST_COLUMN = columns.Text
    PKEY_VAL = 'bacon'
    DATA_VAL = 'monkey'

class TestInteger(BaseColumnIOTest):

    TEST_COLUMN = columns.Integer
    PKEY_VAL = 5
    DATA_VAL = 6

class TestDateTime(BaseColumnIOTest):

    TEST_COLUMN = columns.DateTime
    now = datetime(*datetime.now().timetuple()[:6])
    PKEY_VAL = now
    DATA_VAL = now + timedelta(days=1)

class TestDate(BaseColumnIOTest):

    TEST_COLUMN = columns.Date
    now = datetime.now().date()
    PKEY_VAL = now
    DATA_VAL = now + timedelta(days=1)

class TestUUID(BaseColumnIOTest):

    TEST_COLUMN = columns.UUID

    PKEY_VAL = str(uuid4()), uuid4()
    DATA_VAL = str(uuid4()), uuid4()

    def comparator_converter(self, val):
        return val if isinstance(val, UUID) else UUID(val)

class TestTimeUUID(BaseColumnIOTest):

    TEST_COLUMN = columns.TimeUUID

    PKEY_VAL = str(uuid1()), uuid1()
    DATA_VAL = str(uuid1()), uuid1()

    def comparator_converter(self, val):
        return val if isinstance(val, UUID) else UUID(val)

class TestBooleanIO(BaseColumnIOTest):

    TEST_COLUMN = columns.Boolean

    PKEY_VAL = True
    DATA_VAL = False

class TestFloatIO(BaseColumnIOTest):

    TEST_COLUMN = columns.Float

    PKEY_VAL = 3.14
    DATA_VAL = -1982.11

class TestDecimalIO(BaseColumnIOTest):

    TEST_COLUMN = columns.Decimal

    PKEY_VAL = Decimal('1.35'), 5, '2.4'
    DATA_VAL = Decimal('0.005'), 3.5, '8'

    def comparator_converter(self, val):
        return Decimal(val)
