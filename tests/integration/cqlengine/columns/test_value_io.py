# Copyright 2015 DataStax, Inc.
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

from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid1, uuid4, UUID
import six

from tests.integration.cqlengine.base import BaseCassEngTestCase

from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import ValueQuoter
from cassandra.cqlengine import columns
import unittest


class BaseColumnIOTest(BaseCassEngTestCase):
    """
    Tests that values are come out of cassandra in the format we expect

    To test a column type, subclass this test, define the column, and the primary key
    and data values you want to test
    """

    # The generated test model is assigned here
    _generated_model = None

    # the column we want to test
    column = None

    # the values we want to test against, you can
    # use a single value, or multiple comma separated values
    pkey_val = None
    data_val = None

    @classmethod
    def setUpClass(cls):
        super(BaseColumnIOTest, cls).setUpClass()

        # if the test column hasn't been defined, bail out
        if not cls.column:
            return

        # create a table with the given column
        class IOTestModel(Model):

            table_name = cls.column.db_type + "_io_test_model_{}".format(uuid4().hex[:8])
            pkey = cls.column(primary_key=True)
            data = cls.column()
        cls._generated_model = IOTestModel
        sync_table(cls._generated_model)

        # tupleify the tested values
        if not isinstance(cls.pkey_val, tuple):
            cls.pkey_val = cls.pkey_val,
        if not isinstance(cls.data_val, tuple):
            cls.data_val = cls.data_val,

    @classmethod
    def tearDownClass(cls):
        super(BaseColumnIOTest, cls).tearDownClass()
        if not cls.column:
            return
        drop_table(cls._generated_model)

    def comparator_converter(self, val):
        """ If you want to convert the original value used to compare the model vales """
        return val

    def test_column_io(self):
        """ Tests the given models class creates and retrieves values as expected """
        if not self.column:
            return
        for pkey, data in zip(self.pkey_val, self.data_val):
            # create
            m1 = self._generated_model.create(pkey=pkey, data=data)

            # get
            m2 = self._generated_model.get(pkey=pkey)
            assert m1.pkey == m2.pkey == self.comparator_converter(pkey), self.column
            assert m1.data == m2.data == self.comparator_converter(data), self.column

            # delete
            self._generated_model.filter(pkey=pkey).delete()


class TestBlobIO(BaseColumnIOTest):

    column = columns.Blob
    pkey_val = six.b('blake'), uuid4().bytes
    data_val = six.b('eggleston'), uuid4().bytes


class TestBlobIO2(BaseColumnIOTest):

    column = columns.Blob
    pkey_val = bytearray(six.b('blake')), uuid4().bytes
    data_val = bytearray(six.b('eggleston')), uuid4().bytes


class TestTextIO(BaseColumnIOTest):

    column = columns.Text
    pkey_val = 'bacon'
    data_val = 'monkey'


class TestNonBinaryTextIO(BaseColumnIOTest):

    column = columns.Text
    pkey_val = 'bacon'
    data_val = '0xmonkey'


class TestInteger(BaseColumnIOTest):

    column = columns.Integer
    pkey_val = 5
    data_val = 6


class TestBigInt(BaseColumnIOTest):

    column = columns.BigInt
    pkey_val = 6
    data_val = pow(2, 63) - 1


class TestDateTime(BaseColumnIOTest):

    column = columns.DateTime

    now = datetime(*datetime.now().timetuple()[:6])
    pkey_val = now
    data_val = now + timedelta(days=1)


class TestDate(BaseColumnIOTest):

    column = columns.Date

    now = datetime.now().date()
    pkey_val = now
    data_val = now + timedelta(days=1)


class TestUUID(BaseColumnIOTest):

    column = columns.UUID

    pkey_val = str(uuid4()), uuid4()
    data_val = str(uuid4()), uuid4()

    def comparator_converter(self, val):
        return val if isinstance(val, UUID) else UUID(val)


class TestTimeUUID(BaseColumnIOTest):

    column = columns.TimeUUID

    pkey_val = str(uuid1()), uuid1()
    data_val = str(uuid1()), uuid1()

    def comparator_converter(self, val):
        return val if isinstance(val, UUID) else UUID(val)


# until Floats are implicitly single:
class FloatSingle(columns.Float):
    def __init__(self, **kwargs):
        super(FloatSingle, self).__init__(double_precision=False, **kwargs)


class TestFloatIO(BaseColumnIOTest):

    column = FloatSingle

    pkey_val = 4.75
    data_val = -1.5


class TestDoubleIO(BaseColumnIOTest):

    column = columns.Double

    pkey_val = 3.14
    data_val = -1982.11


class TestDecimalIO(BaseColumnIOTest):

    column = columns.Decimal

    pkey_val = Decimal('1.35'), 5, '2.4'
    data_val = Decimal('0.005'), 3.5, '8'

    def comparator_converter(self, val):
        return Decimal(val)


class TestQuoter(unittest.TestCase):

    def test_equals(self):
        assert ValueQuoter(False) == ValueQuoter(False)
        assert ValueQuoter(1) == ValueQuoter(1)
        assert ValueQuoter("foo") == ValueQuoter("foo")
        assert ValueQuoter(1.55) == ValueQuoter(1.55)
