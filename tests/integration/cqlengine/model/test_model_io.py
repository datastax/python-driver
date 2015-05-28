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

from uuid import uuid4, UUID
import random
import unittest
from datetime import datetime, date, time
from decimal import Decimal
from operator import itemgetter

from cassandra.cqlengine import columns
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import drop_table
from cassandra.cqlengine.models import Model
from cassandra.util import Date, Time

from tests.integration import PROTOCOL_VERSION
from tests.integration.cqlengine.base import BaseCassEngTestCase

class TestModel(Model):

    id      = columns.UUID(primary_key=True, default=lambda: uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)
    a_bool  = columns.Boolean(default=False)


class TestModelIO(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestModelIO, cls).setUpClass()
        sync_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestModelIO, cls).tearDownClass()
        drop_table(TestModel)

    def test_model_save_and_load(self):
        """
        Tests that models can be saved and retrieved
        """
        tm = TestModel.create(count=8, text='123456789')
        self.assertIsInstance(tm, TestModel)

        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertIsInstance(tm2, TestModel)

        for cname in tm._columns.keys():
            self.assertEqual(getattr(tm, cname), getattr(tm2, cname))

    def test_model_read_as_dict(self):
        """
        Tests that columns of an instance can be read as a dict.
        """
        tm = TestModel.create(count=8, text='123456789', a_bool=True)
        column_dict = {
            'id': tm.id,
            'count': tm.count,
            'text': tm.text,
            'a_bool': tm.a_bool,
        }
        self.assertEqual(sorted(tm.keys()), sorted(column_dict.keys()))

        self.assertItemsEqual(tm.values(), column_dict.values())
        self.assertEqual(
            sorted(tm.items(), key=itemgetter(0)),
            sorted(column_dict.items(), key=itemgetter(0)))
        self.assertEqual(len(tm), len(column_dict))
        for column_id in column_dict.keys():
            self.assertEqual(tm[column_id], column_dict[column_id])

        tm['count'] = 6
        self.assertEqual(tm.count, 6)

    def test_model_updating_works_properly(self):
        """
        Tests that subsequent saves after initial model creation work
        """
        tm = TestModel.objects.create(count=8, text='123456789')

        tm.count = 100
        tm.a_bool = True
        tm.save()

        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertEqual(tm.count, tm2.count)
        self.assertEqual(tm.a_bool, tm2.a_bool)

    def test_model_deleting_works_properly(self):
        """
        Tests that an instance's delete method deletes the instance
        """
        tm = TestModel.create(count=8, text='123456789')
        tm.delete()
        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertIsNone(tm2)

    def test_column_deleting_works_properly(self):
        """
        """
        tm = TestModel.create(count=8, text='123456789')
        tm.text = None
        tm.save()

        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertIsInstance(tm2, TestModel)

        assert tm2.text is None
        assert tm2._values['text'].previous_value is None

    def test_a_sensical_error_is_raised_if_you_try_to_create_a_table_twice(self):
        """
        """
        sync_table(TestModel)
        sync_table(TestModel)

    def test_can_insert_model_with_all_column_types(self):
        """
        Test for inserting all column types into a Model

        test_can_insert_model_with_all_column_types tests that each cqlengine column type can be inserted into a Model.
        It first creates a Model that has each cqlengine column type. It then creates a Model instance where all the fields
        have corresponding data, which performs the insert into the Cassandra table.
        Finally, it verifies that each column read from the Model from Cassandra is the same as the input parameters.

        @since 2.6.0
        @jira_ticket PYTHON-246
        @expected_result The Model is inserted with each column type, and the resulting read yields proper data for each column.

        @test_category data_types:primitive
        """

        class AllDatatypesModel(Model):
            id = columns.Integer(primary_key=True)
            a = columns.Ascii()
            b = columns.BigInt()
            c = columns.Blob()
            d = columns.Boolean()
            e = columns.DateTime()
            f = columns.Decimal()
            g = columns.Double()
            h = columns.Float(double_precision=False)
            i = columns.Inet()
            j = columns.Integer()
            k = columns.Text()
            l = columns.TimeUUID()
            m = columns.UUID()
            n = columns.VarInt()

        sync_table(AllDatatypesModel)

        input = ['ascii', 2 ** 63 - 1, bytearray(b'hello world'), True, datetime.utcfromtimestamp(872835240),
                 Decimal('12.3E+7'), 2.39, 3.4028234663852886e+38, '123.123.123.123', 2147483647, 'text',
                 UUID('FE2B4360-28C6-11E2-81C1-0800200C9A66'), UUID('067e6162-3b6f-4ae2-a171-2470b63dff00'),
                 int(str(2147483647) + '000')]

        AllDatatypesModel.create(id=0, a='ascii', b=2 ** 63 - 1, c=bytearray(b'hello world'), d=True,
                                 e=datetime.utcfromtimestamp(872835240), f=Decimal('12.3E+7'), g=2.39,
                                 h=3.4028234663852886e+38, i='123.123.123.123', j=2147483647, k='text',
                                 l=UUID('FE2B4360-28C6-11E2-81C1-0800200C9A66'),
                                 m=UUID('067e6162-3b6f-4ae2-a171-2470b63dff00'), n=int(str(2147483647) + '000'))

        self.assertEqual(1, AllDatatypesModel.objects.count())
        output = AllDatatypesModel.objects().first()

        for i, i_char in enumerate(range(ord('a'), ord('a') + 14)):
            self.assertEqual(input[i], output[chr(i_char)])

    def test_can_insert_model_with_all_protocol_v4_column_types(self):
        """
        Test for inserting all protocol v4 column types into a Model

        test_can_insert_model_with_all_protocol_v4_column_types tests that each cqlengine protocol v4 column type can be
        inserted into a Model. It first creates a Model that has each cqlengine protocol v4 column type. It then creates
        a Model instance where all the fields have corresponding data, which performs the insert into the Cassandra table.
        Finally, it verifies that each column read from the Model from Cassandra is the same as the input parameters.

        @since 2.6.0
        @jira_ticket PYTHON-245
        @expected_result The Model is inserted with each protocol v4 column type, and the resulting read yields proper data for each column.

        @test_category data_types:primitive
        """

        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Protocol v4 datatypes require native protocol 4+, currently using: {0}".format(PROTOCOL_VERSION))

        class v4DatatypesModel(Model):
            id = columns.Integer(primary_key=True)
            a = columns.Date()
            b = columns.SmallInt()
            c = columns.Time()
            d = columns.TinyInt()

        sync_table(v4DatatypesModel)

        input = [Date(date(1970, 1, 1)), 32523, Time(time(16, 47, 25, 7)), 123]

        v4DatatypesModel.create(id=0, a=date(1970, 1, 1), b=32523, c=time(16, 47, 25, 7), d=123)

        self.assertEqual(1, v4DatatypesModel.objects.count())
        output = v4DatatypesModel.objects().first()

        for i, i_char in enumerate(range(ord('a'), ord('a') + 3)):
            self.assertEqual(input[i], output[chr(i_char)])

    def test_can_insert_double_and_float(self):
        """
        Test for inserting single-precision and double-precision values into a Float and Double columns

        test_can_insert_double_and_float tests a Float can only hold a single-precision value, unless
        "double_precision" attribute is specified as True or is unspecified. This test first tests that an AttributeError
        is raised when attempting to input a double-precision value into a single-precision Float. It then verifies that
        Double, Float(double_precision=True) and Float() can hold double-precision values by default. It also verifies that
        columns.Float(double_precision=False) can hold a single-precision value, and a Double can hold a single-precision value.

        @since 2.6.0
        @jira_ticket PYTHON-246
        @expected_result Each floating point column type is able to hold their respective precision values.

        @test_category data_types:primitive
        """

        class FloatingPointModel(Model):
            id = columns.Integer(primary_key=True)
            a = columns.Float(double_precision=False)
            b = columns.Float(double_precision=True)
            c = columns.Float()
            d = columns.Double()

        sync_table(FloatingPointModel)

        FloatingPointModel.create(id=0, a=2.39)
        output = FloatingPointModel.objects().first()
        self.assertEqual(2.390000104904175, output.a)

        FloatingPointModel.create(id=0, a=3.4028234663852886e+38, b=2.39, c=2.39, d=2.39)
        output = FloatingPointModel.objects().first()

        self.assertEqual(3.4028234663852886e+38, output.a)
        self.assertEqual(2.39, output.b)
        self.assertEqual(2.39, output.c)
        self.assertEqual(2.39, output.d)

        FloatingPointModel.create(id=0, d=3.4028234663852886e+38)
        output = FloatingPointModel.objects().first()
        self.assertEqual(3.4028234663852886e+38, output.d)


class TestMultiKeyModel(Model):

    partition   = columns.Integer(primary_key=True)
    cluster     = columns.Integer(primary_key=True)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False)


class TestDeleting(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestDeleting, cls).setUpClass()
        drop_table(TestMultiKeyModel)
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(TestDeleting, cls).tearDownClass()
        drop_table(TestMultiKeyModel)

    def test_deleting_only_deletes_one_object(self):
        partition = random.randint(0, 1000)
        for i in range(5):
            TestMultiKeyModel.create(partition=partition, cluster=i, count=i, text=str(i))

        assert TestMultiKeyModel.filter(partition=partition).count() == 5

        TestMultiKeyModel.get(partition=partition, cluster=0).delete()

        assert TestMultiKeyModel.filter(partition=partition).count() == 4

        TestMultiKeyModel.filter(partition=partition).delete()


class TestUpdating(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestUpdating, cls).setUpClass()
        drop_table(TestMultiKeyModel)
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(TestUpdating, cls).tearDownClass()
        drop_table(TestMultiKeyModel)

    def setUp(self):
        super(TestUpdating, self).setUp()
        self.instance = TestMultiKeyModel.create(
            partition=random.randint(0, 1000),
            cluster=random.randint(0, 1000),
            count=0,
            text='happy'
        )

    def test_vanilla_update(self):
        self.instance.count = 5
        self.instance.save()

        check = TestMultiKeyModel.get(partition=self.instance.partition, cluster=self.instance.cluster)
        assert check.count == 5
        assert check.text == 'happy'

    def test_deleting_only(self):
        self.instance.count = None
        self.instance.text = None
        self.instance.save()

        check = TestMultiKeyModel.get(partition=self.instance.partition, cluster=self.instance.cluster)
        assert check.count is None
        assert check.text is None

    def test_get_changed_columns(self):
        assert self.instance.get_changed_columns() == []
        self.instance.count = 1
        changes = self.instance.get_changed_columns()
        assert len(changes) == 1
        assert changes == ['count']
        self.instance.save()
        assert self.instance.get_changed_columns() == []


class TestCanUpdate(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCanUpdate, cls).setUpClass()
        drop_table(TestModel)
        sync_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestCanUpdate, cls).tearDownClass()
        drop_table(TestModel)

    def test_success_case(self):
        tm = TestModel(count=8, text='123456789')

        # object hasn't been saved,
        # shouldn't be able to update
        assert not tm._is_persisted
        assert not tm._can_update()

        tm.save()

        # object has been saved,
        # should be able to update
        assert tm._is_persisted
        assert tm._can_update()

        tm.count = 200

        # primary keys haven't changed,
        # should still be able to update
        assert tm._can_update()
        tm.save()

        tm.id = uuid4()

        # primary keys have changed,
        # should not be able to update
        assert not tm._can_update()


class IndexDefinitionModel(Model):

    key     = columns.UUID(primary_key=True)
    val     = columns.Text(index=True)


class TestIndexedColumnDefinition(BaseCassEngTestCase):

    def test_exception_isnt_raised_if_an_index_is_defined_more_than_once(self):
        sync_table(IndexDefinitionModel)
        sync_table(IndexDefinitionModel)


class ReservedWordModel(Model):

    token   = columns.Text(primary_key=True)
    insert  = columns.Integer(index=True)


class TestQueryQuoting(BaseCassEngTestCase):

    def test_reserved_cql_words_can_be_used_as_column_names(self):
        """
        """
        sync_table(ReservedWordModel)

        model1 = ReservedWordModel.create(token='1', insert=5)

        model2 = ReservedWordModel.filter(token='1')

        assert len(model2) == 1
        assert model1.token == model2[0].token
        assert model1.insert == model2[0].insert


class TestQueryModel(Model):

    test_id = columns.UUID(primary_key=True, default=uuid4)
    date = columns.Date(primary_key=True)
    description = columns.Text()


class TestQuerying(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Date query tests require native protocol 4+, currently using: {0}".format(PROTOCOL_VERSION))

        super(TestQuerying, cls).setUpClass()
        drop_table(TestQueryModel)
        sync_table(TestQueryModel)

    @classmethod
    def tearDownClass(cls):
        super(TestQuerying, cls).tearDownClass()
        drop_table(TestQueryModel)

    def test_query_with_date(self):
        uid = uuid4()
        day = date(2013, 11, 26)
        obj = TestQueryModel.create(test_id=uid, date=day, description=u'foo')

        self.assertEqual(obj.description, u'foo')

        inst = TestQueryModel.filter(
            TestQueryModel.test_id == uid,
            TestQueryModel.date == day).limit(1).first()

        assert inst.test_id == uid
        assert inst.date == day


def test_none_filter_fails():
    class NoneFilterModel(Model):

        pk = columns.Integer(primary_key=True)
        v = columns.Integer()
    sync_table(NoneFilterModel)

    try:
        NoneFilterModel.objects(pk=None)
        raise Exception("fail")
    except CQLEngineException as e:
        pass
