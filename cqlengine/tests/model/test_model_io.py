from uuid import uuid4
import random
from datetime import date
from operator import itemgetter
from cqlengine.exceptions import CQLEngineException
from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.management import sync_table
from cqlengine.management import drop_table
from cqlengine.models import Model
from cqlengine import columns

class TestModel(Model):
    __keyspace__ = 'test'
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)
    a_bool  = columns.Boolean(default=False)

class TestModel(Model):
    __keyspace__ = 'test'
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
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
            self.assertEquals(getattr(tm, cname), getattr(tm2, cname))

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
        self.assertEquals(sorted(tm.keys()), sorted(column_dict.keys()))

        self.assertItemsEqual(tm.values(), column_dict.values())
        self.assertEquals(
            sorted(tm.items(), key=itemgetter(0)),
            sorted(column_dict.items(), key=itemgetter(0)))
        self.assertEquals(len(tm), len(column_dict))
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
        self.assertEquals(tm.count, tm2.count)
        self.assertEquals(tm.a_bool, tm2.a_bool)

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


class TestMultiKeyModel(Model):
    __keyspace__ = 'test'
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
        partition = random.randint(0,1000)
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
    __keyspace__ = 'test'
    key     = columns.UUID(primary_key=True)
    val     = columns.Text(index=True)

class TestIndexedColumnDefinition(BaseCassEngTestCase):

    def test_exception_isnt_raised_if_an_index_is_defined_more_than_once(self):
        sync_table(IndexDefinitionModel)
        sync_table(IndexDefinitionModel)

class ReservedWordModel(Model):
    __keyspace__ = 'test'
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
    __keyspace__ = 'test'
    test_id = columns.UUID(primary_key=True, default=uuid4)
    date = columns.Date(primary_key=True)
    description = columns.Text()


class TestQuerying(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
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
        __keyspace__ = 'test'
        pk = columns.Integer(primary_key=True)
        v = columns.Integer()
    sync_table(NoneFilterModel)

    try:
        NoneFilterModel.objects(pk=None)
        raise Exception("fail")
    except CQLEngineException as e:
        pass



