from datetime import datetime, timedelta
import json
from uuid import uuid4
import six

from cqlengine import Model, ValidationError
from cqlengine import columns
from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase


class TestSetModel(Model):
    __keyspace__ = 'test'

    partition = columns.UUID(primary_key=True, default=uuid4)
    int_set = columns.Set(columns.Integer, required=False)
    text_set = columns.Set(columns.Text, required=False)


class JsonTestColumn(columns.Column):

    db_type = 'text'

    def to_python(self, value):
        if value is None: return
        if isinstance(value, six.string_types):
            return json.loads(value)
        else:
            return value

    def to_database(self, value):
        if value is None: return
        return json.dumps(value)


class TestSetColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestSetColumn, cls).setUpClass()
        drop_table(TestSetModel)
        sync_table(TestSetModel)

    @classmethod
    def tearDownClass(cls):
        super(TestSetColumn, cls).tearDownClass()
        drop_table(TestSetModel)

    def test_add_none_fails(self):
        with self.assertRaises(ValidationError):
            m = TestSetModel.create(int_set=set([None]))

    def test_empty_set_initial(self):
        """
        tests that sets are set() by default, should never be none
        :return:
        """
        m = TestSetModel.create()
        m.int_set.add(5)
        m.save()

    def test_deleting_last_item_should_succeed(self):
        m = TestSetModel.create()
        m.int_set.add(5)
        m.save()
        m.int_set.remove(5)
        m.save()

        m = TestSetModel.get(partition=m.partition)
        self.assertNotIn(5, m.int_set)

    def test_blind_deleting_last_item_should_succeed(self):
        m = TestSetModel.create()
        m.int_set.add(5)
        m.save()

        TestSetModel.objects(partition=m.partition).update(int_set=set())

        m = TestSetModel.get(partition=m.partition)
        self.assertNotIn(5, m.int_set)

    def test_empty_set_retrieval(self):
        m = TestSetModel.create()
        m2 = TestSetModel.get(partition=m.partition)
        m2.int_set.add(3)

    def test_io_success(self):
        """ Tests that a basic usage works as expected """
        m1 = TestSetModel.create(int_set={1, 2}, text_set={'kai', 'andreas'})
        m2 = TestSetModel.get(partition=m1.partition)

        assert isinstance(m2.int_set, set)
        assert isinstance(m2.text_set, set)

        assert 1 in m2.int_set
        assert 2 in m2.int_set

        assert 'kai' in m2.text_set
        assert 'andreas' in m2.text_set

    def test_type_validation(self):
        """
        Tests that attempting to use the wrong types will raise an exception
        """
        with self.assertRaises(ValidationError):
            TestSetModel.create(int_set={'string', True}, text_set={1, 3.0})

    def test_element_count_validation(self):
        """
        Tests that big collections are detected and raise an exception.
        """
        TestSetModel.create(text_set={str(uuid4()) for i in range(65535)})
        with self.assertRaises(ValidationError):
            TestSetModel.create(text_set={str(uuid4()) for i in range(65536)})

    def test_partial_updates(self):
        """ Tests that partial udpates work as expected """
        m1 = TestSetModel.create(int_set={1, 2, 3, 4})

        m1.int_set.add(5)
        m1.int_set.remove(1)
        assert m1.int_set == {2, 3, 4, 5}

        m1.save()

        m2 = TestSetModel.get(partition=m1.partition)
        assert m2.int_set == {2, 3, 4, 5}

    def test_instantiation_with_column_class(self):
        """
        Tests that columns instantiated with a column class work properly
        and that the class is instantiated in the constructor
        """
        column = columns.Set(columns.Text)
        assert isinstance(column.value_col, columns.Text)

    def test_instantiation_with_column_instance(self):
        """
        Tests that columns instantiated with a column instance work properly
        """
        column = columns.Set(columns.Text(min_length=100))
        assert isinstance(column.value_col, columns.Text)

    def test_to_python(self):
        """ Tests that to_python of value column is called """
        column = columns.Set(JsonTestColumn)
        val = {1, 2, 3}
        db_val = column.to_database(val)
        assert db_val.value == {json.dumps(v) for v in val}
        py_val = column.to_python(db_val.value)
        assert py_val == val

    def test_default_empty_container_saving(self):
        """ tests that the default empty container is not saved if it hasn't been updated """
        pkey = uuid4()
        # create a row with set data
        TestSetModel.create(partition=pkey, int_set={3, 4})
        # create another with no set data
        TestSetModel.create(partition=pkey)

        m = TestSetModel.get(partition=pkey)
        self.assertEqual(m.int_set, {3, 4})


class TestListModel(Model):
    __keyspace__ = 'test'

    partition = columns.UUID(primary_key=True, default=uuid4)
    int_list = columns.List(columns.Integer, required=False)
    text_list = columns.List(columns.Text, required=False)


class TestListColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestListColumn, cls).setUpClass()
        drop_table(TestListModel)
        sync_table(TestListModel)

    @classmethod
    def tearDownClass(cls):
        super(TestListColumn, cls).tearDownClass()
        drop_table(TestListModel)

    def test_initial(self):
        tmp = TestListModel.create()
        tmp.int_list.append(1)

    def test_initial(self):
        tmp = TestListModel.create()
        tmp2 = TestListModel.get(partition=tmp.partition)
        tmp2.int_list.append(1)

    def test_io_success(self):
        """ Tests that a basic usage works as expected """
        m1 = TestListModel.create(int_list=[1, 2], text_list=['kai', 'andreas'])
        m2 = TestListModel.get(partition=m1.partition)

        assert isinstance(m2.int_list, list)
        assert isinstance(m2.text_list, list)

        assert len(m2.int_list) == 2
        assert len(m2.text_list) == 2

        assert m2.int_list[0] == 1
        assert m2.int_list[1] == 2

        assert m2.text_list[0] == 'kai'
        assert m2.text_list[1] == 'andreas'

    def test_type_validation(self):
        """
        Tests that attempting to use the wrong types will raise an exception
        """
        with self.assertRaises(ValidationError):
            TestListModel.create(int_list=['string', True], text_list=[1, 3.0])

    def test_element_count_validation(self):
        """
        Tests that big collections are detected and raise an exception.
        """
        TestListModel.create(text_list=[str(uuid4()) for i in range(65535)])
        with self.assertRaises(ValidationError):
            TestListModel.create(text_list=[str(uuid4()) for i in range(65536)])

    def test_partial_updates(self):
        """ Tests that partial udpates work as expected """
        final = list(range(10))
        initial = final[3:7]
        m1 = TestListModel.create(int_list=initial)

        m1.int_list = final
        m1.save()

        m2 = TestListModel.get(partition=m1.partition)
        assert list(m2.int_list) == final

    def test_instantiation_with_column_class(self):
        """
        Tests that columns instantiated with a column class work properly
        and that the class is instantiated in the constructor
        """
        column = columns.List(columns.Text)
        assert isinstance(column.value_col, columns.Text)

    def test_instantiation_with_column_instance(self):
        """
        Tests that columns instantiated with a column instance work properly
        """
        column = columns.List(columns.Text(min_length=100))
        assert isinstance(column.value_col, columns.Text)

    def test_to_python(self):
        """ Tests that to_python of value column is called """
        column = columns.List(JsonTestColumn)
        val = [1, 2, 3]
        db_val = column.to_database(val)
        assert db_val.value == [json.dumps(v) for v in val]
        py_val = column.to_python(db_val.value)
        assert py_val == val

    def test_default_empty_container_saving(self):
        """ tests that the default empty container is not saved if it hasn't been updated """
        pkey = uuid4()
        # create a row with list data
        TestListModel.create(partition=pkey, int_list=[1,2,3,4])
        # create another with no list data
        TestListModel.create(partition=pkey)

        m = TestListModel.get(partition=pkey)
        self.assertEqual(m.int_list, [1,2,3,4])

    def test_remove_entry_works(self):
        pkey = uuid4()
        tmp = TestListModel.create(partition=pkey, int_list=[1,2])
        tmp.int_list.pop()
        tmp.update()
        tmp = TestListModel.get(partition=pkey)
        self.assertEqual(tmp.int_list, [1])

    def test_update_from_non_empty_to_empty(self):
        pkey = uuid4()
        tmp = TestListModel.create(partition=pkey, int_list=[1,2])
        tmp.int_list = []
        tmp.update()

        tmp = TestListModel.get(partition=pkey)
        self.assertEqual(tmp.int_list, [])

    def test_insert_none(self):
        pkey = uuid4()
        with self.assertRaises(ValidationError):
            TestListModel.create(partition=pkey, int_list=[None])

    def test_blind_list_updates_from_none(self):
        """ Tests that updates from None work as expected """
        m = TestListModel.create(int_list=None)
        expected = [1, 2]
        m.int_list = expected
        m.save()

        m2 = TestListModel.get(partition=m.partition)
        assert m2.int_list == expected

        TestListModel.objects(partition=m.partition).update(int_list=[])

        m3 = TestListModel.get(partition=m.partition)
        assert m3.int_list == []

class TestMapModel(Model):
    __keyspace__ = 'test'

    partition = columns.UUID(primary_key=True, default=uuid4)
    int_map = columns.Map(columns.Integer, columns.UUID, required=False)
    text_map = columns.Map(columns.Text, columns.DateTime, required=False)


class TestMapColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestMapColumn, cls).setUpClass()
        drop_table(TestMapModel)
        sync_table(TestMapModel)

    @classmethod
    def tearDownClass(cls):
        super(TestMapColumn, cls).tearDownClass()
        drop_table(TestMapModel)

    def test_empty_default(self):
        tmp = TestMapModel.create()
        tmp.int_map['blah'] = 1

    def test_add_none_as_map_key(self):
        with self.assertRaises(ValidationError):
            TestMapModel.create(int_map={None:1})

    def test_add_none_as_map_value(self):
        with self.assertRaises(ValidationError):
            TestMapModel.create(int_map={None:1})

    def test_empty_retrieve(self):
        tmp = TestMapModel.create()
        tmp2 = TestMapModel.get(partition=tmp.partition)
        tmp2.int_map['blah'] = 1

    def test_remove_last_entry_works(self):
        tmp = TestMapModel.create()
        tmp.text_map["blah"] = datetime.now()
        tmp.save()
        del tmp.text_map["blah"]
        tmp.save()

        tmp = TestMapModel.get(partition=tmp.partition)
        self.assertNotIn("blah", tmp.int_map)

    def test_io_success(self):
        """ Tests that a basic usage works as expected """
        k1 = uuid4()
        k2 = uuid4()
        now = datetime.now()
        then = now + timedelta(days=1)
        m1 = TestMapModel.create(int_map={1: k1, 2: k2}, text_map={'now': now, 'then': then})
        m2 = TestMapModel.get(partition=m1.partition)

        assert isinstance(m2.int_map, dict)
        assert isinstance(m2.text_map, dict)

        assert 1 in m2.int_map
        assert 2 in m2.int_map
        assert m2.int_map[1] == k1
        assert m2.int_map[2] == k2

        assert 'now' in m2.text_map
        assert 'then' in m2.text_map
        assert (now - m2.text_map['now']).total_seconds() < 0.001
        assert (then - m2.text_map['then']).total_seconds() < 0.001

    def test_type_validation(self):
        """
        Tests that attempting to use the wrong types will raise an exception
        """
        with self.assertRaises(ValidationError):
            TestMapModel.create(int_map={'key': 2, uuid4(): 'val'}, text_map={2: 5})

    def test_element_count_validation(self):
        """
        Tests that big collections are detected and raise an exception.
        """
        TestMapModel.create(text_map={str(uuid4()): i for i in range(65535)})
        with self.assertRaises(ValidationError):
            TestMapModel.create(text_map={str(uuid4()): i for i in range(65536)})

    def test_partial_updates(self):
        """ Tests that partial udpates work as expected """
        now = datetime.now()
        #derez it a bit
        now = datetime(*now.timetuple()[:-3])
        early = now - timedelta(minutes=30)
        earlier = early - timedelta(minutes=30)
        later = now + timedelta(minutes=30)

        initial = {'now': now, 'early': earlier}
        final = {'later': later, 'early': early}

        m1 = TestMapModel.create(text_map=initial)

        m1.text_map = final
        m1.save()

        m2 = TestMapModel.get(partition=m1.partition)
        assert m2.text_map == final

    def test_updates_from_none(self):
        """ Tests that updates from None work as expected """
        m = TestMapModel.create(int_map=None)
        expected = {1: uuid4()}
        m.int_map = expected
        m.save()

        m2 = TestMapModel.get(partition=m.partition)
        assert m2.int_map == expected

        m2.int_map = None
        m2.save()
        m3 = TestMapModel.get(partition=m.partition)
        assert m3.int_map != expected

    def test_blind_updates_from_none(self):
        """ Tests that updates from None work as expected """
        m = TestMapModel.create(int_map=None)
        expected = {1: uuid4()}
        m.int_map = expected
        m.save()

        m2 = TestMapModel.get(partition=m.partition)
        assert m2.int_map == expected

        TestMapModel.objects(partition=m.partition).update(int_map={})

        m3 = TestMapModel.get(partition=m.partition)
        assert m3.int_map != expected

    def test_updates_to_none(self):
        """ Tests that setting the field to None works as expected """
        m = TestMapModel.create(int_map={1: uuid4()})
        m.int_map = None
        m.save()

        m2 = TestMapModel.get(partition=m.partition)
        assert m2.int_map == {}

    def test_instantiation_with_column_class(self):
        """
        Tests that columns instantiated with a column class work properly
        and that the class is instantiated in the constructor
        """
        column = columns.Map(columns.Text, columns.Integer)
        assert isinstance(column.key_col, columns.Text)
        assert isinstance(column.value_col, columns.Integer)

    def test_instantiation_with_column_instance(self):
        """
        Tests that columns instantiated with a column instance work properly
        """
        column = columns.Map(columns.Text(min_length=100), columns.Integer())
        assert isinstance(column.key_col, columns.Text)
        assert isinstance(column.value_col, columns.Integer)

    def test_to_python(self):
        """ Tests that to_python of value column is called """
        column = columns.Map(JsonTestColumn, JsonTestColumn)
        val = {1: 2, 3: 4, 5: 6}
        db_val = column.to_database(val)
        assert db_val.value == {json.dumps(k):json.dumps(v) for k,v in val.items()}
        py_val = column.to_python(db_val.value)
        assert py_val == val

    def test_default_empty_container_saving(self):
        """ tests that the default empty container is not saved if it hasn't been updated """
        pkey = uuid4()
        tmap = {1: uuid4(), 2: uuid4()}
        # create a row with set data
        TestMapModel.create(partition=pkey, int_map=tmap)
        # create another with no set data
        TestMapModel.create(partition=pkey)

        m = TestMapModel.get(partition=pkey)
        self.assertEqual(m.int_map, tmap)

#    def test_partial_update_creation(self):
#        """
#        Tests that proper update statements are created for a partial list update
#        :return:
#        """
#        final = range(10)
#        initial = final[3:7]
#
#        ctx = {}
#        col = columns.List(columns.Integer, db_field="TEST")
#        statements = col.get_update_statement(final, initial, ctx)
#
#        assert len([v for v in ctx.values() if [0,1,2] == v.value]) == 1
#        assert len([v for v in ctx.values() if [7,8,9] == v.value]) == 1
#        assert len([s for s in statements if '"TEST" = "TEST" +' in s]) == 1
#        assert len([s for s in statements if '+ "TEST"' in s]) == 1


class TestCamelMapModel(Model):
    __keyspace__ = 'test'

    partition = columns.UUID(primary_key=True, default=uuid4)
    camelMap = columns.Map(columns.Text, columns.Integer, required=False)


class TestCamelMapColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCamelMapColumn, cls).setUpClass()
        drop_table(TestCamelMapModel)
        sync_table(TestCamelMapModel)

    @classmethod
    def tearDownClass(cls):
        super(TestCamelMapColumn, cls).tearDownClass()
        drop_table(TestCamelMapModel)

    def test_camelcase_column(self):
        TestCamelMapModel.create(partition=None, camelMap={'blah': 1})
