from datetime import datetime, timedelta
from uuid import uuid4

from cqlengine import Model, ValidationError
from cqlengine import columns
from cqlengine.management import create_table, delete_table
from cqlengine.tests.base import BaseCassEngTestCase

class TestSetModel(Model):
    partition   = columns.UUID(primary_key=True, default=uuid4)
    int_set     = columns.Set(columns.Integer, required=False)
    text_set    = columns.Set(columns.Text, required=False)

class TestSetColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestSetColumn, cls).setUpClass()
        delete_table(TestSetModel)
        create_table(TestSetModel)

    @classmethod
    def tearDownClass(cls):
        super(TestSetColumn, cls).tearDownClass()
        delete_table(TestSetModel)

    def test_io_success(self):
        """ Tests that a basic usage works as expected """
        m1 = TestSetModel.create(int_set={1,2}, text_set={'kai', 'andreas'})
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

    def test_partial_updates(self):
        """ Tests that partial udpates work as expected """
        m1 = TestSetModel.create(int_set={1,2,3,4})

        m1.int_set.add(5)
        m1.int_set.remove(1)
        assert m1.int_set == {2,3,4,5}

        m1.save()

        m2 =  TestSetModel.get(partition=m1.partition)
        assert m2.int_set == {2,3,4,5}

    def test_partial_update_creation(self):
        """
        Tests that proper update statements are created for a partial set update
        :return:
        """
        ctx = {}
        col = columns.Set(columns.Integer, db_field="TEST")
        statements = col.get_update_statement({1,2,3,4}, {2,3,4,5}, ctx)

        assert len([v for v in ctx.values() if {1} == v.value]) == 1
        assert len([v for v in ctx.values() if {5} == v.value]) == 1
        assert len([s for s in statements if '"TEST" = "TEST" -' in s]) == 1
        assert len([s for s in statements if '"TEST" = "TEST" +' in s]) == 1

class TestListModel(Model):
    partition   = columns.UUID(primary_key=True, default=uuid4)
    int_list    = columns.List(columns.Integer, required=False)
    text_list   = columns.List(columns.Text, required=False)

class TestListColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestListColumn, cls).setUpClass()
        delete_table(TestListModel)
        create_table(TestListModel)

    @classmethod
    def tearDownClass(cls):
        super(TestListColumn, cls).tearDownClass()
        delete_table(TestListModel)

    def test_io_success(self):
        """ Tests that a basic usage works as expected """
        m1 = TestListModel.create(int_list=[1,2], text_list=['kai', 'andreas'])
        m2 = TestListModel.get(partition=m1.partition)

        assert isinstance(m2.int_list, tuple)
        assert isinstance(m2.text_list, tuple)

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


class TestMapModel(Model):
    partition   = columns.UUID(primary_key=True, default=uuid4)
    int_map     = columns.Map(columns.Integer, columns.UUID, required=False)
    text_map    = columns.Map(columns.Text, columns.DateTime, required=False)

class TestMapColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestMapColumn, cls).setUpClass()
        delete_table(TestMapModel)
        create_table(TestMapModel)

    @classmethod
    def tearDownClass(cls):
        super(TestMapColumn, cls).tearDownClass()
        delete_table(TestMapModel)

    def test_io_success(self):
        """ Tests that a basic usage works as expected """
        k1 = uuid4()
        k2 = uuid4()
        now = datetime.now()
        then = now + timedelta(days=1)
        m1 = TestMapModel.create(int_map={1:k1,2:k2}, text_map={'now':now, 'then':then})
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
            TestMapModel.create(int_map={'key':2,uuid4():'val'}, text_map={2:5})
