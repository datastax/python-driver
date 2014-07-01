from cqlengine.management import sync_table, drop_table, create_keyspace, delete_keyspace
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from cqlengine import columns
from uuid import uuid4
import mock
from cqlengine.connection import get_session


class TestChechExistModel(Model):

    __keyspace__ = 'cqlengine_test_checkexist'

    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)


class BaseCheckExistTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseCheckExistTest, cls).setUpClass()
        """
        when receiving an insert statement with 'if not exist', cassandra would
        perform a read with QUORUM level. Unittest would be failed if replica_factor
        is 3 and one node only. Therefore I have create a new keyspace with
        replica_factor:1.
        """
        create_keyspace(TestChechExistModel.__keyspace__, replication_factor=1)
        sync_table(TestChechExistModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseCassEngTestCase, cls).tearDownClass()
        drop_table(TestChechExistModel)
        delete_keyspace(TestChechExistModel.__keyspace__)


class CheckExistInsertTests(BaseCheckExistTest):

    def test_insert_check_exist_success(self):
        """ tests that insertion with check_exist work as expected """
        id = uuid4()

        TestChechExistModel.create(id=id, count=8, text='123456789')
        TestChechExistModel.check_exist(True).create(id=id, count=9, text='111111111111')

        q = TestChechExistModel.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 8)
        self.assertEquals(tm.text, '123456789')

    def test_insert_check_exist_failure(self):
        """ tests that insertion with check_exist failure """
        id = uuid4()

        TestChechExistModel.create(id=id, count=8, text='123456789')
        TestChechExistModel.check_exist(False).create(id=id, count=9, text='111111111111')

        q = TestChechExistModel.objects(id=id)
        self.assertEquals(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 9)
        self.assertEquals(tm.text, '111111111111')


class CheckExistModelTest(BaseCheckExistTest):

    def test_check_exist_included_on_create(self):
        """ tests that check_exist on models works as expected """

        session = get_session()

        with mock.patch.object(session, 'execute') as m:
            TestChechExistModel.check_exist(True).create(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_check_exist_included_on_save(self):

        session = get_session()

        with mock.patch.object(session, 'execute') as m:
            tm = TestChechExistModel(count=8)
            tm.check_exist(True).save()

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_queryset_is_returned_on_class(self):
        """ ensure we get a queryset description back """
        qs = TestChechExistModel.check_exist(True)
        self.assertTrue(isinstance(qs, TestChechExistModel.__queryset__), type(qs))


class CheckExistInstanceTest(BaseCheckExistTest):

    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.check_exist(True).save()
        scenario
        """
        o = TestChechExistModel.create(text="whatever")
        o.text = "new stuff"
        o = o.check_exist(True)
        self.assertEqual(True, o._check_exist)

    def test_check_exist_is_not_include_with_query_on_update(self):
        """
        make sure we don't put 'IF NOT EXIST' in update statements
        """
        session = get_session()

        o = TestChechExistModel.create(text="whatever")
        o.text = "new stuff"
        o = o.check_exist(True)

        with mock.patch.object(session, 'execute') as m:
            o.save()

        query = m.call_args[0][0].query_string
        self.assertNotIn("IF NOT EXIST", query)


