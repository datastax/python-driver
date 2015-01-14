from unittest import skipUnless
from cqlengine.management import sync_table, drop_table, create_keyspace, delete_keyspace
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.tests.base import PROTOCOL_VERSION
from cqlengine.models import Model
from cqlengine.exceptions import LWTException
from cqlengine import columns, BatchQuery
from uuid import uuid4
import mock


class TestIfNotExistsModel(Model):

    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)


class BaseIfNotExistsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfNotExistsTest, cls).setUpClass()
        """
        when receiving an insert statement with 'if not exist', cassandra would
        perform a read with QUORUM level. Unittest would be failed if replica_factor
        is 3 and one node only. Therefore I have create a new keyspace with
        replica_factor:1.
        """
        sync_table(TestIfNotExistsModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseCassEngTestCase, cls).tearDownClass()
        drop_table(TestIfNotExistsModel)


class IfNotExistsInsertTests(BaseIfNotExistsTest):

    @skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_insert_if_not_exists_success(self):
        """ tests that insertion with if_not_exists work as expected """

        id = uuid4()

        TestIfNotExistsModel.create(id=id, count=8, text='123456789')
        self.assertRaises(
            LWTException,
            TestIfNotExistsModel.if_not_exists().create, id=id, count=9, text='111111111111'
            )

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 8)
        self.assertEquals(tm.text, '123456789')

    def test_insert_if_not_exists_failure(self):
        """ tests that insertion with if_not_exists failure """

        id = uuid4()

        TestIfNotExistsModel.create(id=id, count=8, text='123456789')
        TestIfNotExistsModel.create(id=id, count=9, text='111111111111')

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEquals(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 9)
        self.assertEquals(tm.text, '111111111111')

    @skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_insert_if_not_exists_success(self):
        """ tests that batch insertion with if_not_exists work as expected """

        id = uuid4()

        with BatchQuery() as b:
            TestIfNotExistsModel.batch(b).if_not_exists().create(id=id, count=8, text='123456789')

        b = BatchQuery()
        TestIfNotExistsModel.batch(b).if_not_exists().create(id=id, count=9, text='111111111111')
        self.assertRaises(LWTException, b.execute)

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 8)
        self.assertEquals(tm.text, '123456789')

    def test_batch_insert_if_not_exists_failure(self):
        """ tests that batch insertion with if_not_exists failure """
        id = uuid4()

        with BatchQuery() as b:
            TestIfNotExistsModel.batch(b).create(id=id, count=8, text='123456789')
        with BatchQuery() as b:
            TestIfNotExistsModel.batch(b).create(id=id, count=9, text='111111111111')

        q = TestIfNotExistsModel.objects(id=id)
        self.assertEquals(len(q), 1)

        tm = q.first()
        self.assertEquals(tm.count, 9)
        self.assertEquals(tm.text, '111111111111')


class IfNotExistsModelTest(BaseIfNotExistsTest):

    def test_if_not_exists_included_on_create(self):
        """ tests that if_not_exists on models works as expected """

        with mock.patch.object(self.session, 'execute') as m:
            TestIfNotExistsModel.if_not_exists().create(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_if_not_exists_included_on_save(self):
        """ tests if we correctly put 'IF NOT EXISTS' for insert statement """

        with mock.patch.object(self.session, 'execute') as m:
            tm = TestIfNotExistsModel(count=8)
            tm.if_not_exists(True).save()

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_queryset_is_returned_on_class(self):
        """ ensure we get a queryset description back """
        qs = TestIfNotExistsModel.if_not_exists()
        self.assertTrue(isinstance(qs, TestIfNotExistsModel.__queryset__), type(qs))

    def test_batch_if_not_exists(self):
        """ ensure 'IF NOT EXISTS' exists in statement when in batch """
        with mock.patch.object(self.session, 'execute') as m:
            with BatchQuery() as b:
                TestIfNotExistsModel.batch(b).if_not_exists().create(count=8)

        self.assertIn("IF NOT EXISTS", m.call_args[0][0].query_string)


class IfNotExistsInstanceTest(BaseIfNotExistsTest):

    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.if_not_exists(True).save()
        scenario
        """
        o = TestIfNotExistsModel.create(text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists(True)
        self.assertEqual(True, o._if_not_exists)

    def test_if_not_exists_is_not_include_with_query_on_update(self):
        """
        make sure we don't put 'IF NOT EXIST' in update statements
        """
        o = TestIfNotExistsModel.create(text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists(True)

        with mock.patch.object(self.session, 'execute') as m:
            o.save()

        query = m.call_args[0][0].query_string
        self.assertNotIn("IF NOT EXIST", query)


