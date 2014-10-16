__author__ = 'Tim Martin'
from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from cqlengine.exceptions import TransactionException
from uuid import uuid4
from cqlengine import columns
import mock
from cqlengine import ALL, BatchQuery


class TestTransactionModel(Model):
    __keyspace__ = 'test'
    id = columns.UUID(primary_key=True, default=lambda:uuid4())
    count = columns.Integer()
    text = columns.Text(required=False)


class TestTransaction(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestTransaction, cls).setUpClass()
        sync_table(TestTransactionModel)

    @classmethod
    def tearDownClass(cls):
        super(TestTransaction, cls).tearDownClass()
        drop_table(TestTransactionModel)

    def test_create_uses_transaction(self):
        qs = TestTransactionModel.transaction(not_exists=True)
        with mock.patch.object(self.session, 'execute') as m:
            qs.create(text='blah blah', count=2)
        args = m.call_args
        self.assertIn('IF NOT EXISTS', args[0][0].query_string)

    def test_queryset_returned_on_create(self):
        qs = TestTransactionModel.transaction(not_exists=True)
        self.assertTrue(isinstance(qs, TestTransactionModel.__queryset__), type(qs))

    def test_update_using_transaction(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'new blah'
        with mock.patch.object(self.session, 'execute') as m:
            t.transaction(text='blah blah').save()

        args = m.call_args
        self.assertIn('IF "text" = %(0)s', args[0][0].query_string)

    def test_update_failure(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'new blah'
        t = t.transaction(text='something wrong')
        self.assertRaises(TransactionException, t.save)

    def test_creation_failure(self):
        t = TestTransactionModel.create(text='blah blah')
        t_clone = TestTransactionModel.transaction(not_exists=True)
        self.assertRaises(TransactionException, t_clone.create, id=t.id, count=t.count, text=t.text)

    def test_blind_update(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'something else'
        uid = t.id

        with mock.patch.object(self.session, 'execute') as m:
            TestTransactionModel.objects(id=uid).transaction(text='blah blah').update(text='oh hey der')

        args = m.call_args
        self.assertIn('IF "text" = %(1)s', args[0][0].query_string)

    def test_blind_update_fail(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'something else'
        uid = t.id
        qs = TestTransactionModel.objects(id=uid).transaction(text='Not dis!')
        self.assertRaises(TransactionException, qs.update, text='this will never work')