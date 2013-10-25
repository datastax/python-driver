from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from uuid import uuid4
from cqlengine import columns
import mock
from cqlengine.connection import ConnectionPool
from cqlengine import ALL

class TestConsistencyModel(Model):
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)

class BaseConsistencyTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseConsistencyTest, cls).setUpClass()
        sync_table(TestConsistencyModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseConsistencyTest, cls).tearDownClass()
        drop_table(TestConsistencyModel)


class TestConsistency(BaseConsistencyTest):
    def test_create_uses_consistency(self):

        with mock.patch.object(ConnectionPool, 'execute') as m:
            TestConsistencyModel.consistency(ALL).create(text="i am not fault tolerant this way")

        args = m.call_args
        self.assertEqual(ALL, args[0][2])

    def test_queryset_is_returned_on_create(self):
        qs = TestConsistencyModel.consistency(ALL)
        self.assertTrue(isinstance(qs, TestConsistencyModel.__queryset__), type(qs))


