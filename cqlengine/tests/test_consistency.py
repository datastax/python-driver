from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from uuid import uuid4
from cqlengine import columns
import mock
from cqlengine import ALL, BatchQuery

class TestConsistencyModel(Model):
    __keyspace__ = 'test'
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

        qs = TestConsistencyModel.consistency(ALL)
        with mock.patch.object(self.session, 'execute') as m:
            qs.create(text="i am not fault tolerant this way")

        args = m.call_args
        self.assertEqual(ALL, args[0][0].consistency_level)

    def test_queryset_is_returned_on_create(self):
        qs = TestConsistencyModel.consistency(ALL)
        self.assertTrue(isinstance(qs, TestConsistencyModel.__queryset__), type(qs))

    def test_update_uses_consistency(self):
        t = TestConsistencyModel.create(text="bacon and eggs")
        t.text = "ham sandwich"

        with mock.patch.object(self.session, 'execute') as m:
            t.consistency(ALL).save()

        args = m.call_args
        self.assertEqual(ALL, args[0][0].consistency_level)


    def test_batch_consistency(self):

        with mock.patch.object(self.session, 'execute') as m:
            with BatchQuery(consistency=ALL) as b:
                TestConsistencyModel.batch(b).create(text="monkey")

        args = m.call_args

        self.assertEqual(ALL, args[0][0].consistency_level)

        with mock.patch.object(self.session, 'execute') as m:
            with BatchQuery() as b:
                TestConsistencyModel.batch(b).create(text="monkey")

        args = m.call_args
        self.assertNotEqual(ALL, args[0][0].consistency_level)

    def test_blind_update(self):
        t = TestConsistencyModel.create(text="bacon and eggs")
        t.text = "ham sandwich"
        uid = t.id

        with mock.patch.object(self.session, 'execute') as m:
            TestConsistencyModel.objects(id=uid).consistency(ALL).update(text="grilled cheese")

        args = m.call_args
        self.assertEqual(ALL, args[0][0].consistency_level)


    def test_delete(self):
        # ensures we always carry consistency through on delete statements
        t = TestConsistencyModel.create(text="bacon and eggs")
        t.text = "ham and cheese sandwich"
        uid = t.id

        with mock.patch.object(self.session, 'execute') as m:
            t.consistency(ALL).delete()

        with mock.patch.object(self.session, 'execute') as m:
            TestConsistencyModel.objects(id=uid).consistency(ALL).delete()

        args = m.call_args
        self.assertEqual(ALL, args[0][0].consistency_level)
