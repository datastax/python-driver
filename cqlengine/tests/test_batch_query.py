from unittest import skip
from uuid import uuid4
import random

import mock
import sure

from cqlengine import Model, columns
from cqlengine.management import drop_table, sync_table
from cqlengine.query import BatchQuery
from cqlengine.tests.base import BaseCassEngTestCase

class TestMultiKeyModel(Model):
    __keyspace__ = 'test'
    partition   = columns.Integer(primary_key=True)
    cluster     = columns.Integer(primary_key=True)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False)


class BatchQueryTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BatchQueryTests, cls).setUpClass()
        drop_table(TestMultiKeyModel)
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(BatchQueryTests, cls).tearDownClass()
        drop_table(TestMultiKeyModel)

    def setUp(self):
        super(BatchQueryTests, self).setUp()
        self.pkey = 1
        for obj in TestMultiKeyModel.filter(partition=self.pkey):
            obj.delete()

    def test_insert_success_case(self):

        b = BatchQuery()
        inst = TestMultiKeyModel.batch(b).create(partition=self.pkey, cluster=2, count=3, text='4')

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(partition=self.pkey, cluster=2)

        b.execute()

        TestMultiKeyModel.get(partition=self.pkey, cluster=2)

    def test_update_success_case(self):

        inst = TestMultiKeyModel.create(partition=self.pkey, cluster=2, count=3, text='4')

        b = BatchQuery()

        inst.count = 4
        inst.batch(b).save()

        inst2 = TestMultiKeyModel.get(partition=self.pkey, cluster=2)
        assert inst2.count == 3

        b.execute()

        inst3 = TestMultiKeyModel.get(partition=self.pkey, cluster=2)
        assert inst3.count == 4

    def test_delete_success_case(self):

        inst = TestMultiKeyModel.create(partition=self.pkey, cluster=2, count=3, text='4')

        b = BatchQuery()

        inst.batch(b).delete()

        TestMultiKeyModel.get(partition=self.pkey, cluster=2)

        b.execute()

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(partition=self.pkey, cluster=2)

    def test_context_manager(self):

        with BatchQuery() as b:
            for i in range(5):
                TestMultiKeyModel.batch(b).create(partition=self.pkey, cluster=i, count=3, text='4')

            for i in range(5):
                with self.assertRaises(TestMultiKeyModel.DoesNotExist):
                    TestMultiKeyModel.get(partition=self.pkey, cluster=i)

        for i in range(5):
            TestMultiKeyModel.get(partition=self.pkey, cluster=i)

    def test_bulk_delete_success_case(self):

        for i in range(1):
            for j in range(5):
                TestMultiKeyModel.create(partition=i, cluster=j, count=i*j, text='{}:{}'.format(i,j))

        with BatchQuery() as b:
            TestMultiKeyModel.objects.batch(b).filter(partition=0).delete()
            assert TestMultiKeyModel.filter(partition=0).count() == 5

        assert TestMultiKeyModel.filter(partition=0).count() == 0
        #cleanup
        for m in TestMultiKeyModel.all():
            m.delete()

    def test_empty_batch(self):
        b = BatchQuery()
        b.execute()

        with BatchQuery() as b:
            pass

class BatchQueryCallbacksTests(BaseCassEngTestCase):

    def test_API_managing_callbacks(self):

        # Callbacks can be added at init and after

        def my_callback(*args, **kwargs):
            pass

        # adding on init:
        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 2, named_arg='value')
        batch.add_callback(my_callback, 1, 3)

        assert batch._callbacks == [
            (my_callback, (), {}),
            (my_callback, (2,), {'named_arg':'value'}),
            (my_callback, (1, 3), {})
        ]

    def test_callbacks_properly_execute_callables_and_tuples(self):

        call_history = []
        def my_callback(*args, **kwargs):
            call_history.append(args)

        # adding on init:
        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 'more', 'args')

        batch.execute()

        assert len(call_history) == 2
        assert [(), ('more', 'args')] == call_history

    def test_callbacks_tied_to_execute(self):
        """Batch callbacks should NOT fire if batch is not executed in context manager mode"""

        call_history = []
        def my_callback(*args, **kwargs):
            call_history.append(args)

        with BatchQuery() as batch:
            batch.add_callback(my_callback)
            pass

        assert len(call_history) == 1

        class SomeError(Exception):
            pass

        with self.assertRaises(SomeError):
            with BatchQuery() as batch:
                batch.add_callback(my_callback)
                # this error bubbling up through context manager
                # should prevent callback runs (along with b.execute())
                raise SomeError

        # still same call history. Nothing added
        assert len(call_history) == 1

        # but if execute ran, even with an error bubbling through
        # the callbacks also would have fired
        with self.assertRaises(SomeError):
            with BatchQuery(execute_on_exception=True) as batch:
                batch.add_callback(my_callback)
                # this error bubbling up through context manager
                # should prevent callback runs (along with b.execute())
                raise SomeError

        # still same call history
        assert len(call_history) == 2
