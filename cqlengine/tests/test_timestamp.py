"""
Tests surrounding the blah.timestamp( timedelta(seconds=30) ) format.
"""
from datetime import timedelta

import unittest
from uuid import uuid4
import mock
import sure
from cqlengine import Model, columns, BatchQuery
from cqlengine.connection import ConnectionPool
from cqlengine.management import sync_table
from cqlengine.tests.base import BaseCassEngTestCase


class TestTimestampModel(Model):
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()


class BaseTimestampTest(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseTimestampTest, cls).setUpClass()
        sync_table(TestTimestampModel)

class BatchTest(BaseTimestampTest):
    def test_batch_is_included(self):
        with mock.patch.object(ConnectionPool, "execute") as m, BatchQuery(timestamp=timedelta(seconds=30)) as b:
            TestTimestampModel.batch(b).create(count=1)

        "USING TIMESTAMP".should.be.within(m.call_args[0][0])



class CreateWithTimestampTest(BaseTimestampTest):

    def test_batch(self):
        with mock.patch.object(ConnectionPool, "execute") as m, BatchQuery() as b:
            TestTimestampModel.timestamp(timedelta(seconds=10)).batch(b).create(count=1)

        query = m.call_args[0][0]
        
        query.should.match(r"INSERT.*USING TIMESTAMP")
        query.should_not.match(r"TIMESTAMP.*INSERT")



    def test_timestamp_not_included_on_normal_create(self):
        with mock.patch.object(ConnectionPool, "execute") as m:
            TestTimestampModel.create(count=2)

        "USING TIMESTAMP".shouldnt.be.within(m.call_args[0][0])

    def test_timestamp_is_set_on_model_queryset(self):
        delta = timedelta(seconds=30)
        tmp = TestTimestampModel.timestamp(delta)
        tmp._timestamp.should.equal(delta)

    def test_non_batch_syntax_integration(self):
        tmp = TestTimestampModel.timestamp(timedelta(seconds=30)).create(count=1)
        tmp.should.be.ok

    def test_non_batch_syntax_unit(self):

        with mock.patch.object(ConnectionPool, "execute") as m:
            TestTimestampModel.timestamp(timedelta(seconds=30)).create(count=1)

        query = m.call_args[0][0]

        "USING TIMESTAMP".should.be.within(query)


class UpdateWithTimestampTest(BaseTimestampTest):
    def setUp(self):
        self.instance = TestTimestampModel.create(count=1)

    def test_instance_update_includes_timestamp_in_query(self):

        with mock.patch.object(ConnectionPool, "execute") as m:
            self.instance.timestamp(timedelta(seconds=30)).update(count=2)

        "USING TIMESTAMP".should.be.within(m.call_args[0][0])

class DeleteWithTimestampTest(BaseTimestampTest):
    def test_non_batch(self):
        """
        we don't expect the model to come back at the end because the deletion timestamp should be in the future
        """
        uid = uuid4()
        tmp = TestTimestampModel.create(id=uid, count=1)

        TestTimestampModel.get(id=uid).should.be.ok

        tmp.timestamp(timedelta(seconds=5)).delete()

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(id=uid)

        tmp = TestTimestampModel.create(id=uid, count=1)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(id=uid)






