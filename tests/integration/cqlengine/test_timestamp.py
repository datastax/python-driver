# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta, datetime
import sure
from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine import mock_execute_async


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
        with mock_execute_async() as m:
            with BatchQuery(timestamp=timedelta(seconds=30)) as b:
                TestTimestampModel.batch(b).create(count=1)

        "USING TIMESTAMP".should.be.within(m.call_args[0][0])


class CreateWithTimestampTest(BaseTimestampTest):

    def test_batch(self):
        with mock_execute_async() as m:
            with BatchQuery() as b:
                TestTimestampModel.timestamp(timedelta(seconds=10)).batch(b).create(count=1)

        query = m.call_args[0][0]

        query.should.match(r"INSERT.*USING TIMESTAMP")
        query.should_not.match(r"TIMESTAMP.*INSERT")

    def test_timestamp_not_included_on_normal_create(self):
        with mock_execute_async() as m:
            TestTimestampModel.create(count=2)

        "USING TIMESTAMP".shouldnt.be.within(m.call_args[0][0].query_string)

    def test_timestamp_is_set_on_model_queryset(self):
        delta = timedelta(seconds=30)
        tmp = TestTimestampModel.timestamp(delta)
        tmp._timestamp.should.equal(delta)

    def test_non_batch_syntax_integration(self):
        tmp = TestTimestampModel.timestamp(timedelta(seconds=30)).create(count=1)
        tmp.should.be.ok

    def test_non_batch_syntax_unit(self):

        with mock_execute_async() as m:
            TestTimestampModel.timestamp(timedelta(seconds=30)).create(count=1)

        query = m.call_args[0][0].query_string

        "USING TIMESTAMP".should.be.within(query)


class UpdateWithTimestampTest(BaseTimestampTest):
    def setUp(self):
        self.instance = TestTimestampModel.create(count=1)
        super(UpdateWithTimestampTest, self).setUp()

    def test_instance_update_includes_timestamp_in_query(self):
        # not a batch

        with mock_execute_async() as m:
            self.instance.timestamp(timedelta(seconds=30)).update(count=2)

        "USING TIMESTAMP".should.be.within(m.call_args[0][0].query_string)

    def test_instance_update_in_batch(self):
        with mock_execute_async() as m:
            with BatchQuery() as b:
                self.instance.batch(b).timestamp(timedelta(seconds=30)).update(count=2)

        query = m.call_args[0][0]
        "USING TIMESTAMP".should.be.within(query)


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

        # calling .timestamp sets the TS on the model
        tmp.timestamp(timedelta(seconds=5))
        tmp._timestamp.should.be.ok

        # calling save clears the set timestamp
        tmp.save()
        tmp._timestamp.shouldnt.be.ok

        tmp.timestamp(timedelta(seconds=5))
        tmp.update()
        tmp._timestamp.shouldnt.be.ok

    def test_blind_delete(self):
        """
        we don't expect the model to come back at the end because the deletion timestamp should be in the future
        """
        uid = uuid4()
        tmp = TestTimestampModel.create(id=uid, count=1)

        TestTimestampModel.get(id=uid).should.be.ok

        TestTimestampModel.objects(id=uid).timestamp(timedelta(seconds=5)).delete()

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(id=uid)

        tmp = TestTimestampModel.create(id=uid, count=1)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(id=uid)

    def test_blind_delete_with_datetime(self):
        """
        we don't expect the model to come back at the end because the deletion timestamp should be in the future
        """
        uid = uuid4()
        tmp = TestTimestampModel.create(id=uid, count=1)

        TestTimestampModel.get(id=uid).should.be.ok

        plus_five_seconds = datetime.now() + timedelta(seconds=5)

        TestTimestampModel.objects(id=uid).timestamp(plus_five_seconds).delete()

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(id=uid)

        tmp = TestTimestampModel.create(id=uid, count=1)

        with self.assertRaises(TestTimestampModel.DoesNotExist):
            TestTimestampModel.get(id=uid)

    def test_delete_in_the_past(self):
        uid = uuid4()
        tmp = TestTimestampModel.create(id=uid, count=1)

        TestTimestampModel.get(id=uid).should.be.ok

        # delete the in past, should not affect the object created above
        TestTimestampModel.objects(id=uid).timestamp(timedelta(seconds=-60)).delete()

        TestTimestampModel.get(id=uid)


