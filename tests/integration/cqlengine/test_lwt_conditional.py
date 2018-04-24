# Copyright DataStax, Inc.
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
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import six
from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery, LWTException
from cassandra.cqlengine.statements import ConditionalClause

from tests.integration.cqlengine.base import BaseCassEngTestCase, UUID_int_text_model
from tests.integration.cqlengine import mock_execute_async

from tests.integration.cqlengine.base import BaseCassEngTestCase


class TestConditionalModel(Model):
    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()
    text = columns.Text(required=False)


class TestUpdateModel(Model):
    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    value = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)


class TestConditional(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestConditional, cls).setUpClass()
        sync_table(UUID_int_text_model)

    @classmethod
    def tearDownClass(cls):
        super(TestConditional, cls).tearDownClass()
        drop_table(UUID_int_text_model)

    def test_update_using_conditional(self):
        t = UUID_int_text_model.create(text='blah blah')
        t.text = 'new blah'
        with mock_execute_async() as m:
            t.iff(text='blah blah').save()

        args = m.call_args
        self.assertIn('IF "text" = %(0)s', args[0][0].query_string)

    def test_update_conditional_success(self):
        t = UUID_int_text_model.create(text='blah blah', count=5)
        id = t.id
        t.text = 'new blah'
        t.iff(text='blah blah').save()

        updated = UUID_int_text_model.objects(id=id).first()
        self.assertEqual(updated.count, 5)
        self.assertEqual(updated.text, 'new blah')

    def test_update_failure(self):
        t = UUID_int_text_model.create(text='blah blah')
        t.text = 'new blah'
        t = t.iff(text='something wrong')

        with self.assertRaises(LWTException) as assertion:
            t.save()

        self.assertEqual(assertion.exception.existing, {
            'text': 'blah blah',
            '[applied]': False,
        })

    def test_blind_update(self):
        t = UUID_int_text_model.create(text='blah blah')
        t.text = 'something else'
        uid = t.id

        with mock_execute_async() as m:
            UUID_int_text_model.objects(id=uid).iff(text='blah blah').update(text='oh hey der')

        args = m.call_args
        self.assertIn('IF "text" = %(1)s', args[0][0].query_string)

    def test_blind_update_fail(self):
        t = UUID_int_text_model.create(text='blah blah')
        t.text = 'something else'
        uid = t.id
        qs = UUID_int_text_model.objects(id=uid).iff(text='Not dis!')
        with self.assertRaises(LWTException) as assertion:
            qs.update(text='this will never work')

        self.assertEqual(assertion.exception.existing, {
            'text': 'blah blah',
            '[applied]': False,
        })

    def test_conditional_clause(self):
        tc = ConditionalClause('some_value', 23)
        tc.set_context_id(3)

        self.assertEqual('"some_value" = %(3)s', six.text_type(tc))
        self.assertEqual('"some_value" = %(3)s', str(tc))

    def test_batch_update_conditional(self):
        t = UUID_int_text_model.create(text='something', count=5)
        id = t.id
        with BatchQuery() as b:
            t.batch(b).iff(count=5).update(text='something else')

        updated = UUID_int_text_model.objects(id=id).first()
        self.assertEqual(updated.text, 'something else')

        b = BatchQuery()
        updated.batch(b).iff(count=6).update(text='and another thing')
        with self.assertRaises(LWTException) as assertion:
            b.execute()

        self.assertEqual(assertion.exception.existing, {
            'id': id,
            'count': 5,
            '[applied]': False,
        })

        updated = UUID_int_text_model.objects(id=id).first()
        self.assertEqual(updated.text, 'something else')

    @unittest.skip("Skipping until PYTHON-943 is resolved")
    def test_batch_update_conditional_several_rows(self):
        sync_table(TestUpdateModel)
        self.addCleanup(drop_table, TestUpdateModel)

        first_row = TestUpdateModel.create(partition=1, cluster=1, value=5, text="something")
        second_row = TestUpdateModel.create(partition=1, cluster=2, value=5, text="something")

        b = BatchQuery()
        TestUpdateModel.batch(b).if_not_exists().create(partition=1, cluster=1, value=5, text='something else')
        TestUpdateModel.batch(b).if_not_exists().create(partition=1, cluster=2, value=5, text='something else')
        TestUpdateModel.batch(b).if_not_exists().create(partition=1, cluster=3, value=5, text='something else')

        # The response will be more than two rows because two of the inserts will fail
        with self.assertRaises(LWTException):
            b.execute()

        first_row.delete()
        second_row.delete()
        b.execute()


    def test_delete_conditional(self):
        # DML path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            t.iff(count=9999).delete()
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        t.iff(count=5).delete()
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 0)

        # QuerySet path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            UUID_int_text_model.objects(id=t.id).iff(count=9999).delete()
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        UUID_int_text_model.objects(id=t.id).iff(count=5).delete()
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 0)

    def test_delete_lwt_ne(self):
        """
        Test to ensure that deletes using IF and not equals are honored correctly

        @since 3.2
        @jira_ticket PYTHON-328
        @expected_result Delete conditional with NE should be honored

        @test_category object_mapper
        """

        # DML path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            t.iff(count__ne=5).delete()
        t.iff(count__ne=2).delete()
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 0)

        # QuerySet path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            UUID_int_text_model.objects(id=t.id).iff(count__ne=5).delete()
        UUID_int_text_model.objects(id=t.id).iff(count__ne=2).delete()
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 0)

    def test_update_lwt_ne(self):
        """
        Test to ensure that update using IF and not equals are honored correctly

        @since 3.2
        @jira_ticket PYTHON-328
        @expected_result update conditional with NE should be honored

        @test_category object_mapper
        """

        # DML path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            t.iff(count__ne=5).update(text='nothing')
        t.iff(count__ne=2).update(text='nothing')
        self.assertEqual(UUID_int_text_model.objects(id=t.id).first().text, 'nothing')
        t.delete()

        # QuerySet path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            UUID_int_text_model.objects(id=t.id).iff(count__ne=5).update(text='nothing')
        UUID_int_text_model.objects(id=t.id).iff(count__ne=2).update(text='nothing')
        self.assertEqual(UUID_int_text_model.objects(id=t.id).first().text, 'nothing')
        t.delete()

    def test_update_to_none(self):
        # This test is done because updates to none are split into deletes
        # for old versions of cassandra. Can be removed when we drop that code
        # https://github.com/datastax/python-driver/blob/3.1.1/cassandra/cqlengine/query.py#L1197-L1200

        # DML path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            t.iff(count=9999).update(text=None)
        self.assertIsNotNone(UUID_int_text_model.objects(id=t.id).first().text)
        t.iff(count=5).update(text=None)
        self.assertIsNone(UUID_int_text_model.objects(id=t.id).first().text)

        # QuerySet path
        t = UUID_int_text_model.create(text='something', count=5)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).count(), 1)
        with self.assertRaises(LWTException):
            UUID_int_text_model.objects(id=t.id).iff(count=9999).update(text=None)
        self.assertIsNotNone(UUID_int_text_model.objects(id=t.id).first().text)
        UUID_int_text_model.objects(id=t.id).iff(count=5).update(text=None)
        self.assertIsNone(UUID_int_text_model.objects(id=t.id).first().text)

    def test_column_delete_after_update(self):
        # DML path
        t = UUID_int_text_model.create(text='something', count=5)
        t.iff(count=5).update(text=None, count=6)

        self.assertIsNone(t.text)
        self.assertEqual(t.count, 6)

        # QuerySet path
        t = UUID_int_text_model.create(text='something', count=5)
        UUID_int_text_model.objects(id=t.id).iff(count=5).update(text=None, count=6)

        self.assertIsNone(UUID_int_text_model.objects(id=t.id).first().text)
        self.assertEqual(UUID_int_text_model.objects(id=t.id).first().count, 6)

    def test_conditional_without_instance(self):
        """
        Test to ensure that the iff method is honored if it's called
        directly from the Model class

        @jira_ticket PYTHON-505
        @expected_result the value is updated

        @test_category object_mapper
        """
        uuid = uuid4()
        UUID_int_text_model.create(id=uuid, text='test_for_cassandra', count=5)

        # This uses the iff method directly from the model class without
        # an instance having been created
        UUID_int_text_model.iff(count=5).filter(id=uuid).update(text=None, count=6)

        t = UUID_int_text_model.filter(id=uuid).first()
        self.assertIsNone(t.text)
        self.assertEqual(t.count, 6)
