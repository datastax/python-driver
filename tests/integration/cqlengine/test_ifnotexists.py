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

from uuid import uuid4

from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.query import BatchQuery, LWTException, IfNotExistsWithCounterColumn

from tests.integration.cqlengine import mock_execute_async
from tests.integration.cqlengine.base import BaseCassEngTestCase, UUID_int_text_model, WithCounterModel


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
        sync_table(UUID_int_text_model)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfNotExistsTest, cls).tearDownClass()
        drop_table(UUID_int_text_model)


class BaseIfNotExistsWithCounterTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfNotExistsWithCounterTest, cls).setUpClass()
        sync_table(WithCounterModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfNotExistsWithCounterTest, cls).tearDownClass()
        drop_table(WithCounterModel)


class IfNotExistsInsertTests(BaseIfNotExistsTest):

    def test_insert_if_not_exists(self):
        """ tests that insertion with if_not_exists work as expected """

        id = uuid4()

        UUID_int_text_model.create(id=id, count=8, text='123456789')

        with self.assertRaises(LWTException) as assertion:
            UUID_int_text_model.if_not_exists().create(id=id, count=9, text='111111111111')
        return
        with self.assertRaises(LWTException) as assertion:
            UUID_int_text_model.objects(count=9, text='111111111111').if_not_exists().create(id=id)

        self.assertEqual(assertion.exception.existing, {
            'count': 8,
            'id': id,
            'text': '123456789',
            '[applied]': False,
        })

        q = UUID_int_text_model.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, '123456789')

    def test_batch_insert_if_not_exists(self):
        """ tests that batch insertion with if_not_exists work as expected """

        id = uuid4()

        with BatchQuery() as b:
            UUID_int_text_model.batch(b).if_not_exists().create(id=id, count=8, text='123456789')

        b = BatchQuery()
        UUID_int_text_model.batch(b).if_not_exists().create(id=id, count=9, text='111111111111')
        with self.assertRaises(LWTException) as assertion:
            b.execute()

        self.assertEqual(assertion.exception.existing, {
            'count': 8,
            'id': id,
            'text': '123456789',
            '[applied]': False,
        })

        q = UUID_int_text_model.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, '123456789')


class IfNotExistsModelTest(BaseIfNotExistsTest):

    def test_if_not_exists_included_on_create(self):
        """ tests that if_not_exists on models works as expected """

        with mock_execute_async() as m:
            UUID_int_text_model.if_not_exists().create(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_if_not_exists_included_on_save(self):
        """ tests if we correctly put 'IF NOT EXISTS' for insert statement """

        with mock_execute_async() as m:
            tm = UUID_int_text_model(count=8)
            tm.if_not_exists().save()

        query = m.call_args[0][0].query_string
        self.assertIn("IF NOT EXISTS", query)

    def test_queryset_is_returned_on_class(self):
        """ ensure we get a queryset description back """
        qs = UUID_int_text_model.if_not_exists()
        self.assertTrue(isinstance(qs, UUID_int_text_model.__queryset__), type(qs))

    def test_batch_if_not_exists(self):
        """ ensure 'IF NOT EXISTS' exists in statement when in batch """
        with mock_execute_async() as m:
            with BatchQuery() as b:
                UUID_int_text_model.batch(b).if_not_exists().create(count=8)
            
        self.assertIn("IF NOT EXISTS", m.call_args[0][0])


class IfNotExistsInstanceTest(BaseIfNotExistsTest):

    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.if_not_exists().save()
        scenario
        """
        o = UUID_int_text_model.create(text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists()
        self.assertEqual(True, o._if_not_exists)

    def test_if_not_exists_is_not_include_with_query_on_update(self):
        """
        make sure we don't put 'IF NOT EXIST' in update statements
        """
        o = UUID_int_text_model.create(text="whatever")
        o.text = "new stuff"
        o = o.if_not_exists()

        with mock_execute_async() as m:
            o.save()

        query = m.call_args[0][0].query_string
        self.assertNotIn("IF NOT EXIST", query)


class IfNotExistWithCounterTest(BaseIfNotExistsWithCounterTest):

    def test_instance_raise_exception(self):
        """ make sure exception is raised when calling
        if_not_exists on table with counter column
        """
        id = uuid4()
        with self.assertRaises(IfNotExistsWithCounterColumn):
            WithCounterModel.if_not_exists()

