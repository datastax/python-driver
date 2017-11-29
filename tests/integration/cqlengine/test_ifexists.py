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
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from uuid import uuid4

from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.query import BatchQuery, LWTException, IfExistsWithCounterColumn

from tests.integration.cqlengine import mock_execute_async
from tests.integration.cqlengine.base import BaseCassEngTestCase, UUID_int_text_model, TestIfExistsModel2, \
    WithCounterModel


class BaseIfExistsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfExistsTest, cls).setUpClass()
        sync_table(UUID_int_text_model)
        sync_table(TestIfExistsModel2)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfExistsTest, cls).tearDownClass()
        drop_table(UUID_int_text_model)
        drop_table(TestIfExistsModel2)


class BaseIfExistsWithCounterTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfExistsWithCounterTest, cls).setUpClass()
        sync_table(WithCounterModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfExistsWithCounterTest, cls).tearDownClass()
        drop_table(WithCounterModel)


class IfExistsUpdateTests(BaseIfExistsTest):

    def test_update_if_exists(self):
        """
        Tests that update with if_exists work as expected

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result updates to be applied when primary key exists, otherwise LWT exception to be thrown

        @test_category object_mapper
        """

        id = uuid4()

        m = UUID_int_text_model.create(id=id, count=8, text='123456789')
        m.text = 'changed'
        m.if_exists().update()
        m = UUID_int_text_model.get(id=id)
        self.assertEqual(m.text, 'changed')

        # save()
        m.text = 'changed_again'
        m.if_exists().save()
        m = UUID_int_text_model.get(id=id)
        self.assertEqual(m.text, 'changed_again')

        m = UUID_int_text_model(id=uuid4(), count=44)  # do not exists
        with self.assertRaises(LWTException) as assertion:
            m.if_exists().update()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        # queryset update
        with self.assertRaises(LWTException) as assertion:
            UUID_int_text_model.objects(id=uuid4()).if_exists().update(count=8)

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    def test_batch_update_if_exists_success(self):
        """
        Tests that batch update with if_exists work as expected

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result

        @test_category object_mapper
        """

        id = uuid4()

        m = UUID_int_text_model.create(id=id, count=8, text='123456789')

        with BatchQuery() as b:
            m.text = '111111111'
            m.batch(b).if_exists().update()

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m = UUID_int_text_model(id=uuid4(), count=42)  # Doesn't exist
                m.batch(b).if_exists().update()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        q = UUID_int_text_model.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, '111111111')

    def test_batch_mixed_update_if_exists_success(self):
        """
        Tests that batch update with with one bad query will still fail with LWTException

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result

        @test_category object_mapper
        """

        m = TestIfExistsModel2.create(id=1, count=8, text='123456789')
        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m.text = '111111112'
                m.batch(b).if_exists().update()  # Does exist
                n = TestIfExistsModel2(id=1, count=10, text="Failure")  # Doesn't exist
                n.batch(b).if_exists().update()

        self.assertEqual(assertion.exception.existing.get('[applied]'), False)

    def test_delete_if_exists(self):
        """
        Tests that delete with if_exists work, and throw proper LWT exception when they are are not applied

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result Deletes will be preformed if they exist, otherwise throw LWT exception

        @test_category object_mapper
        """

        id = uuid4()

        m = UUID_int_text_model.create(id=id, count=8, text='123456789')
        m.if_exists().delete()
        q = UUID_int_text_model.objects(id=id)
        self.assertEqual(len(q), 0)

        m = UUID_int_text_model(id=uuid4(), count=44)  # do not exists
        with self.assertRaises(LWTException) as assertion:
            m.if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        # queryset delete
        with self.assertRaises(LWTException) as assertion:
            UUID_int_text_model.objects(id=uuid4()).if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    def test_batch_delete_if_exists_success(self):
        """
        Tests that batch deletes with if_exists work, and throw proper LWTException when they are are not applied

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result Deletes will be preformed if they exist, otherwise throw LWTException

        @test_category object_mapper
        """

        id = uuid4()

        m = UUID_int_text_model.create(id=id, count=8, text='123456789')

        with BatchQuery() as b:
            m.batch(b).if_exists().delete()

        q = UUID_int_text_model.objects(id=id)
        self.assertEqual(len(q), 0)

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m = UUID_int_text_model(id=uuid4(), count=42)  # Doesn't exist
                m.batch(b).if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    def test_batch_delete_mixed(self):
        """
        Tests that batch deletes  with multiple queries and throw proper LWTException when they are are not all applicable

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result If one delete clause doesn't exist all should fail.

        @test_category object_mapper
        """

        m = TestIfExistsModel2.create(id=3, count=8, text='123456789')

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m.batch(b).if_exists().delete()  # Does exist
                n = TestIfExistsModel2(id=3, count=42, text='1111111')  # Doesn't exist
                n.batch(b).if_exists().delete()

        self.assertEqual(assertion.exception.existing.get('[applied]'), False)
        q = TestIfExistsModel2.objects(id=3, count=8)
        self.assertEqual(len(q), 1)


class IfExistsQueryTest(BaseIfExistsTest):

    def test_if_exists_included_on_queryset_update(self):

        with mock_execute_async() as m:
            UUID_int_text_model.objects(id=uuid4()).if_exists().update(count=42)

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)

    def test_if_exists_included_on_update(self):
        """ tests that if_exists on models update works as expected """

        with mock_execute_async() as m:
            UUID_int_text_model(id=uuid4()).if_exists().update(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)

    def test_if_exists_included_on_delete(self):
        """ tests that if_exists on models delete works as expected """

        with mock_execute_async() as m:
            UUID_int_text_model(id=uuid4()).if_exists().delete()

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)


class IfExistWithCounterTest(BaseIfExistsWithCounterTest):

    def test_instance_raise_exception(self):
        """
        Tests if exists is used with a counter column model that exception are thrown

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result Deletes will be preformed if they exist, otherwise throw LWTException

        @test_category object_mapper
        """
        id = uuid4()
        with self.assertRaises(IfExistsWithCounterColumn):
            WithCounterModel.if_exists()

