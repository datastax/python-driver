# Copyright 2013-2016 DataStax, Inc.
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

import mock
from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery, LWTException, IfExistsWithCounterColumn

from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration import PROTOCOL_VERSION


class TestIfExistsModel(Model):

    id = columns.UUID(primary_key=True, default=lambda: uuid4())
    count = columns.Integer()
    text = columns.Text(required=False)


class TestIfExistsWithCounterModel(Model):

    id = columns.UUID(primary_key=True, default=lambda: uuid4())
    likes = columns.Counter()


class BaseIfExistsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfExistsTest, cls).setUpClass()
        sync_table(TestIfExistsModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfExistsTest, cls).tearDownClass()
        drop_table(TestIfExistsModel)


class BaseIfExistsWithCounterTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfExistsWithCounterTest, cls).setUpClass()
        sync_table(TestIfExistsWithCounterModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfExistsWithCounterTest, cls).tearDownClass()
        drop_table(TestIfExistsWithCounterModel)


class IfExistsUpdateTests(BaseIfExistsTest):

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_update_if_exists(self):
        """ tests that update with if_exists work as expected """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')
        m.text = 'changed'
        m.if_exists().update()
        m = TestIfExistsModel.get(id=id)
        self.assertEqual(m.text, 'changed')

        # save()
        m.text = 'changed_again'
        m.if_exists().save()
        m = TestIfExistsModel.get(id=id)
        self.assertEqual(m.text, 'changed_again')

        m = TestIfExistsModel(id=uuid4(), count=44)  # do not exists
        with self.assertRaises(LWTException) as assertion:
            m.if_exists().update()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        # queryset update
        with self.assertRaises(LWTException) as assertion:
            TestIfExistsModel.objects(id=uuid4()).if_exists().update(count=8)

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_update_if_exists_success(self):
        """ tests that batch update with if_exists work as expected """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')

        with BatchQuery() as b:
            m.text = '111111111'
            m.batch(b).if_exists().update()

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m = TestIfExistsModel(id=uuid4(), count=42)  # Doesn't exist
                m.batch(b).if_exists().update()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        q = TestIfExistsModel.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, '111111111')

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_delete_if_exists(self):
        """ tests that delete with if_exists work as expected """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')
        m.if_exists().delete()
        q = TestIfExistsModel.objects(id=id)
        self.assertEqual(len(q), 0)

        m = TestIfExistsModel(id=uuid4(), count=44)  # do not exists
        with self.assertRaises(LWTException) as assertion:
            m.if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        # queryset delete
        with self.assertRaises(LWTException) as assertion:
            TestIfExistsModel.objects(id=uuid4()).if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })


    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_delete_if_exists_success(self):
        """ tests that batch delete with if_exists work as expected """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')

        with BatchQuery() as b:
            m.batch(b).if_exists().delete()

        q = TestIfExistsModel.objects(id=id)
        self.assertEqual(len(q), 0)

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m = TestIfExistsModel(id=uuid4(), count=42)  # Doesn't exist
                m.batch(b).if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })


class IfExistsQueryTest(BaseIfExistsTest):

    def test_if_exists_included_on_queryset_update(self):
        """ tests that if_exists on queryset works as expected """

        with mock.patch.object(self.session, 'execute') as m:
            TestIfExistsModel.objects(id=uuid4()).if_exists().update(count=42)

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)

    def test_if_exists_included_on_update(self):
        """ tests that if_exists on models update works as expected """

        with mock.patch.object(self.session, 'execute') as m:
            TestIfExistsModel(id=uuid4()).if_exists().update(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)

    def test_if_exists_included_on_delete(self):
        """ tests that if_exists on models delete works as expected """

        with mock.patch.object(self.session, 'execute') as m:
            TestIfExistsModel(id=uuid4()).if_exists().delete()

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)


class IfExistWithCounterTest(BaseIfExistsWithCounterTest):

    def test_instance_raise_exception(self):
        """ make sure exception is raised when calling
        if_exists on table with counter column
        """
        id = uuid4()
        with self.assertRaises(IfExistsWithCounterColumn):
            TestIfExistsWithCounterModel.if_exists()

