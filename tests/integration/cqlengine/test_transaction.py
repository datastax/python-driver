# Copyright 2015 DataStax, Inc.
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

import mock
import six
from unittest import skipUnless
from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.exceptions import LWTException
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.statements import TransactionClause

from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration import CASSANDRA_VERSION

class TestTransactionModel(Model):
    id = columns.UUID(primary_key=True, default=lambda:uuid4())
    count = columns.Integer()
    text = columns.Text(required=False)


@skipUnless(CASSANDRA_VERSION >= '2.0.0', "transactions only supported on cassandra 2.0 or higher")
class TestTransaction(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestTransaction, cls).setUpClass()
        sync_table(TestTransactionModel)

    @classmethod
    def tearDownClass(cls):
        super(TestTransaction, cls).tearDownClass()
        drop_table(TestTransactionModel)

    def test_update_using_transaction(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'new blah'
        with mock.patch.object(self.session, 'execute') as m:
            t.iff(text='blah blah').save()

        args = m.call_args
        self.assertIn('IF "text" = %(0)s', args[0][0].query_string)

    def test_update_transaction_success(self):
        t = TestTransactionModel.create(text='blah blah', count=5)
        id = t.id
        t.text = 'new blah'
        t.iff(text='blah blah').save()

        updated = TestTransactionModel.objects(id=id).first()
        self.assertEqual(updated.count, 5)
        self.assertEqual(updated.text, 'new blah')

    def test_update_failure(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'new blah'
        t = t.iff(text='something wrong')
        self.assertRaises(LWTException, t.save)

    def test_blind_update(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'something else'
        uid = t.id

        with mock.patch.object(self.session, 'execute') as m:
            TestTransactionModel.objects(id=uid).iff(text='blah blah').update(text='oh hey der')

        args = m.call_args
        self.assertIn('IF "text" = %(1)s', args[0][0].query_string)

    def test_blind_update_fail(self):
        t = TestTransactionModel.create(text='blah blah')
        t.text = 'something else'
        uid = t.id
        qs = TestTransactionModel.objects(id=uid).iff(text='Not dis!')
        self.assertRaises(LWTException, qs.update, text='this will never work')

    def test_transaction_clause(self):
        tc = TransactionClause('some_value', 23)
        tc.set_context_id(3)

        self.assertEqual('"some_value" = %(3)s', six.text_type(tc))
        self.assertEqual('"some_value" = %(3)s', str(tc))

    def test_batch_update_transaction(self):
        t = TestTransactionModel.create(text='something', count=5)
        id = t.id
        with BatchQuery() as b:
            t.batch(b).iff(count=5).update(text='something else')

        updated = TestTransactionModel.objects(id=id).first()
        self.assertEqual(updated.text, 'something else')

        b = BatchQuery()
        updated.batch(b).iff(count=6).update(text='and another thing')
        self.assertRaises(LWTException, b.execute)

        updated = TestTransactionModel.objects(id=id).first()
        self.assertEqual(updated.text, 'something else')
