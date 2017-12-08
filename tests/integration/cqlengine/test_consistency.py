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

from uuid import uuid4

from cassandra import ConsistencyLevel as CL, ConsistencyLevel
from cassandra.cluster import Session, EXEC_PROFILE_DEFAULT
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.query import BatchQuery

from tests.integration.cqlengine.base import BaseCassEngTestCase, UUID_int_text_model
from tests.integration.cqlengine import mock_execute_async


class BaseConsistencyTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseConsistencyTest, cls).setUpClass()
        sync_table(UUID_int_text_model)

    @classmethod
    def tearDownClass(cls):
        super(BaseConsistencyTest, cls).tearDownClass()
        drop_table(UUID_int_text_model)


class TestConsistency(BaseConsistencyTest):
    def test_create_uses_consistency(self):

        qs = UUID_int_text_model.consistency(CL.ALL)
        with mock_execute_async() as m:
            qs.create(text="i am not fault tolerant this way")

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_queryset_is_returned_on_create(self):
        qs = UUID_int_text_model.consistency(CL.ALL)
        self.assertTrue(isinstance(qs, UUID_int_text_model.__queryset__), type(qs))

    def test_update_uses_consistency(self):
        t = UUID_int_text_model.create(text="bacon and eggs")
        t.text = "ham sandwich"

        with mock_execute_async() as m:
            t.consistency(CL.ALL).save()

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_batch_consistency(self):

        with mock_execute_async() as m:
            with BatchQuery(consistency=CL.ALL) as b:
                UUID_int_text_model.batch(b).create(text="monkey")

        args = m.call_args

        self.assertEqual(CL.ALL, args[0][2])

        with mock_execute_async() as m:
            with BatchQuery() as b:
                UUID_int_text_model.batch(b).create(text="monkey")

        args = m.call_args
        self.assertNotEqual(CL.ALL, args[0][2])

    def test_blind_update(self):
        t = UUID_int_text_model.create(text="bacon and eggs")
        t.text = "ham sandwich"
        uid = t.id

        with mock_execute_async() as m:
            UUID_int_text_model.objects(id=uid).consistency(CL.ALL).update(text="grilled cheese")

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_delete(self):
        # ensures we always carry consistency through on delete statements
        t = UUID_int_text_model.create(text="bacon and eggs")
        t.text = "ham and cheese sandwich"
        uid = t.id

        with mock_execute_async() as m:
            t.consistency(CL.ALL).delete()

        with mock_execute_async() as m:
            UUID_int_text_model.objects(id=uid).consistency(CL.ALL).delete()

        args = m.call_args
        self.assertEqual(CL.ALL, args[0][0].consistency_level)

    def test_default_consistency(self):
        # verify that this session default is set according to connection.setup
        # assumes tests/cqlengine/__init__ setup uses CL.ONE
        session = connection.get_session()
        self.assertEqual(session._get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level, ConsistencyLevel.ONE)
