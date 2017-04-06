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

from uuid import uuid4

from mock import patch
from cassandra.cqlengine import ValidationError

from tests.integration.cqlengine.base import BaseCassEngTestCase
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table


class TestUpdateModel(Model):

    partition   = columns.UUID(primary_key=True, default=uuid4)
    cluster     = columns.UUID(primary_key=True, default=uuid4)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False, index=True)


class ModelUpdateTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(ModelUpdateTests, cls).setUpClass()
        sync_table(TestUpdateModel)

    @classmethod
    def tearDownClass(cls):
        super(ModelUpdateTests, cls).tearDownClass()
        drop_table(TestUpdateModel)

    def test_update_model(self):
        """ tests calling udpate on models with no values passed in """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        # independently save over a new count value, unknown to original instance
        m1 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        m1.count = 6
        m1.save()

        # update the text, and call update
        m0.text = 'monkey land'
        m0.update()

        # database should reflect both updates
        m2 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        self.assertEqual(m2.count, m1.count)
        self.assertEqual(m2.text, m0.text)

    def test_update_values(self):
        """ tests calling update on models with values passed in """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        # independently save over a new count value, unknown to original instance
        m1 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        m1.count = 6
        m1.save()

        # update the text, and call update
        m0.update(text='monkey land')
        self.assertEqual(m0.text, 'monkey land')

        # database should reflect both updates
        m2 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        self.assertEqual(m2.count, m1.count)
        self.assertEqual(m2.text, m0.text)

    def test_noop_model_direct_update(self):
        """ Tests that calling update on a model with no changes will do nothing. """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        with patch.object(self.session, 'execute') as execute:
            m0.update()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m0.update(count=5)
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m0.update(partition=m0.partition)

        with patch.object(self.session, 'execute') as execute:
            m0.update(cluster=m0.cluster)

    def test_noop_model_assignation_update(self):
        """ Tests that assigning the same value on a model will do nothing. """
        # Create object and fetch it back to eliminate any hidden variable
        # cache effect.
        m0 = TestUpdateModel.create(count=5, text='monkey')
        m1 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)

        with patch.object(self.session, 'execute') as execute:
            m1.save()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m1.count = 5
            m1.save()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m1.partition = m0.partition
            m1.save()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m1.cluster = m0.cluster
            m1.save()
        assert execute.call_count == 0

    def test_invalid_update_kwarg(self):
        """ tests that passing in a kwarg to the update method that isn't a column will fail """
        m0 = TestUpdateModel.create(count=5, text='monkey')
        with self.assertRaises(ValidationError):
            m0.update(numbers=20)

    def test_primary_key_update_failure(self):
        """ tests that attempting to update the value of a primary key will fail """
        m0 = TestUpdateModel.create(count=5, text='monkey')
        with self.assertRaises(ValidationError):
            m0.update(partition=uuid4())


class ModelWithDefault(Model):
    id          = columns.Integer(primary_key=True)
    mf          = columns.Map(columns.Integer, columns.Integer)
    dummy       = columns.Integer(default=42)


class ModelWithDefaultCollection(Model):
    id          = columns.Integer(primary_key=True)
    mf          = columns.Map(columns.Integer, columns.Integer, default={2:2})
    dummy       = columns.Integer(default=42)


class ModelWithDefaultTests(BaseCassEngTestCase):
    def setUp(self):
        sync_table(ModelWithDefault)

    def tearDown(self):
        drop_table(ModelWithDefault)

    def test_value_override_with_default(self):
        """
        Updating a row with a new Model instance shouldn't set columns to defaults

        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result column value should not change

        @test_category object_mapper
        """
        initial = ModelWithDefault(id=1, mf={0: 0}, dummy=0)
        initial.save()

        self.assertEqual(ModelWithDefault.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 0, 'mf': {0: 0}})

        second = ModelWithDefault(id=1)
        second.update(mf={0: 1})

        self.assertEqual(ModelWithDefault.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 0, 'mf': {0: 1}})

    def test_value_is_written_if_is_default(self):
        """
        Check if the we try to update with the default value, the update
        happens correctly
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result column value should be updated

        @test_category object_mapper
        :return:
        """
        initial = ModelWithDefault(id=1)
        initial.mf = {0: 0}
        initial.dummy = 42
        initial.update()

        self.assertEqual(ModelWithDefault.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 42, 'mf': {0: 0}})

    def test_null_update_is_respected(self):
        """
        Check if the we try to update with None under particular
        circumstances, it works correctly
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result column value should be updated to None

        @test_category object_mapper
        :return:
        """
        ModelWithDefault.create(id=1, mf={0: 0}).save()

        q = ModelWithDefault.objects.all().allow_filtering()
        obj = q.filter(id=1).get()

        obj.update(dummy=None)

        self.assertEqual(ModelWithDefault.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': None, 'mf': {0: 0}})

    def test_only_set_values_is_updated(self):
        """
        Test the updates work as expected when an object is deleted
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result the non updated column is None and the
        updated column has the set value

        @test_category object_mapper
        """

        ModelWithDefault.create(id=1, mf={1: 1}, dummy=1).save()

        item = ModelWithDefault.filter(id=1).first()
        ModelWithDefault.objects(id=1).delete()
        item.mf = {1: 2}

        item.save()

        self.assertEqual(ModelWithDefault.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': None, 'mf': {1: 2}})

    def test_collections(self):
        """
        Test the updates work as expected when an object is deleted
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result the non updated column is None and the
        updated column has the set value

        @test_category object_mapper
        """
        ModelWithDefault.create(id=1, mf={1: 1, 2: 1}, dummy=1).save()
        item = ModelWithDefault.filter(id=1).first()

        item.update(mf={2:1})
        self.assertEqual(ModelWithDefault.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {2: 1}})

    def test_collection_with_default(self):
        """
        Test the updates work as expected when an object is deleted
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result the non updated column is None and the
        updated column has the set value

        @test_category object_mapper
        """
        sync_table(ModelWithDefaultCollection)
        item = ModelWithDefaultCollection.create(id=1, mf={1: 1}, dummy=1).save()
        self.assertEqual(ModelWithDefaultCollection.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {1: 1}})

        item.update(mf={2: 2})
        self.assertEqual(ModelWithDefaultCollection.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {2: 2}})

        item.update(mf=None)
        self.assertEqual(ModelWithDefaultCollection.objects().all().get()._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {}})

        item = ModelWithDefaultCollection.create(id=2, dummy=2).save()
        self.assertEqual(ModelWithDefaultCollection.objects().all().get(id=2)._as_dict(),
                         {'id': 2, 'dummy': 2, 'mf': {2: 2}})

        item.update(mf={1: 1, 4: 4})
        self.assertEqual(ModelWithDefaultCollection.objects().all().get(id=2)._as_dict(),
                         {'id': 2, 'dummy': 2, 'mf': {1: 1, 4: 4}})

        drop_table(ModelWithDefaultCollection)
