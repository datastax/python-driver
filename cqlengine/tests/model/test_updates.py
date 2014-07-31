from uuid import uuid4

from mock import patch
from cqlengine.exceptions import ValidationError

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from cqlengine import columns
from cqlengine.management import sync_table, drop_table


class TestUpdateModel(Model):
    __keyspace__ = 'test'
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

    def test_noop_model_update(self):
        """ tests that calling update on a model with no changes will do nothing. """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        with patch.object(self.session, 'execute') as execute:
            m0.update()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m0.update(count=5)
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

