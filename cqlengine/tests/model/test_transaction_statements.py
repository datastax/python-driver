__author__ = 'Tim Martin'
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

    def test_transaction_insertion(self):
        m = TestUpdateModel(count=5, text='something').transaction(exists=True)
        m.save()
        x = 10