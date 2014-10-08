__author__ = 'Tim Martin'
from uuid import uuid4

from mock import patch
from cqlengine.exceptions import ValidationError

from unittest import TestCase
from cqlengine.models import Model
from cqlengine import columns
from cqlengine.management import sync_table, drop_table
from cqlengine import connection

connection.setup(['192.168.56.103'], 'test')


class TestUpdateModel(Model):
    __keyspace__ = 'test'
    partition   = columns.UUID(primary_key=True, default=uuid4)
    cluster     = columns.UUID(primary_key=True, default=uuid4)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False, index=True)


class ModelUpdateTests(TestCase):

    @classmethod
    def setUpClass(cls):
        super(ModelUpdateTests, cls).setUpClass()
        sync_table(TestUpdateModel)

    @classmethod
    def tearDownClass(cls):
        super(ModelUpdateTests, cls).tearDownClass()
        drop_table(TestUpdateModel)

    def test_transaction_insertion(self):
        m = TestUpdateModel.objects.create(count=5, text='something')
        all_models = TestUpdateModel.objects.all()
        for x in all_models:
            pass
        m = TestUpdateModel.objects
        m = m.transaction(not_exists=True)
        m = m.create(count=5, text='something')
        m.transaction(count=6).update(text='something else')
        x = 10