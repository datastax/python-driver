"""
Tests surrounding the blah.timestamp( timedelta(seconds=30) ) format.
"""
from datetime import timedelta

import unittest
from uuid import uuid4
import sure
from cqlengine import Model, columns
from cqlengine.management import sync_table
from cqlengine.tests.base import BaseCassEngTestCase


class TestTimestampModel(Model):
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()


class CreateWithTimestampTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(CreateWithTimestampTest, cls).setUpClass()
        sync_table(TestTimestampModel)

    def test_batch(self):
        pass

    def test_non_batch(self):
        tmp = TestTimestampModel.timestamp(timedelta(seconds=30)).create(count=1)
        tmp.should.be.ok

