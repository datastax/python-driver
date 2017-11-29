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

import sys

from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.connection import get_session
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns

from uuid import uuid4


class BaseCassEngTestCase(unittest.TestCase):

    session = None

    def setUp(self):
        self.session = get_session()

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{0} doesn't have attribute: {1}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{0} shouldn't have the attribute: {1}".format(obj, attr))

    if sys.version_info > (3, 0):
        def assertItemsEqual(self, first, second, msg=None):
            return self.assertCountEqual(first, second, msg)


class BaseCassEngTestCaseWithTable(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseCassEngTestCaseWithTable, cls).setUpClass()
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseCassEngTestCaseWithTable, cls).tearDownClass()
        drop_table(TestMultiKeyModel)


class CollectionsModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)
    text_set = columns.Set(columns.Text, required=False)
    text_list = columns.List(columns.Text, required=False)
    text_map = columns.Map(columns.Text, columns.Text, required=False)


class TestSetModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    int_set = columns.Set(columns.Integer, required=False)
    text_set = columns.Set(columns.Text, required=False)


class TestListModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    int_list = columns.List(columns.Integer, required=False)
    text_list = columns.List(columns.Text, required=False)


class TestNestedModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    list_list = columns.List(columns.List(columns.Integer), required=False)
    map_list = columns.Map(columns.Text, columns.List(columns.Text), required=False)
    set_tuple = columns.Set(columns.Tuple(columns.Integer, columns.Integer), required=False)


class TestTupleModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    int_tuple = columns.Tuple(columns.Integer, columns.Integer, columns.Integer, required=False)
    text_tuple = columns.Tuple(columns.Text, columns.Text, required=False)
    mixed_tuple = columns.Tuple(columns.Text, columns.Integer, columns.Text, required=False)


class TestMapModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    int_map = columns.Map(columns.Integer, columns.UUID, required=False)
    text_map = columns.Map(columns.Text, columns.DateTime, required=False)


class TestCamelMapModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    camelMap = columns.Map(columns.Text, columns.Integer, required=False)


class TestMultiKeyModel(Model):
    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)


class IntegerParititionModel(Model):
    __keyspace__ = 'ks1'

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer()
    text = columns.Text()


class IntegerParititionModel_(Model):
    __keyspace__ = 'ks1'

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer()
    text = columns.Text()


class TestIfExistsModel2(Model):
    id = columns.Integer(primary_key=True)
    count = columns.Integer(primary_key=True, required=False)
    text = columns.Text(required=False)


class WithCounterModel(Model):
    id = columns.UUID(primary_key=True, default=lambda: uuid4())
    likes = columns.Counter()


class UUID_int_text_model(Model):
    id = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer()
    text = columns.Text(required=False)


class TestTimestampModel(Model):
    id = columns.UUID(primary_key=True, default=lambda:uuid4())
    count = columns.Integer()


class TestUpdateModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    count = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)
