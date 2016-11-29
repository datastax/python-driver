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

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType


class UDTTest(unittest.TestCase):

    def test_initialization_without_existing_connection(self):
        """
        Test that users can define models with UDTs without initializing
        connections.

        Written to reproduce PYTHON-649.
        """

        class Value(UserType):
            t = columns.Text()

        class DummyUDT(Model):
            __keyspace__ = 'ks'
            primary_key = columns.Integer(primary_key=True)
            value = columns.UserDefinedType(Value)
