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

from unittest import TestCase

from cassandra.cqlengine.models import Model, ModelDefinitionException
from cassandra.cqlengine import columns


class TestModel(TestCase):
    """ Tests the non-io functionality of models """

    def test_instance_equality(self):
        """ tests the model equality functionality """
        class EqualityModel(Model):

            pk = columns.Integer(primary_key=True)

        m0 = EqualityModel(pk=0)
        m1 = EqualityModel(pk=1)

        self.assertEqual(m0, m0)
        self.assertNotEqual(m0, m1)

    def test_model_equality(self):
        """ tests the model equality functionality """
        class EqualityModel0(Model):

            pk = columns.Integer(primary_key=True)

        class EqualityModel1(Model):

            kk = columns.Integer(primary_key=True)

        m0 = EqualityModel0(pk=0)
        m1 = EqualityModel1(kk=1)

        self.assertEqual(m0, m0)
        self.assertNotEqual(m0, m1)


class BuiltInAttributeConflictTest(TestCase):
    """tests Model definitions that conflict with built-in attributes/methods"""

    def test_model_with_attribute_name_conflict(self):
        """should raise exception when model defines column that conflicts with built-in attribute"""
        with self.assertRaises(ModelDefinitionException):
            class IllegalTimestampColumnModel(Model):

                my_primary_key = columns.Integer(primary_key=True)
                timestamp = columns.BigInt()

    def test_model_with_method_name_conflict(self):
        """should raise exception when model defines column that conflicts with built-in method"""
        with self.assertRaises(ModelDefinitionException):
            class IllegalFilterColumnModel(Model):

                my_primary_key = columns.Integer(primary_key=True)
                filter = columns.Text()


