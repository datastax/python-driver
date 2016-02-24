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

from mock import patch

from cassandra.cqlengine import columns, CQLEngineException
from cassandra.cqlengine.management import sync_table, drop_table, create_keyspace_simple, drop_keyspace
from cassandra.cqlengine import models
from cassandra.cqlengine.models import Model, ModelDefinitionException


class TestModel(unittest.TestCase):
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

    def test_keywords_as_names(self):
        """
        Test for CQL keywords as names

        test_keywords_as_names tests that CQL keywords are properly and automatically quoted in cqlengine. It creates
        a keyspace, keyspace, which should be automatically quoted to "keyspace" in CQL. It then creates a table, table,
        which should also be automatically quoted to "table". It then verfies that operations can be done on the
        "keyspace"."table" which has been created. It also verifies that table alternations work and operations can be
        performed on the altered table.

        @since 2.6.0
        @jira_ticket PYTHON-244
        @expected_result Cqlengine should quote CQL keywords properly when creating keyspaces and tables.

        @test_category schema:generation
        """

        # If the keyspace exists, it will not be re-created
        create_keyspace_simple('keyspace', 1)

        class table(Model):
            __keyspace__ = 'keyspace'
            select = columns.Integer(primary_key=True)
            table = columns.Text()

        # In case the table already exists in keyspace
        drop_table(table)

        # Create should work
        sync_table(table)

        created = table.create(select=0, table='table')
        selected = table.objects(select=0)[0]
        self.assertEqual(created.select, selected.select)
        self.assertEqual(created.table, selected.table)

        # Alter should work
        class table(Model):
            __keyspace__ = 'keyspace'
            select = columns.Integer(primary_key=True)
            table = columns.Text()
            where = columns.Text()

        sync_table(table)

        created = table.create(select=1, table='table')
        selected = table.objects(select=1)[0]
        self.assertEqual(created.select, selected.select)
        self.assertEqual(created.table, selected.table)
        self.assertEqual(created.where, selected.where)

        drop_keyspace('keyspace')

    def test_column_family(self):
        class TestModel(Model):
            k = columns.Integer(primary_key=True)

        # no model keyspace uses default
        self.assertEqual(TestModel.column_family_name(), "%s.test_model" % (models.DEFAULT_KEYSPACE,))

        # model keyspace overrides
        TestModel.__keyspace__ = "my_test_keyspace"
        self.assertEqual(TestModel.column_family_name(), "%s.test_model" % (TestModel.__keyspace__,))

        # neither set should raise CQLEngineException before failing or formatting an invalid name
        del TestModel.__keyspace__
        with patch('cassandra.cqlengine.models.DEFAULT_KEYSPACE', None):
            self.assertRaises(CQLEngineException, TestModel.column_family_name)
            # .. but we can still get the bare CF name
            self.assertEqual(TestModel.column_family_name(include_keyspace=False), "test_model")

    def test_column_family_case_sensitive(self):
        class TestModel(Model):
            __table_name__ = 'TestModel'
            __table_name_case_sensitive__ = True

            k = columns.Integer(primary_key=True)

        self.assertEqual(TestModel.column_family_name(), '%s."TestModel"' % (models.DEFAULT_KEYSPACE,))

        TestModel.__keyspace__ = "my_test_keyspace"
        self.assertEqual(TestModel.column_family_name(), '%s."TestModel"' % (TestModel.__keyspace__,))

        del TestModel.__keyspace__
        with patch('cassandra.cqlengine.models.DEFAULT_KEYSPACE', None):
            self.assertRaises(CQLEngineException, TestModel.column_family_name)
            self.assertEqual(TestModel.column_family_name(include_keyspace=False), '"TestModel"')


class BuiltInAttributeConflictTest(unittest.TestCase):
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


