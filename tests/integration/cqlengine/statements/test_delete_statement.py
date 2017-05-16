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

from unittest import TestCase

from cassandra.cqlengine.columns import Column
from cassandra.cqlengine.statements import DeleteStatement, WhereClause, MapDeleteClause, ConditionalClause
from cassandra.cqlengine.operators import *
import six


class DeleteStatementTests(TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ds = DeleteStatement('table', 'field')
        self.assertEqual(len(ds.fields), 1)
        self.assertEqual(ds.fields[0].field, 'field')

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ds = DeleteStatement('table', ['f1', 'f2'])
        self.assertTrue(six.text_type(ds).startswith('DELETE "f1", "f2"'), six.text_type(ds))
        self.assertTrue(str(ds).startswith('DELETE "f1", "f2"'), str(ds))

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ds = DeleteStatement('table', None)
        self.assertTrue(six.text_type(ds).startswith('DELETE FROM'), six.text_type(ds))
        self.assertTrue(str(ds).startswith('DELETE FROM'), str(ds))

    def test_table_rendering(self):
        ds = DeleteStatement('table', None)
        self.assertTrue(six.text_type(ds).startswith('DELETE FROM table'), six.text_type(ds))
        self.assertTrue(str(ds).startswith('DELETE FROM table'), str(ds))

    def test_where_clause_rendering(self):
        ds = DeleteStatement('table', None)
        ds.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        self.assertEqual(six.text_type(ds), 'DELETE FROM table WHERE "a" = %(0)s', six.text_type(ds))

    def test_context_update(self):
        ds = DeleteStatement('table', None)
        ds.add_field(MapDeleteClause('d', {1: 2}, {1: 2, 3: 4}))
        ds.add_where(Column(db_field='a'), EqualsOperator(), 'b')

        ds.update_context_id(7)
        self.assertEqual(six.text_type(ds), 'DELETE "d"[%(8)s] FROM table WHERE "a" = %(7)s')
        self.assertEqual(ds.get_context(), {'7': 'b', '8': 3})

    def test_context(self):
        ds = DeleteStatement('table', None)
        ds.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        self.assertEqual(ds.get_context(), {'0': 'b'})

    def test_range_deletion_rendering(self):
        ds = DeleteStatement('table', None)
        ds.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        ds.add_where(Column(db_field='created_at'), GreaterThanOrEqualOperator(), '0')
        ds.add_where(Column(db_field='created_at'), LessThanOrEqualOperator(), '10')
        self.assertEqual(six.text_type(ds), 'DELETE FROM table WHERE "a" = %(0)s AND "created_at" >= %(1)s AND "created_at" <= %(2)s', six.text_type(ds))

        ds = DeleteStatement('table', None)
        ds.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        ds.add_where(Column(db_field='created_at'), InOperator(), ['0', '10', '20'])
        self.assertEqual(six.text_type(ds), 'DELETE FROM table WHERE "a" = %(0)s AND "created_at" IN %(1)s', six.text_type(ds))

        ds = DeleteStatement('table', None)
        ds.add_where(Column(db_field='a'), NotEqualsOperator(), 'b')
        self.assertEqual(six.text_type(ds), 'DELETE FROM table WHERE "a" != %(0)s', six.text_type(ds))

    def test_delete_conditional(self):
        where = [WhereClause('id', EqualsOperator(), 1)]
        conditionals = [ConditionalClause('f0', 'value0'), ConditionalClause('f1', 'value1')]
        ds = DeleteStatement('table', where=where, conditionals=conditionals)
        self.assertEqual(len(ds.conditionals), len(conditionals))
        self.assertEqual(six.text_type(ds), 'DELETE FROM table WHERE "id" = %(0)s IF "f0" = %(1)s AND "f1" = %(2)s', six.text_type(ds))
        fields = ['one', 'two']
        ds = DeleteStatement('table', fields=fields, where=where, conditionals=conditionals)
        self.assertEqual(six.text_type(ds), 'DELETE "one", "two" FROM table WHERE "id" = %(0)s IF "f0" = %(1)s AND "f1" = %(2)s', six.text_type(ds))
