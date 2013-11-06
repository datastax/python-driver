from unittest import TestCase
from cqlengine.statements import DeleteStatement, WhereClause, MapDeleteClause
from cqlengine.operators import *


class DeleteStatementTests(TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ds = DeleteStatement('table', 'field')
        self.assertEqual(len(ds.fields), 1)
        self.assertEqual(ds.fields[0].field, 'field')

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ds = DeleteStatement('table', ['f1', 'f2'])
        self.assertTrue(unicode(ds).startswith('DELETE "f1", "f2"'), unicode(ds))
        self.assertTrue(str(ds).startswith('DELETE "f1", "f2"'), str(ds))

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ds = DeleteStatement('table', None)
        self.assertTrue(unicode(ds).startswith('DELETE FROM'), unicode(ds))
        self.assertTrue(str(ds).startswith('DELETE FROM'), str(ds))

    def test_table_rendering(self):
        ds = DeleteStatement('table', None)
        self.assertTrue(unicode(ds).startswith('DELETE FROM table'), unicode(ds))
        self.assertTrue(str(ds).startswith('DELETE FROM table'), str(ds))

    def test_where_clause_rendering(self):
        ds = DeleteStatement('table', None)
        ds.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(unicode(ds), 'DELETE FROM table WHERE "a" = :0', unicode(ds))

    def test_context_update(self):
        ds = DeleteStatement('table', None)
        ds.add_field(MapDeleteClause('d', {1: 2}, {1:2, 3: 4}))
        ds.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))

        ds.update_context_id(7)
        self.assertEqual(unicode(ds), 'DELETE "d"[:8] FROM table WHERE "a" = :7')
        self.assertEqual(ds.get_context(), {'7': 'b', '8': 3})

    def test_context(self):
        ds = DeleteStatement('table', None)
        ds.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(ds.get_context(), {'0': 'b'})
