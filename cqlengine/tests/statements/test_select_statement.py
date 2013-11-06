from unittest import TestCase
from cqlengine.statements import SelectStatement, WhereClause
from cqlengine.operators import *


class SelectStatementTests(TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ss = SelectStatement('table', 'field')
        self.assertEqual(ss.fields, ['field'])

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ss = SelectStatement('table', ['f1', 'f2'])
        self.assertTrue(unicode(ss).startswith('SELECT "f1", "f2"'), unicode(ss))
        self.assertTrue(str(ss).startswith('SELECT "f1", "f2"'), str(ss))

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ss = SelectStatement('table')
        self.assertTrue(unicode(ss).startswith('SELECT *'), unicode(ss))
        self.assertTrue(str(ss).startswith('SELECT *'), str(ss))

    def test_table_rendering(self):
        ss = SelectStatement('table')
        self.assertTrue(unicode(ss).startswith('SELECT * FROM table'), unicode(ss))
        self.assertTrue(str(ss).startswith('SELECT * FROM table'), str(ss))

    def test_where_clause_rendering(self):
        ss = SelectStatement('table')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(unicode(ss), 'SELECT * FROM table WHERE "a" = :0', unicode(ss))

    def test_count(self):
        ss = SelectStatement('table', count=True, limit=10, order_by='d')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(unicode(ss), 'SELECT COUNT(*) FROM table WHERE "a" = :0', unicode(ss))
        self.assertNotIn('LIMIT', unicode(ss))
        self.assertNotIn('ORDER', unicode(ss))

    def test_context(self):
        ss = SelectStatement('table')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(ss.get_context(), {'0': 'b'})

    def test_context_id_update(self):
        """ tests that the right things happen the the context id """
        ss = SelectStatement('table')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(ss.get_context(), {'0': 'b'})
        self.assertEqual(str(ss), 'SELECT * FROM table WHERE "a" = :0')

        ss.update_context_id(5)
        self.assertEqual(ss.get_context(), {'5': 'b'})
        self.assertEqual(str(ss), 'SELECT * FROM table WHERE "a" = :5')

    def test_additional_rendering(self):
        ss = SelectStatement(
            'table',
            None,
            order_by=['x', 'y'],
            limit=15,
            allow_filtering=True
        )
        qstr = unicode(ss)
        self.assertIn('LIMIT 15', qstr)
        self.assertIn('ORDER BY x, y', qstr)
        self.assertIn('ALLOW FILTERING', qstr)

