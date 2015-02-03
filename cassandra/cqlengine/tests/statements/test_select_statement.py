from unittest import TestCase
from cqlengine.statements import SelectStatement, WhereClause
from cqlengine.operators import *
import six

class SelectStatementTests(TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ss = SelectStatement('table', 'field')
        self.assertEqual(ss.fields, ['field'])

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ss = SelectStatement('table', ['f1', 'f2'])
        self.assertTrue(six.text_type(ss).startswith('SELECT "f1", "f2"'), six.text_type(ss))
        self.assertTrue(str(ss).startswith('SELECT "f1", "f2"'), str(ss))

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ss = SelectStatement('table')
        self.assertTrue(six.text_type(ss).startswith('SELECT *'), six.text_type(ss))
        self.assertTrue(str(ss).startswith('SELECT *'), str(ss))

    def test_table_rendering(self):
        ss = SelectStatement('table')
        self.assertTrue(six.text_type(ss).startswith('SELECT * FROM table'), six.text_type(ss))
        self.assertTrue(str(ss).startswith('SELECT * FROM table'), str(ss))

    def test_where_clause_rendering(self):
        ss = SelectStatement('table')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(six.text_type(ss), 'SELECT * FROM table WHERE "a" = %(0)s', six.text_type(ss))

    def test_count(self):
        ss = SelectStatement('table', count=True, limit=10, order_by='d')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(six.text_type(ss), 'SELECT COUNT(*) FROM table WHERE "a" = %(0)s LIMIT 10', six.text_type(ss))
        self.assertIn('LIMIT', six.text_type(ss))
        self.assertNotIn('ORDER', six.text_type(ss))

    def test_context(self):
        ss = SelectStatement('table')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(ss.get_context(), {'0': 'b'})

    def test_context_id_update(self):
        """ tests that the right things happen the the context id """
        ss = SelectStatement('table')
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(ss.get_context(), {'0': 'b'})
        self.assertEqual(str(ss), 'SELECT * FROM table WHERE "a" = %(0)s')

        ss.update_context_id(5)
        self.assertEqual(ss.get_context(), {'5': 'b'})
        self.assertEqual(str(ss), 'SELECT * FROM table WHERE "a" = %(5)s')

    def test_additional_rendering(self):
        ss = SelectStatement(
            'table',
            None,
            order_by=['x', 'y'],
            limit=15,
            allow_filtering=True
        )
        qstr = six.text_type(ss)
        self.assertIn('LIMIT 15', qstr)
        self.assertIn('ORDER BY x, y', qstr)
        self.assertIn('ALLOW FILTERING', qstr)

