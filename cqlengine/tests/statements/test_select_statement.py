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
        ss = SelectStatement('table', None)
        self.assertTrue(unicode(ss).startswith('SELECT *'), unicode(ss))
        self.assertTrue(str(ss).startswith('SELECT *'), str(ss))

    def test_table_rendering(self):
        ss = SelectStatement('table', None)
        self.assertTrue(unicode(ss).startswith('SELECT * FROM table'), unicode(ss))
        self.assertTrue(str(ss).startswith('SELECT * FROM table'), str(ss))

    def test_where_clause_rendering(self):
        ss = SelectStatement('table', None)
        ss.add_where_clause(WhereClause('a', EqualsOperator(), 'b'))
        self.assertEqual(unicode(ss), 'SELECT * FROM table WHERE "a" = b', unicode(ss))

    def test_order_by_rendering(self):
        pass

    def test_limit_rendering(self):
        pass

    def test_allow_filtering_rendering(self):
        pass
