from unittest import TestCase
from cqlengine.columns import Set, List
from cqlengine.operators import *
from cqlengine.statements import (UpdateStatement, WhereClause,
                                  AssignmentClause, SetUpdateClause,
                                  ListUpdateClause)
import six


class UpdateStatementTests(TestCase):

    def test_table_rendering(self):
        """ tests that fields are properly added to the select statement """
        us = UpdateStatement('table')
        self.assertTrue(six.text_type(us).startswith('UPDATE table SET'), six.text_type(us))
        self.assertTrue(str(us).startswith('UPDATE table SET'), str(us))

    def test_rendering(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(AssignmentClause('a', 'b'))
        us.add_assignment_clause(AssignmentClause('c', 'd'))
        us.add_where_clause(WhereClause('a', EqualsOperator(), 'x'))
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = %(0)s, "c" = %(1)s WHERE "a" = %(2)s', six.text_type(us))

    def test_context(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(AssignmentClause('a', 'b'))
        us.add_assignment_clause(AssignmentClause('c', 'd'))
        us.add_where_clause(WhereClause('a', EqualsOperator(), 'x'))
        self.assertEqual(us.get_context(), {'0': 'b', '1': 'd', '2': 'x'})

    def test_context_update(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(AssignmentClause('a', 'b'))
        us.add_assignment_clause(AssignmentClause('c', 'd'))
        us.add_where_clause(WhereClause('a', EqualsOperator(), 'x'))
        us.update_context_id(3)
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = %(4)s, "c" = %(5)s WHERE "a" = %(3)s')
        self.assertEqual(us.get_context(), {'4': 'b', '5': 'd', '3': 'x'})

    def test_additional_rendering(self):
        us = UpdateStatement('table', ttl=60)
        us.add_assignment_clause(AssignmentClause('a', 'b'))
        us.add_where_clause(WhereClause('a', EqualsOperator(), 'x'))
        self.assertIn('USING TTL 60', six.text_type(us))

    def test_update_set_add(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(SetUpdateClause('a', Set.Quoter({1}), operation='add'))
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = "a" + %(0)s')

    def test_update_empty_set_add_does_not_assign(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(SetUpdateClause('a', Set.Quoter(set()), operation='add'))
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = "a" + %(0)s')

    def test_update_empty_set_removal_does_not_assign(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(SetUpdateClause('a', Set.Quoter(set()), operation='remove'))
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = "a" - %(0)s')

    def test_update_list_prepend_with_empty_list(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(ListUpdateClause('a', List.Quoter([]), operation='prepend'))
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = %(0)s + "a"')

    def test_update_list_append_with_empty_list(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(ListUpdateClause('a', List.Quoter([]), operation='append'))
        self.assertEqual(six.text_type(us), 'UPDATE table SET "a" = "a" + %(0)s')
