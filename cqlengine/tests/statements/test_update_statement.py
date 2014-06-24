from unittest import TestCase
from cqlengine.statements import UpdateStatement, WhereClause, AssignmentClause
from cqlengine.operators import *


class UpdateStatementTests(TestCase):

    def test_table_rendering(self):
        """ tests that fields are properly added to the select statement """
        us = UpdateStatement('table')
        self.assertTrue(unicode(us).startswith('UPDATE table SET'), unicode(us))
        self.assertTrue(str(us).startswith('UPDATE table SET'), str(us))

    def test_rendering(self):
        us = UpdateStatement('table')
        us.add_assignment_clause(AssignmentClause('a', 'b'))
        us.add_assignment_clause(AssignmentClause('c', 'd'))
        us.add_where_clause(WhereClause('a', EqualsOperator(), 'x'))
        self.assertEqual(unicode(us), 'UPDATE table SET "a" = %(0)s, "c" = %(1)s WHERE "a" = %(2)s', unicode(us))

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
        self.assertEqual(unicode(us), 'UPDATE table SET "a" = %(4)s, "c" = %(5)s WHERE "a" = %(3)s')
        self.assertEqual(us.get_context(), {'4': 'b', '5': 'd', '3': 'x'})

    def test_additional_rendering(self):
        us = UpdateStatement('table', ttl=60)
        us.add_assignment_clause(AssignmentClause('a', 'b'))
        us.add_where_clause(WhereClause('a', EqualsOperator(), 'x'))
        self.assertIn('USING TTL 60', unicode(us))

