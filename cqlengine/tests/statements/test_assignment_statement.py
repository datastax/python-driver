from unittest import TestCase
from cqlengine.statements import AssignmentStatement, StatementException


class AssignmentStatementTest(TestCase):

    def test_add_assignment_type_checking(self):
        """ tests that only assignment clauses can be added to queries """
        stmt = AssignmentStatement('table', [])
        with self.assertRaises(StatementException):
            stmt.add_assignment_clause('x=5')