from unittest import TestCase
from cqlengine.statements import InsertStatement, StatementException, AssignmentClause


class InsertStatementTests(TestCase):

    def test_where_clause_failure(self):
        """ tests that where clauses cannot be added to Insert statements """
        ist = InsertStatement('table', None)
        with self.assertRaises(StatementException):
            ist.add_where_clause('s')

    def test_statement(self):
        ist = InsertStatement('table', None)
        ist.add_assignment_clause(AssignmentClause('a', 'b'))
        ist.add_assignment_clause(AssignmentClause('c', 'd'))

        self.assertEqual(
            unicode(ist),
            'INSERT INTO table ("a", "c") VALUES (:0, :1)'
        )

    def test_additional_rendering(self):
        self.fail("Implement ttl and consistency")
