from unittest import TestCase
from cqlengine.statements import InsertStatement, StatementException


class InsertStatementTests(TestCase):

    def test_where_clause_failure(self):
        """ tests that where clauses cannot be added to Insert statements """
        ist = InsertStatement('table')
        with self.assertRaises(StatementException):
            ist.add_where_clause('s')