from unittest import TestCase
from cqlengine.statements import BaseCQLStatement, StatementException


class BaseStatementTest(TestCase):

    def test_where_clause_type_checking(self):
        """ tests that only assignment clauses can be added to queries """
        stmt = BaseCQLStatement('table', [])
        with self.assertRaises(StatementException):
            stmt.add_where_clause('x=5')
