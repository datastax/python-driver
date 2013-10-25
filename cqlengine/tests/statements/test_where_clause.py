from unittest import TestCase
from cqlengine.operators import EqualsOperator
from cqlengine.statements import StatementException, WhereClause


class TestWhereClause(TestCase):

    def test_operator_check(self):
        """ tests that creating a where statement with a non BaseWhereOperator object fails """
        with self.assertRaises(StatementException):
            WhereClause('a', 'b', 'c')

    def test_where_clause_rendering(self):
        """ tests that where clauses are rendered properly """
        wc = WhereClause('a', EqualsOperator(), 'c')
        self.assertEqual("a = c", unicode(wc))
        self.assertEqual("a = c", str(wc))
