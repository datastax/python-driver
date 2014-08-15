from unittest import TestCase
import six
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
        wc.set_context_id(5)

        self.assertEqual('"a" = %(5)s', six.text_type(wc), six.text_type(wc))
        self.assertEqual('"a" = %(5)s', str(wc), type(wc))

    def test_equality_method(self):
        """ tests that 2 identical where clauses evaluate as == """
        wc1 = WhereClause('a', EqualsOperator(), 'c')
        wc2 = WhereClause('a', EqualsOperator(), 'c')
        assert wc1 == wc2
