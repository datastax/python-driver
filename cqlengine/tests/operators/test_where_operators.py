from unittest import TestCase
from cqlengine.operators import *


class TestWhereOperators(TestCase):

    def test_symbol_lookup(self):
        """ tests where symbols are looked up properly """

        def check_lookup(symbol, expected):
            op = BaseWhereOperator.get_operator(symbol)
            self.assertEqual(op, expected)

        check_lookup('EQ', EqualsOperator)
        check_lookup('IN', InOperator)
        check_lookup('GT', GreaterThanOperator)
        check_lookup('GTE', GreaterThanOrEqualOperator)
        check_lookup('LT', LessThanOperator)
        check_lookup('LTE', LessThanOrEqualOperator)

    def test_operator_rendering(self):
        """ tests symbols are rendered properly """
        self.assertEqual("=", unicode(EqualsOperator()))
        self.assertEqual("IN", unicode(InOperator()))
        self.assertEqual(">", unicode(GreaterThanOperator()))
        self.assertEqual(">=", unicode(GreaterThanOrEqualOperator()))
        self.assertEqual("<", unicode(LessThanOperator()))
        self.assertEqual("<=", unicode(LessThanOrEqualOperator()))


