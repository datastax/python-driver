__author__ = 'Tim Martin'
from unittest import TestCase
from cqlengine.statements import TransactionClause
import six

class TestTransactionClause(TestCase):

    def test_not_exists_clause(self):
        tc = TransactionClause('not_exists', True)

        self.assertEqual('NOT EXISTS', six.text_type(tc))
        self.assertEqual('NOT EXISTS', str(tc))

    def test_normal_transaction(self):
        tc = TransactionClause('some_value', 23)
        tc.set_context_id(3)

        self.assertEqual('"some_value" = %(3)s', six.text_type(tc))
        self.assertEqual('"some_value" = %(3)s', str(tc))

    def test_equality(self):
        tc1 = TransactionClause('some_value', 5)
        tc2 = TransactionClause('some_value', 5)

        assert tc1 == tc2

        tc3 = TransactionClause('not_exists', True)
        tc4 = TransactionClause('not_exists', True)

        assert tc3 == tc4