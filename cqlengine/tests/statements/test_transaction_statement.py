__author__ = 'Tim Martin'
from unittest import TestCase
from cqlengine.statements import TransactionClause
import six


class TestTransactionClause(TestCase):

    def test_normal_transaction(self):
        tc = TransactionClause('some_value', 23)
        tc.set_context_id(3)

        self.assertEqual('"some_value" = %(3)s', six.text_type(tc))
        self.assertEqual('"some_value" = %(3)s', str(tc))