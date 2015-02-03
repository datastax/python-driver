from unittest import TestCase
from cqlengine.operators import BaseQueryOperator, QueryOperatorException


class BaseOperatorTest(TestCase):

    def test_get_operator_cannot_be_called_from_base_class(self):
        with self.assertRaises(QueryOperatorException):
            BaseQueryOperator.get_operator('*')