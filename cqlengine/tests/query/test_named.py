from cqlengine import query
from cqlengine.named import NamedKeyspace
from cqlengine.tests.query.test_queryset import TestModel
from cqlengine.tests.base import BaseCassEngTestCase


class TestQuerySetOperation(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetOperation, cls).setUpClass()
        cls.keyspace = NamedKeyspace('cqlengine_test')
        cls.table = cls.keyspace.table('test_model')

    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = self.table.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op, query.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op, query.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_query_expression_parsing(self):
        """ Tests that query experessions are evaluated properly """
        query1 = self.table.filter(self.table.column('test_id') == 5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op, query.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(self.table.column('expected_result') >= 1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op, query.GreaterThanOrEqualOperator)
        assert op.value == 1
