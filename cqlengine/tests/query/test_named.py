from cqlengine import query
from cqlengine.named import NamedKeyspace
from cqlengine.tests.query.test_queryset import TestModel, BaseQuerySetUsage
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

    def test_filter_method_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = self.table.objects(test_id=5)
        ids = [o.query_value.identifier for o in query1._where]
        where = query1._where_clause()
        assert where == '"test_id" = :{}'.format(*ids)

        query2 = query1.filter(expected_result__gte=1)
        ids = [o.query_value.identifier for o in query2._where]
        where = query2._where_clause()
        assert where == '"test_id" = :{} AND "expected_result" >= :{}'.format(*ids)

    def test_query_expression_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = self.table.objects(self.table.column('test_id') == 5)
        ids = [o.query_value.identifier for o in query1._where]
        where = query1._where_clause()
        assert where == '"test_id" = :{}'.format(*ids)

        query2 = query1.filter(self.table.column('expected_result') >= 1)
        ids = [o.query_value.identifier for o in query2._where]
        where = query2._where_clause()
        assert where == '"test_id" = :{} AND "expected_result" >= :{}'.format(*ids)


class TestQuerySetCountSelectionAndIteration(BaseQuerySetUsage):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetCountSelectionAndIteration, cls).setUpClass()
        ks,tn = TestModel.column_family_name().split('.')
        cls.keyspace = NamedKeyspace(ks)
        cls.table = cls.keyspace.table(tn)


    def test_count(self):
        """ Tests that adding filtering statements affects the count query as expected """
        assert self.table.objects.count() == 12

        q = self.table.objects(test_id=0)
        assert q.count() == 4

    def test_query_expression_count(self):
        """ Tests that adding query statements affects the count query as expected """
        assert self.table.objects.count() == 12

        q = self.table.objects(self.table.column('test_id') == 0)
        assert q.count() == 4


