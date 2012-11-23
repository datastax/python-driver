from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.exceptions import ModelException
from cqlengine.management import create_column_family
from cqlengine.management import delete_column_family
from cqlengine.models import Model
from cqlengine import columns
from cqlengine import query

class TestModel(Model):
    test_id = columns.Integer(primary_key=True)
    attempt_id = columns.Integer(primary_key=True)
    description = columns.Text()
    expected_result = columns.Integer()
    test_result = columns.Integer(index=True)

class TestQuerySetOperation(BaseCassEngTestCase):

    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op, query.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op, query.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_using_invalid_column_names_in_filter_kwargs_raises_error(self):
        """
        Tests that using invalid or nonexistant column names for filter args raises an error
        """
        with self.assertRaises(query.QueryException):
            query0 = TestModel.objects(nonsense=5)

    def test_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = TestModel.objects(test_id=5)
        ids = [o.identifier for o in query1._where]
        where = query1._where_clause()
        assert where == 'test_id = :{}'.format(*ids)

        query2 = query1.filter(expected_result__gte=1)
        ids = [o.identifier for o in query2._where]
        where = query2._where_clause()
        assert where == 'test_id = :{} AND expected_result >= :{}'.format(*ids)


    def test_querystring_generation(self):
        """
        Tests the select querystring creation
        """

    def test_queryset_is_immutable(self):
        """
        Tests that calling a queryset function that changes it's state returns a new queryset
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

    def test_queryset_slicing(self):
        """
        Check that the limit and start is implemented as iterator slices
        """

    def test_proper_delete_behavior(self):
        """
        Tests that deleting the contents of a queryset works properly
        """

    def test_the_all_method_clears_where_filter(self):
        """
        Tests that calling all on a queryset with previously defined filters returns a queryset with no filters
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        query3 = query2.all()
        assert len(query3._where) == 0

    def test_defining_only_and_defer_fails(self):
        """
        Tests that trying to add fields to either only or defer, or doing so more than once fails
        """

    def test_defining_only_or_defer_on_nonexistant_fields_fails(self):
        """
        Tests that setting only or defer fields that don't exist raises an exception
        """

class TestQuerySetUsage(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetUsage, cls).setUpClass()
        try: delete_column_family(TestModel)
        except: pass
        create_column_family(TestModel)

        TestModel.objects.create(test_id=0, attempt_id=0, description='try1', expected_result=5, test_result=30)
        TestModel.objects.create(test_id=0, attempt_id=1, description='try2', expected_result=10, test_result=30)
        TestModel.objects.create(test_id=0, attempt_id=2, description='try3', expected_result=15, test_result=30)
        TestModel.objects.create(test_id=0, attempt_id=3, description='try4', expected_result=20, test_result=25)

        TestModel.objects.create(test_id=1, attempt_id=0, description='try5', expected_result=5, test_result=25)
        TestModel.objects.create(test_id=1, attempt_id=1, description='try6', expected_result=10, test_result=25)
        TestModel.objects.create(test_id=1, attempt_id=2, description='try7', expected_result=15, test_result=25)
        TestModel.objects.create(test_id=1, attempt_id=3, description='try8', expected_result=20, test_result=20)

        TestModel.objects.create(test_id=2, attempt_id=0, description='try9', expected_result=50, test_result=40)
        TestModel.objects.create(test_id=2, attempt_id=1, description='try10', expected_result=60, test_result=40)
        TestModel.objects.create(test_id=2, attempt_id=2, description='try11', expected_result=70, test_result=45)
        TestModel.objects.create(test_id=2, attempt_id=3, description='try12', expected_result=75, test_result=45)

    @classmethod
    def tearDownClass(cls):
        super(TestQuerySetUsage, cls).tearDownClass()
        #TestModel.objects._delete_column_family()
        delete_column_family(TestModel)

    def test_count(self):
        q = TestModel.objects(test_id=0)
        assert q.count() == 4

    def test_iteration(self):
        q = TestModel.objects(test_id=0)
        #tuple of expected attempt_id, expected_result values
        compare_set = set([(0,5), (1,10), (2,15), (3,20)])
        for t in q:
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        q = TestModel.objects(attempt_id=3)
        assert len(q) == 3
        #tuple of expected test_id, expected_result values
        compare_set = set([(0,20), (1,20), (2,75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0



