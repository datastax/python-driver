from datetime import datetime
import time
from unittest import TestCase, skipUnless
from uuid import uuid1, uuid4
import uuid

from cqlengine.tests.base import BaseCassEngTestCase
import mock
from cqlengine.exceptions import ModelException
from cqlengine import functions
from cqlengine.management import sync_table, drop_table, sync_table
from cqlengine.management import drop_table
from cqlengine.models import Model
from cqlengine import columns
from cqlengine import query
from datetime import timedelta
from datetime import tzinfo

from cqlengine import statements
from cqlengine import operators


from cqlengine.connection import get_cluster, get_session

cluster = get_cluster()


class TzOffset(tzinfo):
    """Minimal implementation of a timezone offset to help testing with timezone
    aware datetimes.
    """

    def __init__(self, offset):
        self._offset = timedelta(hours=offset)

    def utcoffset(self, dt):
        return self._offset

    def tzname(self, dt):
        return 'TzOffset: {}'.format(self._offset.hours)

    def dst(self, dt):
        return timedelta(0)


class TestModel(Model):
    __keyspace__ = 'test'
    test_id = columns.Integer(primary_key=True)
    attempt_id = columns.Integer(primary_key=True)
    description = columns.Text()
    expected_result = columns.Integer()
    test_result = columns.Integer()


class IndexedTestModel(Model):
    __keyspace__ = 'test'
    test_id = columns.Integer(primary_key=True)
    attempt_id = columns.Integer(index=True)
    description = columns.Text()
    expected_result = columns.Integer()
    test_result = columns.Integer(index=True)


class TestMultiClusteringModel(Model):
    __keyspace__ = 'test'
    one = columns.Integer(primary_key=True)
    two = columns.Integer(primary_key=True)
    three = columns.Integer(primary_key=True)


class TestQuerySetOperation(BaseCassEngTestCase):
    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]

        assert isinstance(op, statements.WhereClause)
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        self.assertIsInstance(op, statements.WhereClause)
        self.assertIsInstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_query_expression_parsing(self):
        """ Tests that query experessions are evaluated properly """
        query1 = TestModel.filter(TestModel.test_id == 5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op, statements.WhereClause)
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(TestModel.expected_result >= 1)
        assert len(query2._where) == 2

        op = query2._where[1]
        self.assertIsInstance(op, statements.WhereClause)
        self.assertIsInstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_using_invalid_column_names_in_filter_kwargs_raises_error(self):
        """
        Tests that using invalid or nonexistant column names for filter args raises an error
        """
        with self.assertRaises(query.QueryException):
            TestModel.objects(nonsense=5)

    def test_using_nonexistant_column_names_in_query_args_raises_error(self):
        """
        Tests that using invalid or nonexistant columns for query args raises an error
        """
        with self.assertRaises(AttributeError):
            TestModel.objects(TestModel.nonsense == 5)

    def test_using_non_query_operators_in_query_args_raises_error(self):
        """
        Tests that providing query args that are not query operator instances raises an error
        """
        with self.assertRaises(query.QueryException):
            TestModel.objects(5)

    def test_queryset_is_immutable(self):
        """
        Tests that calling a queryset function that changes it's state returns a new queryset
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2
        assert len(query1._where) == 1

    def test_queryset_limit_immutability(self):
        """
        Tests that calling a queryset function that changes it's state returns a new queryset with same limit
        """
        query1 = TestModel.objects(test_id=5).limit(1)
        assert query1._limit == 1

        query2 = query1.filter(expected_result__gte=1)
        assert query2._limit == 1

        query3 = query1.filter(expected_result__gte=1).limit(2)
        assert query1._limit == 1
        assert query3._limit == 2

    def test_the_all_method_duplicates_queryset(self):
        """
        Tests that calling all on a queryset with previously defined filters duplicates queryset
        """
        query1 = TestModel.objects(test_id=5)
        assert len(query1._where) == 1

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        query3 = query2.all()
        assert query3 == query2

    def test_defining_only_and_defer_fails(self):
        """
        Tests that trying to add fields to either only or defer, or doing so more than once fails
        """

    def test_defining_only_or_defer_on_nonexistant_fields_fails(self):
        """
        Tests that setting only or defer fields that don't exist raises an exception
        """


class BaseQuerySetUsage(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseQuerySetUsage, cls).setUpClass()
        drop_table(TestModel)
        drop_table(IndexedTestModel)
        sync_table(TestModel)
        sync_table(IndexedTestModel)
        sync_table(TestMultiClusteringModel)

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

        IndexedTestModel.objects.create(test_id=0, attempt_id=0, description='try1', expected_result=5, test_result=30)
        IndexedTestModel.objects.create(test_id=1, attempt_id=1, description='try2', expected_result=10, test_result=30)
        IndexedTestModel.objects.create(test_id=2, attempt_id=2, description='try3', expected_result=15, test_result=30)
        IndexedTestModel.objects.create(test_id=3, attempt_id=3, description='try4', expected_result=20, test_result=25)

        IndexedTestModel.objects.create(test_id=4, attempt_id=0, description='try5', expected_result=5, test_result=25)
        IndexedTestModel.objects.create(test_id=5, attempt_id=1, description='try6', expected_result=10, test_result=25)
        IndexedTestModel.objects.create(test_id=6, attempt_id=2, description='try7', expected_result=15, test_result=25)
        IndexedTestModel.objects.create(test_id=7, attempt_id=3, description='try8', expected_result=20, test_result=20)

        IndexedTestModel.objects.create(test_id=8, attempt_id=0, description='try9', expected_result=50, test_result=40)
        IndexedTestModel.objects.create(test_id=9, attempt_id=1, description='try10', expected_result=60,
                                        test_result=40)
        IndexedTestModel.objects.create(test_id=10, attempt_id=2, description='try11', expected_result=70,
                                        test_result=45)
        IndexedTestModel.objects.create(test_id=11, attempt_id=3, description='try12', expected_result=75,
                                        test_result=45)

    @classmethod
    def tearDownClass(cls):
        super(BaseQuerySetUsage, cls).tearDownClass()
        drop_table(TestModel)
        drop_table(IndexedTestModel)
        drop_table(TestMultiClusteringModel)


class TestQuerySetCountSelectionAndIteration(BaseQuerySetUsage):
    def test_count(self):
        """ Tests that adding filtering statements affects the count query as expected """
        assert TestModel.objects.count() == 12

        q = TestModel.objects(test_id=0)
        assert q.count() == 4

    def test_query_expression_count(self):
        """ Tests that adding query statements affects the count query as expected """
        assert TestModel.objects.count() == 12

        q = TestModel.objects(TestModel.test_id == 0)
        assert q.count() == 4

    def test_query_limit_count(self):
        """ Tests that adding query with a limit affects the count as expected """
        assert TestModel.objects.count() == 12

        q = TestModel.objects(TestModel.test_id == 0).limit(2)
        result = q.count()
        self.assertEqual(2, result)

    def test_iteration(self):
        """ Tests that iterating over a query set pulls back all of the expected results """
        q = TestModel.objects(test_id=0)
        #tuple of expected attempt_id, expected_result values
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q:
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with regular filtering
        q = TestModel.objects(attempt_id=3).allow_filtering()
        assert len(q) == 3
        #tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with query method
        q = TestModel.objects(TestModel.attempt_id == 3).allow_filtering()
        assert len(q) == 3
        #tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

    def test_multiple_iterations_work_properly(self):
        """ Tests that iterating over a query set more than once works """
        # test with both the filtering method and the query method
        for q in (TestModel.objects(test_id=0), TestModel.objects(TestModel.test_id == 0)):
            #tuple of expected attempt_id, expected_result values
            compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
            for t in q:
                val = t.attempt_id, t.expected_result
                assert val in compare_set
                compare_set.remove(val)
            assert len(compare_set) == 0

            #try it again
            compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
            for t in q:
                val = t.attempt_id, t.expected_result
                assert val in compare_set
                compare_set.remove(val)
            assert len(compare_set) == 0

    def test_multiple_iterators_are_isolated(self):
        """
        tests that the use of one iterator does not affect the behavior of another
        """
        for q in (TestModel.objects(test_id=0), TestModel.objects(TestModel.test_id == 0)):
            q = q.order_by('attempt_id')
            expected_order = [0, 1, 2, 3]
            iter1 = iter(q)
            iter2 = iter(q)
            for attempt_id in expected_order:
                assert next(iter1).attempt_id == attempt_id
                assert next(iter2).attempt_id == attempt_id

    def test_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = TestModel.objects.get(test_id=0, attempt_id=0)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = TestModel.objects(test_id=0, attempt_id=0)
        m = q.get()
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = TestModel.objects(test_id=0)
        m = q.get(attempt_id=0)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

    def test_query_expression_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = TestModel.get(TestModel.test_id == 0, TestModel.attempt_id == 0)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = TestModel.objects(TestModel.test_id == 0, TestModel.attempt_id == 0)
        m = q.get()
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = TestModel.objects(TestModel.test_id == 0)
        m = q.get(TestModel.attempt_id == 0)
        assert isinstance(m, TestModel)
        assert m.test_id == 0
        assert m.attempt_id == 0

    def test_get_doesnotexist_exception(self):
        """
        Tests that get calls that don't return a result raises a DoesNotExist error
        """
        with self.assertRaises(TestModel.DoesNotExist):
            TestModel.objects.get(test_id=100)

    def test_get_multipleobjects_exception(self):
        """
        Tests that get calls that return multiple results raise a MultipleObjectsReturned error
        """
        with self.assertRaises(TestModel.MultipleObjectsReturned):
            TestModel.objects.get(test_id=1)

    def test_allow_filtering_flag(self):
        """
        """


def test_non_quality_filtering():
    class NonEqualityFilteringModel(Model):
        __keyspace__ = 'test'
        example_id = columns.UUID(primary_key=True, default=uuid.uuid4)
        sequence_id = columns.Integer(primary_key=True)  # sequence_id is a clustering key
        example_type = columns.Integer(index=True)
        created_at = columns.DateTime()

    drop_table(NonEqualityFilteringModel)
    sync_table(NonEqualityFilteringModel)

    # setup table, etc.

    NonEqualityFilteringModel.create(sequence_id=1, example_type=0, created_at=datetime.now())
    NonEqualityFilteringModel.create(sequence_id=3, example_type=0, created_at=datetime.now())
    NonEqualityFilteringModel.create(sequence_id=5, example_type=1, created_at=datetime.now())

    qA = NonEqualityFilteringModel.objects(NonEqualityFilteringModel.sequence_id > 3).allow_filtering()
    num = qA.count()
    assert num == 1, num



class TestQuerySetOrdering(BaseQuerySetUsage):

    def test_order_by_success_case(self):
        q = TestModel.objects(test_id=0).order_by('attempt_id')
        expected_order = [0, 1, 2, 3]
        for model, expect in zip(q, expected_order):
            assert model.attempt_id == expect

        q = q.order_by('-attempt_id')
        expected_order.reverse()
        for model, expect in zip(q, expected_order):
            assert model.attempt_id == expect

    def test_ordering_by_non_second_primary_keys_fail(self):
        # kwarg filtering
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(test_id=0).order_by('test_id')

        # kwarg filtering
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(TestModel.test_id == 0).order_by('test_id')

    def test_ordering_by_non_primary_keys_fails(self):
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(test_id=0).order_by('description')

    def test_ordering_on_indexed_columns_fails(self):
        with self.assertRaises(query.QueryException):
            q = IndexedTestModel.objects(test_id=0).order_by('attempt_id')

    def test_ordering_on_multiple_clustering_columns(self):
        TestMultiClusteringModel.create(one=1, two=1, three=4)
        TestMultiClusteringModel.create(one=1, two=1, three=2)
        TestMultiClusteringModel.create(one=1, two=1, three=5)
        TestMultiClusteringModel.create(one=1, two=1, three=1)
        TestMultiClusteringModel.create(one=1, two=1, three=3)

        results = TestMultiClusteringModel.objects.filter(one=1, two=1).order_by('-two', '-three')
        assert [r.three for r in results] == [5, 4, 3, 2, 1]

        results = TestMultiClusteringModel.objects.filter(one=1, two=1).order_by('two', 'three')
        assert [r.three for r in results] == [1, 2, 3, 4, 5]

        results = TestMultiClusteringModel.objects.filter(one=1, two=1).order_by('two').order_by('three')
        assert [r.three for r in results] == [1, 2, 3, 4, 5]


class TestQuerySetSlicing(BaseQuerySetUsage):
    def test_out_of_range_index_raises_error(self):
        q = TestModel.objects(test_id=0).order_by('attempt_id')
        with self.assertRaises(IndexError):
            q[10]

    def test_array_indexing_works_properly(self):
        q = TestModel.objects(test_id=0).order_by('attempt_id')
        expected_order = [0, 1, 2, 3]
        for i in range(len(q)):
            assert q[i].attempt_id == expected_order[i]

    def test_negative_indexing_works_properly(self):
        q = TestModel.objects(test_id=0).order_by('attempt_id')
        expected_order = [0, 1, 2, 3]
        assert q[-1].attempt_id == expected_order[-1]
        assert q[-2].attempt_id == expected_order[-2]

    def test_slicing_works_properly(self):
        q = TestModel.objects(test_id=0).order_by('attempt_id')
        expected_order = [0, 1, 2, 3]
        for model, expect in zip(q[1:3], expected_order[1:3]):
            assert model.attempt_id == expect

    def test_negative_slicing(self):
        q = TestModel.objects(test_id=0).order_by('attempt_id')
        expected_order = [0, 1, 2, 3]
        for model, expect in zip(q[-3:], expected_order[-3:]):
            assert model.attempt_id == expect
        for model, expect in zip(q[:-1], expected_order[:-1]):
            assert model.attempt_id == expect


class TestQuerySetValidation(BaseQuerySetUsage):
    def test_primary_key_or_index_must_be_specified(self):
        """
        Tests that queries that don't have an equals relation to a primary key or indexed field fail
        """
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(test_result=25)
            list([i for i in q])

    def test_primary_key_or_index_must_have_equal_relation_filter(self):
        """
        Tests that queries that don't have non equal (>,<, etc) relation to a primary key or indexed field fail
        """
        with self.assertRaises(query.QueryException):
            q = TestModel.objects(test_id__gt=0)
            list([i for i in q])

    def test_indexed_field_can_be_queried(self):
        """
        Tests that queries on an indexed field will work without any primary key relations specified
        """
        q = IndexedTestModel.objects(test_result=25)
        assert q.count() == 4


class TestQuerySetDelete(BaseQuerySetUsage):
    def test_delete(self):
        TestModel.objects.create(test_id=3, attempt_id=0, description='try9', expected_result=50, test_result=40)
        TestModel.objects.create(test_id=3, attempt_id=1, description='try10', expected_result=60, test_result=40)
        TestModel.objects.create(test_id=3, attempt_id=2, description='try11', expected_result=70, test_result=45)
        TestModel.objects.create(test_id=3, attempt_id=3, description='try12', expected_result=75, test_result=45)

        assert TestModel.objects.count() == 16
        assert TestModel.objects(test_id=3).count() == 4

        TestModel.objects(test_id=3).delete()

        assert TestModel.objects.count() == 12
        assert TestModel.objects(test_id=3).count() == 0

    def test_delete_without_partition_key(self):
        """ Tests that attempting to delete a model without defining a partition key fails """
        with self.assertRaises(query.QueryException):
            TestModel.objects(attempt_id=0).delete()

    def test_delete_without_any_where_args(self):
        """ Tests that attempting to delete a whole table without any arguments will fail """
        with self.assertRaises(query.QueryException):
            TestModel.objects(attempt_id=0).delete()


class TestQuerySetConnectionHandling(BaseQuerySetUsage):
    def test_conn_is_returned_after_filling_cache(self):
        """
        Tests that the queryset returns it's connection after it's fetched all of it's results
        """
        q = TestModel.objects(test_id=0)
        #tuple of expected attempt_id, expected_result values
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q:
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)

        assert q._con is None
        assert q._cur is None


class TimeUUIDQueryModel(Model):
    __keyspace__ = 'test'
    partition = columns.UUID(primary_key=True)
    time = columns.TimeUUID(primary_key=True)
    data = columns.Text(required=False)


class TestMinMaxTimeUUIDFunctions(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestMinMaxTimeUUIDFunctions, cls).setUpClass()
        sync_table(TimeUUIDQueryModel)

    @classmethod
    def tearDownClass(cls):
        super(TestMinMaxTimeUUIDFunctions, cls).tearDownClass()
        drop_table(TimeUUIDQueryModel)

    def test_tzaware_datetime_support(self):
        """Test that using timezone aware datetime instances works with the
        MinTimeUUID/MaxTimeUUID functions.
        """
        pk = uuid4()
        midpoint_utc = datetime.utcnow().replace(tzinfo=TzOffset(0))
        midpoint_helsinki = midpoint_utc.astimezone(TzOffset(3))

        # Assert pre-condition that we have the same logical point in time
        assert midpoint_utc.utctimetuple() == midpoint_helsinki.utctimetuple()
        assert midpoint_utc.timetuple() != midpoint_helsinki.timetuple()

        TimeUUIDQueryModel.create(
            partition=pk,
            time=columns.TimeUUID.from_datetime(midpoint_utc - timedelta(minutes=1)),
            data='1')

        TimeUUIDQueryModel.create(
            partition=pk,
            time=columns.TimeUUID.from_datetime(midpoint_utc),
            data='2')

        TimeUUIDQueryModel.create(
            partition=pk,
            time=columns.TimeUUID.from_datetime(midpoint_utc + timedelta(minutes=1)),
            data='3')

        assert ['1', '2'] == [o.data for o in TimeUUIDQueryModel.filter(
            TimeUUIDQueryModel.partition == pk,
            TimeUUIDQueryModel.time <= functions.MaxTimeUUID(midpoint_utc))]

        assert ['1', '2'] == [o.data for o in TimeUUIDQueryModel.filter(
            TimeUUIDQueryModel.partition == pk,
            TimeUUIDQueryModel.time <= functions.MaxTimeUUID(midpoint_helsinki))]

        assert ['2', '3'] == [o.data for o in TimeUUIDQueryModel.filter(
            TimeUUIDQueryModel.partition == pk,
            TimeUUIDQueryModel.time >= functions.MinTimeUUID(midpoint_utc))]

        assert ['2', '3'] == [o.data for o in TimeUUIDQueryModel.filter(
            TimeUUIDQueryModel.partition == pk,
            TimeUUIDQueryModel.time >= functions.MinTimeUUID(midpoint_helsinki))]

    def test_success_case(self):
        """ Test that the min and max time uuid functions work as expected """
        pk = uuid4()
        TimeUUIDQueryModel.create(partition=pk, time=uuid1(), data='1')
        time.sleep(0.2)
        TimeUUIDQueryModel.create(partition=pk, time=uuid1(), data='2')
        time.sleep(0.2)
        midpoint = datetime.utcnow()
        time.sleep(0.2)
        TimeUUIDQueryModel.create(partition=pk, time=uuid1(), data='3')
        time.sleep(0.2)
        TimeUUIDQueryModel.create(partition=pk, time=uuid1(), data='4')
        time.sleep(0.2)

        # test kwarg filtering
        q = TimeUUIDQueryModel.filter(partition=pk, time__lte=functions.MaxTimeUUID(midpoint))
        q = [d for d in q]
        assert len(q) == 2
        datas = [d.data for d in q]
        assert '1' in datas
        assert '2' in datas

        q = TimeUUIDQueryModel.filter(partition=pk, time__gte=functions.MinTimeUUID(midpoint))
        assert len(q) == 2
        datas = [d.data for d in q]
        assert '3' in datas
        assert '4' in datas

        # test query expression filtering
        q = TimeUUIDQueryModel.filter(
            TimeUUIDQueryModel.partition == pk,
            TimeUUIDQueryModel.time <= functions.MaxTimeUUID(midpoint)
        )
        q = [d for d in q]
        assert len(q) == 2
        datas = [d.data for d in q]
        assert '1' in datas
        assert '2' in datas

        q = TimeUUIDQueryModel.filter(
            TimeUUIDQueryModel.partition == pk,
            TimeUUIDQueryModel.time >= functions.MinTimeUUID(midpoint)
        )
        assert len(q) == 2
        datas = [d.data for d in q]
        assert '3' in datas
        assert '4' in datas


class TestInOperator(BaseQuerySetUsage):
    def test_kwarg_success_case(self):
        """ Tests the in operator works with the kwarg query method """
        q = TestModel.filter(test_id__in=[0, 1])
        assert q.count() == 8

    def test_query_expression_success_case(self):
        """ Tests the in operator works with the query expression query method """
        q = TestModel.filter(TestModel.test_id.in_([0, 1]))
        assert q.count() == 8


class TestValuesList(BaseQuerySetUsage):
    def test_values_list(self):
        q = TestModel.objects.filter(test_id=0, attempt_id=1)
        item = q.values_list('test_id', 'attempt_id', 'description', 'expected_result', 'test_result').first()
        assert item == [0, 1, 'try2', 10, 30]

        item = q.values_list('expected_result', flat=True).first()
        assert item == 10


class TestObjectsProperty(BaseQuerySetUsage):
    def test_objects_property_returns_fresh_queryset(self):
        assert TestModel.objects._result_cache is None
        len(TestModel.objects) # evaluate queryset
        assert TestModel.objects._result_cache is None


@skipUnless(cluster.protocol_version >= 2, "only runs against the cql3 protocol v2.0")
def test_paged_result_handling():
    # addresses #225
    class PagingTest(Model):
        id = columns.Integer(primary_key=True)
        val = columns.Integer()
    sync_table(PagingTest)

    PagingTest.create(id=1, val=1)
    PagingTest.create(id=2, val=2)

    session = get_session()
    with mock.patch.object(session, 'default_fetch_size', 1):
        results = PagingTest.objects()[:]

    assert len(results) == 2


