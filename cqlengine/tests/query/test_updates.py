from uuid import uuid4
from cqlengine.exceptions import ValidationError
from cqlengine.query import QueryException

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from cqlengine.management import sync_table, drop_table
from cqlengine import columns


class TestQueryUpdateModel(Model):
    __keyspace__ = 'test'
    partition   = columns.UUID(primary_key=True, default=uuid4)
    cluster     = columns.Integer(primary_key=True)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False, index=True)
    text_set    = columns.Set(columns.Text, required=False)
    text_list   = columns.List(columns.Text, required=False)
    text_map    = columns.Map(columns.Text, columns.Text, required=False)

class QueryUpdateTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(QueryUpdateTests, cls).setUpClass()
        sync_table(TestQueryUpdateModel)

    @classmethod
    def tearDownClass(cls):
        super(QueryUpdateTests, cls).tearDownClass()
        drop_table(TestQueryUpdateModel)

    def test_update_values(self):
        """ tests calling udpate on a queryset """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == i
            assert row.text == str(i)

        # perform update
        TestQueryUpdateModel.objects(partition=partition, cluster=3).update(count=6)

        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == (6 if i == 3 else i)
            assert row.text == str(i)

    def test_update_values_validation(self):
        """ tests calling udpate on models with values passed in """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == i
            assert row.text == str(i)

        # perform update
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=partition, cluster=3).update(count='asdf')

    def test_invalid_update_kwarg(self):
        """ tests that passing in a kwarg to the update method that isn't a column will fail """
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=uuid4(), cluster=3).update(bacon=5000)

    def test_primary_key_update_failure(self):
        """ tests that attempting to update the value of a primary key will fail """
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=uuid4(), cluster=3).update(cluster=5000)

    def test_null_update_deletes_column(self):
        """ setting a field to null in the update should issue a delete statement """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == i
            assert row.text == str(i)

        # perform update
        TestQueryUpdateModel.objects(partition=partition, cluster=3).update(text=None)

        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == i
            assert row.text == (None if i == 3 else str(i))

    def test_mixed_value_and_null_update(self):
        """ tests that updating a columns value, and removing another works properly """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == i
            assert row.text == str(i)

        # perform update
        TestQueryUpdateModel.objects(partition=partition, cluster=3).update(count=6, text=None)

        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            assert row.cluster == i
            assert row.count == (6 if i == 3 else i)
            assert row.text == (None if i == 3 else str(i))

    def test_counter_updates(self):
        pass

    def test_set_add_updates(self):
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_set={"foo"})
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(text_set__add={'bar'})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, {"foo", "bar"})

    def test_set_add_updates_new_record(self):
        """ If the key doesn't exist yet, an update creates the record
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(text_set__add={'bar'})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, {"bar"})

    def test_set_remove_updates(self):
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_set={"foo", "baz"})
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_set__remove={'foo'})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, {"baz"})

    def test_set_remove_new_record(self):
        """ Removing something not in the set should silently do nothing
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_set={"foo"})
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_set__remove={'afsd'})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, {"foo"})

    def test_list_append_updates(self):
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_list=["foo"])
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_list__append=['bar'])
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_list, ["foo", "bar"])

    def test_list_prepend_updates(self):
        """ Prepend two things since order is reversed by default by CQL """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_list=["foo"])
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_list__prepend=['bar', 'baz'])
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_list, ["bar", "baz", "foo"])

    def test_map_update_updates(self):
        """ Merge a dictionary into existing value """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster,
                text_map={"foo": '1', "bar": '2'})
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_map__update={"bar": '3', "baz": '4'})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_map, {"foo": '1', "bar": '3', "baz": '4'})

    def test_map_update_none_deletes_key(self):
        """ The CQL behavior is if you set a key in a map to null it deletes
        that key from the map.  Test that this works with __update.

        This test fails because of a bug in the cql python library not
        converting None to null (and the cql library is no longer in active
        developement).
        """
        # partition = uuid4()
        # cluster = 1
        # TestQueryUpdateModel.objects.create(
        #         partition=partition, cluster=cluster,
        #         text_map={"foo": '1', "bar": '2'})
        # TestQueryUpdateModel.objects(
        #         partition=partition, cluster=cluster).update(
        #         text_map__update={"bar": None})
        # obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        # self.assertEqual(obj.text_map, {"foo": '1'})
