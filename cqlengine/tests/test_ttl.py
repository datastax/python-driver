from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from uuid import uuid4
from cqlengine import columns
import mock
from cqlengine.connection import ConnectionPool

class TestTTLModel(Model):
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)


class BaseTTLTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseTTLTest, cls).setUpClass()
        sync_table(TestTTLModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseTTLTest, cls).tearDownClass()
        drop_table(TestTTLModel)



class TTLQueryTests(BaseTTLTest):

    def test_update_queryset_ttl_success_case(self):
        """ tests that ttls on querysets work as expected """

    def test_select_ttl_failure(self):
        """ tests that ttls on select queries raise an exception """


class TTLModelTests(BaseTTLTest):

    def test_ttl_included_on_create(self):
        """ tests that ttls on models work as expected """
        with mock.patch.object(ConnectionPool, 'execute') as m:
            TestTTLModel.ttl(60).create(text="hello blake")

        query = m.call_args[0][0]
        self.assertIn("USING TTL", query)

    def test_queryset_is_returned_on_class(self):
        """
        ensures we get a queryset descriptor back
        """
        qs = TestTTLModel.ttl(60)
        self.assertTrue(isinstance(qs, TestTTLModel.__queryset__), type(qs))



class TTLInstanceUpdateTest(BaseTTLTest):
    def test_update_includes_ttl(self):
        model = TestTTLModel.create(text="goodbye blake")
        with mock.patch.object(ConnectionPool, 'execute') as m:
            model.ttl(60).update(text="goodbye forever")

        query = m.call_args[0][0]
        self.assertIn("USING TTL", query)

    def test_update_syntax_valid(self):
        # sanity test that ensures the TTL syntax is accepted by cassandra
        model = TestTTLModel.create(text="goodbye blake")
        model.ttl(60).update(text="goodbye forever")





class TTLInstanceTest(BaseTTLTest):
    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.ttl(60).save() scenario
        :return:
        """
        o = TestTTLModel.create(text="whatever")
        o.text = "new stuff"
        o = o.ttl(60)
        self.assertEqual(60, o._ttl)

    def test_ttl_is_include_with_query_on_update(self):
        o = TestTTLModel.create(text="whatever")
        o.text = "new stuff"
        o = o.ttl(60)

        with mock.patch.object(ConnectionPool, 'execute') as m:
            o.save()
        query = m.call_args[0][0]
        self.assertIn("USING TTL", query)


class TTLBlindUpdateTest(BaseTTLTest):
    def test_ttl_included_with_blind_update(self):
        o = TestTTLModel.create(text="whatever")
        tid = o.id

        with mock.patch.object(ConnectionPool, 'execute') as m:
            TestTTLModel.objects(id=tid).ttl(60).update(text="bacon")

        query = m.call_args[0][0]
        self.assertIn("USING TTL", query)




