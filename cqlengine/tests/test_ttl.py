from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from uuid import uuid4
from cqlengine import columns
import mock
from cqlengine.connection import get_session


class TestTTLModel(Model):
    __keyspace__ = 'test'
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


class TestDefaultTTLModel(Model):
    __keyspace__ = 'test'
    __default_ttl__ = 20
    id      = columns.UUID(primary_key=True, default=lambda:uuid4())
    count   = columns.Integer()
    text    = columns.Text(required=False)


class BaseDefaultTTLTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseDefaultTTLTest, cls).setUpClass()
        sync_table(TestDefaultTTLModel)
        sync_table(TestTTLModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseDefaultTTLTest, cls).tearDownClass()
        drop_table(TestDefaultTTLModel)
        drop_table(TestTTLModel)


class TTLQueryTests(BaseTTLTest):

    def test_update_queryset_ttl_success_case(self):
        """ tests that ttls on querysets work as expected """

    def test_select_ttl_failure(self):
        """ tests that ttls on select queries raise an exception """


class TTLModelTests(BaseTTLTest):

    def test_ttl_included_on_create(self):
        """ tests that ttls on models work as expected """
        session = get_session()

        with mock.patch.object(session, 'execute') as m:
            TestTTLModel.ttl(60).create(text="hello blake")

        query = m.call_args[0][0].query_string
        self.assertIn("USING TTL", query)

    def test_queryset_is_returned_on_class(self):
        """
        ensures we get a queryset descriptor back
        """
        qs = TestTTLModel.ttl(60)
        self.assertTrue(isinstance(qs, TestTTLModel.__queryset__), type(qs))



class TTLInstanceUpdateTest(BaseTTLTest):
    def test_update_includes_ttl(self):
        session = get_session()

        model = TestTTLModel.create(text="goodbye blake")
        with mock.patch.object(session, 'execute') as m:
            model.ttl(60).update(text="goodbye forever")

        query = m.call_args[0][0].query_string
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
        session = get_session()

        o = TestTTLModel.create(text="whatever")
        o.text = "new stuff"
        o = o.ttl(60)

        with mock.patch.object(session, 'execute') as m:
            o.save()

        query = m.call_args[0][0].query_string
        self.assertIn("USING TTL", query)


class TTLBlindUpdateTest(BaseTTLTest):
    def test_ttl_included_with_blind_update(self):
        session = get_session()

        o = TestTTLModel.create(text="whatever")
        tid = o.id

        with mock.patch.object(session, 'execute') as m:
            TestTTLModel.objects(id=tid).ttl(60).update(text="bacon")

        query = m.call_args[0][0].query_string
        self.assertIn("USING TTL", query)


class TTLDefaultTest(BaseDefaultTTLTest):
    def test_default_ttl_not_set(self):
        session = get_session()

        o = TestTTLModel.create(text="some text")
        tid = o.id

        self.assertIsNone(o._ttl)

        with mock.patch.object(session, 'execute') as m:
            TestTTLModel.objects(id=tid).update(text="aligators")

        query = m.call_args[0][0].query_string
        self.assertNotIn("USING TTL", query)

    def test_default_ttl_set(self):
        session = get_session()
        o = TestDefaultTTLModel.create(text="some text on ttl")
        tid = o.id

        self.assertEqual(o._ttl, TestDefaultTTLModel.__default_ttl__)

        with mock.patch.object(session, 'execute') as m:
            TestDefaultTTLModel.objects(id=tid).update(text="aligators expired")

        query = m.call_args[0][0].query_string
        self.assertIn("USING TTL", query)

    def test_override_default_ttl(self):
        session = get_session()
        o = TestDefaultTTLModel.create(text="some text on ttl")
        tid = o.id

        self.assertEqual(o._ttl, TestDefaultTTLModel.__default_ttl__)
        o.ttl(3600)
        self.assertEqual(o._ttl, 3600)

        with mock.patch.object(session, 'execute') as m:
            TestDefaultTTLModel.objects(id=tid).ttl(None).update(text="aligators expired")

        query = m.call_args[0][0].query_string
        self.assertNotIn("USING TTL", query)