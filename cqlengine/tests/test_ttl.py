from cqlengine.management import sync_table, drop_table
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.models import Model
from uuid import uuid4
from cqlengine import columns


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

    def test_model_ttl_success_case(self):
        """ tests that ttls on models work as expected """

    def test_queryset_is_returned_on_class(self):
        """
        ensures we get a queryset descriptor back
        """
        qs = TestTTLModel.ttl(60)
        self.assertTrue(isinstance(qs, TestTTLModel.__queryset__), type(qs))

class TTLInstanceTest(BaseTTLTest):
    def test_instance_is_returned(self):
        """
        ensures that we properly handle the instance.ttl(60).save() scenario
        :return:
        """
        o = TestTTLModel.create(text="whatever")
        o.text = "new stuff"
        o.ttl(60)
        self.assertEqual(60, o._ttl)
        o.save()



class QuerySetTTLFragmentTest(BaseTTLTest):
    pass
