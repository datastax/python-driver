from cqlengine.tests.base import BaseCassEngTestCase


class TTLQueryTests(BaseCassEngTestCase):

    def test_update_queryset_ttl_success_case(self):
        """ tests that ttls on querysets work as expected """

    def test_select_ttl_failure(self):
        """ tests that ttls on select queries raise an exception """


class TTLModelTests(BaseCassEngTestCase):

    def test_model_ttl_success_case(self):
        """ tests that ttls on models work as expected """