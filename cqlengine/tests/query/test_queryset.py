from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.exceptions import ModelException
from cqlengine.models import Model
from cqlengine import columns

class TestQuerySet(BaseCassEngTestCase):

    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method
        """

    def test_using_invalid_column_names_in_filter_kwargs_raises_error(self):
        """
        Tests that using invalid or nonexistant column names for filter args raises an error
        """

    def test_where_clause_generation(self):
        """
        Tests the where clause creation
        """

    def test_querystring_generation(self):
        """
        Tests the select querystring creation
        """

    def test_queryset_is_immutable(self):
        """
        Tests that calling a queryset function that changes it's state returns a new queryset
        """

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

    def test_defining_only_and_defer_fails(self):
        """
        Tests that trying to add fields to either only or defer, or doing so more than once fails
        """

    def test_defining_only_or_defer_fields_fails(self):
        """
        Tests that setting only or defer fields that don't exist raises an exception
        """
