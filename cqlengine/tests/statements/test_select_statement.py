from unittest import TestCase
from cqlengine.statements import SelectStatement


class SelectStatementTests(TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ss = SelectStatement('table', 'field')
        self.assertEqual(ss.fields, ['field'])

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ss = SelectStatement('table', ['f1', 'f2'])
        self.assertTrue(unicode(ss).startswith('SELECT f1, f2'))

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ss = SelectStatement('table', None)
        self.assertTrue(unicode(ss).startswith('SELECT *'))
