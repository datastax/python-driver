from unittest import TestCase

from cqlengine.models import Model, ModelDefinitionException
from cqlengine import columns


class TestModel(TestCase):
    """ Tests the non-io functionality of models """

    def test_instance_equality(self):
        """ tests the model equality functionality """
        class EqualityModel(Model):
            __keyspace__ = 'test'
            pk = columns.Integer(primary_key=True)

        m0 = EqualityModel(pk=0)
        m1 = EqualityModel(pk=1)

        self.assertEqual(m0, m0)
        self.assertNotEqual(m0, m1)

    def test_model_equality(self):
        """ tests the model equality functionality """
        class EqualityModel0(Model):
            __keyspace__ = 'test'
            pk = columns.Integer(primary_key=True)

        class EqualityModel1(Model):
            __keyspace__ = 'test'
            kk = columns.Integer(primary_key=True)

        m0 = EqualityModel0(pk=0)
        m1 = EqualityModel1(kk=1)

        self.assertEqual(m0, m0)
        self.assertNotEqual(m0, m1)


class BuiltInAttributeConflictTest(TestCase):
    """tests Model definitions that conflict with built-in attributes/methods"""

    def test_model_with_attribute_name_conflict(self):
        """should raise exception when model defines column that conflicts with built-in attribute"""
        with self.assertRaises(ModelDefinitionException):
            class IllegalTimestampColumnModel(Model):
                __keyspace__ = 'test'
                my_primary_key = columns.Integer(primary_key=True)
                timestamp = columns.BigInt()

    def test_model_with_method_name_conflict(self):
        """should raise exception when model defines column that conflicts with built-in method"""
        with self.assertRaises(ModelDefinitionException):
            class IllegalFilterColumnModel(Model):
                __keyspace__ = 'test'
                my_primary_key = columns.Integer(primary_key=True)
                filter = columns.Text()


