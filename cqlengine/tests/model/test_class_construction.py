from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.exceptions import ModelException
from cqlengine.models import Model
from cqlengine import columns
import cqlengine

class TestModelClassFunction(BaseCassEngTestCase):
    """
    Tests verifying the behavior of the Model metaclass
    """

    def test_column_attributes_handled_correctly(self):
        """
        Tests that column attributes are moved to a _columns dict
        and replaced with simple value attributes
        """

        class TestModel(Model):
            text = columns.Text()

        #check class attibutes
        self.assertHasAttr(TestModel, '_columns')
        self.assertHasAttr(TestModel, 'id')
        self.assertHasAttr(TestModel, 'text')

        #check instance attributes
        inst = TestModel()
        self.assertHasAttr(inst, 'id')
        self.assertHasAttr(inst, 'text')
        self.assertIsNone(inst.id)
        self.assertIsNone(inst.text)

    def test_db_map(self):
        """
        Tests that the db_map is properly defined
        -the db_map allows columns
        """
        class WildDBNames(Model):
            content = columns.Text(db_field='words_and_whatnot')
            numbers = columns.Integer(db_field='integers_etc')

        db_map = WildDBNames._db_map
        self.assertEquals(db_map['words_and_whatnot'], 'content')
        self.assertEquals(db_map['integers_etc'], 'numbers')

    def test_attempting_to_make_duplicate_column_names_fails(self):
        """
        Tests that trying to create conflicting db column names will fail
        """

        with self.assertRaises(ModelException):
            class BadNames(Model):
                words = columns.Text()
                content = columns.Text(db_field='words')

    def test_column_ordering_is_preserved(self):
        """
        Tests that the _columns dics retains the ordering of the class definition
        """

        class Stuff(Model):
            words = columns.Text()
            content = columns.Text()
            numbers = columns.Integer()

        self.assertEquals(Stuff._columns.keys(), ['id', 'words', 'content', 'numbers'])

    def test_value_managers_are_keeping_model_instances_isolated(self):
        """
        Tests that instance value managers are isolated from other instances
        """
        class Stuff(Model):
            num = columns.Integer()

        inst1 = Stuff(num=5)
        inst2 = Stuff(num=7)

        self.assertNotEquals(inst1.num, inst2.num)
        self.assertEquals(inst1.num, 5)
        self.assertEquals(inst2.num, 7)

    def test_superclass_fields_are_inherited(self):
        """
        Tests that fields defined on the super class are inherited properly
        """
        class TestModel(Model):
            text = columns.Text()

        class InheritedModel(TestModel):
            numbers = columns.Integer()

        assert 'text' in InheritedModel._columns
        assert 'numbers' in InheritedModel._columns

    def test_normal_fields_can_be_defined_between_primary_keys(self):
        """
        Tests tha non primary key fields can be defined between primary key fields
        """

    def test_at_least_one_non_primary_key_column_is_required(self):
        """
        Tests that an error is raised if a model doesn't contain at least one primary key field
        """

    def test_model_keyspace_attribute_must_be_a_string(self):
        """
        Tests that users can't set the keyspace to None, or something else
        """

    def test_indexes_arent_allowed_on_models_with_multiple_primary_keys(self):
        """
        Tests that attempting to define an index on a model with multiple primary keys fails
        """

    def test_meta_data_is_not_inherited(self):
        """
        Test that metadata defined in one class, is not inherited by subclasses
        """

    def test_partition_keys(self):
        """
        Test compound partition key definition
        """
        class ModelWithPartitionKeys(cqlengine.Model):
            c1 = cqlengine.Text(primary_key=True)
            p1 = cqlengine.Text(partition_key=True)
            p2 = cqlengine.Text(partition_key=True)

        cols = ModelWithPartitionKeys._columns

        self.assertTrue(cols['c1'].primary_key)
        self.assertFalse(cols['c1'].partition_key)

        self.assertTrue(cols['p1'].primary_key)
        self.assertTrue(cols['p1'].partition_key)
        self.assertTrue(cols['p2'].primary_key)
        self.assertTrue(cols['p2'].partition_key)

        obj = ModelWithPartitionKeys(p1='a', p2='b')
        self.assertEquals(obj.pk, ('a', 'b'))

    def test_del_attribute_is_assigned_properly(self):
        """ Tests that columns that can be deleted have the del attribute """
        class DelModel(Model):
            key = columns.Integer(primary_key=True)
            data = columns.Integer(required=False)

        model = DelModel(key=4, data=5)
        del model.data
        with self.assertRaises(AttributeError):
            del model.key

    def test_does_not_exist_exceptions_are_not_shared_between_model(self):
        """ Tests that DoesNotExist exceptions are not the same exception between models """

        class Model1(Model):
            pass
        class Model2(Model):
            pass

        try:
            raise Model1.DoesNotExist
        except Model2.DoesNotExist:
            assert False, "Model1 exception should not be caught by Model2"
        except Model1.DoesNotExist:
            #expected
            pass

    def test_does_not_exist_inherits_from_superclass(self):
        """ Tests that a DoesNotExist exception can be caught by it's parent class DoesNotExist """
        class Model1(Model):
            pass
        class Model2(Model1):
            pass

        try:
            raise Model2.DoesNotExist
        except Model1.DoesNotExist:
            #expected
            pass
        except Exception:
            assert False, "Model2 exception should not be caught by Model1"

class TestManualTableNaming(BaseCassEngTestCase):
    
    class RenamedTest(cqlengine.Model):
        keyspace = 'whatever'
        table_name = 'manual_name'
        
        id = cqlengine.UUID(primary_key=True)
        data = cqlengine.Text()
        
    def test_proper_table_naming(self):
        assert self.RenamedTest.column_family_name(include_keyspace=False) == 'manual_name'
        assert self.RenamedTest.column_family_name(include_keyspace=True) == 'whatever.manual_name'

    def test_manual_table_name_is_not_inherited(self):
        class InheritedTest(self.RenamedTest): pass
        assert InheritedTest.table_name is None











