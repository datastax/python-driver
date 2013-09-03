import uuid

from cqlengine import columns
from cqlengine import models
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import management


class TestPolymorphicClassConstruction(BaseCassEngTestCase):

    def test_multiple_polymorphic_key_failure(self):
        """ Tests that defining a model with more than one polymorphic key fails """
        with self.assertRaises(models.ModelDefinitionException):
            class M(models.Model):
                partition = columns.Integer(primary_key=True)
                type1 = columns.Integer(polymorphic_key=True)
                type2 = columns.Integer(polymorphic_key=True)

    def test_polymorphic_key_inheritance(self):
        """ Tests that polymorphic_key attribute is not inherited """
        class Base(models.Model):
            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(polymorphic_key=True)

        class M1(Base):
            __polymorphic_key__ = 1

        class M2(M1):
            pass

        assert M2.__polymorphic_key__ is None

    def test_polymorphic_metaclass(self):
        """ Tests that the model meta class configures polymorphic models properly """
        class Base(models.Model):
            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(polymorphic_key=True)

        class M1(Base):
            __polymorphic_key__ = 1

        assert Base._is_polymorphic
        assert M1._is_polymorphic

        assert Base._is_polymorphic_base
        assert not M1._is_polymorphic_base

        assert Base._polymorphic_column == Base.type1
        assert M1._polymorphic_column == M1.type1

        assert Base._polymorphic_column_name == 'type1'
        assert M1._polymorphic_column_name == 'type1'

    def test_table_names_are_inherited_from_poly_base(self):
        class Base(models.Model):
            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(polymorphic_key=True)

        class M1(Base):
            __polymorphic_key__ = 1

        assert Base.column_family_name() == M1.column_family_name()


class PolyBase(models.Model):
    partition = columns.UUID(primary_key=True, default=uuid.uuid4)
    row_type = columns.Integer(polymorphic_key=True)


class Poly1(PolyBase):
    __polymorphic_key__ = 1
    data1 = columns.Text()


class Poly2(PolyBase):
    __polymorphic_key__ = 2
    data2 = columns.Text()


class TestPolymorphicModel(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestPolymorphicModel, cls).setUpClass()
        management.sync_table(Poly1)
        management.sync_table(Poly2)

    @classmethod
    def tearDownClass(cls):
        super(TestPolymorphicModel, cls).tearDownClass()
        management.drop_table(Poly1)
        management.drop_table(Poly2)

    def test_saving_base_model_fails(self):
        with self.assertRaises(models.PolyMorphicModelException):
            PolyBase.create()

    def test_saving_subclass_saves_poly_key(self):
        p1 = Poly1.create(data1='pickle')
        p2 = Poly2.create(data2='bacon')

        assert p1.row_type == Poly1.__polymorphic_key__
        assert p2.row_type == Poly2.__polymorphic_key__

    def test_query_deserialization(self):
        p1 = Poly1.create(data1='pickle')
        p2 = Poly2.create(data2='bacon')

        p1r = PolyBase.get(partition=p1.partition)
        p2r = PolyBase.get(partition=p2.partition)

        assert isinstance(p1r, Poly1)
        assert isinstance(p2r, Poly2)


class TestIndexedPolymorphicQuery(BaseCassEngTestCase):

    def test_success_case(self):
        pass

    def test_polymorphic_key_is_added_to_queries(self):
        pass


class TestUnindexedPolymorphicQuery(BaseCassEngTestCase):

    def test_non_conflicting_type_results_work(self):
        pass

    def test_conflicting_type_results(self):
        pass

    def test_allow_filtering_filters_types(self):
        pass
