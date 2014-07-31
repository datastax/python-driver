import uuid
import mock

from cqlengine import columns
from cqlengine import models
from cqlengine.connection import get_session
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import management


class TestPolymorphicClassConstruction(BaseCassEngTestCase):

    def test_multiple_polymorphic_key_failure(self):
        """ Tests that defining a model with more than one polymorphic key fails """
        with self.assertRaises(models.ModelDefinitionException):
            class M(models.Model):
                __keyspace__ = 'test'
                partition = columns.Integer(primary_key=True)
                type1 = columns.Integer(polymorphic_key=True)
                type2 = columns.Integer(polymorphic_key=True)

    def test_polymorphic_key_inheritance(self):
        """ Tests that polymorphic_key attribute is not inherited """
        class Base(models.Model):
            __keyspace__ = 'test'
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
            __keyspace__ = 'test'
            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(polymorphic_key=True)

        class M1(Base):
            __polymorphic_key__ = 1

        assert Base._is_polymorphic
        assert M1._is_polymorphic

        assert Base._is_polymorphic_base
        assert not M1._is_polymorphic_base

        assert Base._polymorphic_column is Base._columns['type1']
        assert M1._polymorphic_column is M1._columns['type1']

        assert Base._polymorphic_column_name == 'type1'
        assert M1._polymorphic_column_name == 'type1'

    def test_table_names_are_inherited_from_poly_base(self):
        class Base(models.Model):
            __keyspace__ = 'test'
            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(polymorphic_key=True)

        class M1(Base):
            __polymorphic_key__ = 1

        assert Base.column_family_name() == M1.column_family_name()

    def test_collection_columns_cant_be_polymorphic_keys(self):
        with self.assertRaises(models.ModelDefinitionException):
            class Base(models.Model):
                __keyspace__ = 'test'
                partition = columns.Integer(primary_key=True)
                type1 = columns.Set(columns.Integer, polymorphic_key=True)


class PolyBase(models.Model):
    __keyspace__ = 'test'
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

    def test_delete_on_polymorphic_subclass_does_not_include_polymorphic_key(self):
        p1 = Poly1.create()
        session = get_session()
        with mock.patch.object(session, 'execute') as m:
            Poly1.objects(partition=p1.partition).delete()

        # make sure our polymorphic key isn't in the CQL
        # not sure how we would even get here if it was in there
        # since the CQL would fail.

        self.assertNotIn("row_type", m.call_args[0][0].query_string)





class UnindexedPolyBase(models.Model):
    __keyspace__ = 'test'
    partition = columns.UUID(primary_key=True, default=uuid.uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid.uuid4)
    row_type = columns.Integer(polymorphic_key=True)


class UnindexedPoly1(UnindexedPolyBase):
    __polymorphic_key__ = 1
    data1 = columns.Text()


class UnindexedPoly2(UnindexedPolyBase):
    __polymorphic_key__ = 2
    data2 = columns.Text()


class UnindexedPoly3(UnindexedPoly2):
    __polymorphic_key__ = 3
    data3 = columns.Text()


class TestUnindexedPolymorphicQuery(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestUnindexedPolymorphicQuery, cls).setUpClass()
        management.sync_table(UnindexedPoly1)
        management.sync_table(UnindexedPoly2)
        management.sync_table(UnindexedPoly3)

        cls.p1 = UnindexedPoly1.create(data1='pickle')
        cls.p2 = UnindexedPoly2.create(partition=cls.p1.partition, data2='bacon')
        cls.p3 = UnindexedPoly3.create(partition=cls.p1.partition, data3='turkey')

    @classmethod
    def tearDownClass(cls):
        super(TestUnindexedPolymorphicQuery, cls).tearDownClass()
        management.drop_table(UnindexedPoly1)
        management.drop_table(UnindexedPoly2)
        management.drop_table(UnindexedPoly3)

    def test_non_conflicting_type_results_work(self):
        p1, p2, p3 = self.p1, self.p2, self.p3
        assert len(list(UnindexedPoly1.objects(partition=p1.partition, cluster=p1.cluster))) == 1
        assert len(list(UnindexedPoly2.objects(partition=p1.partition, cluster=p2.cluster))) == 1

    def test_subclassed_model_results_work_properly(self):
        p1, p2, p3 = self.p1, self.p2, self.p3
        assert len(list(UnindexedPoly2.objects(partition=p1.partition, cluster__in=[p2.cluster, p3.cluster]))) == 2

    def test_conflicting_type_results(self):
        with self.assertRaises(models.PolyMorphicModelException):
            list(UnindexedPoly1.objects(partition=self.p1.partition))
        with self.assertRaises(models.PolyMorphicModelException):
            list(UnindexedPoly2.objects(partition=self.p1.partition))


class IndexedPolyBase(models.Model):
    __keyspace__ = 'test'
    partition = columns.UUID(primary_key=True, default=uuid.uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid.uuid4)
    row_type = columns.Integer(polymorphic_key=True, index=True)


class IndexedPoly1(IndexedPolyBase):
    __polymorphic_key__ = 1
    data1 = columns.Text()


class IndexedPoly2(IndexedPolyBase):
    __polymorphic_key__ = 2
    data2 = columns.Text()


class TestIndexedPolymorphicQuery(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestIndexedPolymorphicQuery, cls).setUpClass()
        management.sync_table(IndexedPoly1)
        management.sync_table(IndexedPoly2)

        cls.p1 = IndexedPoly1.create(data1='pickle')
        cls.p2 = IndexedPoly2.create(partition=cls.p1.partition, data2='bacon')

    @classmethod
    def tearDownClass(cls):
        super(TestIndexedPolymorphicQuery, cls).tearDownClass()
        management.drop_table(IndexedPoly1)
        management.drop_table(IndexedPoly2)

    def test_success_case(self):
        assert len(list(IndexedPoly1.objects(partition=self.p1.partition))) == 1
        assert len(list(IndexedPoly2.objects(partition=self.p1.partition))) == 1


