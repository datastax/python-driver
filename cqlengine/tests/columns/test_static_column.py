from uuid import uuid4
from unittest import skipUnless
from cqlengine import Model
from cqlengine import columns
from cqlengine.management import sync_table, drop_table
from cqlengine.models import ModelDefinitionException
from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine.tests.base import CASSANDRA_VERSION, PROTOCOL_VERSION


class TestStaticModel(Model):

    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    static = columns.Text(static=True)
    text = columns.Text()


class TestStaticColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestStaticColumn, cls).setUpClass()
        drop_table(TestStaticModel)
        if CASSANDRA_VERSION >= 20:
            sync_table(TestStaticModel)

    @classmethod
    def tearDownClass(cls):
        super(TestStaticColumn, cls).tearDownClass()
        drop_table(TestStaticModel)

    @skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_mixed_updates(self):
        """ Tests that updates on both static and non-static columns work as intended """
        instance = TestStaticModel.create()
        instance.static = "it's shared"
        instance.text = "some text"
        instance.save()

        u = TestStaticModel.get(partition=instance.partition)
        u.static = "it's still shared"
        u.text = "another text"
        u.update()
        actual = TestStaticModel.get(partition=u.partition)

        assert actual.static == "it's still shared"

    @skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_static_only_updates(self):
        """ Tests that updates on static only column work as intended """
        instance = TestStaticModel.create()
        instance.static = "it's shared"
        instance.text = "some text"
        instance.save()

        u = TestStaticModel.get(partition=instance.partition)
        u.static = "it's still shared"
        u.update()
        actual = TestStaticModel.get(partition=u.partition)
        assert actual.static == "it's still shared"

