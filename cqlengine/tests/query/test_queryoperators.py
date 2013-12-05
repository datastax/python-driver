from datetime import datetime
from cqlengine.columns import DateTime

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import columns, Model
from cqlengine import functions
from cqlengine import query
from cqlengine.statements import WhereClause
from cqlengine.operators import EqualsOperator
from cqlengine.management import sync_table, drop_table

class TestQuerySetOperation(BaseCassEngTestCase):

    def test_maxtimeuuid_function(self):
        """
        Tests that queries with helper functions are generated properly
        """
        now = datetime.now()
        where = WhereClause('time', EqualsOperator(), functions.MaxTimeUUID(now))
        where.set_context_id(5)

        self.assertEqual(str(where), '"time" = MaxTimeUUID(:5)')
        ctx = {}
        where.update_context(ctx)
        self.assertEqual(ctx, {'5': DateTime().to_database(now)})

    def test_mintimeuuid_function(self):
        """
        Tests that queries with helper functions are generated properly
        """
        now = datetime.now()
        where = WhereClause('time', EqualsOperator(), functions.MinTimeUUID(now))
        where.set_context_id(5)

        self.assertEqual(str(where), '"time" = MinTimeUUID(:5)')
        ctx = {}
        where.update_context(ctx)
        self.assertEqual(ctx, {'5': DateTime().to_database(now)})


class TokenTestModel(Model):
    key = columns.Integer(primary_key=True)
    val = columns.Integer()


class TestTokenFunction(BaseCassEngTestCase):

    def setUp(self):
        super(TestTokenFunction, self).setUp()
        sync_table(TokenTestModel)

    def tearDown(self):
        super(TestTokenFunction, self).tearDown()
        drop_table(TokenTestModel)

    def test_token_function(self):
        """ Tests that token functions work properly """
        assert TokenTestModel.objects().count() == 0
        for i in range(10):
            TokenTestModel.create(key=i, val=i)
        assert TokenTestModel.objects().count() == 10
        seen_keys = set()
        last_token = None
        for instance in TokenTestModel.objects().limit(5):
            last_token = instance.key
            seen_keys.add(last_token)
        assert len(seen_keys) == 5
        for instance in TokenTestModel.objects(pk__token__gt=functions.Token(last_token)):
            seen_keys.add(instance.key)

        assert len(seen_keys) == 10
        assert all([i in seen_keys for i in range(10)])

    def test_compound_pk_token_function(self):

        class TestModel(Model):
            p1 = columns.Text(partition_key=True)
            p2 = columns.Text(partition_key=True)

        func = functions.Token('a', 'b')

        q = TestModel.objects.filter(pk__token__gt=func)
        where = q._where[0]
        where.set_context_id(1)
        self.assertEquals(str(where), 'token("p1", "p2") > token(:{}, :{})'.format(1, 2))

        # Token(tuple()) is also possible for convenience
        # it (allows for Token(obj.pk) syntax)
        func = functions.Token(('a', 'b'))

        q = TestModel.objects.filter(pk__token__gt=func)
        where = q._where[0]
        where.set_context_id(1)
        self.assertEquals(str(where), 'token("p1", "p2") > token(:{}, :{})'.format(1, 2))

