from datetime import datetime
from cqlengine.columns import DateTime

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import columns, Model
from cqlengine import functions
from cqlengine import query
from cqlengine.statements import WhereClause
from cqlengine.operators import EqualsOperator

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

    def test_token_function(self):

        class TestModel(Model):
            p1 = columns.Text(partition_key=True)
            p2 = columns.Text(partition_key=True)

        func = functions.Token('a', 'b')

        q = TestModel.objects.filter(pk__token__gt=func)
        where = q._where[0]
        where.set_context_id(1)
        self.assertEquals(str(where), 'token("p1", "p2") > token(:{}, :{})'.format(1, 2))

        # Verify that a SELECT query can be successfully generated
        str(q._select_query())

        # Token(tuple()) is also possible for convenience
        # it (allows for Token(obj.pk) syntax)
        func = functions.Token(('a', 'b'))

        q = TestModel.objects.filter(pk__token__gt=func)
        where = q._where[0]
        where.set_context_id(1)
        self.assertEquals(str(where), 'token("p1", "p2") > token(:{}, :{})'.format(1, 2))
        str(q._select_query())

        # The 'pk__token' virtual column may only be compared to a Token
        self.assertRaises(query.QueryException, TestModel.objects.filter, pk__token__gt=10)

        # A Token may only be compared to the `pk__token' virtual column
        func = functions.Token('a', 'b')
        self.assertRaises(query.QueryException, TestModel.objects.filter, p1__gt=func)

        # The # of arguments to Token must match the # of partition keys
        func = functions.Token('a')
        self.assertRaises(query.QueryException, TestModel.objects.filter, pk__token__gt=func)
