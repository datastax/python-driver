from unittest import TestCase
from cqlengine.statements import BaseClause


class BaseClauseTests(TestCase):

    def test_context_updating(self):
        ss = BaseClause('a', 'b')
        assert ss.get_context_size() == 1

        ctx = {}
        ss.set_context_id(10)
        ss.update_context(ctx)
        assert ctx == {'10': 'b'}


