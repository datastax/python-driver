from unittest import TestCase
from cqlengine.statements import AssignmentClause, SetUpdateClause


class AssignmentClauseTests(TestCase):

    def test_rendering(self):
        pass

    def test_insert_tuple(self):
        ac = AssignmentClause('a', 'b')
        ac.set_context_id(10)
        self.assertEqual(ac.insert_tuple(), ('a', 10))


class SetUpdateClauseTests(TestCase):

    def test_update_from_none(self):
        c = SetUpdateClause('s', {1, 2}, None)
        c._analyze()
        c.set_context_id(0)

        self.assertEqual(c._assignments, {1, 2})
        self.assertIsNone(c._additions)
        self.assertIsNone(c._removals)

        self.assertEqual(c.get_context_size(), 1)
        self.assertEqual(str(c), '"s" = :0')

        ctx = {}
        c.update_context(ctx)
        self.assertEqual(ctx, {'0': {1, 2}})

    def test_null_update(self):
        """ tests setting a set to None creates an empty update statement """
        c = SetUpdateClause('s', None, {1, 2})
        c._analyze()
        c.set_context_id(0)

        self.assertIsNone(c._assignments)
        self.assertIsNone(c._additions)
        self.assertIsNone(c._removals)

        self.assertEqual(c.get_context_size(), 0)
        self.assertEqual(str(c), '')

        ctx = {}
        c.update_context(ctx)
        self.assertEqual(ctx, {})

    def test_no_update(self):
        """ tests an unchanged value creates an empty update statement """
        c = SetUpdateClause('s', {1, 2}, {1, 2})
        c._analyze()
        c.set_context_id(0)

        self.assertIsNone(c._assignments)
        self.assertIsNone(c._additions)
        self.assertIsNone(c._removals)

        self.assertEqual(c.get_context_size(), 0)
        self.assertEqual(str(c), '')

        ctx = {}
        c.update_context(ctx)
        self.assertEqual(ctx, {})

    def test_additions(self):
        c = SetUpdateClause('s', {1, 2, 3}, {1, 2})
        c._analyze()
        c.set_context_id(0)

        self.assertIsNone(c._assignments)
        self.assertEqual(c._additions, {3})
        self.assertIsNone(c._removals)

        self.assertEqual(c.get_context_size(), 1)
        self.assertEqual(str(c), '"s" = "s" + :0')

        ctx = {}
        c.update_context(ctx)
        self.assertEqual(ctx, {'0': {3}})

    def test_removals(self):
        c = SetUpdateClause('s', {1, 2}, {1, 2, 3})
        c._analyze()
        c.set_context_id(0)

        self.assertIsNone(c._assignments)
        self.assertIsNone(c._additions)
        self.assertEqual(c._removals, {3})

        self.assertEqual(c.get_context_size(), 1)
        self.assertEqual(str(c), '"s" = "s" - :0')

        ctx = {}
        c.update_context(ctx)
        self.assertEqual(ctx, {'0': {3}})

    def test_additions_and_removals(self):
        c = SetUpdateClause('s', {2, 3}, {1, 2})
        c._analyze()
        c.set_context_id(0)

        self.assertIsNone(c._assignments)
        self.assertEqual(c._additions, {3})
        self.assertEqual(c._removals, {1})

        self.assertEqual(c.get_context_size(), 2)
        self.assertEqual(str(c), '"s" = "s" + :0, "s" = "s" - :1')

        ctx = {}
        c.update_context(ctx)
        self.assertEqual(ctx, {'0': {3}, '1': {1}})

