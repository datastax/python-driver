from unittest import TestCase
from cqlengine.statements import AssignmentClause


class AssignmentClauseTests(TestCase):

    def test_rendering(self):
        pass

    def test_insert_tuple(self):
        ac = AssignmentClause('a', 'b')
        ac.set_context_id(10)
        self.assertEqual(ac.insert_tuple(), ('a', 10))
