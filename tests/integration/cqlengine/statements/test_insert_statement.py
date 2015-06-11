# Copyright 2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.cqlengine.statements import InsertStatement, StatementException, AssignmentClause

import six

class InsertStatementTests(unittest.TestCase):

    def test_where_clause_failure(self):
        """ tests that where clauses cannot be added to Insert statements """
        ist = InsertStatement('table', None)
        with self.assertRaises(StatementException):
            ist.add_where_clause('s')

    def test_statement(self):
        ist = InsertStatement('table', None)
        ist.add_assignment_clause(AssignmentClause('a', 'b'))
        ist.add_assignment_clause(AssignmentClause('c', 'd'))

        self.assertEqual(
            six.text_type(ist),
            'INSERT INTO table ("a", "c") VALUES (%(0)s, %(1)s)'
        )

    def test_context_update(self):
        ist = InsertStatement('table', None)
        ist.add_assignment_clause(AssignmentClause('a', 'b'))
        ist.add_assignment_clause(AssignmentClause('c', 'd'))

        ist.update_context_id(4)
        self.assertEqual(
            six.text_type(ist),
            'INSERT INTO table ("a", "c") VALUES (%(4)s, %(5)s)'
        )
        ctx = ist.get_context()
        self.assertEqual(ctx, {'4': 'b', '5': 'd'})

    def test_additional_rendering(self):
        ist = InsertStatement('table', ttl=60)
        ist.add_assignment_clause(AssignmentClause('a', 'b'))
        ist.add_assignment_clause(AssignmentClause('c', 'd'))
        self.assertIn('USING TTL 60', six.text_type(ist))
