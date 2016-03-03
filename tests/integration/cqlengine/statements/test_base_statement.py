# Copyright 2013-2016 DataStax, Inc.
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

from cassandra.query import FETCH_SIZE_UNSET
from cassandra.cqlengine.statements import BaseCQLStatement, StatementException


class BaseStatementTest(unittest.TestCase):

    def test_where_clause_type_checking(self):
        """ tests that only assignment clauses can be added to queries """
        stmt = BaseCQLStatement('table', [])
        with self.assertRaises(StatementException):
            stmt.add_where_clause('x=5')

    def test_fetch_size(self):
        """ tests that fetch_size is correctly set """
        stmt = BaseCQLStatement('table', None, fetch_size=1000)
        self.assertEqual(stmt.fetch_size, 1000)

        stmt = BaseCQLStatement('table', None, fetch_size=None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)

        stmt = BaseCQLStatement('table', None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)
