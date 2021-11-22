# Copyright DataStax, Inc.
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

from cassandra.cqlengine.operators import *

from uuid import uuid4

from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.operators import IsNotNullOperator
from cassandra.cqlengine.statements import IsNotNull
from cassandra import InvalidRequest

from tests.integration.cqlengine.base import TestQueryUpdateModel, BaseCassEngTestCase
from tests.integration.cqlengine.operators import check_lookup
from tests.integration import greaterthanorequalcass30

import six


class TestWhereOperators(unittest.TestCase):

    def test_symbol_lookup(self):
        """ tests where symbols are looked up properly """

        check_lookup(self, 'EQ', EqualsOperator)
        check_lookup(self, 'NE', NotEqualsOperator)
        check_lookup(self, 'IN', InOperator)
        check_lookup(self, 'GT', GreaterThanOperator)
        check_lookup(self, 'GTE', GreaterThanOrEqualOperator)
        check_lookup(self, 'LT', LessThanOperator)
        check_lookup(self, 'LTE', LessThanOrEqualOperator)
        check_lookup(self, 'CONTAINS', ContainsOperator)
        check_lookup(self, 'LIKE', LikeOperator)

    def test_operator_rendering(self):
        """ tests symbols are rendered properly """
        self.assertEqual("=", six.text_type(EqualsOperator()))
        self.assertEqual("!=", six.text_type(NotEqualsOperator()))
        self.assertEqual("IN", six.text_type(InOperator()))
        self.assertEqual(">", six.text_type(GreaterThanOperator()))
        self.assertEqual(">=", six.text_type(GreaterThanOrEqualOperator()))
        self.assertEqual("<", six.text_type(LessThanOperator()))
        self.assertEqual("<=", six.text_type(LessThanOrEqualOperator()))
        self.assertEqual("CONTAINS", six.text_type(ContainsOperator()))
        self.assertEqual("LIKE", six.text_type(LikeOperator()))


class TestIsNotNull(BaseCassEngTestCase):
    def test_is_not_null_to_cql(self):
        """
        Verify that IsNotNull is converted correctly to CQL

        @since 2.5
        @jira_ticket PYTHON-968
        @expected_result the strings match

        @test_category cqlengine
        """

        check_lookup(self, 'IS NOT NULL', IsNotNullOperator)

        # The * is not expanded because there are no referred fields
        self.assertEqual(
            str(TestQueryUpdateModel.filter(IsNotNull("text")).limit(2)),
            'SELECT * FROM cqlengine_test.test_query_update_model WHERE "text" IS NOT NULL LIMIT 2'
        )

        # We already know partition so cqlengine doesn't query for it
        self.assertEqual(
            str(TestQueryUpdateModel.filter(IsNotNull("text"), partition=uuid4())),
            ('SELECT "cluster", "count", "text", "text_set", '
             '"text_list", "text_map" FROM cqlengine_test.test_query_update_model '
             'WHERE "text" IS NOT NULL AND "partition" = %(0)s LIMIT 10000')
        )

    @greaterthanorequalcass30
    def test_is_not_null_execution(self):
        """
        Verify that CQL statements have correct syntax when executed
        If we wanted them to return something meaningful and not a InvalidRequest
        we'd have to create an index in search for the column we are using
        IsNotNull

        @since 2.5
        @jira_ticket PYTHON-968
        @expected_result InvalidRequest is arisen

        @test_category cqlengine
        """
        sync_table(TestQueryUpdateModel)
        self.addCleanup(drop_table, TestQueryUpdateModel)

        # Raises InvalidRequest instead of dse.protocol.SyntaxException
        with self.assertRaises(InvalidRequest):
            list(TestQueryUpdateModel.filter(IsNotNull("text")))

        with self.assertRaises(InvalidRequest):
            list(TestQueryUpdateModel.filter(IsNotNull("text"), partition=uuid4()))
