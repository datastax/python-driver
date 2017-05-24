# Copyright 2013-2017 DataStax, Inc.
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

from uuid import uuid4

from cassandra.query import FETCH_SIZE_UNSET
from cassandra.cqlengine.statements import BaseCQLStatement
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.statements import InsertStatement, UpdateStatement, SelectStatement, DeleteStatement, \
    WhereClause
from cassandra.cqlengine.operators import EqualsOperator
from cassandra.cqlengine.columns import Column

from tests.integration.cqlengine.base import BaseCassEngTestCase, TestQueryUpdateModel
from tests.integration.cqlengine import DEFAULT_KEYSPACE
from cassandra.cqlengine.connection import execute


class BaseStatementTest(unittest.TestCase):

    def test_fetch_size(self):
        """ tests that fetch_size is correctly set """
        stmt = BaseCQLStatement('table', None, fetch_size=1000)
        self.assertEqual(stmt.fetch_size, 1000)

        stmt = BaseCQLStatement('table', None, fetch_size=None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)

        stmt = BaseCQLStatement('table', None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)


class ExecuteStatementTest(BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        super(ExecuteStatementTest, cls).setUpClass()
        sync_table(TestQueryUpdateModel)
        cls.table_name = '{0}.test_query_update_model'.format(DEFAULT_KEYSPACE)

    @classmethod
    def tearDownClass(cls):
        super(ExecuteStatementTest, cls).tearDownClass()
        drop_table(TestQueryUpdateModel)

    def _verify_statement(self, original):
        st = SelectStatement(self.table_name)
        result = execute(st)
        response = result[0]

        for assignment in original.assignments:
            self.assertEqual(response[assignment.field], assignment.value)
        self.assertEqual(len(response), 7)

    def test_insert_statement_execute(self):
        """
        Test to verify the execution of BaseCQLStatements using connection.execute

        @since 3.10
        @jira_ticket PYTHON-505
        @expected_result inserts a row in C*, updates the rows and then deletes
        all the rows using BaseCQLStatements

        @test_category data_types:object_mapper
        """
        partition = uuid4()
        cluster = 1

        #Verifying insert statement
        st = InsertStatement(self.table_name)
        st.add_assignment(Column(db_field='partition'), partition)
        st.add_assignment(Column(db_field='cluster'), cluster)

        st.add_assignment(Column(db_field='count'), 1)
        st.add_assignment(Column(db_field='text'), "text_for_db")
        st.add_assignment(Column(db_field='text_set'), set(("foo", "bar")))
        st.add_assignment(Column(db_field='text_list'), ["foo", "bar"])
        st.add_assignment(Column(db_field='text_map'), {"foo": '1', "bar": '2'})

        execute(st)
        self._verify_statement(st)

        # Verifying update statement
        where = [WhereClause('partition', EqualsOperator(), partition),
                 WhereClause('cluster', EqualsOperator(), cluster)]

        st = UpdateStatement(self.table_name, where=where)
        st.add_assignment(Column(db_field='count'), 2)
        st.add_assignment(Column(db_field='text'), "text_for_db_update")
        st.add_assignment(Column(db_field='text_set'), set(("foo_update", "bar_update")))
        st.add_assignment(Column(db_field='text_list'), ["foo_update", "bar_update"])
        st.add_assignment(Column(db_field='text_map'), {"foo": '3', "bar": '4'})

        execute(st)
        self._verify_statement(st)

        # Verifying delete statement
        execute(DeleteStatement(self.table_name, where=where))
        self.assertEqual(TestQueryUpdateModel.objects.count(), 0)
