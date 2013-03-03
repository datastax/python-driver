from datetime import datetime
import time

from cqlengine.tests.base import BaseCassEngTestCase
from cqlengine import columns, Model
from cqlengine import functions
from cqlengine import query

class TestQuerySetOperation(BaseCassEngTestCase):

    def test_maxtimeuuid_function(self):
        """
        Tests that queries with helper functions are generated properly
        """
        now = datetime.now()
        col = columns.DateTime()
        col.set_column_name('time')
        qry = query.EqualsOperator(col, functions.MaxTimeUUID(now))

        assert qry.cql == '"time" = MaxTimeUUID(:{})'.format(qry.identifier)

    def test_mintimeuuid_function(self):
        """
        Tests that queries with helper functions are generated properly
        """
        now = datetime.now()
        col = columns.DateTime()
        col.set_column_name('time')
        qry = query.EqualsOperator(col, functions.MinTimeUUID(now))

        assert qry.cql == '"time" = MinTimeUUID(:{})'.format(qry.identifier)



