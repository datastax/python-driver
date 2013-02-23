from datetime import datetime, timedelta
from uuid import uuid4

from cqlengine.tests.base import BaseCassEngTestCase

from cqlengine.exceptions import ModelException
from cqlengine.management import create_table
from cqlengine.management import delete_table
from cqlengine.models import Model
from cqlengine import columns
from cqlengine import query

class DateTimeQueryTestModel(Model):
    user        = columns.Integer(primary_key=True)
    day         = columns.DateTime(primary_key=True)
    data        = columns.Text()

class TestDateTimeQueries(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestDateTimeQueries, cls).setUpClass()
        create_table(DateTimeQueryTestModel)

        cls.base_date = datetime.now() - timedelta(days=10)
        for x in range(7):
            for y in range(10):
                DateTimeQueryTestModel.create(
                    user=x,
                    day=(cls.base_date+timedelta(days=y)),
                    data=str(uuid4())
                )


    @classmethod
    def tearDownClass(cls):
        super(TestDateTimeQueries, cls).tearDownClass()
        delete_table(DateTimeQueryTestModel)

    def test_range_query(self):
        """ Tests that loading from a range of dates works properly """
        start = datetime(*self.base_date.timetuple()[:3])
        end = start + timedelta(days=3)

        results = DateTimeQueryTestModel.filter(user=0, day__gte=start, day__lt=end)
        assert  len(results) == 3

