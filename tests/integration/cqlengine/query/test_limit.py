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
    import unittest

from cassandra.cluster import Cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra import util
from cassandra.cqlengine import connection
import cassandra
import datetime

from cassandra.cqlengine.management import create_keyspace_simple
from tests.integration import PROTOCOL_VERSION

## Create DB object
class test_table(Model):

    __keyspace__ = "test_keyspace"
    user = columns.Integer(primary_key=True)
    prop = columns.Text()

# Test
class LimitTest(unittest.TestCase):

    def setUp(self):
        self.KEYSPACE = 'test_keyspace'
        self.TABLE = 'test_table'
        self.COLUMNS_NUM = 10100
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=['127.0.0.1', '127.0.0.2', '127.0.0.3'])
        self.session = self.cluster.connect()
        self.c = connection.setup(['127.0.0.1'], 'cqlengine', protocol_version=3)

    def create_database(self):
        print("----- CREATE KEYSPACE & TABLE -----")

        create_keyspace_simple(self.KEYSPACE, replication_factor=3)
        self.session.set_keyspace(self.KEYSPACE)

        try:
            self.session.execute("CREATE TABLE " + self.TABLE + " (user int, prop text, PRIMARY KEY (user))")
        except cassandra.AlreadyExists:
            print('TABLE ' + self.TABLE + " is already exists")

    def insert_data(self, num=1):
        print("----- INSERT COLUMNS -----")

        INSERT_USER = 0
        INSERT_PROP = 0

        while (INSERT_USER < num):
            self.table.create(user=str(INSERT_USER), prop=str(INSERT_PROP))
            INSERT_USER += 1
            INSERT_PROP += 2


    def tearDown(self):
        self.cluster.shutdown()

    def test_check_number_columns(self):

        self.create_database()

        self.session.set_keyspace(self.KEYSPACE)

        self.table = test_table()
        self.insert_data(self.COLUMNS_NUM)

        # No limit()
        Limit = len(self.table.all())

        # None
        All = len(self.table.all().limit(None))

        self.assertEqual(Limit, All)




if __name__ == '__main__':

    unittest.main(exit=False)
