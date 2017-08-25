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

KEYSPACE = 'test_keyspace'
TABLE = "test_table"
COLUMNS_NUM = 10100

cluster = Cluster(['127.0.0.1', '127.0.0.2', '127.0.0.3'])
session = cluster.connect()

## Connection Setting
c = connection.setup(['127.0.0.1'], 'cqlengine', protocol_version=3)

## Create DB object
class test_table(Model):
    __keyspace__=KEYSPACE

    user = columns.Integer(primary_key=True)
    prop = columns.Text()


def create_database():
    print("----- CREATE KEYSPACE & TABLE -----")

    create_keyspace_simple(KEYSPACE, replication_factor=3)
    session.set_keyspace(KEYSPACE)

    try:
        session.execute("CREATE TABLE " + TABLE + " (user int, prop text, PRIMARY KEY (user))")
    except cassandra.AlreadyExists:
        print('TABLE ' + TABLE + " is already exists")


def insert_data(num=1):
    print("----- INSERT COLUMNS -----")

    INSERT_USER = 0
    INSERT_PROP = 0

    while (INSERT_USER < num):
        test_table.create(user=str(INSERT_USER), prop=str(INSERT_USER))
        INSERT_USER += 1
        INSERT_PROP += 1

# Test
class LimitTest(unittest.TestCase):

    def test_check_number_columns(self):
        # No limit()
        Limit = len(test_table.all())
        # None
        All = len(test_table.all().limit(None))

        self.assertEqual(Limit, All)

if __name__ == '__main__':

    create_database()
    insert_data(COLUMNS_NUM)

    session.set_keyspace(KEYSPACE)

    unittest.main(exit=False)
