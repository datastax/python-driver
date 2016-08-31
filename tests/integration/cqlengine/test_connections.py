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

from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from cassandra.cqlengine import columns, CQLEngineException
from cassandra.cqlengine import connection as conn
from cassandra.cqlengine.management import drop_keyspace, sync_table, drop_table, create_keyspace_simple
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import ContextQuery, BatchQuery
from tests.integration.cqlengine import setup_connection, DEFAULT_KEYSPACE
from tests.integration.cqlengine.base import BaseCassEngTestCase


class TestModel(Model):

    __keyspace__ = 'ks1'

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer()
    text = columns.Text()


class AnotherTestModel(Model):

    __keyspace__ = 'ks1'

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer()
    text = columns.Text()


class ContextQueryConnectionTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(ContextQueryConnectionTests, cls).setUpClass()
        create_keyspace_simple('ks1', 1)

        conn.unregister_connection('default')
        conn.register_connection('fake_cluster', ['127.0.0.100'], lazy_connect=True, retry_connect=True, default=True)
        conn.register_connection('cluster', ['127.0.0.1'])

        with ContextQuery(TestModel, connection='cluster') as tm:
            sync_table(tm)

    @classmethod
    def tearDownClass(cls):
        super(ContextQueryConnectionTests, cls).tearDownClass()

        with ContextQuery(TestModel, connection='cluster') as tm:
            drop_table(tm)
        drop_keyspace('ks1', connections=['cluster'])


        # reset the default connection
        conn.unregister_connection('fake_cluster')
        conn.unregister_connection('cluster')
        setup_connection(DEFAULT_KEYSPACE)


    def setUp(self):
        super(BaseCassEngTestCase, self).setUp()

    def test_context_connection_priority(self):

        # Set the default connection on the Model
        TestModel.__connection__ = 'cluster'
        with ContextQuery(TestModel) as tm:
            tm.objects.create(partition=1, cluster=1)

        # ContextQuery connection should have priority over default one
        with ContextQuery(TestModel, connection='fake_cluster') as tm:
            with self.assertRaises(NoHostAvailable):
                tm.objects.create(partition=1, cluster=1)

        # Explicit connection should have priority over ContextQuery one
        with ContextQuery(TestModel, connection='fake_cluster') as tm:
            tm.objects.using(connection='cluster').create(partition=1, cluster=1)

        # Reset the default conn of the model
        TestModel.__connection__ = None

        # No model connection and an invalid default connection
        with ContextQuery(TestModel) as tm:
            with self.assertRaises(NoHostAvailable):
                tm.objects.create(partition=1, cluster=1)

    def test_context_connection_with_keyspace(self):

        # ks2 doesn't exist
        with ContextQuery(TestModel, connection='cluster', keyspace='ks2') as tm:
            with self.assertRaises(InvalidRequest):
                tm.objects.create(partition=1, cluster=1)


class ManagementConnectionTests(BaseCassEngTestCase):

    keyspaces = ['ks1', 'ks2']
    conns = ['cluster']

    @classmethod
    def setUpClass(cls):
        super(ManagementConnectionTests, cls).setUpClass()
        conn.unregister_connection('default')
        conn.register_connection('fake_cluster', ['127.0.0.100'], lazy_connect=True, retry_connect=True, default=True)
        conn.register_connection('cluster', ['127.0.0.1'])


    @classmethod
    def tearDownClass(cls):
        super(ManagementConnectionTests, cls).tearDownClass()

        # reset the default connection
        conn.unregister_connection('fake_cluster')
        conn.unregister_connection('cluster')
        setup_connection(DEFAULT_KEYSPACE)

    def setUp(self):
        super(BaseCassEngTestCase, self).setUp()

    def test_create_drop_keyspace(self):

        # No connection (default is fake)
        with self.assertRaises(NoHostAvailable):
            create_keyspace_simple(self.keyspaces[0], 1)

        # Explicit connections
        for ks in self.keyspaces:
            create_keyspace_simple(ks, 1, connections=self.conns)

        for ks in self.keyspaces:
            drop_keyspace(ks, connections=self.conns)

    def test_create_drop_table(self):

        for ks in self.keyspaces:
            create_keyspace_simple(ks, 1, connections=self.conns)

        # No connection (default is fake)
        with self.assertRaises(NoHostAvailable):
            sync_table(TestModel)

        # Explicit connections
        sync_table(TestModel, connections=self.conns)

        # Explicit drop
        drop_table(TestModel, connections=self.conns)

        # Model connection
        TestModel.__connection__ = 'cluster'
        sync_table(TestModel)
        TestModel.__connection__ = None

        # No connection (default is fake)
        with self.assertRaises(NoHostAvailable):
            drop_table(TestModel)

        # Model connection
        TestModel.__connection__ = 'cluster'
        drop_table(TestModel)
        TestModel.__connection__ = None

        # Model connection
        for ks in self.keyspaces:
            drop_keyspace(ks, connections=self.conns)


class BatchQueryConnectionTests(BaseCassEngTestCase):

    conns = ['cluster']

    @classmethod
    def setUpClass(cls):
        super(BatchQueryConnectionTests, cls).setUpClass()

        create_keyspace_simple('ks1', 1)
        sync_table(TestModel)
        sync_table(AnotherTestModel)

        conn.unregister_connection('default')
        conn.register_connection('fake_cluster', ['127.0.0.100'], lazy_connect=True, retry_connect=True, default=True)
        conn.register_connection('cluster', ['127.0.0.1'])


    @classmethod
    def tearDownClass(cls):
        super(BatchQueryConnectionTests, cls).tearDownClass()

        # reset the default connection
        conn.unregister_connection('fake_cluster')
        conn.unregister_connection('cluster')
        setup_connection(DEFAULT_KEYSPACE)

        drop_keyspace('ks1')

    def setUp(self):
        super(BaseCassEngTestCase, self).setUp()

    def test_basic_batch_query(self):
        """Test BatchQuery requests"""

        # No connection with a QuerySet (default is a fake one)
        with self.assertRaises(NoHostAvailable):
            with BatchQuery() as b:
                TestModel.objects.batch(b).create(partition=1, cluster=1)

        # Explicit connection with a QuerySet
        with BatchQuery(connection='cluster') as b:
            TestModel.objects.batch(b).create(partition=1, cluster=1)

        # Get an object from the BD
        with ContextQuery(TestModel, connection='cluster') as tm:
            obj = tm.objects.get(partition=1, cluster=1)
            obj.__connection__ = None

        # No connection with a model (default is a fake one)
        with self.assertRaises(NoHostAvailable):
            with BatchQuery() as b:
                obj.count = 2
                obj.batch(b).save()

        # Explicit connection with a model
        with BatchQuery(connection='cluster') as b:
            obj.count = 2
            obj.batch(b).save()

    def test_batch_query_different_connection(self):
        """Test BatchQuery with Models that have a different connection"""

        # Testing on a model class
        TestModel.__connection__ = 'cluster'
        AnotherTestModel.__connection__ = 'cluster2'

        with self.assertRaises(CQLEngineException):
            with BatchQuery() as b:
                TestModel.objects.batch(b).create(partition=1, cluster=1)
                AnotherTestModel.objects.batch(b).create(partition=1, cluster=1)

        TestModel.__connection__ = None
        AnotherTestModel.__connection__ = None

        with BatchQuery(connection='cluster') as b:
            TestModel.objects.batch(b).create(partition=1, cluster=1)
            AnotherTestModel.objects.batch(b).create(partition=1, cluster=1)

        # Testing on a model instance
        with ContextQuery(TestModel, AnotherTestModel, connection='cluster') as (tm, atm):
            obj1 = tm.objects.get(partition=1, cluster=1)
            obj2 = atm.objects.get(partition=1, cluster=1)

            obj1.__connection__ = 'cluster'
            obj2.__connection__ = 'cluster2'

            obj1.count = 4
            obj2.count = 4

        with self.assertRaises(CQLEngineException):
            with BatchQuery() as b:
                obj1.batch(b).save()
                obj2.batch(b).save()

    def test_batch_query_connection_override(self):
        """Test that we cannot override a BatchQuery connection per model"""

        with self.assertRaises(CQLEngineException):
            with BatchQuery(connection='cluster') as b:
                TestModel.batch(b).using(connection='test').save()

        with self.assertRaises(CQLEngineException):
            with BatchQuery(connection='cluster') as b:
                TestModel.using(connection='test').batch(b).save()

        with ContextQuery(TestModel, AnotherTestModel, connection='cluster') as (tm, atm):
            obj1 = tm.objects.get(partition=1, cluster=1)
            obj1.__connection__ = None

        with self.assertRaises(CQLEngineException):
            with BatchQuery(connection='cluster') as b:
                obj1.using(connection='test').batch(b).save()

        with self.assertRaises(CQLEngineException):
            with BatchQuery(connection='cluster') as b:
                obj1.batch(b).using(connection='test').save()


class UsingDescriptorTests(BaseCassEngTestCase):

    conns = ['cluster']
    keyspaces = ['ks1', 'ks2']

    @classmethod
    def setUpClass(cls):
        super(UsingDescriptorTests, cls).setUpClass()

        conn.unregister_connection('default')
        conn.register_connection('fake_cluster', ['127.0.0.100'], lazy_connect=True, retry_connect=True, default=True)
        conn.register_connection('cluster', ['127.0.0.1'])


    @classmethod
    def tearDownClass(cls):
        super(UsingDescriptorTests, cls).tearDownClass()

        # reset the default connection
        conn.unregister_connection('fake_cluster')
        conn.unregister_connection('cluster')
        setup_connection(DEFAULT_KEYSPACE)

        for ks in cls.keyspaces:
            drop_keyspace(ks)

    def setUp(self):
        super(BaseCassEngTestCase, self).setUp()

    def _reset_data(self):

        for ks in self.keyspaces:
            drop_keyspace(ks, connections=self.conns)

        for ks in self.keyspaces:
            create_keyspace_simple(ks, 1, connections=self.conns)
        sync_table(TestModel, keyspaces=self.keyspaces, connections=self.conns)

    def test_keyspace(self):

        self._reset_data()

        with ContextQuery(TestModel, connection='cluster') as tm:

            # keyspace Model class
            tm.objects.using(keyspace='ks2').create(partition=1, cluster=1)
            tm.objects.using(keyspace='ks2').create(partition=2, cluster=2)

            with self.assertRaises(TestModel.DoesNotExist):
                tm.objects.get(partition=1, cluster=1)  # default keyspace ks1
            obj1 = tm.objects.using(keyspace='ks2').get(partition=1, cluster=1)

            obj1.count = 2
            obj1.save()

        with self.assertRaises(NoHostAvailable):
            TestModel.objects.using(keyspace='ks2').get(partition=1, cluster=1)

        obj2 = TestModel.objects.using(connection='cluster', keyspace='ks2').get(partition=1, cluster=1)
        self.assertEqual(obj2.count, 2)

        # Update test
        TestModel.objects(partition=2, cluster=2).using(connection='cluster', keyspace='ks2').update(count=5)
        obj3 = TestModel.objects.using(connection='cluster', keyspace='ks2').get(partition=2, cluster=2)
        self.assertEqual(obj3.count, 5)

        TestModel.objects(partition=2, cluster=2).using(connection='cluster', keyspace='ks2').delete()
        with self.assertRaises(TestModel.DoesNotExist):
            TestModel.objects.using(connection='cluster', keyspace='ks2').get(partition=2, cluster=2)

    def test_connection(self):

        self._reset_data()

        # Model class
        with self.assertRaises(NoHostAvailable):
            TestModel.objects.create(partition=1, cluster=1)

        TestModel.objects.using(connection='cluster').create(partition=1, cluster=1)
        TestModel.objects(partition=1, cluster=1).using(connection='cluster').update(count=2)
        obj1 = TestModel.objects.using(connection='cluster').get(partition=1, cluster=1)
        self.assertEqual(obj1.count, 2)

        obj1.using(connection='cluster').update(count=5)
        obj1 = TestModel.objects.using(connection='cluster').get(partition=1, cluster=1)
        self.assertEqual(obj1.count, 5)

        obj1.using(connection='cluster').delete()
        with self.assertRaises(TestModel.DoesNotExist):
            TestModel.objects.using(connection='cluster').get(partition=1, cluster=1)
