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
from cassandra import ConsistencyLevel
from cassandra.cqlengine.connection import CQLENGINE_PROFILE
from cassandra.policies import RoundRobinPolicy

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import sync_table
from cassandra.cluster import Cluster, _clusters_for_shutdown, EXEC_PROFILE_DEFAULT, ExecutionProfile, _ConfigMode
from cassandra.query import dict_factory

from tests.integration import PROTOCOL_VERSION, execute_with_long_wait_retry, local, CASSANDRA_IP
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine import DEFAULT_KEYSPACE, setup_connection
from cassandra.cqlengine import models

from mock import patch


class TestConnectModel(Model):

    id = columns.Integer(primary_key=True)
    keyspace = columns.Text()


class ConnectionTest(unittest.TestCase):
    def tearDown(self):
        connection.unregister_connection("default")

    @local
    def test_connection_setup_with_setup(self):
        connection.setup(hosts=None, default_keyspace=None)
        self.assertIsNotNone(connection.get_connection("default").cluster.metadata.get_host("127.0.0.1"))

    @local
    def test_connection_setup_with_default(self):
        connection.default()
        self.assertIsNotNone(connection.get_connection("default").cluster.metadata.get_host("127.0.0.1"))

    def test_only_one_connection_is_created(self):
        """
        Test to ensure that only one new connection is created by
        connection.register_connection

        @since 3.12
        @jira_ticket PYTHON-814
        @expected_result Only one connection is created

        @test_category object_mapper
        """
        number_of_clusters_before = len(_clusters_for_shutdown)
        connection.default()
        number_of_clusters_after = len(_clusters_for_shutdown)
        self.assertEqual(number_of_clusters_after - number_of_clusters_before, 1)


class SeveralConnectionsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        connection.unregister_connection('default')
        cls.keyspace1 = 'ctest1'
        cls.keyspace2 = 'ctest2'
        super(SeveralConnectionsTest, cls).setUpClass()
        cls.setup_cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.setup_session = cls.setup_cluster.connect()
        ddl = "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{1}'}}".format(cls.keyspace1, 1)
        execute_with_long_wait_retry(cls.setup_session, ddl)
        ddl = "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{1}'}}".format(cls.keyspace2, 1)
        execute_with_long_wait_retry(cls.setup_session, ddl)

    @classmethod
    def tearDownClass(cls):
        execute_with_long_wait_retry(cls.setup_session, "DROP KEYSPACE {0}".format(cls.keyspace1))
        execute_with_long_wait_retry(cls.setup_session, "DROP KEYSPACE {0}".format(cls.keyspace2))
        models.DEFAULT_KEYSPACE = DEFAULT_KEYSPACE
        cls.setup_cluster.shutdown()
        setup_connection(DEFAULT_KEYSPACE)
        models.DEFAULT_KEYSPACE

    def setUp(self):
        self.c = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session1 = self.c.connect(keyspace=self.keyspace1)
        self.session1.row_factory = dict_factory
        self.session2 = self.c.connect(keyspace=self.keyspace2)
        self.session2.row_factory = dict_factory

    def tearDown(self):
        self.c.shutdown()

    def test_connection_session_switch(self):
        """
        Test to ensure that when the default keyspace is changed in a session and that session,
        is set in the connection class, that the new defaul keyspace is honored.

        @since 3.1
        @jira_ticket PYTHON-486
        @expected_result CQLENGINE adopts whatever keyspace is passed in vai the set_session method as default

        @test_category object_mapper
        """

        connection.set_session(self.session1)
        sync_table(TestConnectModel)
        TCM1 = TestConnectModel.create(id=1, keyspace=self.keyspace1)
        connection.set_session(self.session2)
        sync_table(TestConnectModel)
        TCM2 = TestConnectModel.create(id=1, keyspace=self.keyspace2)
        connection.set_session(self.session1)
        self.assertEqual(1, TestConnectModel.objects.count())
        self.assertEqual(TestConnectModel.objects.first(), TCM1)
        connection.set_session(self.session2)
        self.assertEqual(1, TestConnectModel.objects.count())
        self.assertEqual(TestConnectModel.objects.first(), TCM2)


class LegacyConnectionsModel(Model):
    key = columns.Integer(primary_key=True)
    some_data = columns.Text()


class LegacyConnectionsTest(unittest.TestCase):
    def test_default_connection_uses_execution_profile(self):
        connection.default()
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.PROFILES)

    def test_connection_with_exeuction_profile(self):
        connection.setup(
            hosts=[CASSANDRA_IP],
            default_keyspace=DEFAULT_KEYSPACE,
            execution_profile=ExecutionProfile()
        )
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.PROFILES)

    def test_connection_with_legacy_settings(self):
        connection.setup(
            hosts=[CASSANDRA_IP],
            default_keyspace=DEFAULT_KEYSPACE,
            consistency=ConsistencyLevel.LOCAL_ONE
        )
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.LEGACY)

    def test_connection_from_session_with_execution_profile(self):
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile()})
        session = cluster.connect()
        connection.default()
        connection.set_session(session)
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.PROFILES)

    def test_connection_from_session_with_legacy_settings(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        session.row_factory = dict_factory
        connection.set_session(session)
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.LEGACY)

    def test_uncommitted_session_uses_execution_profile(self):
        cluster = Cluster()
        session = cluster.connect()
        connection.set_session(session)
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.PROFILES)

    def test_row_factory_unaltered(self):
        cluster = Cluster()
        session = cluster.connect()
        connection.set_session(session)
        self.assertIn('dict_factory', str(session.get_execution_profile(CQLENGINE_PROFILE).row_factory))
        self.assertIn('named_tuple_factory', str(session.get_execution_profile(EXEC_PROFILE_DEFAULT).row_factory))

    def test_legacy_insert_query(self):
        connection.setup(
            hosts=[CASSANDRA_IP],
            default_keyspace=DEFAULT_KEYSPACE,
            consistency=ConsistencyLevel.LOCAL_ONE
        )
        sync_table(LegacyConnectionsModel)
        LegacyConnectionsModel.objects.create(key=0, some_data='text0')
        LegacyConnectionsModel.objects.create(key=1, some_data='text1')
        self.assertEqual(LegacyConnectionsModel.objects(key=0)[0].some_data, 'text0')
