# Copyright 2013-2014 DataStax, Inc.
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

import logging
import time
import socket

from ccmlib import common
from ccmlib.cluster import Cluster as CCMCluster

from tests.integration import setup_package, teardown_package, PROTOCOL_VERSION, CASSANDRA_DIR, CASSANDRA_VERSION, path
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


try:
    import unittest2 as unittest
except ImportError:
    import unittest

log = logging.getLogger(__name__)

AUTH_CLUSTER_NAME = 'auth_test_cluster'


def wait_for_cassandra(port=7000, maxwait=120):
    """
    Wait until there is connection to specified port. Use cassandra port 7000 by default.
    :param port: port to try
    :param maxwait: maximum wait time in seconds
    :return: True if can connect within 2 minutes, False otherwise
    """
    sleeptime = 2
    server_address = ('localhost', port)

    wait_time = 0
    while wait_time < maxwait:
        try:
            s = socket.create_connection(server_address)
            s.close()
            log.debug("Cassandra ready after %d seconds" % wait_time)
            return True
        except socket.error:
            wait_time += sleeptime
            time.sleep(sleeptime)

    return False


def try_connecting(username='', password=''):
    """
    Wait until can connect to cluster.
    When cluster starts up there is some time while it is not possible to connect to it even though
    Cassandra is listening on port 7000. Here we wait until we can actually issue successful Cluster.connect()
     method.
    :param username: optional user name for connection
    :param password: optional password for connection
    :return: True if can successfully connect to cluster within 2 minutes, False otherwise
    """
    if username and password:
        ap = PlainTextAuthProvider(username=username, password=password)
    else:
        ap = None
    maxwait = 120  # in seconds
    sleeptime = 1

    wait_time = 0
    while wait_time < maxwait:
        try:
            cluster = Cluster(protocol_version=PROTOCOL_VERSION, auth_provider=ap)
            cluster.connect()
            log.debug("Can connect after %d seconds" % wait_time)
            return True
        except Exception:
            wait_time += sleeptime
            time.sleep(sleeptime)

    return False


######################################################
# Module level setup and teardown
#
def setup_module():
    """
    Stop existing cluster started by generic tests init
    """
    teardown_package()


def teardown_module():
    """
    Start generic tests cluster
    """
    setup_package()
    wait_for_cassandra()
    try_connecting()

#
# End of Module level setup and teardown
####################################################


class AuthenticationTests(unittest.TestCase):
    """
    Tests to cover basic authorization functionality
    """

    ccm_cluster = None

    password = '12345'
    suser = 'new_suser'
    test_user = 'test_user'
    test_other_user = 'other_user'

    ##########################
    # Class level setup and teardown
    ##
    @classmethod
    def setup_class(cls):
        """
        Class-level fixture. Called once for all tests.
        We create a cluster object here and save it in the provided class instance
        Create 2 regular users and a new superuser.
        Enable authentication by setting 'authenticator': 'PasswordAuthenticator'.
        If invoked from authorisation class enable authorization by setting 'authorizer': 'CassandraAuthorizer'.
        """
        try:
            try:
                ccm_cluster = CCMCluster.load(path, AUTH_CLUSTER_NAME)
                log.debug("Found existing ccm test authentication cluster, removing")
                ccm_cluster.remove()
            except Exception:
                log.debug("Can not load cluster %s ....." % AUTH_CLUSTER_NAME)

            log.debug("Creating new ccm test authentication cluster")
            if CASSANDRA_DIR:
                ccm_cluster = CCMCluster(path, AUTH_CLUSTER_NAME, cassandra_dir=CASSANDRA_DIR)
            else:
                ccm_cluster = CCMCluster(path, AUTH_CLUSTER_NAME, cassandra_version=CASSANDRA_VERSION)

            ccm_cluster.set_configuration_options({'start_native_transport': True})
            ccm_cluster.set_configuration_options({'authenticator': 'PasswordAuthenticator'})
            ccm_cluster.set_configuration_options({'authorizer': 'CassandraAuthorizer'})

            common.switch_cluster(path, AUTH_CLUSTER_NAME)
            ccm_cluster.populate(1)

            log.debug("Starting ccm test authentication cluster")
            ccm_cluster.start(wait_for_binary_proto=True)
        except Exception:
            log.exception("Failed to start ccm authentication cluster:")
            raise

        if not wait_for_cassandra() or not try_connecting('cassandra', 'cassandra'):
            log.exception("Can not talk to cassandra")
            raise

        log.debug("Switched to AUTH_CLUSTER_NAME cluster")
        cls.ccm_cluster = ccm_cluster

        cls.root_cluster = cls.cluster_as('cassandra', 'cassandra')
        cls.root_session = cls.root_cluster.connect()
        cls.create_user(cls.root_cluster, cls.test_user, cls.password)
        cls.create_user(cls.root_cluster, cls.test_other_user, cls.password)
        cls.create_user(cls.root_cluster, cls.suser, cls.password, su=True)

    @classmethod
    def tearDownClass(cls):
        """
        Remove authentication cluster
        """
        cls.ccm_cluster.remove()

    ##
    # End of class-level setup and teardown
    ########################################################

    def setUp(self):
        """
        Class method fixture setup. Called before each test.
        Create clean keyspace and table
        """
        self.session = self.root_cluster.connect()
        ddl = '''

            CREATE KEYSPACE temp
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}'''
        self.session.execute(ddl)

        ddl = '''
            CREATE TABLE temp.test (
                k int PRIMARY KEY,
                v int )'''
        self.session.execute(ddl)

    def tearDown(self):
        """
        Class method fixture setup. Called after each test
        Reverse settings for users to test default values
        DROP test keyspace
        """
        self.session = self.root_cluster.connect()
        self.session.execute("ALTER USER %s WITH PASSWORD '%s' SUPERUSER " % (self.suser, self.password))
        self.session.execute("ALTER USER %s WITH PASSWORD '%s' NOSUPERUSER " % (self.test_other_user, self.password))
        self.session.execute("ALTER USER %s WITH PASSWORD '%s' NOSUPERUSER " % (self.test_user, self.password))
        self.users = self.list_users(self.root_cluster)
        # Revoke permissions
        for u in self.users:
            self.session.execute("REVOKE ALL PERMISSIONS ON ALL KEYSPACES FROM %s " % (u))
        # Drop keyspace
        self.session.execute("DROP KEYSPACE %s" % 'temp')

        self.session.shutdown()

    @classmethod
    def enable_authorizer(cls):
        cls.authorizer = True

    @staticmethod
    def create_user(cluster, usr, pwd, su=False):
        """
        Create user with given username and password and optional status
        """
        status = ''
        if su:
            status = 'SUPERUSER'
        session = cluster.connect()
        session.execute("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s' %s " % (usr, pwd, status))
        session.shutdown()

    @staticmethod
    def list_users(cluster):
        """
        List existing cluster users
        :param cluster: cluster object
        :return: list of user names
        """
        return AuthenticationTests.get_user_data(cluster).keys()

    @staticmethod
    def get_user_data(cluster):
        """
        Get information about users as a dictionary with user name and superuser status
        :param cluster: cluster to use
        :return: dictionary with user name as key and su status as value
        """
        session = cluster.connect()
        result = session.execute("LIST USERS")
        users = dict()
        for row in result:
            users[row.name] = row.super
        session.shutdown()
        return users

    @staticmethod
    def get_user_permissions(cluster):
        """
        Get information about users permissions as a dictionary with user name and superuser status
        :param cluster: cluster to use
        :param username: username whose permissions we want
        :return: dictionary with keyspace as key and and user permissions for this keyspace as value
        """
        session = cluster.connect()
        result = session.execute("LIST USERS")
        users = dict()
        for row in result:
            users[row.name] = row.super
        session.shutdown()
        return users

    @staticmethod
    def cluster_as(usr, pwd):
        """
        Create CLuster object authenticated with specified user and password
        :rtype : Cluster
        :param usr: username
        :param pwd: user password
        :return: CLuster object
        """
        return Cluster(protocol_version=PROTOCOL_VERSION,
                       auth_provider=PlainTextAuthProvider(username=usr,
                                                           password=pwd))

#################################################

    def test_su_list_all_permissions(self):
        """
        su_list_all_permissions: superuser can  LIST ALL PERMISSIONS
        """
        self.session = self.root_cluster.connect()
        permissions = self.session.execute("LIST ALL PERMISSIONS NORECURSIVE")
        self.assertIsNone(permissions)

    def test_su_grant_all_permissions(self):
        """
        su_grant: superuser can grant user permissions and user can exercise them
        GRANT ALL PERMISSIONS ON ALL KEYSPACES TO regular user

        """
        session = self.root_cluster.connect()
        session.execute(" GRANT ALL PERMISSIONS ON ALL KEYSPACES TO %s " % (self.test_user))
        users = self.get_user_permissions(self.root_cluster)
        self.assertEqual(users[self.test_user], True, msg="%s has SUPERUSER set to %s"
                                                          % (self.test_user, users[self.test_user]))
