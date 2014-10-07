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
from cassandra.cluster import Cluster, NoHostAvailable, Unauthorized, InvalidRequest
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
    Test is skipped if run with cql version < 2
    Stop existing cluster started by generic tests init
    """

    if PROTOCOL_VERSION < 2:
        raise unittest.SkipTest(
            "SASL Authentication is not supported in version 1 of the protocol")

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
    Tests to cover basic authentication functionality
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
        If invoked from authorization class enable authorization by setting 'authorizer': 'CassandraAuthorizer'.
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

            #
            # This method is called either with AuthenticationTests class or with AuthorizedAuthenticationTests class.
            # In the second case we enable CassandraAuthorizer
            #
            if cls.__name__ == 'AuthorizedAuthenticationTests':
                print "Running tests with Cassandra Authorizer Enabled"
                log.info("Running tests with Cassandra Authorizer Enabled")
                ccm_cluster.set_configuration_options({'authorizer': 'CassandraAuthorizer'})
            else:
                print "Running tests with Cassandra Authorizer Disabled"
                log.info("Running tests with Cassandra Authorizer Disabled")

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

    def tearDown(self):
        """
        Class method fixture setup. Called after each test
        Reverse settings for users to test default values
        """
        self.session = self.root_cluster.connect()
        self.session.execute("ALTER USER %s WITH PASSWORD '%s' SUPERUSER " % (self.suser, self.password))
        self.session.execute("ALTER USER %s WITH PASSWORD '%s' NOSUPERUSER " % (self.test_other_user, self.password))
        self.session.execute("ALTER USER %s WITH PASSWORD '%s' NOSUPERUSER " % (self.test_user, self.password))

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

    def test_new_user_created(self):
        """
        new_user_created: superuser can create users
        """
        self.assertIn(self.test_user, self.list_users(self.root_cluster), 'user %s is missing' % self.test_user)
        self.assertIn(self.suser, self.list_users(self.root_cluster), 'user %s is missing' % self.suser)

    def test_user_status(self):
        """
        user_status: regular user and superuser have right status in LIST USERS output
        """
        users = self.get_user_data(self.root_cluster)
        self.assertIn(self.test_user, users.keys())
        self.assertEqual(users[self.test_user], False)

    def test_user_can_connect(self):
        """
        user_can_connect: regular user can connect to C* and LIST USERS
        """
        cluster = self.cluster_as(self.test_user, self.password)
        # Verify that regular user can LIST USERS
        self.assertIn(self.test_user, self.list_users(cluster), 'user %s is missing' % self.test_user)

    def test_new_superuser_can_connect(self):
        """
        new_superuser_can_connect: new superuser can connect to C* and LIST USERS
        """
        # First verify that suser is created
        self.assertIn(self.suser, self.list_users(self.root_cluster), 'user %s is missing' % self.suser)
        # Now verify that we can connect as suser
        cluster = self.cluster_as(self.suser, self.password)
        self.assertIn(self.suser, self.list_users(cluster), 'user %s is missing' % self.suser)

    def test_user_cannot_create_user(self):
        """
        user_cannot_create_user: regular user can not CREATE USER
        """
        cluster = self.cluster_as(self.test_user, self.password)
        self.assertRaisesRegexp(Unauthorized,
                                '.*Only superusers are allowed to perform CREATE USER queries.*',
                                lambda: self.create_user(cluster, 'test_user_1', '12345'))

    def test_user_cannot_change_su_pwd(self):
        """
        user_cannot_change_su_pwd: regular user cannot change password for superuser
        """
        cluster = self.cluster_as(self.test_user, self.password)
        session = cluster.connect()
        stmt = "ALTER USER cassandra WITH PASSWORD 'new_su_pwd' "
        self.assertRaisesRegexp(Unauthorized,
                                ".*You aren't allowed to alter this user.*",
                                lambda: session.execute(stmt))

    def test_user_cannot_change_other_user_pwd(self):
        """
        user_cannot_change_other_user_pwd: regular user cannot change password for other user
        """
        cluster = self.cluster_as(self.test_user, self.password)
        session = cluster.connect()
        stmt = "ALTER USER other_user WITH PASSWORD 'new_reg_pwd' "
        self.assertRaisesRegexp(Unauthorized,
                                ".*You aren't allowed to alter this user.*",
                                lambda: session.execute(stmt))

    def test_regular_user_can_change_his_pwd(self):
        """
        regular_user_can_change_his_pwd: regular user can change his password and connect to C* with new pwd
        and cannot connect with old pwd
        """
        cluster = self.cluster_as(self.test_user, self.password)
        session = cluster.connect()

        new_password = 'AxaXax'
        session.execute("ALTER USER %s WITH PASSWORD '%s' " % (self.test_user, new_password))
        session.shutdown()

        # and can connect to the cluster with new password
        cluster = self.cluster_as(self.test_user, new_password)
        session = cluster.connect()
        session.shutdown()
        # Should be able to execute some commands
        self.assertIn(self.test_user, self.list_users(cluster), 'user %s is missing' % self.test_user)

        # Cannot connect with old pwd
        cluster = self.cluster_as(self.test_user, self.password)
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_su_can_alter_reg_user_status(self):
        """
        su_can_alter_reg_user_status: superuser can change regular user status
        """
        session = self.root_cluster.connect()

         # Turn test_user into superuser
        session.execute("ALTER USER %s WITH PASSWORD '%s' SUPERUSER " % (self.test_user, self.password))
        users = self.get_user_data(self.root_cluster)
        self.assertEqual(users[self.test_user], True, msg="%s has SUPERUSER set to %s"
                                                          % (self.test_user, users[self.test_user]))

    def test_su_can_alter_other_su_status(self):
        """
        su_can_alter_other_su_status: superuser can change other superuser status
        """
        session = self.root_cluster.connect()

         # Turn suser into regular user
        session.execute("ALTER USER %s WITH PASSWORD '%s' NOSUPERUSER " % (self.suser, self.password))
        users = self.get_user_data(self.root_cluster)
        self.assertEqual(users[self.suser], False, msg="%s has SUPERUSER set to %s" % (self.suser, users[self.suser]))

    def test_su_cannot_alter_his_status(self):
        """
        su_cannot_alter_his_status: superuser can't change his status
        """
        session = self.root_cluster.connect()
        stmt = "ALTER USER cassandra WITH PASSWORD 'cassandra' NOSUPERUSER "
        self.assertRaisesRegexp(Unauthorized,
                                ".*You aren't allowed to alter your own superuser status.*",
                                lambda: session.execute(stmt))

    def test_su_can_change_her_pwd(self):
        """
        su_can_change_her_pwd: superuser can change her password and connect to C* with new pwd
        and cannot connect with old pwd
        """
        cluster = self.cluster_as(self.suser, self.password)
        session = cluster.connect()

        new_password = 'AxaXax'
        session.execute("ALTER USER %s WITH PASSWORD '%s' " % (self.suser, new_password))
        session.shutdown()

        # and can connect to the cluster with new password
        cluster = self.cluster_as(self.suser, new_password)
        session = cluster.connect()
        # Should be able to execute some commands
        self.assertIn(self.test_user, self.list_users(cluster), 'user %s is missing' % self.suser)
        session.shutdown()
        # Cannot connect with old pwd
        cluster = self.cluster_as(self.suser, self.password)
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_su_can_drop_other_superuser(self):
        """
        su_can_drop_other_superuser: superuser can drop other superuser
        """
        session = self.root_cluster.connect()
        self.create_user(self.root_cluster, 'temp_su_user', self.password, su=True)
        session.execute("DROP USER temp_su_user")
        self.assertNotIn('temp_du_user', self.list_users(self.root_cluster), 'user temp_su_user NOT dropped')

    def test_su_can_drop_regular_user(self):
        """
        su_can_drop_regular_user: superuser can drop regular user
        """
        session = self.root_cluster.connect()
        self.create_user(self.root_cluster, 'temp_reg_user', self.password)
        session.execute("DROP USER temp_reg_user")
        self.assertNotIn('temp_reg_user', self.list_users(self.root_cluster), 'user temp_reg_user NOT dropped')

    def test_su_cannot_drop_herself(self):
        """
        su_cannot_drop_herself: superuser can't drop herself
        """
        self.create_user(self.root_cluster, 'temp_su_user', self.password, su=True)
        cluster = self.cluster_as('temp_su_user', self.password)
        self.session = cluster.connect()
        stmt = "DROP USER temp_su_user"
        self.assertRaisesRegexp(InvalidRequest,
                                ".*Users aren't allowed to DROP themselves.*",
                                lambda: self.session.execute(stmt))

    def test_su_connect_wrong_pwd(self):
        """
        su_connect_wrong_pwd: can't connect with bad password
        Verifies that trying to connect with wrong password triggers
         "AuthenticationFailed /Bad credentials / Username and/or
        password are incorrect"
        """
        cluster = self.cluster_as('cassandra', 'who_is_cassandra')
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_su_connect_wrong_username(self):
        """
        su_connect_wrong_username: can't connect with bad username
        Try to connect with wrong superuser name fails with: "AuthenticationFailed /Bad credentials / Username and/or
        password are incorrect"
        """
        cluster = self.cluster_as('Cassandra', 'cassandra')
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_su_connect_empty_pwd(self):
        """
        su_connect_empty_pwd: can't connect with an empty password
        Empty superuser password triggers "AuthenticationFailed /Bad credentials /
        Username and/or password are incorrect"
        """
        cluster = self.cluster_as('Cassandra', '')
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_su_connect_no_user_specified(self):
        """
        su_connect_no_user_specified: can't connect with an empty user
        No user specified triggers "AuthenticationFailed /Remote end requires authentication
        """
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Remote end requires authentication.*',
                                cluster.connect)


class AuthorizedAuthenticationTests(AuthenticationTests):
    """
    Same test as AuthenticationTests but enables authorization
    """
    pass

