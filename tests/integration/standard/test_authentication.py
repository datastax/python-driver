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

from tests.integration import use_singledc, get_cluster, remove_cluster, PROTOCOL_VERSION
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider, SASLClient, SaslAuthProvider


try:
    import unittest2 as unittest
except ImportError:
    import unittest

log = logging.getLogger(__name__)


def setup_module():
    use_singledc(start=False)
    ccm_cluster = get_cluster()
    ccm_cluster.stop()
    config_options = {'authenticator': 'PasswordAuthenticator',
                      'authorizer': 'CassandraAuthorizer'}
    ccm_cluster.set_configuration_options(config_options)
    log.debug("Starting ccm test cluster with %s", config_options)
    ccm_cluster.start(wait_for_binary_proto=True)


def teardown_module():
    remove_cluster()  # this test messes with config


class AuthenticationTests(unittest.TestCase):
    """
    Tests to cover basic authentication functionality
    """

    def get_authentication_provider(self, username, password):
        """
        Return correct authentication provider based on protocol version.
        There is a difference in the semantics of authentication provider argument with protocol versions 1 and 2
        For protocol version 2 and higher it should be a PlainTextAuthProvider object.
        For protocol version 1 it should be a function taking hostname as an argument and returning a dictionary
        containing username and password.
        :param username: authentication username
        :param password: authentication password
        :return: authentication object suitable for Cluster.connect()
        """
        if PROTOCOL_VERSION < 2:
            return lambda(hostname): dict(username=username, password=password)
        else:
            return PlainTextAuthProvider(username=username, password=password)

    def cluster_as(self, usr, pwd):
        return Cluster(protocol_version=PROTOCOL_VERSION,
                       auth_provider=self.get_authentication_provider(username=usr, password=pwd))

    def test_auth_connect(self):
        user = 'u'
        passwd = 'password'

        root_session = self.cluster_as('cassandra', 'cassandra').connect()
        root_session.execute('CREATE USER %s WITH PASSWORD %s', (user, passwd))

        cluster = self.cluster_as(user, passwd)
        session = cluster.connect()
        self.assertTrue(session.execute('SELECT release_version FROM system.local'))
        cluster.shutdown()

        root_session.execute('DROP USER %s', user)
        root_session.cluster.shutdown()

    def test_connect_wrong_pwd(self):
        cluster = self.cluster_as('cassandra', 'wrong_pass')
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_connect_wrong_username(self):
        cluster = self.cluster_as('wrong_user', 'cassandra')
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_connect_empty_pwd(self):
        cluster = self.cluster_as('Cassandra', '')
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Bad credentials.*Username and/or '
                                'password are incorrect.*',
                                cluster.connect)

    def test_connect_no_auth_provider(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.assertRaisesRegexp(NoHostAvailable,
                                '.*AuthenticationFailed.*Remote end requires authentication.*',
                                cluster.connect)


class SaslAuthenticatorTests(AuthenticationTests):
    """
    Test SaslAuthProvider as PlainText
    """

    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest('Sasl authentication not available for protocol v1')
        if SASLClient is None:
            raise unittest.SkipTest('pure-sasl is not installed')

    def get_authentication_provider(self, username, password):
        sasl_kwargs = {'host': 'localhost',
                       'service': 'cassandra',
                       'mechanism': 'PLAIN',
                       'qops': ['auth'],
                       'username': username,
                       'password': password}
        return SaslAuthProvider(**sasl_kwargs)
