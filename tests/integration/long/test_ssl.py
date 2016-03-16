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

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os, sys, traceback, logging, ssl
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from tests.integration import use_singledc, PROTOCOL_VERSION, get_cluster, remove_cluster, CONTACT_POINTS

log = logging.getLogger(__name__)

DEFAULT_PASSWORD = "cassandra"

# Server keystore trust store locations
SERVER_KEYSTORE_PATH = "tests/integration/long/ssl/server_keystore.jks"
SERVER_TRUSTSTORE_PATH = "tests/integration/long/ssl/server_trust.jks"

# Client specific keys/certs
CLIENT_CA_CERTS = 'tests/integration/long/ssl/driver_ca_cert.pem'
DRIVER_KEYFILE = "tests/integration/long/ssl/python_driver_no_pass.key"
DRIVER_CERTFILE = "tests/integration/long/ssl/python_driver.pem"
DRIVER_CERTFILE_BAD = "tests/integration/long/ssl/python_driver_bad.pem"


def setup_cluster_ssl(client_auth=False):
    """
    We need some custom setup for this module. This will start the ccm cluster with basic
    ssl connectivity, and client authenticiation if needed.
    """

    use_singledc(start=False)
    ccm_cluster = get_cluster()
    ccm_cluster.stop()

    # Fetch the absolute path to the keystore for ccm.
    abs_path_server_keystore_path = os.path.abspath(SERVER_KEYSTORE_PATH)

    # Configure ccm to use ssl.

    config_options = {'client_encryption_options': {'enabled': True,
                                                    'keystore': abs_path_server_keystore_path,
                                                    'keystore_password': DEFAULT_PASSWORD}}

    if(client_auth):
        abs_path_server_truststore_path = os.path.abspath(SERVER_TRUSTSTORE_PATH)
        client_encyrption_options = config_options['client_encryption_options']
        client_encyrption_options['require_client_auth'] = True
        client_encyrption_options['truststore'] = abs_path_server_truststore_path
        client_encyrption_options['truststore_password'] = DEFAULT_PASSWORD

    ccm_cluster.set_configuration_options(config_options)
    ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)


def teardown_module():
    """
    The rest of the tests don't need ssl enabled, remove the cluster so as to not interfere with other tests.
    """

    ccm_cluster = get_cluster()
    ccm_cluster.stop()
    remove_cluster()


class SSLConnectionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl()

    def test_can_connect_with_ssl_ca(self):
        """
        Test to validate that we are able to connect to a cluster using ssl.

        test_can_connect_with_ssl_ca performs a simple sanity check to ensure that we can connect to a cluster with ssl
        authentication via simple server-side shared certificate authority. The client is able to validate the identity
        of the server, however by using this method the server can't trust the client unless additional authentication
        has been provided.

        @since 2.6.0
        @jira_ticket PYTHON-332
        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """

        # find absolute path to client CA_CERTS
        abs_path_ca_cert_path = os.path.abspath(CLIENT_CA_CERTS)

        tries = 0
        while True:
            if tries > 5:
                raise RuntimeError("Failed to connect to SSL cluster after 5 attempts")
            try:
                cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                                  contact_points=CONTACT_POINTS,
                                  ssl_options={'ca_certs': abs_path_ca_cert_path,
                                               'ssl_version': ssl.PROTOCOL_TLSv1})
                session = cluster.connect()
                break
            except Exception:
                ex_type, ex, tb = sys.exc_info()
                log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                tries += 1

        # attempt a few simple commands.
        insert_keyspace = """CREATE KEYSPACE ssltest
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
            """
        statement = SimpleStatement(insert_keyspace)
        statement.consistency_level = 3
        session.execute(statement)

        drop_keyspace = "DROP KEYSPACE ssltest"
        statement = SimpleStatement(drop_keyspace)
        statement.consistency_level = ConsistencyLevel.ANY
        session.execute(statement)

        cluster.shutdown()


class SSLConnectionAuthTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl(client_auth=True)

    def test_can_connect_with_ssl_client_auth(self):
        """
        Test to validate that we can connect to a C* cluster that has client_auth enabled.

        This test will setup and use a c* cluster that has client authentication enabled. It will then attempt
        to connect using valid client keys, and certs (that are in the server's truststore), and attempt to preform some
        basic operations
        @since 2.7.0

        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """

        # Need to get absolute paths for certs/key
        abs_path_ca_cert_path = os.path.abspath(CLIENT_CA_CERTS)
        abs_driver_keyfile = os.path.abspath(DRIVER_KEYFILE)
        abs_driver_certfile = os.path.abspath(DRIVER_CERTFILE)

        tries = 0
        while True:
            if tries > 5:
                raise RuntimeError("Failed to connect to SSL cluster after 5 attempts")
            try:
                cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                                  contact_points=CONTACT_POINTS,
                                  ssl_options={'ca_certs': abs_path_ca_cert_path,
                                               'ssl_version': ssl.PROTOCOL_TLSv1,
                                               'keyfile': abs_driver_keyfile,
                                                'certfile': abs_driver_certfile})

                session = cluster.connect()
                break
            except Exception:
                ex_type, ex, tb = sys.exc_info()
                log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                tries += 1

        # attempt a few simple commands.

        insert_keyspace = """CREATE KEYSPACE ssltest
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
            """
        statement = SimpleStatement(insert_keyspace)
        statement.consistency_level = 3
        session.execute(statement)

        drop_keyspace = "DROP KEYSPACE ssltest"
        statement = SimpleStatement(drop_keyspace)
        statement.consistency_level = ConsistencyLevel.ANY
        session.execute(statement)

        cluster.shutdown()

    def test_cannot_connect_without_client_auth(self):
        """
        Test to validate that we cannot connect without client auth.

        This test will omit the keys/certs needed to preform client authentication. It will then attempt to connect
        to a server that has client authentication enabled.

        @since 2.7.0
        @expected_result The client will throw an exception on connect

        @test_category connection:ssl
        """

        abs_path_ca_cert_path = os.path.abspath(CLIENT_CA_CERTS)
        cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                          contact_points=CONTACT_POINTS,
                          ssl_options={'ca_certs': abs_path_ca_cert_path,
                                       'ssl_version': ssl.PROTOCOL_TLSv1})

        # attempt to connect and expect an exception

        with self.assertRaises(NoHostAvailable) as context:
            cluster.connect()

    def test_cannot_connect_with_bad_client_auth(self):
        """
         Test to validate that we cannot connect with invalid client auth.

        This test will use bad keys/certs to preform client authentication. It will then attempt to connect
        to a server that has client authentication enabled.


        @since 2.7.0
        @expected_result The client will throw an exception on connect

        @test_category connection:ssl
        """

        # Setup absolute paths to key/cert files
        abs_path_ca_cert_path = os.path.abspath(CLIENT_CA_CERTS)
        abs_driver_keyfile = os.path.abspath(DRIVER_KEYFILE)
        abs_driver_certfile = os.path.abspath(DRIVER_CERTFILE_BAD)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                          contact_points=CONTACT_POINTS,
                          ssl_options={'ca_certs': abs_path_ca_cert_path,
                                       'ssl_version': ssl.PROTOCOL_TLSv1,
                                       'keyfile': abs_driver_keyfile,
                                       'certfile': abs_driver_certfile})

        with self.assertRaises(NoHostAvailable) as context:
            cluster.connect()
