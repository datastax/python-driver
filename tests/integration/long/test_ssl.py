# Copyright 2013-2015 DataStax, Inc.
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

import os
import ssl
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from tests.integration import use_singledc, PROTOCOL_VERSION, get_cluster


DEFAULT_PASSWORD = "cassandra"

DEFAULT_SERVER_KEYSTORE_PATH = "tests/integration/long/ssl/keystore.jks"

DEFAULT_CLIENT_CA_CERTS = 'tests/integration/long/ssl/driver_ca_cert.pem'


def setup_module():

    """
    We need some custom setup for this module. This will start the ccm cluster with basic
    ssl connectivity. No client authentication is performed at this time.
    """

    use_singledc(start=False)
    ccm_cluster = get_cluster()
    ccm_cluster.stop()

    # Fetch the absolute path to the keystore for ccm.
    abs_path_server_keystore_path = os.path.abspath(DEFAULT_SERVER_KEYSTORE_PATH)

    # Configure ccm to use ssl.
    config_options = {'client_encryption_options': {'enabled': True,
                                                    'keystore': abs_path_server_keystore_path,
                                                    'keystore_password': DEFAULT_PASSWORD}}
    ccm_cluster.set_configuration_options(config_options)
    ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)


def teardown_module():

    """
    The rest of the tests don't need ssl enabled
    reset the config options so as to not interfere with other tests.
    """

    ccm_cluster = get_cluster()
    config_options = {}
    ccm_cluster.set_configuration_options(config_options)
    if ccm_cluster is not None:
        ccm_cluster.stop()


class SSLConnectionTests(unittest.TestCase):

    def test_ssl_connection(self):
        """
         Test to validate that we are able to connect to a cluster using ssl.

        test_ssl_connection Performs a simple sanity check to ensure that we can connect to a cluster with ssl.


        @since 2.6.0
        @jira_ticket PYTHON-332
        @expected_result we can connect and preform some basic operations

        @test_category connection:ssl
        """

        # Setup temporary keyspace.
        abs_path_ca_cert_path = os.path.abspath(DEFAULT_CLIENT_CA_CERTS)

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, ssl_options={'ca_certs': abs_path_ca_cert_path,
                                                                               'ssl_version': ssl.PROTOCOL_TLSv1})
        self.session = self.cluster.connect()

        # attempt a few simple commands.
        insert_keyspace = """CREATE KEYSPACE ssltest
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
            """
        statement = SimpleStatement(insert_keyspace)
        statement.consistency_level = 3
        self.session.execute(statement)

        drop_keyspace = "DROP KEYSPACE ssltest"

        statement = SimpleStatement(drop_keyspace)
        statement.consistency_level = ConsistencyLevel.ANY
        self.session.execute(statement)




