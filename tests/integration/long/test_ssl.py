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

import os, sys, traceback, logging, ssl
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from tests.integration import use_singledc, PROTOCOL_VERSION, get_cluster, remove_cluster

log = logging.getLogger(__name__)

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
    The rest of the tests don't need ssl enabled, remove the cluster so as to not interfere with other tests.
    """

    remove_cluster()


class SSLConnectionTests(unittest.TestCase):

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

        # Setup temporary keyspace.
        abs_path_ca_cert_path = os.path.abspath(DEFAULT_CLIENT_CA_CERTS)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, ssl_options={'ca_certs': abs_path_ca_cert_path,
                                                                               'ssl_version': ssl.PROTOCOL_TLSv1})
        tries = 0
        while True:
            if tries > 5:
                raise RuntimeError("Failed to connect to SSL cluster after 5 attempts")
            try:
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