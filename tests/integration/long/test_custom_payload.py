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

from cassandra.query import ( SimpleStatement, BatchStatement, BatchType)
from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable

from tests.integration import use_singledc, PROTOCOL_VERSION, get_cluster, setup_keyspace

def setup_module():
    """
    We need some custom setup for this module. All unit tests in this module
    require protocol >=4. We won't bother going through the setup required unless that is the
    protocol version we are using.
    :return:
    """

    # If we aren't at protocol v 4 or greater don't waste time setting anything up, all tests will be skipped
    if PROTOCOL_VERSION >= 4:
        # Don't start the ccm cluster until we get the custom jvm argument specified
        use_singledc(start=False)
        ccm_cluster=get_cluster()
        # if needed stop CCM cluster
        ccm_cluster.stop()
        # This will enable the Mirroring query handler which will echo our custom payload k,v pairs back to us
        jmv_args=[" -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler"]
        ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True,jvm_args=jmv_args)
        # wait for nodes to startup
        setup_keyspace()





def tearDownModule():
    """
    The rests of the tests don't need our custom payload query handle so stop the cluster so we
    don't impact other tests
    :return:
    """

    ccm_cluster = get_cluster()
    if ccm_cluster is not None:
        ccm_cluster.stop()

class CustomPayloadTests(unittest.TestCase):

    def setUp(self):
        """
        Test is skipped if run with cql version <4

        """
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest(
                "Protocol 4,0+ is required for custom payloads %r"
                % (PROTOCOL_VERSION,))

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()

    def tearDown(self):

        self.cluster.shutdown()

    def test_custom_query_basic(self):
        """
        Test to validate that custom payloads work with simple queries

        creates a simple query and ensures that custom payloads are passed to C*. A custom
        query provider is used with C* so we can validate that same custom payloads are sent back
        with the results


        @since 2.2
        @jira_ticket PYTHON-280
        @expected_result valid custom payloads should be sent and received

        @test_category queries:basic
        """

        # Create a simple query statement a
        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        # Validate that various types of custom payloads are sent and received okay
        self.validate_various_custom_payloads(statement=statement)

    def test_custom_query_batching(self):
        """
        Test to validate that custom payloads work with batch queries

        creates a batch query and ensures that custom payloads are passed to C*. A custom
        query provider is used with C* so we can validate that same custom payloads are sent back
        with the results


        @since 2.2
        @jira_ticket PYTHON-280
        @expected_result valid custom payloads should be sent and received

        @test_category queries:batch
        """

        # Construct Batch Statement
        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)"), (i, i))

        # Validate that various types of custom payloads are sent and received okay
        self.validate_various_custom_payloads(statement=batch)




    def test_custom_query_prepared(self):
        """
        Test to validate that custom payloads work with prepared queries

        creates a batch query and ensures that custom payloads are passed to C*. A custom
        query provider is used with C* so we can validate that same custom payloads are sent back
        with the results


        @since 2.2
        @jira_ticket PYTHON-280
        @expected_result valid custom payloads should be sent and received

        @test_category queries:binding
        """

        # Construct prepared statement
        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        bound = prepared.bind((1, None))

        # Validate that various custom payloads are validated correctly
        self.validate_various_custom_payloads(statement=bound)

    def validate_various_custom_payloads(self, statement):
        """
        This is a utility method that given a statement will attempt
        to submit the statement with various custom payloads. It will
        validate that the custom payloads are sent and received correctly.

        :param statement: to validate the custom queries in conjunction with

        :return:
        """

        # Simple key value
        custom_payload={'test':'test_return'}
        self.execute_async_validate_custom_payload(statement=statement,custom_payload=custom_payload)

        # no key value
        custom_payload={'':''}
        self.execute_async_validate_custom_payload(statement=statement,custom_payload=custom_payload)

        # Space value
        custom_payload={' ':' '}
        self.execute_async_validate_custom_payload(statement=statement,custom_payload=custom_payload)

        # 100 kev/value pair validation
        custom_payload={}
        for i in range(100):
            custom_payload[str(i)]=str(i)
        self.execute_async_validate_custom_payload(statement=statement,custom_payload=custom_payload)

        # Max supported value key pairs according C* binary protocol v4 should be 65534 (unsigned short max value)
        for i in range(65534):
            custom_payload[str(i)]=str(i)
        self.execute_async_validate_custom_payload(statement=statement,custom_payload=custom_payload)

        #This is too many key value pairs and should fail
        for i in range(65535):
            custom_payload[str(i)]=str(i)
        self.assertRaises(NoHostAvailable,self.execute_async_validate_custom_payload(statement=statement,custom_payload=custom_payload))




    def execute_async_validate_custom_payload(self,statement,custom_payload):
        """
        This is just a simple method that submits a statement with a payload, and validates
        that the custom payload we submitted matches the one that we got back
        :param statement: to execute
        :param custom_payload: to submit with
        :return:
        """

        # Submit the statement with our custom payload. Validate the one
        # we receive from the server matches
        response_future = self.session.execute_async(statement,custom_payload=custom_payload)
        response_future.result(10.0)
        returned_custom_payload=response_future.custom_payload
        self.assertEqual(custom_payload,returned_custom_payload)