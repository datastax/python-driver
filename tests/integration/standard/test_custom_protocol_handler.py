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
    import unittest  # noqa

from cassandra.protocol import ProtocolHandler, ResultMessage, UUIDType, read_int, EventMessage
from cassandra.query import tuple_factory
from cassandra.cluster import Cluster
from tests.integration import use_singledc, PROTOCOL_VERSION, drop_keyspace_shutdown_cluster, CONTACT_POINTS
from tests.integration.datatype_utils import update_datatypes, PRIMITIVE_DATATYPES
from tests.integration.standard.utils import create_table_with_all_types, get_all_primitive_params
from six import binary_type

import uuid


def setup_module():
    use_singledc()
    update_datatypes()


class CustomProtocolHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS)
        cls.session = cls.cluster.connect()
        cls.session.execute("CREATE KEYSPACE custserdes WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        cls.session.set_keyspace("custserdes")

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster("custserdes", cls.session, cls.cluster)

    def test_custom_raw_uuid_row_results(self):
        """
        Test to validate that custom protocol handlers work with raw row results

        Connect and validate that the normal protocol handler is used.
        Re-Connect and validate that the custom protocol handler is used.
        Re-Connect and validate that the normal protocol handler is used.

        @since 2.7
        @jira_ticket PYTHON-313
        @expected_result custom protocol handler is invoked appropriately.

        @test_category data_types:serialization
        """

        # Ensure that we get normal uuid back first
        session = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS).connect(keyspace="custserdes")
        session.row_factory = tuple_factory
        result = session.execute("SELECT schema_version FROM system.local")
        uuid_type = result[0][0]
        self.assertEqual(type(uuid_type), uuid.UUID)

        # use our custom protocol handlder

        session.client_protocol_handler = CustomTestRawRowType
        session.row_factory = tuple_factory
        result_set = session.execute("SELECT schema_version FROM system.local")
        raw_value = result_set[0][0]
        self.assertTrue(isinstance(raw_value, binary_type))
        self.assertEqual(len(raw_value), 16)

        # Ensure that we get normal uuid back when we re-connect
        session.client_protocol_handler = ProtocolHandler
        result_set = session.execute("SELECT schema_version FROM system.local")
        uuid_type = result_set[0][0]
        self.assertEqual(type(uuid_type), uuid.UUID)
        session.shutdown()

    def test_custom_raw_row_results_all_types(self):
        """
        Test to validate that custom protocol handlers work with varying types of
        results

        Connect, create a table with all sorts of data. Query the data, make the sure the custom results handler is
        used correctly.

        @since 2.7
        @jira_ticket PYTHON-313
        @expected_result custom protocol handler is invoked with various result types

        @test_category data_types:serialization
        """
        # Connect using a custom protocol handler that tracks the various types the result message is used with.
        session = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=CONTACT_POINTS).connect(keyspace="custserdes")
        session.client_protocol_handler = CustomProtocolHandlerResultMessageTracked
        session.row_factory = tuple_factory

        colnames = create_table_with_all_types("alltypes", session, 1)
        columns_string = ", ".join(colnames)

        # verify data
        params = get_all_primitive_params(0)
        results = session.execute("SELECT {0} FROM alltypes WHERE primkey=0".format(columns_string))[0]
        for expected, actual in zip(params, results):
            self.assertEqual(actual, expected)
        # Ensure we have covered the various primitive types
        self.assertEqual(len(CustomResultMessageTracked.checked_rev_row_set), len(PRIMITIVE_DATATYPES)-1)
        session.shutdown()


class CustomResultMessageRaw(ResultMessage):
    """
    This is a custom Result Message that is used to return raw results, rather then
    results which contain objects.
    """
    my_type_codes = ResultMessage.type_codes.copy()
    my_type_codes[0xc] = UUIDType
    type_codes = my_type_codes

    @classmethod
    def recv_results_rows(cls, f, protocol_version, user_type_map):
            paging_state, column_metadata = cls.recv_results_metadata(f, user_type_map)
            rowcount = read_int(f)
            rows = [cls.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
            coltypes = [c[3] for c in column_metadata]
            return (paging_state, (coltypes, rows))


class CustomTestRawRowType(ProtocolHandler):
    """
    This is the a custom protocol handler that will substitute the the
    customResultMesageRowRaw Result message for our own implementation
    """
    my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
    my_opcodes[CustomResultMessageRaw.opcode] = CustomResultMessageRaw
    message_types_by_opcode = my_opcodes


class CustomResultMessageTracked(ResultMessage):
    """
    This is a custom Result Message that is use to track what primitive types
    have been processed when it receives results
    """
    my_type_codes = ResultMessage.type_codes.copy()
    my_type_codes[0xc] = UUIDType
    type_codes = my_type_codes
    checked_rev_row_set = set()

    @classmethod
    def recv_results_rows(cls, f, protocol_version, user_type_map):
        paging_state, column_metadata = cls.recv_results_metadata(f, user_type_map)
        rowcount = read_int(f)
        rows = [cls.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
        colnames = [c[2] for c in column_metadata]
        coltypes = [c[3] for c in column_metadata]
        cls.checked_rev_row_set.update(coltypes)
        parsed_rows = [
            tuple(ctype.from_binary(val, protocol_version)
                  for ctype, val in zip(coltypes, row))
            for row in rows]
        return (paging_state, (colnames, parsed_rows))


class CustomProtocolHandlerResultMessageTracked(ProtocolHandler):
    """
    This is the a custom protocol handler that will substitute the the
    CustomTestRawRowTypeTracked Result message for our own implementation
    """
    my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
    my_opcodes[CustomResultMessageTracked.opcode] = CustomResultMessageTracked
    message_types_by_opcode = my_opcodes


