
try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from mock import Mock
from cassandra import ProtocolVersion
from cassandra.protocol import PrepareMessage, QueryMessage, ExecuteMessage

class MessageTest(unittest.TestCase):

    def test_prepare_message(self):
        """
        Test to check the appropriate calls are made

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = PrepareMessage("a")
        io = Mock()

        message.send_body(io,4)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',)])

        io.reset_mock()
        message.send_body(io,5)

        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x00\x00\x00',)])

    def test_execute_message(self):
        message = ExecuteMessage('1',[],4)
        io = Mock()

        message.send_body(io,4)
        self._check_calls(io, [(b'\x00\x01',), (b'1',), (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

        io.reset_mock()
        message.send_body(io, 5)

        self._check_calls(io, [(b'\x00\x01',), (b'1',), (b'\x00\x04',), (b'\x00\x00\x00\x01',), (b'\x00\x00',)])

    def test_query_message(self):
        """
        Test to check the appropriate calls are made

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = QueryMessage("a",3)
        io = Mock()
        
        message.send_body(io,4)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x03',), (b'\x00',)])

        io.reset_mock()
        message.send_body(io,5)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x03',), (b'\x00\x00\x00\x00',)])

    def _check_calls(self, io, expected):
        self.assertEqual(len(io.write.mock_calls), len(expected))
        for call, expect in zip(io.write.mock_calls, expected):
            self.assertEqual(call[1], expect)

    def test_prepare_flag(self):
        """
        Test to check the prepare flag is properly set, This should only happen for V5 at the moment.

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = PrepareMessage("a")
        io = Mock()
        for version in ProtocolVersion.SUPPORTED_VERSIONS:
            message.send_body(io, version)
            if ProtocolVersion.uses_prepare_flags(version):
                # This should pass after PYTHON-696
                self.assertEqual(len(io.write.mock_calls), 3)
                # self.assertEqual(uint32_unpack(io.write.mock_calls[2][1][0]) & _WITH_SERIAL_CONSISTENCY_FLAG, 1)
            else:
                self.assertEqual(len(io.write.mock_calls), 2)
            io.reset_mock()
