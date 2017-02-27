

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa


from mock import Mock

from cassandra import ProtocolVersion
from cassandra.protocol import PrepareMessage


class MessageTest(unittest.TestCase):
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