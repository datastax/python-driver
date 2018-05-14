try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from mock import Mock
from cassandra import ProtocolVersion, UnsupportedOperation
from cassandra.protocol import (PrepareMessage, QueryMessage, ExecuteMessage,
                                BatchMessage)
from cassandra.query import SimpleStatement, BatchType
from tests.unit import driver_context

class MessageTest(unittest.TestCase):

    CODEC_KWARGS = {
        'protocol_version_registry': driver_context.protocol_version_registry
    }

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

        PrepareMessage.encode(io, message, 4, **self.CODEC_KWARGS)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',)])

        io.reset_mock()
        PrepareMessage.encode(io, message, 5, **self.CODEC_KWARGS)

        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x00\x00\x00',)])

    def test_execute_message(self):
        message = ExecuteMessage('1', [], 4)
        io = Mock()

        ExecuteMessage.encode(io, message, 4, **self.CODEC_KWARGS)
        self._check_calls(io, [(b'\x00\x01',), (b'1',), (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

        io.reset_mock()
        message.result_metadata_id = 'foo'
        ExecuteMessage.encode(io, message, 5, **self.CODEC_KWARGS)

        self._check_calls(io, [(b'\x00\x01',), (b'1',),
                               (b'\x00\x03',), (b'foo',),
                               (b'\x00\x04',),
                               (b'\x00\x00\x00\x01',), (b'\x00\x00',)])

    def test_query_message(self):
        """
        Test to check the appropriate calls are made

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = QueryMessage("a", 3)
        io = Mock()

        QueryMessage.encode(io, message, 4, **self.CODEC_KWARGS)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x03',), (b'\x00',)])

        io.reset_mock()
        QueryMessage.encode(io, message, 5, **self.CODEC_KWARGS)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x03',), (b'\x00\x00\x00\x00',)])

    def _check_calls(self, io, expected):
        self.assertEqual(
            tuple(c[1] for c in io.write.mock_calls),
            tuple(expected)
        )

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
        for version in ProtocolVersion.VERSIONS:
            PrepareMessage.encode(io, message, version, **self.CODEC_KWARGS)
            if ProtocolVersion.uses_prepare_flags(version):
                self.assertEqual(len(io.write.mock_calls), 3)
            else:
                self.assertEqual(len(io.write.mock_calls), 2)
            io.reset_mock()

    def test_prepare_flag_with_keyspace(self):
        message = PrepareMessage("a", keyspace='ks')
        io = Mock()

        for version in ProtocolVersion.VERSIONS:
            if ProtocolVersion.uses_keyspace_flag(version):
                PrepareMessage.encode(io, message, version, **self.CODEC_KWARGS)
                self._check_calls(io, [
                    (b'\x00\x00\x00\x01',),
                    (b'a',),
                    (b'\x00\x00\x00\x01',),
                    (b'\x00\x02',),
                    (b'ks',),
                ])
            else:
                with self.assertRaises(UnsupportedOperation):
                    PrepareMessage.encode(io, message, version, **self.CODEC_KWARGS)
            io.reset_mock()

    def test_keyspace_flag_raises_before_v5(self):
        keyspace_message = QueryMessage('a', consistency_level=3, keyspace='ks')
        io = Mock(name='io')

        with self.assertRaisesRegexp(UnsupportedOperation, 'Keyspaces.*set'):
            QueryMessage.encode(io, keyspace_message, protocol_version=4, **self.CODEC_KWARGS)
        io.assert_not_called()

    def test_keyspace_written_with_length(self):
        io = Mock(name='io')
        base_expected = [
            (b'\x00\x00\x00\x01',),
            (b'a',),
            (b'\x00\x03',),
            (b'\x00\x00\x00\x80',),  # options w/ keyspace flag
        ]

        message = QueryMessage('a', consistency_level=3, keyspace='ks')
        QueryMessage.encode(io, message, protocol_version=5, **self.CODEC_KWARGS)
        self._check_calls(io, base_expected + [
            (b'\x00\x02',),  # length of keyspace string
            (b'ks',),
        ])

        io.reset_mock()

        message = QueryMessage('a', consistency_level=3, keyspace='keyspace')
        QueryMessage.encode(io, message, protocol_version=5, **self.CODEC_KWARGS)
        self._check_calls(io, base_expected + [
            (b'\x00\x08',),  # length of keyspace string
            (b'keyspace',),
        ])

    def test_batch_message_with_keyspace(self):
        self.maxDiff = None
        io = Mock(name='io')
        batch = BatchMessage(
            batch_type=BatchType.LOGGED,
            queries=((False, 'stmt a', ('param a',)),
                     (False, 'stmt b', ('param b',)),
                     (False, 'stmt c', ('param c',))
                     ),
            consistency_level=3,
            keyspace='ks'
        )
        BatchMessage.encode(io, batch, protocol_version=5, **self.CODEC_KWARGS)
        self._check_calls(io,
            ((b'\x00',), (b'\x00\x03',), (b'\x00',),
             (b'\x00\x00\x00\x06',), (b'stmt a',),
             (b'\x00\x01',), (b'\x00\x00\x00\x07',), ('param a',),
             (b'\x00',), (b'\x00\x00\x00\x06',), (b'stmt b',),
             (b'\x00\x01',), (b'\x00\x00\x00\x07',), ('param b',),
             (b'\x00',), (b'\x00\x00\x00\x06',), (b'stmt c',),
             (b'\x00\x01',), (b'\x00\x00\x00\x07',), ('param c',),
             (b'\x00\x03',),
             (b'\x00\x00\x00\x80',), (b'\x00\x02',), (b'ks',))
        )
