try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from StringIO import StringIO

from mock import Mock, ANY

from cassandra.connection import (Connection, PROTOCOL_VERSION,
                                  HEADER_DIRECTION_TO_CLIENT,
                                  HEADER_DIRECTION_FROM_CLIENT, ProtocolError)
from cassandra.decoder import (write_stringmultimap, write_int, write_string,
                               SupportedMessage)
from cassandra.marshal import uint8_pack, uint32_pack

class ConnectionTest(unittest.TestCase):

    def make_connection(self):
        c = Connection('1.2.3.4')
        c._socket = Mock()
        c._socket.send.side_effect = lambda x: len(x)
        return c

    def make_header_prefix(self, message_class, version=PROTOCOL_VERSION, stream_id=0):
        return ''.join(map(uint8_pack, [
            0xff & (HEADER_DIRECTION_TO_CLIENT | version),
            0,  # flags (compression)
            stream_id,
            message_class.opcode  # opcode
        ]))

    def make_options_body(self):
        options_buf = StringIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.1'],
            'COMPRESSION': []
        })
        return options_buf.getvalue()

    def make_error_body(self, code, msg):
        buf = StringIO()
        write_int(buf, code)
        write_string(buf, msg)
        return buf.getvalue()

    def make_msg(self, header, body=""):
        return header + uint32_pack(len(body)) + body

    def test_bad_protocol_version(self, *args):
        c = self.make_connection()
        c._id_queue.get_nowait()
        c._callbacks = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0x04)
        options = self.make_options_body()
        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        self.assertIsInstance(args[0], ProtocolError)

    def test_bad_header_direction(self, *args):
        c = self.make_connection()
        c._id_queue.get_nowait()
        c._callbacks = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = ''.join(map(uint8_pack, [
            0xff & (HEADER_DIRECTION_FROM_CLIENT | PROTOCOL_VERSION),
            0,  # flags (compression)
            0,
            SupportedMessage.opcode  # opcode
        ]))
        options = self.make_options_body()
        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        self.assertIsInstance(args[0], ProtocolError)

    def test_negative_body_length(self, *args):
        c = self.make_connection()
        c._id_queue.get_nowait()
        c._callbacks = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)
        c.process_msg(message, -13)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        self.assertIsInstance(args[0], ProtocolError)

    def test_unsupported_cql_version(self, *args):
        c = self.make_connection()
        c._id_queue.get_nowait()
        c._callbacks = {0: c._handle_options_response}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        options_buf = StringIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['7.8.9'],
            'COMPRESSION': []
        })
        options = options_buf.getvalue()

        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        self.assertIsInstance(args[0], ProtocolError)

    def test_not_implemented(self):
        """
        Ensure the following methods throw NIE's. If not, come back and test them.
        """

        c  = self.make_connection()

        self.assertRaises(NotImplementedError, c.close)
        self.assertRaises(NotImplementedError, c.defunct, None)
        self.assertRaises(NotImplementedError, c.send_msg, None, None)
        self.assertRaises(NotImplementedError, c.wait_for_response, None)
        self.assertRaises(NotImplementedError, c.wait_for_responses)
        self.assertRaises(NotImplementedError, c.register_watcher, None, None)
        self.assertRaises(NotImplementedError, c.register_watchers, None)
