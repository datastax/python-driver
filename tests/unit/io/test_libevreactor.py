try:
    import unittest2 as unittest
except ImportError:
    import unittest

import errno
from StringIO import StringIO
from socket import error as socket_error

from mock import patch, Mock

from cassandra.connection import (PROTOCOL_VERSION,
                                  HEADER_DIRECTION_TO_CLIENT,
                                  ProtocolError,
                                  ConnectionException)

from cassandra.decoder import (write_stringmultimap, write_int, write_string,
                               SupportedMessage, ReadyMessage, ServerError)
from cassandra.marshal import uint8_pack, uint32_pack

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError, exc:
    raise unittest.SkipTest('libev does not appear to be installed correctly: %s' % (exc,))

@patch('socket.socket')
@patch('cassandra.io.libevwrapper.IO')
@patch('cassandra.io.libevreactor._start_loop')
class LibevConnectionTest(unittest.TestCase):

    def make_connection(self):
        c = LibevConnection('1.2.3.4')
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

    def test_successful_connection(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, None)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        c._socket.recv.return_value = self.make_msg(header, options)
        c.handle_read(None, None)

        # let it write out a StartupMessage
        c.handle_write(None, None)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, None)

        self.assertTrue(c.connected_event.is_set())

    def test_protocol_error(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, None)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0x04)
        options = self.make_options_body()
        c._socket.recv.return_value = self.make_msg(header, options)
        c.handle_read(None, None)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(c.connected_event.is_set())
        self.assertTrue(isinstance(c.last_error, ProtocolError))

    def test_error_message_on_startup(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, None)

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        c._socket.recv.return_value = self.make_msg(header, options)
        c.handle_read(None, None)

        # let it write out a StartupMessage
        c.handle_write(None, None)

        header = self.make_header_prefix(ServerError, stream_id=1)
        body = self.make_error_body(ServerError.error_code, ServerError.summary)
        c._socket.recv.return_value = self.make_msg(header, body)
        c.handle_read(None, None)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(isinstance(c.last_error, ConnectionException))
        self.assertTrue(c.connected_event.is_set())

    def test_socket_error_on_write(self, *args):
        c = self.make_connection()

        # make the OptionsMessage write fail
        c._socket.send.side_effect = socket_error(errno.EIO, "bad stuff!")
        c.handle_write(None, None)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(isinstance(c.last_error, socket_error))
        self.assertTrue(c.connected_event.is_set())

    def test_blocking_on_write(self, *args):
        c = self.make_connection()

        # make the OptionsMessage write block
        c._socket.send.side_effect = socket_error(errno.EAGAIN, "socket busy")
        c.handle_write(None, None)

        self.assertFalse(c.is_defunct)

        # try again with normal behavior
        c._socket.send.side_effect = lambda x: len(x)
        c.handle_write(None, None)
        self.assertFalse(c.is_defunct)
        self.assertTrue(c._socket.send.call_args is not None)

    def test_partial_send(self, *args):
        c = self.make_connection()

        # only write the first four bytes of the OptionsMessage
        c._socket.send.side_effect = None
        c._socket.send.return_value = 4
        c.handle_write(None, None)

        orig_msg = c._socket.send.call_args[0][0]
        self.assertFalse(c.is_defunct)

        # try again with normal behavior
        c._socket.send.side_effect = lambda x: len(x)
        c.handle_write(None, None)
        self.assertFalse(c.is_defunct)
        self.assertEqual(c._socket.send.call_args[0][0], orig_msg[4:])

    def test_socket_error_on_read(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write(None, None)

        # read in a SupportedMessage response
        c._socket.recv.side_effect = socket_error(errno.EIO, "busy socket")
        c.handle_read(None, None)

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(isinstance(c.last_error, socket_error))
        self.assertTrue(c.connected_event.is_set())

    def test_partial_header_read(self, *args):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first byte
        c._socket.recv.return_value = message[0]
        c.handle_read(None, None)
        self.assertEquals(c._buf, message[0])

        c._socket.recv.return_value = message[1:]
        c.handle_read(None, None)
        self.assertEquals("", c._buf)

        # let it write out a StartupMessage
        c.handle_write(None, None)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, None)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_partial_message_read(self, *args):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first nine bytes
        c._socket.recv.return_value = message[:9]
        c.handle_read(None, None)
        self.assertEquals(c._buf, message[:9])

        # ... then read in the rest
        c._socket.recv.return_value = message[9:]
        c.handle_read(None, None)
        self.assertEquals("", c._buf)

        # let it write out a StartupMessage
        c.handle_write(None, None)

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c._socket.recv.return_value = self.make_msg(header)
        c.handle_read(None, None)

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)
