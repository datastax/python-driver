try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

import errno
import os
from StringIO import StringIO
import socket
from socket import error as socket_error

from mock import patch, Mock

from cassandra.connection import (PROTOCOL_VERSION,
                                  HEADER_DIRECTION_TO_CLIENT,
                                  ConnectionException)

from cassandra.decoder import (write_stringmultimap, write_int, write_string,
                               SupportedMessage, ReadyMessage, ServerError)
from cassandra.marshal import uint8_pack, uint32_pack, int32_pack

from cassandra.io.asyncorereactor import AsyncoreConnection


class AsyncoreConnectionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.socket_patcher = patch('socket.socket', spec=socket.socket)
        cls.mock_socket = cls.socket_patcher.start()
        cls.mock_socket().connect_ex.return_value = 0
        cls.mock_socket().getsockopt.return_value = 0

    @classmethod
    def tearDownClass(cls):
        cls.socket_patcher.stop()

    def make_connection(self):
        c = AsyncoreConnection('1.2.3.4', cql_version='3.0.1')
        c.socket = Mock()
        c.socket.send.side_effect = lambda x: len(x)
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
        c.handle_write()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        c.socket.recv.return_value = self.make_msg(header, options)
        c.handle_read()

        # let it write out a StartupMessage
        c.handle_write()

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c.socket.recv.return_value = self.make_msg(header)
        c.handle_read()

        self.assertTrue(c.connected_event.is_set())
        return c

    def test_egain_on_buffer_size(self, *args):
        # get a connection that's already fully started
        c = self.test_successful_connection()

        header = '\x00\x00\x00\x00' + int32_pack(20000)
        responses = [
            header + ('a' * (4096 - len(header))),
            'a' * 4096,
            socket_error(errno.EAGAIN),
            'a' * 100,
            socket_error(errno.EAGAIN)]

        def side_effect(*args):
            response = responses.pop(0)
            if isinstance(response, socket_error):
                raise response
            else:
                return response

        c.socket.recv.side_effect = side_effect
        c.handle_read()
        self.assertEquals(c._total_reqd_bytes, 20000 + len(header))
        # the EAGAIN prevents it from reading the last 100 bytes
        c._iobuf.seek(0, os.SEEK_END)
        pos = c._iobuf.tell()
        self.assertEquals(pos, 4096 + 4096)

        # now tell it to read the last 100 bytes
        c.handle_read()
        c._iobuf.seek(0, os.SEEK_END)
        pos = c._iobuf.tell()
        self.assertEquals(pos, 4096 + 4096 + 100)

    def test_protocol_error(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0xa4)
        options = self.make_options_body()
        c.socket.recv.return_value = self.make_msg(header, options)
        c.handle_read()

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertTrue(c.connected_event.is_set())
        self.assertIsInstance(c.last_error, ConnectionException)

    def test_error_message_on_startup(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        c.socket.recv.return_value = self.make_msg(header, options)
        c.handle_read()

        # let it write out a StartupMessage
        c.handle_write()

        header = self.make_header_prefix(ServerError, stream_id=1)
        body = self.make_error_body(ServerError.error_code, ServerError.summary)
        c.socket.recv.return_value = self.make_msg(header, body)
        c.handle_read()

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, ConnectionException)
        self.assertTrue(c.connected_event.is_set())

    def test_socket_error_on_write(self, *args):
        c = self.make_connection()

        # make the OptionsMessage write fail
        c.socket.send.side_effect = socket_error(errno.EIO, "bad stuff!")
        c.handle_write()

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_blocking_on_write(self, *args):
        c = self.make_connection()

        # make the OptionsMessage write block
        c.socket.send.side_effect = socket_error(errno.EAGAIN, "socket busy")
        c.handle_write()

        self.assertFalse(c.is_defunct)

        # try again with normal behavior
        c.socket.send.side_effect = lambda x: len(x)
        c.handle_write()
        self.assertFalse(c.is_defunct)
        self.assertTrue(c.socket.send.call_args is not None)

    def test_partial_send(self, *args):
        c = self.make_connection()

        # only write the first four bytes of the OptionsMessage
        c.socket.send.side_effect = None
        c.socket.send.return_value = 4
        c.handle_write()

        self.assertFalse(c.is_defunct)
        self.assertEqual(2, c.socket.send.call_count)
        self.assertEqual(4, len(c.socket.send.call_args[0][0]))

    def test_socket_error_on_read(self, *args):
        c = self.make_connection()

        # let it write the OptionsMessage
        c.handle_write()

        # read in a SupportedMessage response
        c.socket.recv.side_effect = socket_error(errno.EIO, "busy socket")
        c.handle_read()

        # make sure it errored correctly
        self.assertTrue(c.is_defunct)
        self.assertIsInstance(c.last_error, socket_error)
        self.assertTrue(c.connected_event.is_set())

    def test_partial_header_read(self, *args):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first byte
        c.socket.recv.return_value = message[0]
        c.handle_read()
        self.assertEquals(c._iobuf.getvalue(), message[0])

        c.socket.recv.return_value = message[1:]
        c.handle_read()
        self.assertEquals("", c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write()

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c.socket.recv.return_value = self.make_msg(header)
        c.handle_read()

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)

    def test_partial_message_read(self, *args):
        c = self.make_connection()

        header = self.make_header_prefix(SupportedMessage)
        options = self.make_options_body()
        message = self.make_msg(header, options)

        # read in the first nine bytes
        c.socket.recv.return_value = message[:9]
        c.handle_read()
        self.assertEquals(c._iobuf.getvalue(), message[:9])

        # ... then read in the rest
        c.socket.recv.return_value = message[9:]
        c.handle_read()
        self.assertEquals("", c._iobuf.getvalue())

        # let it write out a StartupMessage
        c.handle_write()

        header = self.make_header_prefix(ReadyMessage, stream_id=1)
        c.socket.recv.return_value = self.make_msg(header)
        c.handle_read()

        self.assertTrue(c.connected_event.is_set())
        self.assertFalse(c.is_defunct)
