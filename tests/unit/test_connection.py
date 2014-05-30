# Copyright 2013-2014 DataStax, Inc.
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
import six

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from six import BytesIO

from mock import Mock, ANY

from cassandra.cluster import Cluster
from cassandra.connection import (Connection, HEADER_DIRECTION_TO_CLIENT,
                                  HEADER_DIRECTION_FROM_CLIENT, ProtocolError,
                                  locally_supported_compressions)
from cassandra.marshal import uint8_pack, uint32_pack
from cassandra.protocol import (write_stringmultimap, write_int, write_string,
                                SupportedMessage)


class ConnectionTest(unittest.TestCase):

    protocol_version = 2

    def make_connection(self):
        c = Connection('1.2.3.4')
        c._socket = Mock()
        c._socket.send.side_effect = lambda x: len(x)
        return c

    def make_header_prefix(self, message_class, version=2, stream_id=0):
        return six.binary_type().join(map(uint8_pack, [
            0xff & (HEADER_DIRECTION_TO_CLIENT | version),
            0,  # flags (compression)
            stream_id,
            message_class.opcode  # opcode
        ]))

    def make_options_body(self):
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.1'],
            'COMPRESSION': []
        })
        return options_buf.getvalue()

    def make_error_body(self, code, msg):
        buf = BytesIO()
        write_int(buf, code)
        write_string(buf, msg)
        return buf.getvalue()

    def make_msg(self, header, body=""):
        return header + uint32_pack(len(body)) + body

    def test_bad_protocol_version(self, *args):
        c = self.make_connection()
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
        c._callbacks = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = six.binary_type().join(uint8_pack(i) for i in (
            0xff & (HEADER_DIRECTION_FROM_CLIENT | self.protocol_version),
            0,  # flags (compression)
            0,
            SupportedMessage.opcode  # opcode
        ))
        options = self.make_options_body()
        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        self.assertIsInstance(args[0], ProtocolError)

    def test_negative_body_length(self, *args):
        c = self.make_connection()
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
        c._callbacks = {0: c._handle_options_response}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        options_buf = BytesIO()
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

    def test_prefer_lz4_compression(self, *args):
        c = self.make_connection()
        c._callbacks = {0: c._handle_options_response}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        self.assertEqual(c.decompressor, locally_supported_compressions['lz4'][1])

    def test_requested_compression_not_available(self, *args):
        c = self.make_connection()
        c._callbacks = {0: c._handle_options_response}
        c.defunct = Mock()
        # request lz4 compression
        c.compression = "lz4"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy']
        })
        options = options_buf.getvalue()

        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        self.assertIsInstance(args[0], ProtocolError)

    def test_use_requested_compression(self, *args):
        c = self.make_connection()
        c._callbacks = {0: c._handle_options_response}
        c.defunct = Mock()
        # request snappy compression
        c.compression = "snappy"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        self.assertEqual(c.decompressor, locally_supported_compressions['snappy'][1])

    def test_disable_compression(self, *args):
        c = self.make_connection()
        c._callbacks = {0: c._handle_options_response}
        c.defunct = Mock()
        # disable compression
        c.compression = False

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        self.assertEqual(c.decompressor, None)

    def test_not_implemented(self):
        """
        Ensure the following methods throw NIE's. If not, come back and test them.
        """
        c = self.make_connection()

        self.assertRaises(NotImplementedError, c.close)
        self.assertRaises(NotImplementedError, c.register_watcher, None, None)
        self.assertRaises(NotImplementedError, c.register_watchers, None)

    def test_set_keyspace_blocking(self):
        c = self.make_connection()

        self.assertEqual(c.keyspace, None)
        c.set_keyspace_blocking(None)
        self.assertEqual(c.keyspace, None)

        c.keyspace = 'ks'
        c.set_keyspace_blocking('ks')
        self.assertEqual(c.keyspace, 'ks')

    def test_set_connection_class(self):
        cluster = Cluster(connection_class='test')
        self.assertEqual('test', cluster.connection_class)
