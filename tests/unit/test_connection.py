# Copyright DataStax, Inc.
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
import itertools
import unittest
from io import BytesIO
import time
from threading import Lock
from unittest.mock import Mock, ANY, call, patch

from cassandra import OperationTimedOut
from cassandra.cluster import Cluster
from cassandra.connection import (Connection, HEADER_DIRECTION_TO_CLIENT, ProtocolError,
                                  locally_supported_compressions, ConnectionHeartbeat, _Frame, Timer, TimerManager,
                                  ConnectionException, DefaultEndPoint, ShardAwarePortGenerator)
from cassandra.marshal import uint8_pack, uint32_pack, int32_pack
from cassandra.protocol import (write_stringmultimap, write_int, write_string,
                                SupportedMessage, ProtocolHandler)

from tests.util import wait_until


class ConnectionTest(unittest.TestCase):

    def make_connection(self):
        c = Connection(DefaultEndPoint('1.2.3.4'))
        c._socket = Mock()
        c._socket.send.side_effect = lambda x: len(x)
        return c

    def make_header_prefix(self, message_class, version=Connection.protocol_version, stream_id=0):
        return bytes().join(map(uint8_pack, [
            0xff & (HEADER_DIRECTION_TO_CLIENT | version),
            0,  # flags (compression)
            0,  # MSB for v3+ stream
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

    def test_connection_endpoint(self):
        endpoint = DefaultEndPoint('1.2.3.4')
        c = Connection(endpoint)
        assert c.endpoint == endpoint
        assert c.endpoint.address == endpoint.address

        c = Connection(host=endpoint)  # kwarg
        assert c.endpoint == endpoint
        assert c.endpoint.address == endpoint.address

        c = Connection('10.0.0.1')
        endpoint = DefaultEndPoint('10.0.0.1')
        assert c.endpoint == endpoint
        assert c.endpoint.address == endpoint.address

    def test_bad_protocol_version(self, *args):
        c = self.make_connection()
        c._requests = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0x7f)
        options = self.make_options_body()
        message = self.make_msg(header, options)
        c._iobuf._io_buffer = BytesIO()
        c._iobuf.write(message)
        c.process_io_buffer()

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_negative_body_length(self, *args):
        c = self.make_connection()
        c._requests = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        message = header + int32_pack(-13)
        c._iobuf._io_buffer = BytesIO()
        c._iobuf.write(message)
        c.process_io_buffer()

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_unsupported_cql_version(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['7.8.9'],
            'COMPRESSION': []
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_prefer_lz4_compression(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        assert c.decompressor == locally_supported_compressions['lz4'][1]

    def test_requested_compression_not_available(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        # request lz4 compression
        c.compression = "lz4"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy']
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_use_requested_compression(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        # request snappy compression
        c.compression = "snappy"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        assert c.decompressor == locally_supported_compressions['snappy'][1]

    def test_disable_compression(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message)}
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

        assert c.decompressor == None

    def test_not_implemented(self):
        """
        Ensure the following methods throw NIE's. If not, come back and test them.
        """
        c = self.make_connection()
        self.assertRaises(NotImplementedError, c.close)

    def test_set_keyspace_blocking(self):
        c = self.make_connection()

        assert c.keyspace == None
        c.set_keyspace_blocking(None)
        assert c.keyspace == None

        c.keyspace = 'ks'
        c.set_keyspace_blocking('ks')
        assert c.keyspace == 'ks'

    def test_set_connection_class(self):
        cluster = Cluster(connection_class='test')
        assert 'test' == cluster.connection_class


@patch('cassandra.connection.ConnectionHeartbeat._raise_if_stopped')
class ConnectionHeartbeatTest(unittest.TestCase):

    @staticmethod
    def make_get_holders(len):
        holders = []
        for _ in range(len):
            holder = Mock()
            holder.get_connections = Mock(return_value=[])
            holders.append(holder)
        get_holders = Mock(return_value=holders)
        return get_holders

    def run_heartbeat(self, get_holders_fun, count=2, interval=0.05, timeout=0.05):
        ch = ConnectionHeartbeat(interval, get_holders_fun, timeout=timeout)
        # wait until the thread is started
        wait_until(lambda: get_holders_fun.call_count > 0, 0.01, 100)
        time.sleep(interval * (count-1))
        ch.stop()
        self.assertTrue(get_holders_fun.call_count)

    def test_empty_connections(self, *args):
        count = 3
        get_holders = self.make_get_holders(1)

        self.run_heartbeat(get_holders, count)

        self.assertGreaterEqual(get_holders.call_count, count-1)
        self.assertLessEqual(get_holders.call_count, count)
        holder = get_holders.return_value[0]
        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)

    def test_idle_non_idle(self, *args):
        request_id = 999

        # connection.send_msg(OptionsMessage(), connection.get_request_id(), self._options_callback)
        def send_msg(msg, req_id, msg_callback):
            msg_callback(SupportedMessage([], {}))

        idle_connection = Mock(spec=Connection, host='localhost',
                               max_request_id=127,
                               lock=Lock(),
                               in_flight=0, is_idle=True,
                               is_defunct=False, is_closed=False,
                               get_request_id=lambda: request_id,
                               send_msg=Mock(side_effect=send_msg))
        non_idle_connection = Mock(spec=Connection, in_flight=0, is_idle=False, is_defunct=False, is_closed=False)

        get_holders = self.make_get_holders(1)
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(idle_connection)
        holder.get_connections.return_value.append(non_idle_connection)

        self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert idle_connection.in_flight == 0
        assert non_idle_connection.in_flight == 0

        idle_connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        assert non_idle_connection.send_msg.call_count == 0

    def test_closed_defunct(self, *args):
        get_holders = self.make_get_holders(1)
        closed_connection = Mock(spec=Connection, in_flight=0, is_idle=False, is_defunct=False, is_closed=True)
        defunct_connection = Mock(spec=Connection, in_flight=0, is_idle=False, is_defunct=True, is_closed=False)
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(closed_connection)
        holder.get_connections.return_value.append(defunct_connection)

        self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert closed_connection.in_flight == 0
        assert defunct_connection.in_flight == 0
        assert closed_connection.send_msg.call_count == 0
        assert defunct_connection.send_msg.call_count == 0

    def test_no_req_ids(self, *args):
        in_flight = 3

        get_holders = self.make_get_holders(1)
        max_connection = Mock(spec=Connection, host='localhost',
                              lock=Lock(),
                              max_request_id=in_flight - 1, in_flight=in_flight,
                              is_idle=True, is_defunct=False, is_closed=False)
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(max_connection)

        self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert max_connection.in_flight == in_flight
        assert max_connection.send_msg.call_count == 0
        assert max_connection.send_msg.call_count == 0
        max_connection.defunct.assert_has_calls([call(ANY)] * get_holders.call_count)
        holder.return_connection.assert_has_calls(
            [call(max_connection)] * get_holders.call_count)

    def test_unexpected_response(self, *args):
        request_id = 999

        get_holders = self.make_get_holders(1)

        def send_msg(msg, req_id, msg_callback):
            msg_callback(object())

        connection = Mock(spec=Connection, host='localhost',
                          max_request_id=127,
                          lock=Lock(),
                          in_flight=0, is_idle=True,
                          is_defunct=False, is_closed=False,
                          get_request_id=lambda: request_id,
                          send_msg=Mock(side_effect=send_msg))
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(connection)

        self.run_heartbeat(get_holders)

        assert connection.in_flight == get_holders.call_count
        connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        connection.defunct.assert_has_calls([call(ANY)] * get_holders.call_count)
        exc = connection.defunct.call_args_list[0][0][0]
        assert isinstance(exc, ConnectionException)
        self.assertRegex(exc.args[0], r'^Received unexpected response to OptionsMessage.*')
        holder.return_connection.assert_has_calls(
            [call(connection)] * get_holders.call_count)

    def test_timeout(self, *args):
        request_id = 999

        get_holders = self.make_get_holders(1)

        def send_msg(msg, req_id, msg_callback):
            pass

        # we used endpoint=X here because it's a mock and we need connection.endpoint to be set
        connection = Mock(spec=Connection, endpoint=DefaultEndPoint('localhost'),
                          max_request_id=127,
                          lock=Lock(),
                          in_flight=0, is_idle=True,
                          is_defunct=False, is_closed=False,
                          get_request_id=lambda: request_id,
                          send_msg=Mock(side_effect=send_msg))
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(connection)

        self.run_heartbeat(get_holders)

        assert connection.in_flight == get_holders.call_count
        connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        connection.defunct.assert_has_calls([call(ANY)] * get_holders.call_count)
        exc = connection.defunct.call_args_list[0][0][0]
        assert isinstance(exc, OperationTimedOut)
        assert exc.errors == 'Connection heartbeat timeout after 0.05 seconds'
        assert exc.last_host == DefaultEndPoint('localhost')
        holder.return_connection.assert_has_calls(
            [call(connection)] * get_holders.call_count)


class TimerTest(unittest.TestCase):

    def test_timer_collision(self):
        # simple test demonstrating #466
        # same timeout, comparison will defer to the Timer object itself
        t1 = Timer(0, lambda: None)
        t2 = Timer(0, lambda: None)
        t2.end = t1.end

        tm = TimerManager()
        tm.add_timer(t1)
        tm.add_timer(t2)
        # Prior to #466: "TypeError: unorderable types: Timer() < Timer()"
        tm.service_timeouts()


class DefaultEndPointTest(unittest.TestCase):

    def test_default_endpoint_properties(self):
        endpoint = DefaultEndPoint('10.0.0.1')
        assert endpoint.address == '10.0.0.1'
        assert endpoint.port == 9042
        assert str(endpoint) == '10.0.0.1:9042'

        endpoint = DefaultEndPoint('10.0.0.1', 8888)
        assert endpoint.address == '10.0.0.1'
        assert endpoint.port == 8888
        assert str(endpoint) == '10.0.0.1:8888'

    def test_endpoint_equality(self):
        assert DefaultEndPoint('10.0.0.1') == DefaultEndPoint('10.0.0.1')

        assert DefaultEndPoint('10.0.0.1') == DefaultEndPoint('10.0.0.1', 9042)

        assert DefaultEndPoint('10.0.0.1') != DefaultEndPoint('10.0.0.2')

        assert DefaultEndPoint('10.0.0.1') != DefaultEndPoint('10.0.0.1', 0000)

    def test_endpoint_resolve(self):
        assert DefaultEndPoint('10.0.0.1').resolve() == ('10.0.0.1', 9042)

        assert DefaultEndPoint('10.0.0.1', 3232).resolve() == ('10.0.0.1', 3232)


class TestShardawarePortGenerator(unittest.TestCase):
    @patch('random.randrange')
    def test_generate_ports_basic(self, mock_randrange):
        mock_randrange.return_value = 10005
        gen = ShardAwarePortGenerator(10000, 10020)
        ports = list(itertools.islice(gen.generate(shard_id=1, total_shards=3), 5))

        # Starting from aligned 10005 + shard_id (1), step by 3
        assert ports == [10006, 10009, 10012, 10015, 10018]

    @patch('random.randrange')
    def test_wraps_around_to_start(self, mock_randrange):
        mock_randrange.return_value = 10008
        gen = ShardAwarePortGenerator(10000, 10020)
        ports = list(itertools.islice(gen.generate(shard_id=2, total_shards=4), 5))

        # Expected wrap-around from start_port after end_port is exceeded
        assert ports == [10010, 10014, 10018, 10002, 10006]

    @patch('random.randrange')
    def test_all_ports_have_correct_modulo(self, mock_randrange):
        mock_randrange.return_value = 10012
        total_shards = 5
        shard_id = 3
        gen = ShardAwarePortGenerator(10000, 10020)

        for port in gen.generate(shard_id=shard_id, total_shards=total_shards):
            assert port % total_shards == shard_id

    @patch('random.randrange')
    def test_generate_is_repeatable_with_same_mock(self, mock_randrange):
        mock_randrange.return_value = 10010
        gen = ShardAwarePortGenerator(10000, 10020)

        first_run = list(itertools.islice(gen.generate(0, 2), 5))
        second_run = list(itertools.islice(gen.generate(0, 2), 5))

        assert first_run == second_run
