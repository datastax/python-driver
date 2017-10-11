# Copyright 2013-2017 DataStax, Inc.
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

from __future__ import absolute_import  # to enable import io from stdlib
from collections import defaultdict, deque
import errno
from functools import wraps, partial
from heapq import heappush, heappop
import io
import logging
import six
from six.moves import range
import socket
import struct
import sys
from threading import Thread, Event, RLock
import time

try:
    import ssl
except ImportError:
    ssl = None  # NOQA

if 'gevent.monkey' in sys.modules:
    from gevent.queue import Queue, Empty
else:
    from six.moves.queue import Queue, Empty  # noqa

from cassandra import ConsistencyLevel, AuthenticationFailed, OperationTimedOut, ProtocolVersion
from cassandra.marshal import int32_pack
from cassandra.protocol import (ReadyMessage, AuthenticateMessage, OptionsMessage,
                                StartupMessage, ErrorMessage, CredentialsMessage,
                                QueryMessage, ResultMessage, ProtocolHandler,
                                InvalidRequestException, SupportedMessage,
                                AuthResponseMessage, AuthChallengeMessage,
                                AuthSuccessMessage, ProtocolException,
                                RegisterMessage)
from cassandra.util import OrderedDict


log = logging.getLogger(__name__)

# We use an ordered dictionary and specifically add lz4 before
# snappy so that lz4 will be preferred. Changing the order of this
# will change the compression preferences for the driver.
locally_supported_compressions = OrderedDict()

try:
    import lz4
except ImportError:
    pass
else:
    # The compress and decompress functions we need were moved from the lz4 to
    # the lz4.block namespace, so we try both here.
    try:
        lz4_block = lz4.block
    except AttributeError:
        lz4_block = lz4

    # Cassandra writes the uncompressed message length in big endian order,
    # but the lz4 lib requires little endian order, so we wrap these
    # functions to handle that

    def lz4_compress(byts):
        # write length in big-endian instead of little-endian
        return int32_pack(len(byts)) + lz4_block.compress(byts)[4:]

    def lz4_decompress(byts):
        # flip from big-endian to little-endian
        return lz4_block.decompress(byts[3::-1] + byts[4:])

    locally_supported_compressions['lz4'] = (lz4_compress, lz4_decompress)

try:
    import snappy
except ImportError:
    pass
else:
    # work around apparently buggy snappy decompress
    def decompress(byts):
        if byts == '\x00':
            return ''
        return snappy.decompress(byts)
    locally_supported_compressions['snappy'] = (snappy.compress, decompress)


PROTOCOL_VERSION_MASK = 0x7f

HEADER_DIRECTION_FROM_CLIENT = 0x00
HEADER_DIRECTION_TO_CLIENT = 0x80
HEADER_DIRECTION_MASK = 0x80

frame_header_v1_v2 = struct.Struct('>BbBi')
frame_header_v3 = struct.Struct('>BhBi')


class _Frame(object):
    def __init__(self, version, flags, stream, opcode, body_offset, end_pos):
        self.version = version
        self.flags = flags
        self.stream = stream
        self.opcode = opcode
        self.body_offset = body_offset
        self.end_pos = end_pos

    def __eq__(self, other):  # facilitates testing
        if isinstance(other, _Frame):
            return (self.version == other.version and
                    self.flags == other.flags and
                    self.stream == other.stream and
                    self.opcode == other.opcode and
                    self.body_offset == other.body_offset and
                    self.end_pos == other.end_pos)
        return NotImplemented

    def __str__(self):
        return "ver({0}); flags({1:04b}); stream({2}); op({3}); offset({4}); len({5})".format(self.version, self.flags, self.stream, self.opcode, self.body_offset, self.end_pos - self.body_offset)



NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)


class ConnectionException(Exception):
    """
    An unrecoverable error was hit when attempting to use a connection,
    or the connection was already closed or defunct.
    """

    def __init__(self, message, host=None):
        Exception.__init__(self, message)
        self.host = host


class ConnectionShutdown(ConnectionException):
    """
    Raised when a connection has been marked as defunct or has been closed.
    """
    pass


class ProtocolVersionUnsupported(ConnectionException):
    """
    Server rejected startup message due to unsupported protocol version
    """
    def __init__(self, host, startup_version):
        msg = "Unsupported protocol version on %s: %d" % (host, startup_version)
        super(ProtocolVersionUnsupported, self).__init__(msg, host)
        self.startup_version = startup_version


class ConnectionBusy(Exception):
    """
    An attempt was made to send a message through a :class:`.Connection` that
    was already at the max number of in-flight operations.
    """
    pass


class ProtocolError(Exception):
    """
    Communication did not match the protocol that this driver expects.
    """
    pass


def defunct_on_error(f):

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception as exc:
            self.defunct(exc)
    return wrapper


DEFAULT_CQL_VERSION = '3.0.0'

if six.PY3:
    def int_from_buf_item(i):
        return i
else:
    int_from_buf_item = ord


class Connection(object):

    CALLBACK_ERR_THREAD_THRESHOLD = 100

    in_buffer_size = 4096
    out_buffer_size = 4096

    cql_version = None
    protocol_version = ProtocolVersion.MAX_SUPPORTED

    keyspace = None
    compression = True
    compressor = None
    decompressor = None

    ssl_options = None
    last_error = None

    # The current number of operations that are in flight. More precisely,
    # the number of request IDs that are currently in use.
    in_flight = 0

    # Max concurrent requests allowed per connection. This is set optimistically high, allowing
    # all request ids to be used in protocol version 3+. Normally concurrency would be controlled
    # at a higher level by the application or concurrent.execute_concurrent. This attribute
    # is for lower-level integrations that want some upper bound without reimplementing.
    max_in_flight = 2 ** 15

    # A set of available request IDs.  When using the v3 protocol or higher,
    # this will not initially include all request IDs in order to save memory,
    # but the set will grow if it is exhausted.
    request_ids = None

    # Tracks the highest used request ID in order to help with growing the
    # request_ids set
    highest_request_id = 0

    is_defunct = False
    is_closed = False
    lock = None
    user_type_map = None

    msg_received = False

    is_unsupported_proto_version = False

    is_control_connection = False
    signaled_error = False  # used for flagging at the pool level

    allow_beta_protocol_version = False

    _iobuf = None
    _current_frame = None

    _socket = None

    _socket_impl = socket
    _ssl_impl = ssl

    _check_hostname = False

    def __init__(self, host='127.0.0.1', port=9042, authenticator=None,
                 ssl_options=None, sockopts=None, compression=True,
                 cql_version=None, protocol_version=ProtocolVersion.MAX_SUPPORTED, is_control_connection=False,
                 user_type_map=None, connect_timeout=None, allow_beta_protocol_version=False):
        self.host = host
        self.port = port
        self.authenticator = authenticator
        self.ssl_options = ssl_options.copy() if ssl_options else None
        self.sockopts = sockopts
        self.compression = compression
        self.cql_version = cql_version
        self.protocol_version = protocol_version
        self.is_control_connection = is_control_connection
        self.user_type_map = user_type_map
        self.connect_timeout = connect_timeout
        self.allow_beta_protocol_version = allow_beta_protocol_version
        self._push_watchers = defaultdict(set)
        self._requests = {}
        self._iobuf = io.BytesIO()

        if ssl_options:
            self._check_hostname = bool(self.ssl_options.pop('check_hostname', False))
            if self._check_hostname:
                if not getattr(ssl, 'match_hostname', None):
                    raise RuntimeError("ssl_options specify 'check_hostname', but ssl.match_hostname is not provided. "
                                       "Patch or upgrade Python to use this option.")

        if protocol_version >= 3:
            self.max_request_id = min(self.max_in_flight - 1, (2 ** 15) - 1)
            # Don't fill the deque with 2**15 items right away. Start with some and add
            # more if needed.
            initial_size = min(300, self.max_in_flight)
            self.request_ids = deque(range(initial_size))
            self.highest_request_id = initial_size - 1
        else:
            self.max_request_id = min(self.max_in_flight, (2 ** 7) - 1)
            self.request_ids = deque(range(self.max_request_id + 1))
            self.highest_request_id = self.max_request_id

        self.lock = RLock()
        self.connected_event = Event()

    @classmethod
    def initialize_reactor(cls):
        """
        Called once by Cluster.connect().  This should be used by implementations
        to set up any resources that will be shared across connections.
        """
        pass

    @classmethod
    def handle_fork(cls):
        """
        Called after a forking.  This should cleanup any remaining reactor state
        from the parent process.
        """
        pass

    @classmethod
    def create_timer(cls, timeout, callback):
        raise NotImplementedError()

    @classmethod
    def factory(cls, host, timeout, *args, **kwargs):
        """
        A factory function which returns connections which have
        succeeded in connecting and are ready for service (or
        raises an exception otherwise).
        """
        start = time.time()
        kwargs['connect_timeout'] = timeout
        conn = cls(host, *args, **kwargs)
        elapsed = time.time() - start
        conn.connected_event.wait(timeout - elapsed)
        if conn.last_error:
            if conn.is_unsupported_proto_version:
                raise ProtocolVersionUnsupported(host, conn.protocol_version)
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise OperationTimedOut("Timed out creating connection (%s seconds)" % timeout)
        else:
            return conn

    def _connect_socket(self):
        sockerr = None
        addresses = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        if not addresses:
            raise ConnectionException("getaddrinfo returned empty list for %s" % (self.host,))
        for (af, socktype, proto, canonname, sockaddr) in addresses:
            try:
                self._socket = self._socket_impl.socket(af, socktype, proto)
                if self.ssl_options:
                    if not self._ssl_impl:
                        raise RuntimeError("This version of Python was not compiled with SSL support")
                    self._socket = self._ssl_impl.wrap_socket(self._socket, **self.ssl_options)
                self._socket.settimeout(self.connect_timeout)
                self._socket.connect(sockaddr)
                self._socket.settimeout(None)
                if self._check_hostname:
                    ssl.match_hostname(self._socket.getpeercert(), self.host)
                sockerr = None
                break
            except socket.error as err:
                if self._socket:
                    self._socket.close()
                    self._socket = None
                sockerr = err

        if sockerr:
            raise socket.error(sockerr.errno, "Tried connecting to %s. Last error: %s" % ([a[4] for a in addresses], sockerr.strerror or sockerr))

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

    def close(self):
        raise NotImplementedError()

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct or self.is_closed:
                return
            self.is_defunct = True

        exc_info = sys.exc_info()
        # if we are not handling an exception, just use the passed exception, and don't try to format exc_info with the message
        if any(exc_info):
            log.debug("Defuncting connection (%s) to %s:",
                      id(self), self.host, exc_info=exc_info)
        else:
            log.debug("Defuncting connection (%s) to %s: %s",
                      id(self), self.host, exc)

        self.last_error = exc
        self.close()
        self.error_all_requests(exc)
        self.connected_event.set()
        return exc

    def error_all_requests(self, exc):
        with self.lock:
            requests = self._requests
            self._requests = {}

        if not requests:
            return

        new_exc = ConnectionShutdown(str(exc))
        def try_callback(cb):
            try:
                cb(new_exc)
            except Exception:
                log.warning("Ignoring unhandled exception while erroring requests for a "
                            "failed connection (%s) to host %s:",
                            id(self), self.host, exc_info=True)

        # run first callback from this thread to ensure pool state before leaving
        cb, _, _ = requests.popitem()[1]
        try_callback(cb)

        if not requests:
            return

        # additional requests are optionally errored from a separate thread
        # The default callback and retry logic is fairly expensive -- we don't
        # want to tie up the event thread when there are many requests
        def err_all_callbacks():
            for cb, _, _ in requests.values():
                try_callback(cb)
        if len(requests) < Connection.CALLBACK_ERR_THREAD_THRESHOLD:
            err_all_callbacks()
        else:
            # daemon thread here because we want to stay decoupled from the cluster TPE
            # TODO: would it make sense to just have a driver-global TPE?
            t = Thread(target=err_all_callbacks)
            t.daemon = True
            t.start()

    def get_request_id(self):
        """
        This must be called while self.lock is held.
        """
        try:
            return self.request_ids.popleft()
        except IndexError:
            new_request_id = self.highest_request_id + 1
            # in_flight checks should guarantee this
            assert new_request_id <= self.max_request_id
            self.highest_request_id = new_request_id
            return self.highest_request_id

    def handle_pushed(self, response):
        log.debug("Message pushed from server: %r", response)
        for cb in self._push_watchers.get(response.event_type, []):
            try:
                cb(response.event_args)
            except Exception:
                log.exception("Pushed event handler errored, ignoring:")

    def send_msg(self, msg, request_id, cb, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=None):
        if self.is_defunct:
            raise ConnectionShutdown("Connection to %s is defunct" % self.host)
        elif self.is_closed:
            raise ConnectionShutdown("Connection to %s is closed" % self.host)

        # queue the decoder function with the request
        # this allows us to inject custom functions per request to encode, decode messages
        self._requests[request_id] = (cb, decoder, result_metadata)
        msg = encoder(msg, request_id, self.protocol_version, compressor=self.compressor, allow_beta_protocol_version=self.allow_beta_protocol_version)
        self.push(msg)
        return len(msg)

    def wait_for_response(self, msg, timeout=None):
        return self.wait_for_responses(msg, timeout=timeout)[0]

    def wait_for_responses(self, *msgs, **kwargs):
        """
        Returns a list of (success, response) tuples.  If success
        is False, response will be an Exception.  Otherwise, response
        will be the normal query response.

        If fail_on_error was left as True and one of the requests
        failed, the corresponding Exception will be raised.
        """
        if self.is_closed or self.is_defunct:
            raise ConnectionShutdown("Connection %s is already closed" % (self, ))
        timeout = kwargs.get('timeout')
        fail_on_error = kwargs.get('fail_on_error', True)
        waiter = ResponseWaiter(self, len(msgs), fail_on_error)

        # busy wait for sufficient space on the connection
        messages_sent = 0
        while True:
            needed = len(msgs) - messages_sent
            with self.lock:
                available = min(needed, self.max_request_id - self.in_flight + 1)
                request_ids = [self.get_request_id() for _ in range(available)]
                self.in_flight += available

            for i, request_id in enumerate(request_ids):
                self.send_msg(msgs[messages_sent + i],
                              request_id,
                              partial(waiter.got_response, index=messages_sent + i))
            messages_sent += available

            if messages_sent == len(msgs):
                break
            else:
                if timeout is not None:
                    timeout -= 0.01
                    if timeout <= 0.0:
                        raise OperationTimedOut()
                time.sleep(0.01)

        try:
            return waiter.deliver(timeout)
        except OperationTimedOut:
            raise
        except Exception as exc:
            self.defunct(exc)
            raise

    def register_watcher(self, event_type, callback, register_timeout=None):
        """
        Register a callback for a given event type.
        """
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]),
            timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        """
        Register multiple callback/event type pairs, expressed as a dict.
        """
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()),
            timeout=register_timeout)

    def control_conn_disposed(self):
        self.is_control_connection = False
        self._push_watchers = {}

    @defunct_on_error
    def _read_frame_header(self):
        buf = self._iobuf.getvalue()
        pos = len(buf)
        if pos:
            version = int_from_buf_item(buf[0]) & PROTOCOL_VERSION_MASK
            if version > ProtocolVersion.MAX_SUPPORTED:
                raise ProtocolError("This version of the driver does not support protocol version %d" % version)
            frame_header = frame_header_v3 if version >= 3 else frame_header_v1_v2
            # this frame header struct is everything after the version byte
            header_size = frame_header.size + 1
            if pos >= header_size:
                flags, stream, op, body_len = frame_header.unpack_from(buf, 1)
                if body_len < 0:
                    raise ProtocolError("Received negative body length: %r" % body_len)
                self._current_frame = _Frame(version, flags, stream, op, header_size, body_len + header_size)
        return pos

    def _reset_frame(self):
        self._iobuf = io.BytesIO(self._iobuf.read())
        self._iobuf.seek(0, 2)  # io.SEEK_END == 2 (constant not present in 2.6)
        self._current_frame = None

    def process_io_buffer(self):
        while True:
            if not self._current_frame:
                pos = self._read_frame_header()
            else:
                pos = self._iobuf.tell()

            if not self._current_frame or pos < self._current_frame.end_pos:
                # we don't have a complete header yet or we
                # already saw a header, but we don't have a
                # complete message yet
                return
            else:
                frame = self._current_frame
                self._iobuf.seek(frame.body_offset)
                msg = self._iobuf.read(frame.end_pos - frame.body_offset)
                self.process_msg(frame, msg)
                self._reset_frame()

    @defunct_on_error
    def process_msg(self, header, body):
        self.msg_received = True
        stream_id = header.stream
        if stream_id < 0:
            callback = None
            decoder = ProtocolHandler.decode_message
            result_metadata = None
        else:
            try:
                callback, decoder, result_metadata = self._requests.pop(stream_id)
            # This can only happen if the stream_id was
            # removed due to an OperationTimedOut
            except KeyError:
                return

            with self.lock:
                self.request_ids.append(stream_id)

        try:
            response = decoder(header.version, self.user_type_map, stream_id,
                               header.flags, header.opcode, body, self.decompressor, result_metadata)
        except Exception as exc:
            log.exception("Error decoding response from Cassandra. "
                          "%s; buffer: %r", header, self._iobuf.getvalue())
            if callback is not None:
                callback(exc)
            self.defunct(exc)
            return

        try:
            if stream_id >= 0:
                if isinstance(response, ProtocolException):
                    if 'unsupported protocol version' in response.message:
                        self.is_unsupported_proto_version = True
                    else:
                        log.error("Closing connection %s due to protocol error: %s", self, response.summary_msg())
                    self.defunct(response)
                if callback is not None:
                    callback(response)
            else:
                self.handle_pushed(response)
        except Exception:
            log.exception("Callback handler errored, ignoring:")

    @defunct_on_error
    def _send_options_message(self):
        if self.cql_version is None and (not self.compression or not locally_supported_compressions):
            log.debug("Not sending options message for new connection(%s) to %s "
                      "because compression is disabled and a cql version was not "
                      "specified", id(self), self.host)
            self._compressor = None
            self.cql_version = DEFAULT_CQL_VERSION
            self._send_startup_message()
        else:
            log.debug("Sending initial options message for new connection (%s) to %s", id(self), self.host)
            self.send_msg(OptionsMessage(), self.get_request_id(), self._handle_options_response)

    @defunct_on_error
    def _handle_options_response(self, options_response):
        if self.is_defunct:
            return

        if not isinstance(options_response, SupportedMessage):
            if isinstance(options_response, ConnectionException):
                raise options_response
            else:
                log.error("Did not get expected SupportedMessage response; "
                          "instead, got: %s", options_response)
                raise ConnectionException("Did not get expected SupportedMessage "
                                          "response; instead, got: %s"
                                          % (options_response,))

        log.debug("Received options response on new connection (%s) from %s",
                  id(self), self.host)
        supported_cql_versions = options_response.cql_versions
        remote_supported_compressions = options_response.options['COMPRESSION']

        if self.cql_version:
            if self.cql_version not in supported_cql_versions:
                raise ProtocolError(
                    "cql_version %r is not supported by remote (w/ native "
                    "protocol). Supported versions: %r"
                    % (self.cql_version, supported_cql_versions))
        else:
            self.cql_version = supported_cql_versions[0]

        self._compressor = None
        compression_type = None
        if self.compression:
            overlap = (set(locally_supported_compressions.keys()) &
                       set(remote_supported_compressions))
            if len(overlap) == 0:
                log.debug("No available compression types supported on both ends."
                          " locally supported: %r. remotely supported: %r",
                          locally_supported_compressions.keys(),
                          remote_supported_compressions)
            else:
                compression_type = None
                if isinstance(self.compression, six.string_types):
                    # the user picked a specific compression type ('snappy' or 'lz4')
                    if self.compression not in remote_supported_compressions:
                        raise ProtocolError(
                            "The requested compression type (%s) is not supported by the Cassandra server at %s"
                            % (self.compression, self.host))
                    compression_type = self.compression
                else:
                    # our locally supported compressions are ordered to prefer
                    # lz4, if available
                    for k in locally_supported_compressions.keys():
                        if k in overlap:
                            compression_type = k
                            break

                # set the decompressor here, but set the compressor only after
                # a successful Ready message
                self._compressor, self.decompressor = \
                    locally_supported_compressions[compression_type]

        self._send_startup_message(compression_type)

    @defunct_on_error
    def _send_startup_message(self, compression=None):
        log.debug("Sending StartupMessage on %s", self)
        opts = {}
        if compression:
            opts['COMPRESSION'] = compression
        sm = StartupMessage(cqlversion=self.cql_version, options=opts)
        self.send_msg(sm, self.get_request_id(), cb=self._handle_startup_response)
        log.debug("Sent StartupMessage on %s", self)

    @defunct_on_error
    def _handle_startup_response(self, startup_response, did_authenticate=False):
        if self.is_defunct:
            return
        if isinstance(startup_response, ReadyMessage):
            log.debug("Got ReadyMessage on new connection (%s) from %s", id(self), self.host)
            if self._compressor:
                self.compressor = self._compressor
            self.connected_event.set()
        elif isinstance(startup_response, AuthenticateMessage):
            log.debug("Got AuthenticateMessage on new connection (%s) from %s: %s",
                      id(self), self.host, startup_response.authenticator)

            if self.authenticator is None:
                raise AuthenticationFailed('Remote end requires authentication.')

            if isinstance(self.authenticator, dict):
                log.debug("Sending credentials-based auth response on %s", self)
                cm = CredentialsMessage(creds=self.authenticator)
                callback = partial(self._handle_startup_response, did_authenticate=True)
                self.send_msg(cm, self.get_request_id(), cb=callback)
            else:
                log.debug("Sending SASL-based auth response on %s", self)
                self.authenticator.server_authenticator_class = startup_response.authenticator
                initial_response = self.authenticator.initial_response()
                initial_response = "" if initial_response is None else initial_response
                self.send_msg(AuthResponseMessage(initial_response), self.get_request_id(), self._handle_auth_response)
        elif isinstance(startup_response, ErrorMessage):
            log.debug("Received ErrorMessage on new connection (%s) from %s: %s",
                      id(self), self.host, startup_response.summary_msg())
            if did_authenticate:
                raise AuthenticationFailed(
                    "Failed to authenticate to %s: %s" %
                    (self.host, startup_response.summary_msg()))
            else:
                raise ConnectionException(
                    "Failed to initialize new connection to %s: %s"
                    % (self.host, startup_response.summary_msg()))
        elif isinstance(startup_response, ConnectionShutdown):
            log.debug("Connection to %s was closed during the startup handshake", (self.host))
            raise startup_response
        else:
            msg = "Unexpected response during Connection setup: %r"
            log.error(msg, startup_response)
            raise ProtocolError(msg % (startup_response,))

    @defunct_on_error
    def _handle_auth_response(self, auth_response):
        if self.is_defunct:
            return

        if isinstance(auth_response, AuthSuccessMessage):
            log.debug("Connection %s successfully authenticated", self)
            self.authenticator.on_authentication_success(auth_response.token)
            if self._compressor:
                self.compressor = self._compressor
            self.connected_event.set()
        elif isinstance(auth_response, AuthChallengeMessage):
            response = self.authenticator.evaluate_challenge(auth_response.challenge)
            msg = AuthResponseMessage("" if response is None else response)
            log.debug("Responding to auth challenge on %s", self)
            self.send_msg(msg, self.get_request_id(), self._handle_auth_response)
        elif isinstance(auth_response, ErrorMessage):
            log.debug("Received ErrorMessage on new connection (%s) from %s: %s",
                      id(self), self.host, auth_response.summary_msg())
            raise AuthenticationFailed(
                "Failed to authenticate to %s: %s" %
                (self.host, auth_response.summary_msg()))
        elif isinstance(auth_response, ConnectionShutdown):
            log.debug("Connection to %s was closed during the authentication process", self.host)
            raise auth_response
        else:
            msg = "Unexpected response during Connection authentication to %s: %r"
            log.error(msg, self.host, auth_response)
            raise ProtocolError(msg % (self.host, auth_response))

    def set_keyspace_blocking(self, keyspace):
        if not keyspace or keyspace == self.keyspace:
            return

        query = QueryMessage(query='USE "%s"' % (keyspace,),
                             consistency_level=ConsistencyLevel.ONE)
        try:
            result = self.wait_for_response(query)
        except InvalidRequestException as ire:
            # the keyspace probably doesn't exist
            raise ire.to_exception()
        except Exception as exc:
            conn_exc = ConnectionException(
                "Problem while setting keyspace: %r" % (exc,), self.host)
            self.defunct(conn_exc)
            raise conn_exc

        if isinstance(result, ResultMessage):
            self.keyspace = keyspace
        else:
            conn_exc = ConnectionException(
                "Problem while setting keyspace: %r" % (result,), self.host)
            self.defunct(conn_exc)
            raise conn_exc

    def set_keyspace_async(self, keyspace, callback):
        """
        Use this in order to avoid deadlocking the event loop thread.
        When the operation completes, `callback` will be called with
        two arguments: this connection and an Exception if an error
        occurred, otherwise :const:`None`.

        This method will always increment :attr:`.in_flight` attribute, even if
        it doesn't need to make a request, just to maintain an
        ":attr:`.in_flight` is incremented" invariant.
        """
        # Here we increment in_flight unconditionally, whether we need to issue
        # a request or not. This is bad, but allows callers -- specifically
        # _set_keyspace_for_all_conns -- to assume that we increment
        # self.in_flight during this call. This allows the passed callback to
        # safely call HostConnection{Pool,}.return_connection on this
        # Connection.
        #
        # We use a busy wait on the lock here because:
        # - we'll only spin if the connection is at max capacity, which is very
        #   unlikely for a set_keyspace call
        # - it allows us to avoid signaling a condition every time a request completes
        while True:
            with self.lock:
                if self.in_flight < self.max_request_id:
                    self.in_flight += 1
                    break
            time.sleep(0.001)

        if not keyspace or keyspace == self.keyspace:
            callback(self, None)
            return

        query = QueryMessage(query='USE "%s"' % (keyspace,),
                             consistency_level=ConsistencyLevel.ONE)

        def process_result(result):
            if isinstance(result, ResultMessage):
                self.keyspace = keyspace
                callback(self, None)
            elif isinstance(result, InvalidRequestException):
                callback(self, result.to_exception())
            else:
                callback(self, self.defunct(ConnectionException(
                    "Problem while setting keyspace: %r" % (result,), self.host)))

        # We've incremented self.in_flight above, so we "have permission" to
        # acquire a new request id
        request_id = self.get_request_id()

        self.send_msg(query, request_id, process_result)

    @property
    def is_idle(self):
        return not self.msg_received

    def reset_idle(self):
        self.msg_received = False

    def __str__(self):
        status = ""
        if self.is_defunct:
            status = " (defunct)"
        elif self.is_closed:
            status = " (closed)"

        return "<%s(%r) %s:%d%s>" % (self.__class__.__name__, id(self), self.host, self.port, status)
    __repr__ = __str__


class ResponseWaiter(object):

    def __init__(self, connection, num_responses, fail_on_error):
        self.connection = connection
        self.pending = num_responses
        self.fail_on_error = fail_on_error
        self.error = None
        self.responses = [None] * num_responses
        self.event = Event()

    def got_response(self, response, index):
        with self.connection.lock:
            self.connection.in_flight -= 1
        if isinstance(response, Exception):
            if hasattr(response, 'to_exception'):
                response = response.to_exception()
            if self.fail_on_error:
                self.error = response
                self.event.set()
            else:
                self.responses[index] = (False, response)
        else:
            if not self.fail_on_error:
                self.responses[index] = (True, response)
            else:
                self.responses[index] = response

        self.pending -= 1
        if not self.pending:
            self.event.set()

    def deliver(self, timeout=None):
        """
        If fail_on_error was set to False, a list of (success, response)
        tuples will be returned.  If success is False, response will be
        an Exception.  Otherwise, response will be the normal query response.

        If fail_on_error was left as True and one of the requests
        failed, the corresponding Exception will be raised. Otherwise,
        the normal response will be returned.
        """
        self.event.wait(timeout)
        if self.error:
            raise self.error
        elif not self.event.is_set():
            raise OperationTimedOut()
        else:
            return self.responses


class HeartbeatFuture(object):
    def __init__(self, connection, owner):
        self._exception = None
        self._event = Event()
        self.connection = connection
        self.owner = owner
        log.debug("Sending options message heartbeat on idle connection (%s) %s",
                  id(connection), connection.host)
        with connection.lock:
            if connection.in_flight <= connection.max_request_id:
                connection.in_flight += 1
                connection.send_msg(OptionsMessage(), connection.get_request_id(), self._options_callback)
            else:
                self._exception = Exception("Failed to send heartbeat because connection 'in_flight' exceeds threshold")
                self._event.set()

    def wait(self, timeout):
        self._event.wait(timeout)
        if self._event.is_set():
            if self._exception:
                raise self._exception
        else:
            raise OperationTimedOut("Connection heartbeat timeout after %s seconds" % (timeout,), self.connection.host)

    def _options_callback(self, response):
        if isinstance(response, SupportedMessage):
            log.debug("Received options response on connection (%s) from %s",
                      id(self.connection), self.connection.host)
        else:
            if isinstance(response, ConnectionException):
                self._exception = response
            else:
                self._exception = ConnectionException("Received unexpected response to OptionsMessage: %s"
                                                      % (response,))
        self._event.set()


class ConnectionHeartbeat(Thread):

    def __init__(self, interval_sec, get_connection_holders, timeout):
        Thread.__init__(self, name="Connection heartbeat")
        self._interval = interval_sec
        self._timeout = timeout
        self._get_connection_holders = get_connection_holders
        self._shutdown_event = Event()
        self.daemon = True
        self.start()

    class ShutdownException(Exception):
        pass

    def run(self):
        self._shutdown_event.wait(self._interval)
        while not self._shutdown_event.is_set():
            start_time = time.time()

            futures = []
            failed_connections = []
            try:
                for connections, owner in [(o.get_connections(), o) for o in self._get_connection_holders()]:
                    for connection in connections:
                        self._raise_if_stopped()
                        if not (connection.is_defunct or connection.is_closed):
                            if connection.is_idle:
                                try:
                                    futures.append(HeartbeatFuture(connection, owner))
                                except Exception as e:
                                    log.warning("Failed sending heartbeat message on connection (%s) to %s",
                                                id(connection), connection.host)
                                    failed_connections.append((connection, owner, e))
                            else:
                                connection.reset_idle()
                        else:
                            log.debug("Cannot send heartbeat message on connection (%s) to %s",
                                      id(connection), connection.host)
                            # make sure the owner sees this defunt/closed connection
                            owner.return_connection(connection)
                    self._raise_if_stopped()

                # Wait max `self._timeout` seconds for all HeartbeatFutures to complete
                timeout = self._timeout
                start_time = time.time()
                for f in futures:
                    self._raise_if_stopped()
                    connection = f.connection
                    try:
                        f.wait(timeout)
                        # TODO: move this, along with connection locks in pool, down into Connection
                        with connection.lock:
                            connection.in_flight -= 1
                        connection.reset_idle()
                    except Exception as e:
                        log.warning("Heartbeat failed for connection (%s) to %s",
                                    id(connection), connection.host)
                        failed_connections.append((f.connection, f.owner, e))

                    timeout = self._timeout - (time.time() - start_time)

                for connection, owner, exc in failed_connections:
                    self._raise_if_stopped()
                    if not connection.is_control_connection:
                        # Only HostConnection supports shutdown_on_error
                        owner.shutdown_on_error = True
                    connection.defunct(exc)
                    owner.return_connection(connection)
            except self.ShutdownException:
                pass
            except Exception:
                log.error("Failed connection heartbeat", exc_info=True)

            elapsed = time.time() - start_time
            self._shutdown_event.wait(max(self._interval - elapsed, 0.01))

    def stop(self):
        self._shutdown_event.set()
        self.join()

    def _raise_if_stopped(self):
        if self._shutdown_event.is_set():
            raise self.ShutdownException()


class Timer(object):

    canceled = False

    def __init__(self, timeout, callback):
        self.end = time.time() + timeout
        self.callback = callback

    def __lt__(self, other):
        return self.end < other.end

    def cancel(self):
        self.canceled = True

    def finish(self, time_now):
        if self.canceled:
            return True

        if time_now >= self.end:
            self.callback()
            return True

        return False


class TimerManager(object):

    def __init__(self):
        self._queue = []
        self._new_timers = []

    def add_timer(self, timer):
        """
        called from client thread with a Timer object
        """
        self._new_timers.append((timer.end, timer))

    def service_timeouts(self):
        """
        run callbacks on all expired timers
        Called from the event thread
        :return: next end time, or None
        """
        queue = self._queue
        if self._new_timers:
            new_timers = self._new_timers
            while new_timers:
                heappush(queue, new_timers.pop())

        if queue:
            now = time.time()
            while queue:
                try:
                    timer = queue[0][1]
                    if timer.finish(now):
                        heappop(queue)
                    else:
                        return timer.end
                except Exception:
                    log.exception("Exception while servicing timeout callback: ")

    @property
    def next_timeout(self):
        try:
            return self._queue[0][0]
        except IndexError:
            pass
