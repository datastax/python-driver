# Copyright 2013-2015 DataStax, Inc.
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
import io
import logging
import os
import sys
from threading import Thread, Event, RLock
import time

if 'gevent.monkey' in sys.modules:
    from gevent.queue import Queue, Empty
else:
    from six.moves.queue import Queue, Empty  # noqa

import six
from six.moves import range

from cassandra import ConsistencyLevel, AuthenticationFailed, OperationTimedOut
from cassandra.marshal import int32_pack, header_unpack, v3_header_unpack, int32_unpack
from cassandra.protocol import (ReadyMessage, AuthenticateMessage, OptionsMessage,
                                StartupMessage, ErrorMessage, CredentialsMessage,
                                QueryMessage, ResultMessage, decode_response,
                                InvalidRequestException, SupportedMessage,
                                AuthResponseMessage, AuthChallengeMessage,
                                AuthSuccessMessage, ProtocolException)
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

    # Cassandra writes the uncompressed message length in big endian order,
    # but the lz4 lib requires little endian order, so we wrap these
    # functions to handle that

    def lz4_compress(byts):
        # write length in big-endian instead of little-endian
        return int32_pack(len(byts)) + lz4.compress(byts)[4:]

    def lz4_decompress(byts):
        # flip from big-endian to little-endian
        return lz4.decompress(byts[3::-1] + byts[4:])

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


class Connection(object):

    in_buffer_size = 4096
    out_buffer_size = 4096

    cql_version = None
    protocol_version = 2

    keyspace = None
    compression = True
    compressor = None
    decompressor = None

    ssl_options = None
    last_error = None

    # The current number of operations that are in flight. More precisely,
    # the number of request IDs that are currently in use.
    in_flight = 0

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

    is_control_connection = False
    _iobuf = None

    def __init__(self, host='127.0.0.1', port=9042, authenticator=None,
                 ssl_options=None, sockopts=None, compression=True,
                 cql_version=None, protocol_version=2, is_control_connection=False,
                 user_type_map=None):
        self.host = host
        self.port = port
        self.authenticator = authenticator
        self.ssl_options = ssl_options
        self.sockopts = sockopts
        self.compression = compression
        self.cql_version = cql_version
        self.protocol_version = protocol_version
        self.is_control_connection = is_control_connection
        self.user_type_map = user_type_map
        self._push_watchers = defaultdict(set)
        self._iobuf = io.BytesIO()
        if protocol_version >= 3:
            self._header_unpack = v3_header_unpack
            self._header_length = 5
            self.max_request_id = (2 ** 15) - 1
            # Don't fill the deque with 2**15 items right away. Start with 300 and add
            # more if needed.
            self.request_ids = deque(range(300))
            self.highest_request_id = 299
        else:
            self._header_unpack = header_unpack
            self._header_length = 4
            self.max_request_id = (2 ** 7) - 1
            self.request_ids = deque(range(self.max_request_id + 1))
            self.highest_request_id = self.max_request_id

        # 0         8        16        24        32         40
        # +---------+---------+---------+---------+---------+
        # | version |  flags  |      stream       | opcode  |
        # +---------+---------+---------+---------+---------+
        # |                length                 |
        # +---------+---------+---------+---------+
        # |                                       |
        # .            ...  body ...              .
        # .                                       .
        # .                                       .
        # +----------------------------------------
        self._full_header_length = self._header_length + 4

        self.lock = RLock()

    @classmethod
    def initialize_reactor(self):
        """
        Called once by Cluster.connect().  This should be used by implementations
        to set up any resources that will be shared across connections.
        """
        pass

    @classmethod
    def handle_fork(self):
        """
        Called after a forking.  This should cleanup any remaining reactor state
        from the parent process.
        """
        pass

    def close(self):
        raise NotImplementedError()

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct or self.is_closed:
                return
            self.is_defunct = True

        log.debug("Defuncting connection (%s) to %s:",
                  id(self), self.host, exc_info=exc)

        self.last_error = exc
        self.close()
        self.error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def error_all_callbacks(self, exc):
        with self.lock:
            callbacks = self._callbacks
            self._callbacks = {}
        new_exc = ConnectionShutdown(str(exc))
        for cb in callbacks.values():
            try:
                cb(new_exc)
            except Exception:
                log.warning("Ignoring unhandled exception while erroring callbacks for a "
                            "failed connection (%s) to host %s:",
                            id(self), self.host, exc_info=True)

    def get_request_id(self):
        """
        This must be called while self.lock is held.
        """
        try:
            return self.request_ids.popleft()
        except IndexError:
            self.highest_request_id += 1
            # in_flight checks should guarantee this
            assert self.highest_request_id <= self.max_request_id
            return self.highest_request_id

    def handle_pushed(self, response):
        log.debug("Message pushed from server: %r", response)
        for cb in self._push_watchers.get(response.event_type, []):
            try:
                cb(response.event_args)
            except Exception:
                log.exception("Pushed event handler errored, ignoring:")

    def send_msg(self, msg, request_id, cb):
        if self.is_defunct:
            raise ConnectionShutdown("Connection to %s is defunct" % self.host)
        elif self.is_closed:
            raise ConnectionShutdown("Connection to %s is closed" % self.host)

        self._callbacks[request_id] = cb
        self.push(msg.to_binary(request_id, self.protocol_version, compression=self.compressor))
        return request_id

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
                available = min(needed, self.max_request_id - self.in_flight)
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

    def register_watcher(self, event_type, callback):
        raise NotImplementedError()

    def register_watchers(self, type_callback_dict):
        raise NotImplementedError()

    def control_conn_disposed(self):
        self.is_control_connection = False
        self._push_watchers = {}

    def process_io_buffer(self):
        while True:
            pos = self._iobuf.tell()
            if pos < self._full_header_length or (self._total_reqd_bytes > 0 and pos < self._total_reqd_bytes):
                # we don't have a complete header yet or we
                # already saw a header, but we don't have a
                # complete message yet
                return
            else:
                # have enough for header, read body len from header
                self._iobuf.seek(self._header_length)
                body_len = int32_unpack(self._iobuf.read(4))

                # seek to end to get length of current buffer
                self._iobuf.seek(0, os.SEEK_END)
                pos = self._iobuf.tell()

                if pos >= body_len + self._full_header_length:
                    # read message header and body
                    self._iobuf.seek(0)
                    msg = self._iobuf.read(self._full_header_length + body_len)

                    # leave leftover in current buffer
                    leftover = self._iobuf.read()
                    self._iobuf = io.BytesIO()
                    self._iobuf.write(leftover)

                    self._total_reqd_bytes = 0
                    self.process_msg(msg, body_len)
                else:
                    self._total_reqd_bytes = body_len + self._full_header_length
                    return

    @defunct_on_error
    def process_msg(self, msg, body_len):
        version, flags, stream_id, opcode = self._header_unpack(msg[:self._header_length])
        if stream_id < 0:
            callback = None
        else:
            callback = self._callbacks.pop(stream_id, None)
            with self.lock:
                self.request_ids.append(stream_id)

        self.msg_received = True

        body = None
        try:
            # check that the protocol version is supported
            given_version = version & PROTOCOL_VERSION_MASK
            if given_version != self.protocol_version:
                msg = "Server protocol version (%d) does not match the specified driver protocol version (%d). " +\
                      "Consider setting Cluster.protocol_version to %d."
                raise ProtocolError(msg % (given_version, self.protocol_version, given_version))

            # check that the header direction is correct
            if version & HEADER_DIRECTION_MASK != HEADER_DIRECTION_TO_CLIENT:
                raise ProtocolError(
                    "Header direction in response is incorrect; opcode %04x, stream id %r"
                    % (opcode, stream_id))

            if body_len > 0:
                body = msg[self._full_header_length:]
            elif body_len == 0:
                body = six.binary_type()
            else:
                raise ProtocolError("Got negative body length: %r" % body_len)

            response = decode_response(given_version, self.user_type_map, stream_id,
                                       flags, opcode, body, self.decompressor)
        except Exception as exc:
            log.exception("Error decoding response from Cassandra. "
                          "opcode: %04x; message contents: %r", opcode, msg)
            if callback is not None:
                callback(exc)
            self.defunct(exc)
            return

        try:
            if stream_id >= 0:
                if isinstance(response, ProtocolException):
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

            self.authenticator_class = startup_response.authenticator

            if isinstance(self.authenticator, dict):
                log.debug("Sending credentials-based auth response on %s", self)
                cm = CredentialsMessage(creds=self.authenticator)
                callback = partial(self._handle_startup_response, did_authenticate=True)
                self.send_msg(cm, self.get_request_id(), cb=callback)
            else:
                log.debug("Sending SASL-based auth response on %s", self)
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
        """
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

        request_id = None
        # we use a busy wait on the lock here because:
        # - we'll only spin if the connection is at max capacity, which is very
        #   unlikely for a set_keyspace call
        # - it allows us to avoid signaling a condition every time a request completes
        while True:
            with self.lock:
                if self.in_flight < self.max_request_id:
                    request_id = self.get_request_id()
                    self.in_flight += 1
                    break

            time.sleep(0.001)

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
            if connection.in_flight < connection.max_request_id:
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
            raise OperationTimedOut()

    def _options_callback(self, response):
        if not isinstance(response, SupportedMessage):
            if isinstance(response, ConnectionException):
                self._exception = response
            else:
                self._exception = ConnectionException("Received unexpected response to OptionsMessage: %s"
                                                      % (response,))

        log.debug("Received options response on connection (%s) from %s",
                  id(self.connection), self.connection.host)
        self._event.set()


class ConnectionHeartbeat(Thread):

    def __init__(self, interval_sec, get_connection_holders):
        Thread.__init__(self, name="Connection heartbeat")
        self._interval = interval_sec
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
                                except Exception:
                                    log.warning("Failed sending heartbeat message on connection (%s) to %s",
                                                id(connection), connection.host, exc_info=True)
                                    failed_connections.append((connection, owner))
                            else:
                                connection.reset_idle()
                        else:
                            # make sure the owner sees this defunt/closed connection
                            owner.return_connection(connection)
                    self._raise_if_stopped()

                for f in futures:
                    self._raise_if_stopped()
                    connection = f.connection
                    try:
                        f.wait(self._interval)
                        # TODO: move this, along with connection locks in pool, down into Connection
                        with connection.lock:
                            connection.in_flight -= 1
                        connection.reset_idle()
                    except Exception:
                        log.warning("Heartbeat failed for connection (%s) to %s",
                                    id(connection), connection.host, exc_info=True)
                        failed_connections.append((f.connection, f.owner))

                for connection, owner in failed_connections:
                    self._raise_if_stopped()
                    connection.defunct(Exception('Connection heartbeat failure'))
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
