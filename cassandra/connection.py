import errno
from functools import wraps, partial
import logging
from threading import Event, RLock
from Queue import Queue

from cassandra import ConsistencyLevel, AuthenticationFailed, OperationTimedOut
from cassandra.marshal import int8_unpack, int32_pack
from cassandra.decoder import (ReadyMessage, AuthenticateMessage, OptionsMessage,
                               StartupMessage, ErrorMessage, CredentialsMessage,
                               QueryMessage, ResultMessage, decode_response,
                               InvalidRequestException, SupportedMessage)


log = logging.getLogger(__name__)

locally_supported_compressions = {}

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


MAX_STREAM_PER_CONNECTION = 127

PROTOCOL_VERSION = 0x01
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

    keyspace = None
    compression = True
    compressor = None
    decompressor = None

    ssl_options = None
    last_error = None
    in_flight = 0
    is_defunct = False
    is_closed = False
    lock = None

    def __init__(self, host='127.0.0.1', port=9042, credentials=None,
                 ssl_options=None, sockopts=None, compression=True,
                 cql_version=None):
        self.host = host
        self.port = port
        self.credentials = credentials
        self.ssl_options = ssl_options
        self.sockopts = sockopts
        self.compression = compression
        self.cql_version = cql_version

        self._id_queue = Queue(MAX_STREAM_PER_CONNECTION)
        for i in range(MAX_STREAM_PER_CONNECTION):
            self._id_queue.put_nowait(i)

        self.lock = RLock()

    def close(self):
        raise NotImplementedError()

    def defunct(self, exc):
        raise NotImplementedError()

    def send_msg(self, msg, cb):
        raise NotImplementedError()

    def wait_for_response(self, msg, **kwargs):
        raise NotImplementedError()

    def wait_for_responses(self, *msgs, **kwargs):
        raise NotImplementedError()

    def register_watcher(self, event_type, callback):
        raise NotImplementedError()

    def register_watchers(self, type_callback_dict):
        raise NotImplementedError()

    @defunct_on_error
    def process_msg(self, msg, body_len):
        version, flags, stream_id, opcode = map(int8_unpack, msg[:4])
        if stream_id < 0:
            callback = None
        else:
            callback = self._callbacks.pop(stream_id, None)
            self._id_queue.put_nowait(stream_id)

        body = None
        try:
            # check that the protocol version is supported
            given_version = version & PROTOCOL_VERSION_MASK
            if given_version != PROTOCOL_VERSION:
                raise ProtocolError("Unsupported CQL protocol version: %d" % given_version)

            # check that the header direction is correct
            if version & HEADER_DIRECTION_MASK != HEADER_DIRECTION_TO_CLIENT:
                raise ProtocolError(
                    "Header direction in response is incorrect; opcode %04x, stream id %r"
                    % (opcode, stream_id))

            if body_len > 0:
                body = msg[8:]
            elif body_len == 0:
                body = ""
            else:
                raise ProtocolError("Got negative body length: %r" % body_len)

            response = decode_response(stream_id, flags, opcode, body, self.decompressor)
        except Exception as exc:
            log.exception("Error decoding response from Cassandra. "
                          "opcode: %04x; message contents: %r", opcode, body)
            if callback is not None:
                callback(exc)
            self.defunct(exc)
            return

        try:
            if stream_id < 0:
                self.handle_pushed(response)
            elif callback is not None:
                callback(response)
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
            self.send_msg(OptionsMessage(), self._handle_options_response)

    @defunct_on_error
    def _handle_options_response(self, options_response):
        if self.is_defunct:
            return

        if not isinstance(options_response, SupportedMessage):
            if isinstance(options_response, ConnectionException):
                raise options_response
            else:
                log.error("Did not get expected SupportedMessage response; " \
                          "instead, got: %s", options_response)
                raise ConnectionException("Did not get expected SupportedMessage " \
                                          "response; instead, got: %s" \
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
                compression_type = iter(overlap).next()  # choose any
                # set the decompressor here, but set the compressor only after
                # a successful Ready message
                self._compressor, self.decompressor = \
                    locally_supported_compressions[compression_type]

        self._send_startup_message(compression_type)

    @defunct_on_error
    def _send_startup_message(self, compression=None):
        opts = {}
        if compression:
            opts['COMPRESSION'] = compression
        sm = StartupMessage(cqlversion=self.cql_version, options=opts)
        self.send_msg(sm, cb=self._handle_startup_response)

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
            log.debug("Got AuthenticateMessage on new connection (%s) from %s", id(self), self.host)

            if self.credentials is None:
                raise AuthenticationFailed('Remote end requires authentication.')

            self.authenticator = startup_response.authenticator
            cm = CredentialsMessage(creds=self.credentials)
            callback = partial(self._handle_startup_response, did_authenticate=True)
            self.send_msg(cm, cb=callback)
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
        else:
            msg = "Unexpected response during Connection setup: %r"
            log.error(msg, startup_response)
            raise ProtocolError(msg % (startup_response,))

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

        self.send_msg(query, process_result, wait_for_id=True)

    def __str__(self):
        status = ""
        if self.is_defunct:
            status = " (defunct)"
        elif self.is_closed:
            status = " (closed)"

        return "<%s(%r) %s:%d%s>" % (self.__class__.__name__, id(self), self.host, self.port, status)
    __repr__ = __str__


class ResponseWaiter(object):

    def __init__(self, connection, num_responses):
        self.connection = connection
        self.pending = num_responses
        self.error = None
        self.responses = [None] * num_responses
        self.event = Event()

    def got_response(self, response, index):
        with self.connection.lock:
            self.connection.in_flight -= 1
        if isinstance(response, Exception):
            self.error = response
            self.event.set()
        else:
            self.responses[index] = response
            self.pending -= 1
            if not self.pending:
                self.event.set()

    def deliver(self, timeout=None):
        self.event.wait(timeout)
        if self.error:
            raise self.error
        elif not self.event.is_set():
            raise OperationTimedOut()
        else:
            return self.responses
