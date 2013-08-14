import errno
from functools import wraps, partial
import logging
from threading import Event, Lock, RLock
from Queue import Queue

from cassandra import ConsistencyLevel, AuthenticationFailed
from cassandra.marshal import int8_unpack
from cassandra.decoder import (ReadyMessage, AuthenticateMessage, OptionsMessage,
                               StartupMessage, ErrorMessage, CredentialsMessage,
                               QueryMessage, ResultMessage, decode_response,
                               InvalidRequestException)


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


MAX_STREAM_PER_CONNECTION = 128

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
        except Exception, exc:
            self.defunct(exc)

    return wrapper


class Connection(object):

    in_buffer_size = 4096
    out_buffer_size = 4096

    cql_version = None

    keyspace = None
    compression = True
    compressor = None
    decompressor = None

    last_error = None
    in_flight = 0
    is_defunct = False
    is_closed = False
    lock = None

    def __init__(self, host='127.0.0.1', port=9042, credentials=None, sockopts=None, compression=True, cql_version=None):
        self.host = host
        self.port = port
        self.credentials = credentials
        self.sockopts = sockopts
        self.compression = compression
        self.cql_version = cql_version

        self._id_queue = Queue(MAX_STREAM_PER_CONNECTION)
        for i in range(MAX_STREAM_PER_CONNECTION):
            self._id_queue.put_nowait(i)

        self.lock = RLock()
        self.id_lock = Lock()

    def close(self):
        raise NotImplementedError()

    def defunct(self, exc):
        raise NotImplementedError()

    def send_msg(self, msg, cb):
        raise NotImplementedError()

    def wait_for_response(self, msg):
        raise NotImplementedError()

    def wait_for_responses(self, *msgs):
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
            callback = self._callbacks.pop(stream_id)
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
        except Exception, exc:
            log.exception("Error decoding response from Cassandra. "
                          "opcode: %04x; message contents: %r" % (opcode, body))
            if callback:
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
        log.debug("Sending initial options message for new Connection to %s", self.host)
        self.send_msg(OptionsMessage(), self._handle_options_response)

    @defunct_on_error
    def _handle_options_response(self, options_response):
        if self.is_defunct:
            return
        log.debug("Received options response on new Connection from %s" % self.host)
        self.supported_cql_versions = options_response.cql_versions
        self.remote_supported_compressions = options_response.options['COMPRESSION']

        if self.cql_version:
            if self.cql_version not in self.supported_cql_versions:
                raise ProtocolError(
                    "cql_version %r is not supported by remote (w/ native "
                    "protocol). Supported versions: %r"
                    % (self.cql_version, self.supported_cql_versions))
        else:
            self.cql_version = self.supported_cql_versions[0]

        opts = {}
        self._compressor = None
        if self.compression:
            overlap = (set(locally_supported_compressions.keys()) &
                       set(self.remote_supported_compressions))
            if len(overlap) == 0:
                log.debug("No available compression types supported on both ends."
                          " locally supported: %r. remotely supported: %r"
                          % (locally_supported_compressions.keys(),
                             self.remote_supported_compressions))
            else:
                compression_type = iter(overlap).next() # choose any
                opts['COMPRESSION'] = compression_type
                # set the decompressor here, but set the compressor only after
                # a successful Ready message
                self._compressor, self.decompressor = \
                    locally_supported_compressions[compression_type]

        sm = StartupMessage(cqlversion=self.cql_version, options=opts)
        self.send_msg(sm, cb=self._handle_startup_response)

    @defunct_on_error
    def _handle_startup_response(self, startup_response, did_authenticate=False):
        if self.is_defunct:
            return
        if isinstance(startup_response, ReadyMessage):
            log.debug("Got ReadyMessage on new Connection from %s" % self.host)
            if self._compressor:
                self.compressor = self._compressor
            self.connected_event.set()
        elif isinstance(startup_response, AuthenticateMessage):
            log.debug("Got AuthenticateMessage on new Connection from %s" % self.host)

            if self.credentials is None:
                raise AuthenticationFailed('Remote end requires authentication.')

            self.authenticator = startup_response.authenticator
            cm = CredentialsMessage(creds=self.credentials)
            callback = partial(self._handle_startup_response, did_authenticate=True)
            self.send_msg(cm, cb=callback)
        elif isinstance(startup_response, ErrorMessage):
            log.debug("Received ErrorMessage on new Connection from %s: %s"
                      % (self.host, startup_response.summary_msg()))
            if did_authenticate:
                raise AuthenticationFailed(
                    "Failed to authenticate to %s: %s" %
                    (self.host, startup_response.summary_msg()))
            else:
                raise ConnectionException(
                    "Failed to initialize new connection to %s: %s"
                    % (self.host, startup_response.summary_msg()))
        else:
            msg = "Unexpected response during Connection setup: %r" % (startup_response,)
            log.error(msg)
            raise ProtocolError(msg)

    def set_keyspace(self, keyspace):
        if not keyspace or keyspace == self.keyspace:
            return

        with self.lock:
            query = 'USE "%s"' % (keyspace,)
            try:
                result = self.wait_for_response(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE))
                if isinstance(result, ResultMessage):
                    self.keyspace = keyspace
                else:
                    raise self.defunct(ConnectionException(
                        "Problem while setting keyspace: %r" % (result,), self.host))
            except InvalidRequestException, ire:
                # the keyspace probably doesn't exist
                raise ire.to_exception()
            except Exception, exc:
                raise self.defunct(ConnectionException(
                    "Problem while setting keyspace: %r" % (exc,), self.host))


class ResponseWaiter(object):

    def __init__(self, num_responses):
        self.pending = num_responses
        self.error = None
        self.responses = [None] * num_responses
        self.event = Event()

    def got_response(self, response, index):
        if isinstance(response, Exception):
            self.error = response
            self.event.set()
        else:
            self.responses[index] = response
            self.pending -= 1
            if not self.pending:
                self.event.set()

    def deliver(self):
        self.event.wait()
        if self.error:
            raise self.error
        else:
            return self.responses
