import asynchat
import itertools
import socket

from threading import RLock, Event

from cassandra.marshal import (int8_unpack, int32_unpack)
from cassandra.decoder import (OptionsMessage, ReadyMessage, AuthenticateMessage,
                               StartupMessage, ErrorMessage, CredentialsMessage,
                               decode_response)

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


def warn(msg):
    print msg


class ProgrammingError(Exception):
    pass


class InternalError(Exception):
    pass


class Connection(asynchat.async_chat):

    @classmethod
    def factory(cls, *args, **kwargs):
        conn = cls(*args, **kwargs)
        conn.connected_event.wait()
        return conn

    def __init__(self, host='127.0.0.1', port=9042):
        asynchat.async_chat.__init__(self)
        self.cql_version = "3.0.1"
        self.host = host
        self.port = port

        # TODO cleanup, see todo below
        self.compression = True
        self._compresstype = None
        self._compressor = None
        self.compressor = None
        self.decompressor = None

        self.connected_event = Event()

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))

        self.make_request_id = itertools.cycle(xrange(127)).next
        self._waiting_callbacks = {}
        self._waiting_events = {}
        self._responses = {}
        self._lock = RLock()

    def handle_read(self):
        # simpler to do here than collect_incoming_data()
        header = self.recv(8)
        version, flags, stream_id, opcode = map(int8_unpack, header[:4])
        body_len = int32_unpack(header[4:])
        assert version & PROTOCOL_VERSION_MASK == PROTOCOL_VERSION, \
                "Unsupported CQL protocol version %d" % version
        assert version & HEADER_DIRECTION_MASK == HEADER_DIRECTION_TO_CLIENT, \
                "Unexpected request from server with opcode %04x, stream id %r" % (opcode, stream_id)
        assert body_len >= 0, "Invalid CQL protocol body_len %r" % body_len
        body = self.recv(body_len)

        if stream_id < 0:
            self.handle_pushed(stream_id, flags, opcode, body)
        else:
            try:
                cb = self._waiting_callbacks[stream_id]
            except KeyError:
                # store the response in a location accessible by other threads
                self._responses[stream_id] = (flags, opcode, body)

                # signal to the waiting thread that the response is ready
                self._waiting_events[stream_id].set()
            else:
                cb(stream_id, flags, opcode, body)

    def readable(self):
        # TODO only return True if we have pending responses (except for ControlConnections?)
        return True

    def handle_connect(self):
        self.send_msg(OptionsMessage(), self._handle_options_response)

    def _handle_options_response(self, stream_id, flags, opcode, body):
        options_response = decode_response(stream_id, flags, opcode, body)

        self.supported_cql_versions = options_response.cql_versions
        self.remote_supported_compressions = options_response.options['COMPRESSION']

        if self.cql_version:
            if self.cql_version not in self.supported_cql_versions:
                raise ProgrammingError(
                        "cql_version %r is not supported by remote (w/ native "
                        "protocol). Supported versions: %r"
                        % (self.cql_version, self.supported_cql_versions))
        else:
            self.cql_version = self.supported_cql_versions[0]

        opts = {}
        self._compresstype = None
        if self.compression:
            overlap = set(locally_supported_compressions) \
                    & set(self.remote_supported_compressions)
            if len(overlap) == 0:
                warn("No available compression types supported on both ends."
                     " locally supported: %r. remotely supported: %r"
                     % (locally_supported_compressions,
                        self.remote_supported_compressions))
            else:
                # TODO these compression fields probably don't need to be set
                # on the object; look at cleaning this up
                self._compresstype = iter(overlap).next() # choose any
                opts['COMPRESSION'] = self._compresstype
                # set the decompressor here, but set the compressor only after
                # a successful Ready message
                self._compressor, self.decompressor = locally_supported_compressions[self._compresstype]

        sm = StartupMessage(cqlversion=self.cql_version, options=opts)
        self.send_msg(sm, cb=self._handle_startup_response)

    def _handle_startup_response(self, stream_id, flags, opcode, body):
        startup_response = decode_response(
            stream_id, flags, opcode, body, self.decompressor)

        if isinstance(startup_response, ReadyMessage):
            if self._compresstype:
                self.compressor = self._compressor
            self.connected_event.set()
        elif isinstance(startup_response, AuthenticateMessage):
            self.authenticator = startup_response.authenticator
            if self.credentials is None:
                raise ProgrammingError('Remote end requires authentication.')
            cm = CredentialsMessage(creds=self.credentials)
            self.send_msg(cm, cb=self._handle_startup_response)
        elif isinstance(startup_response, ErrorMessage):
            raise ProgrammingError("Server did not accept credentials. %s"
                                   % startup_response.summary_msg())
        else:
            raise InternalError("Unexpected response %r during connection setup"
                                % startup_response)

    # def handle_error(self):
    #     print "got connection error"  # TODO
    #     self.close()

    def handle_pushed(self, stream_id, flags, opcode, body):
        pass

    def push(self, data):
        # overridden to avoid calling initiate_send() at the end of this
        # and hold the lock
        sabs = self.ac_out_buffer_size
        with self._lock:
            if len(data) > sabs:
                for i in xrange(0, len(data), sabs):
                    self.producer_fifo.append(data[i:i + sabs])
            else:
                self.producer_fifo.append(data)

    def send_msg(self, msg, cb=None):
        request_id = self.make_request_id()
        if cb:
            self._waiting_callbacks[request_id] = cb
        else:
            self._waiting_events[request_id] = Event()

        self.push(msg.to_string(request_id, compression=self.compressor))
        return request_id

    def get_response(self, stream_id):
        """ Blocking wait for a response """
        # TODO waiting on the event in the loop thread will deadlock
        self._waiting_events.pop(stream_id).wait()
        (flags, opcode, body) = self._responses.pop(stream_id)
        return decode_response(stream_id, flags, opcode, body, self.decompressor)
