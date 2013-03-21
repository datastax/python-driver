import itertools
import socket

from threading import RLock

from cassandra.decoder import (OptionsMessage, ReadyMessage, AuthenticateMessage,
                               StartupMessage, ErrorMessage, CredentialsMessage,
                               RegisterMessage, read_frame)

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


def warn(msg):
    print msg


class ProgrammingError(Exception):
    pass


class InternalError(Exception):
    pass


class Connection(object):

    def __init__(self, host='127.0.0.1', port=9042, credentials=None, compression=False, cql_version=None):
        self.host = host
        self.port = port
        self.credentials = credentials
        self.compression = compression
        self.cql_version = cql_version

        self.make_reqid = itertools.cycle(xrange(127)).next
        self.responses = {}
        self.waiting = {}
        self.in_flight = 0
        self._lock = RLock()
        self.conn_ready = False
        self.compressor = self.decompressor = None
        self.event_watchers = {}

    def establish_connection(self):
        self.conn_ready = False
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.socketf = s.makefile(bufsize=0)
        self.sockfd = s
        self.open_socket = True
        supported = self.wait_for_request(OptionsMessage())
        self.supported_cql_versions = supported.cql_versions
        self.remote_supported_compressions = supported.options['COMPRESSION']

        if self.cql_version:
            if self.cql_version not in self.supported_cql_versions:
                raise ProgrammingError(
                        "cql_version %r is not supported by remote (w/ native "
                        "protocol). Supported versions: %r"
                        % (self.cql_version, self.supported_cql_versions))
        else:
            self.cql_version = self.supported_cql_versions[0]

        opts = {}
        compresstype = None
        if self.compression:
            overlap = set(locally_supported_compressions) \
                    & set(self.remote_supported_compressions)
            if len(overlap) == 0:
                warn("No available compression types supported on both ends."
                     " locally supported: %r. remotely supported: %r"
                     % (locally_supported_compressions,
                        self.remote_supported_compressions))
            else:
                compresstype = iter(overlap).next() # choose any
                opts['COMPRESSION'] = compresstype
                compr, decompr = locally_supported_compressions[compresstype]
                # set the decompressor here, but set the compressor only after
                # a successful Ready message
                self.decompressor = decompr

        sm = StartupMessage(cqlversion=self.cql_version, options=opts)
        startup_response = self.wait_for_request(sm)
        while True:
            if isinstance(startup_response, ReadyMessage):
                self.conn_ready = True
                if compresstype:
                    self.compressor = compr
                break
            if isinstance(startup_response, AuthenticateMessage):
                self.authenticator = startup_response.authenticator
                if self.credentials is None:
                    raise ProgrammingError('Remote end requires authentication.')
                cm = CredentialsMessage(creds=self.credentials)
                startup_response = self.wait_for_request(cm)
            elif isinstance(startup_response, ErrorMessage):
                raise ProgrammingError("Server did not accept credentials. %s"
                                       % startup_response.summary_msg())
            else:
                raise InternalError("Unexpected response %r during connection setup"
                                    % startup_response)

    def terminate_connection(self):
        self.socketf.close()
        self.sockfd.close()

    def wait_for_request(self, msg):
        """
        Given a message, send it to the server, wait for a response, and
        return the response.
        """
        return self.wait_for_requests(msg)[0]

    def send_msg(self, msg):
        reqid = self.make_reqid()
        msg.send(self.socketf, reqid, compression=self.compressor)
        return reqid

    def wait_for_requests(self, *msgs):
        """
        Given any number of message objects, send them all to the server
        and wait for responses to each one. Once they arrive, return all
        of the responses in the same order as the messages to which they
        respond.
        """
        reqids = []
        for msg in msgs:
            reqid = self.send_msg(msg)
            reqids.append(reqid)
        resultdict = self.wait_for_results(*reqids)
        return [resultdict[reqid] for reqid in reqids]

    def wait_for_results(self, *reqids):
        """
        Given any number of stream-ids, wait until responses have arrived for
        each one, and return a dictionary mapping the stream-ids to the
        appropriate results.

        For internal use, None may be passed in place of a reqid, which will
        be considered satisfied when a message of any kind is received (and, if
        appropriate, handled).
        """
        waiting_for = set(reqids)
        results = {}
        for r in reqids:
            try:
                result = self.responses.pop(r)
            except KeyError:
                pass
            else:
                results[r] = result
                waiting_for.remove(r)
        while waiting_for:
            newmsg = read_frame(self.socketf, decompressor=self.decompressor)
            if newmsg.stream_id in waiting_for:
                results[newmsg.stream_id] = newmsg
                waiting_for.remove(newmsg.stream_id)
            else:
                self.handle_incoming(newmsg)
            if None in waiting_for:
                results[None] = newmsg
                waiting_for.remove(None)
        return results

    def wait_for_result(self, reqid):
        """
        Given a stream-id, wait until a response arrives with that stream-id,
        and return the msg.
        """
        return self.wait_for_results(reqid)[reqid]

    def handle_incoming(self, msg):
        if msg.stream_id < 0:
            self.handle_pushed(msg)
            return
        try:
            cb = self.waiting.pop(msg.stream_id)
        except KeyError:
            self.responses[msg.stream_id] = msg
        else:
            cb(msg)

    def callback_when(self, reqid, cb):
        """
        Callback cb with a message object once a message with a stream-id
        of reqid is received. The callback may be immediate, if a response
        is already in the received queue.

        Otherwise, note also that the callback may not be called immediately
        upon the arrival of the response packet; it may have to wait until
        something else waits on a result.
        """
        try:
            msg = self.responses.pop(reqid)
        except KeyError:
            pass
        else:
            return cb(msg)
        self.waiting[reqid] = cb

    def request_and_callback(self, msg, cb):
        """
        Given a message msg and a callable cb, send the message to the server
        and call cb with the result once it arrives. Note that the callback
        may not be called immediately upon the arrival of the response packet;
        it may have to wait until something else waits on a result.
        """
        reqid = self.send_msg(msg)
        self.callback_when(reqid, cb)

    def handle_pushed(self, msg):
        """
        Process an incoming message originated by the server.
        """
        watchers = self.event_watchers.get(msg.event_type, ())
        for cb in watchers:
            cb(msg.event_args)

    def register_watcher(self, event_type, callback):
        """
        Request that any events of the given type be passed to the given
        callback when they arrive. Note that the callback may not be called
        immediately upon the arrival of the event packet; it may have to wait
        until something else waits on a result, or until wait_for_even() is
        called.

        If the event type has not been registered for already, this may
        block while a new REGISTER message is sent to the server.

        The available event types are in the cql.native.known_event_types
        list.

        When an event arrives, a dictionary will be passed to the callback
        with the info about the event. Some example result dictionaries:

        (For STATUS_CHANGE events:)

          {'change_type': u'DOWN', 'address': ('12.114.19.76', 8000)}

        (For TOPOLOGY_CHANGE events:)

          {'change_type': u'NEW_NODE', 'address': ('19.10.122.13', 8000)}
        """
        return self.register_watchers({event_type: callback})

    def register_watchers(self, event_type_callbacks):
        to_watch = {}
        to_register = []
        for event_type, callback in event_type_callbacks.items():
            if isinstance(event_type, str):
                event_type = event_type.decode('utf8')
            try:
                watchers = self.event_watchers[event_type]
            except KeyError:
                to_register.append(event_type)
                watchers = self.event_watchers.setdefault(event_type, [])

            to_watch[watchers] = callback

        if to_register:
            ans = self.wait_for_request(RegisterMessage(event_list=to_register))
            if isinstance(ans, ErrorMessage):
                raise ProgrammingError(
                    "Server did not accept registration for events: %s"
                    % (ans.summary_msg(),))

            for watchers, callback in to_watch.items():
                watchers.append(callback)

    def unregister_watcher(self, event_type, cb):
        """
        Given an event_type and a callback previously registered with
        register_watcher(), remove that callback from the list of watchers for
        the given event type.
        """

        if isinstance(event_type, str):
            event_type = event_type.decode('utf8')
        self.event_watchers[event_type].remove(cb)

    def wait_for_event(self):
        """
        Wait for any sort of event to arrive, and handle it via the
        registered callbacks. It is recommended that some event watchers
        be registered before calling this; otherwise, no events will be
        sent by the server.
        """
        events_seen = []

        def i_saw_an_event(ev):
            events_seen.append(ev)

        wlists = self.event_watchers.values()
        for wlist in wlists:
            wlist.append(i_saw_an_event)
        while not events_seen:
            self.wait_for_result(None)
        for wlist in wlists:
            wlist.remove(i_saw_an_event)
