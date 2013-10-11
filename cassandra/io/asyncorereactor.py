from collections import defaultdict, deque
from functools import partial
import logging
import os
import socket
import sys
from threading import Event, Lock, Thread
import traceback
import Queue
from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, EINVAL, EISCONN, errorcode

import asyncore

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # ignore flake8 warning: # NOQA

try:
    import ssl
except ImportError:
    ssl = None  # NOQA

from cassandra.connection import (Connection, ResponseWaiter, ConnectionShutdown,
                                  ConnectionBusy, ConnectionException, NONBLOCKING)
from cassandra.decoder import RegisterMessage
from cassandra.marshal import int32_unpack

log = logging.getLogger(__name__)

_loop_started = False
_loop_lock = Lock()

_starting_conns = set()
_starting_conns_lock = Lock()


def _run_loop():
    global _loop_started
    log.debug("Starting asyncore event loop")
    with _loop_lock:
        while True:
            try:
                asyncore.loop(timeout=0.001, use_poll=True, count=None)
            except Exception:
                log.debug("Asyncore event loop stopped unexepectedly", exc_info=True)
                break

            with _starting_conns_lock:
                if not _starting_conns:
                    break

        _loop_started = False
        if log:
            # this can happen during interpreter shutdown
            log.debug("Asyncore event loop ended")


def _start_loop():
    global _loop_started
    should_start = False
    did_acquire = False
    try:
        did_acquire = _loop_lock.acquire(False)
        if did_acquire and not _loop_started:
            _loop_started = True
            should_start = True
    finally:
        if did_acquire:
            _loop_lock.release()

    if should_start:
        t = Thread(target=_run_loop, name="event_loop")
        t.daemon = True
        t.start()


class AsyncoreConnection(Connection, asyncore.dispatcher):
    """
    An implementation of :class:`.Connection` that utilizes the ``asyncore``
    module in the Python standard library for its event loop.
    """

    _total_reqd_bytes = 0
    _writable = False
    _readable = False
    _have_listeners = False

    @classmethod
    def factory(cls, *args, **kwargs):
        conn = cls(*args, **kwargs)
        conn.connected_event.wait()
        if conn.last_error:
            raise conn.last_error
        else:
            return conn

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)
        asyncore.dispatcher.__init__(self)

        self.connected_event = Event()
        self._iobuf = StringIO()

        self._callbacks = {}
        self._push_watchers = defaultdict(set)
        self.deque = deque()

        with _starting_conns_lock:
            _starting_conns.add(self)

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((self.host, self.port))

        if self.sockopts:
            for args in self.sockopts:
                self.socket.setsockopt(*args)

        self._writable = True
        self._readable = True

        # start the global event loop if needed
        _start_loop()

    def create_socket(self, family, type):
        # copied from asyncore, but with the line to set the socket in
        # non-blocking mode removed (we will do that after connecting)
        self.family_and_type = family, type
        sock = socket.socket(family, type)
        if self.ssl_options:
            if not ssl:
                raise Exception("This version of Python was not compiled with SSL support")
            sock = ssl.wrap_socket(sock, **self.ssl_options)
        self.set_socket(sock)

    def connect(self, address):
        # this is copied directly from asyncore.py, except that
        # a timeout is set before connecting
        self.connected = False
        self.connecting = True
        self.socket.settimeout(1.0)
        err = self.socket.connect_ex(address)
        if err in (EINPROGRESS, EALREADY, EWOULDBLOCK) \
        or err == EINVAL and os.name in ('nt', 'ce'):
            raise ConnectionException("Timed out connecting to %s" % (address[0]))
        if err in (0, EISCONN):
            self.addr = address
            self.setblocking(0)
            self.handle_connect_event()
        else:
            raise socket.error(err, errorcode[err])

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection to %s", self.host)
        self._writable = False
        self._readable = False
        asyncore.dispatcher.close(self)
        log.debug("Closed socket to %s", self.host)

        with _starting_conns_lock:
            _starting_conns.discard(self)

        # don't leave in-progress operations hanging
        self.connected_event.set()
        if not self.is_defunct:
            self._error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))

    def __del__(self):
        try:
            self.close()
        except TypeError:
            pass

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct:
                return
            self.is_defunct = True

        trace = traceback.format_exc(exc)
        if trace != "None":
            log.debug("Defuncting connection to %s: %s\n%s",
                      self.host, exc, traceback.format_exc(exc))
        else:
            log.debug("Defuncting connection to %s: %s", self.host, exc)

        self.last_error = exc
        self._error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def _error_all_callbacks(self, exc):
        new_exc = ConnectionShutdown(str(exc))
        for cb in self._callbacks.values():
            cb(new_exc)

    def handle_connect(self):
        with _starting_conns_lock:
            _starting_conns.discard(self)
        self._send_options_message()

    def handle_error(self):
        self.defunct(sys.exc_info()[1])

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        try:
            next_msg = self.deque.popleft()
        except IndexError:
            self._writable = False
            return

        try:
            sent = self.send(next_msg)
        except socket.error as err:
            if (err.args[0] in NONBLOCKING):
                self.deque.appendleft(next_msg)
            else:
                self.defunct(err)
            return
        else:
            if sent < len(next_msg):
                self.deque.appendleft(next_msg[sent:])

            if not self.deque:
                self._writable = False

        self._readable = True

    def handle_read(self):
        try:
            while True:
                buf = self.recv(self.in_buffer_size)
                self._iobuf.write(buf)
                if len(buf) < self.in_buffer_size:
                    break
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.defunct(err)
            return

        if self._iobuf.tell():
            while True:
                pos = self._iobuf.tell()
                if pos < 8 or (self._total_reqd_bytes > 0 and pos < self._total_reqd_bytes):
                    # we don't have a complete header yet or we
                    # already saw a header, but we don't have a
                    # complete message yet
                    break
                else:
                    self._iobuf.seek(4)
                    body_len = int32_unpack(self._iobuf.read(4))

                    # seek to end to get length of current buffer
                    self._iobuf.seek(0, os.SEEK_END)
                    pos = self._iobuf.tell()

                    if pos >= body_len + 8:
                        # read message header and body
                        self._iobuf.seek(0)
                        msg = self._iobuf.read(8 + body_len)

                        # leave leftover in current buffer
                        leftover = self._iobuf.read()
                        self._iobuf = StringIO()
                        self._iobuf.write(leftover)

                        self._total_reqd_bytes = 0
                        self.process_msg(msg, body_len)
                    else:
                        self._total_reqd_bytes = body_len + 8
                        break

            if not self._callbacks:
                self._readable = False
        else:
            self.close()

    def handle_pushed(self, response):
        log.debug("Message pushed from server: %r", response)
        for cb in self._push_watchers.get(response.event_type, []):
            try:
                cb(response.event_args)
            except Exception:
                log.exception("Pushed event handler errored, ignoring:")

    def push(self, data):
        sabs = self.out_buffer_size
        if len(data) > sabs:
            chunks = []
            for i in xrange(0, len(data), sabs):
                chunks.append(data[i:i + sabs])
        else:
            chunks = [data]

        with self.lock:
            self.deque.extend(chunks)

        self._writable = True

    def writable(self):
        return self._writable

    def readable(self):
        return self._readable or (self._have_listeners and not (self.is_defunct or self.is_closed))

    def send_msg(self, msg, cb, wait_for_id=False):
        if self.is_defunct:
            raise ConnectionShutdown("Connection to %s is defunct" % self.host)
        elif self.is_closed:
            raise ConnectionShutdown("Connection to %s is closed" % self.host)

        if not wait_for_id:
            try:
                request_id = self._id_queue.get_nowait()
            except Queue.Empty:
                raise ConnectionBusy(
                    "Connection to %s is at the max number of requests" % self.host)
        else:
            request_id = self._id_queue.get()

        self._callbacks[request_id] = cb
        self.push(msg.to_string(request_id, compression=self.compressor))
        return request_id

    def wait_for_response(self, msg):
        return self.wait_for_responses(msg)[0]

    def wait_for_responses(self, *msgs):
        waiter = ResponseWaiter(len(msgs))
        with self.lock:
            # we're not checking to make sure in_flight is < 128,
            # but that's okay because we'll do a blocking wait
            # on getting a request ID from the queue
            self.in_flight += len(msgs)

        for i, msg in enumerate(msgs):
            self.send_msg(msg, partial(waiter.got_response, index=i), wait_for_id=True)

        try:
            return waiter.deliver()
        finally:
            with self.lock:
                self.in_flight -= len(msgs)

    def register_watcher(self, event_type, callback):
        self._push_watchers[event_type].add(callback)
        self._have_listeners = True
        self.wait_for_response(RegisterMessage(event_list=[event_type]))

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self._have_listeners = True
        self.wait_for_response(RegisterMessage(event_list=type_callback_dict.keys()))
