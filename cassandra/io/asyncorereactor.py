from collections import defaultdict, deque
from functools import partial
import logging
import socket
import sys
from threading import Event, Lock, Thread
import traceback
from Queue import Queue

import asyncore

from cassandra.connection import (Connection, ResponseWaiter, ConnectionException,
                                  ConnectionBusy, NONBLOCKING)
from cassandra.marshal import int32_unpack

log = logging.getLogger(__name__)

_loop_started = None
_loop_lock = Lock()

def _run_loop():
    log.debug("Starting asyncore event loop")
    asyncore.loop(timeout=0.001, use_poll=True, count=None)
    log.debug("Asyncore event loop ended")
    with _loop_lock:
        global _loop_started
        _loop_started = False

def _start_loop():
    global _loop_started
    should_start = False
    with _loop_lock:
        if not _loop_started:
            _loop_started = True
            should_start = True

    if should_start:
        t = Thread(target=_run_loop, name="event_loop")
        t.daemon = True
        t.start()

    return should_start


class AsyncoreConnection(Connection, asyncore.dispatcher):

    _buf = ""
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

        self._callbacks = {}
        self._push_watchers = defaultdict(set)
        self.deque = deque()

        log.debug("Opening socket to %s", self.host)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((self.host, self.port))

        if self.sockopts:
            for args in self.sockopts:
                self.socket.setsockopt(*args)

        self._writable = True
        self._readable = True

        # start the global event loop if needed
        _start_loop()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection to %s" % (self.host,))
        self._writable = False
        self._readable = False
        asyncore.dispatcher.close(self)
        log.debug("Closed socket to %s" % (self.host,))

        # don't leave in-progress operations hanging
        self.connected_event.set()
        if not self.is_defunct:
            self._error_all_callbacks(
                ConnectionException("Connection to %s was closed" % self.host))

    def __del__(self):
        self.close()

    def defunct(self, exc):
        if self.is_defunct:
            return

        log.debug("Defuncting connection to %s: %s\n%s" %
                  (self.host, exc, traceback.format_exc(exc)))
        self.last_error = exc
        self.is_defunct = True
        self._error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def _error_all_callbacks(self, exc):
        for cb in self._callbacks.values():
            cb(exc)

    def handle_connect(self):
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
        except socket.error, err:
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
            buf = self.recv(self.in_buffer_size)
        except socket.error, err:
            if err.args[0] not in NONBLOCKING:
                self.defunct(err)
            return

        if buf:
            self._buf += buf
            while True:
                if len(self._buf) < 8:
                    # we don't have a complete header yet
                    break
                elif self._total_reqd_bytes and len(self._buf) < self._total_reqd_bytes:
                    # we already saw a header, but we don't have a complete message yet
                    break
                else:
                    body_len = int32_unpack(self._buf[4:8])
                    if len(self._buf) - 8 >= body_len:
                        msg = self._buf[:8 + body_len]
                        self._buf = self._buf[8 + body_len:]
                        self._total_reqd_bytes = 0
                        self.process_msg(msg, body_len)
                    else:
                        self._total_reqd_bytes = body_len + 8

            if not self._callbacks:
                self._readable = False
        else:
            log.debug("connection closed by server")
            self.close()

    def handle_pushed(self, response):
        for cb in self._push_watchers[response.type]:
            try:
                cb(response)
            except:
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
        return self._readable or (self._have_listeners and not self.is_defunct or self.is_closed)

    def send_msg(self, msg, cb):
        if self.is_defunct:
            raise ConnectionException("Connection to %s is defunct" % self.host)
        elif self.is_closed:
            raise ConnectionException("Connection to %s is closed" % self.host)

        try:
            request_id = self._id_queue.get_nowait()
        except Queue.EMPTY:
            raise ConnectionBusy(
                "Connection to %s is at the max number of requests" % self.host)

        self._callbacks[request_id] = cb
        self.push(msg.to_string(request_id, compression=self.compressor))
        return request_id

    def wait_for_response(self, msg):
        return self.wait_for_responses(msg)[0]

    def wait_for_responses(self, *msgs):
        waiter = ResponseWaiter(len(msgs))
        for i, msg in enumerate(msgs):
            self.send_msg(msg, partial(waiter.got_response, index=i))

        return waiter.deliver()

    def register_watcher(self, event_type, callback):
        self._push_watchers[event_type].add(callback)
        self._have_listeners = True

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self.register_watcher(event_type, callback)
