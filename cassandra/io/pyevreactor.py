from collections import defaultdict, deque
from functools import partial, wraps
import logging
import socket
from threading import Event, Lock, Thread
import traceback
from Queue import Queue

import pyev

from cassandra.connection import (Connection, ResponseWaiter, ConnectionException,
                                  ConnectionBusy, NONBLOCKING)
from cassandra.marshal import int32_unpack

log = logging.getLogger(__name__)


_loop = pyev.default_loop(pyev.EVBACKEND_SELECT)

_loop_notifier = _loop.async(lambda *a, **kw: None)
_loop_notifier.start()

# prevent _loop_notifier from keeping the loop from returning
_loop.unref()

_loop_started = None
_loop_lock = Lock()

def _run_loop():
    while True:
        end_condition = _loop.start()
        # there are still active watchers, no deadlock
        with _loop_lock:
            if end_condition:
                log.debug("Restarting event loop")
                continue
            else:
                # all Connections have been closed, no active watchers
                log.debug("All Connections currently closed, event loop ended")
                global _loop_started
                _loop_started = False
                break

def _start_loop():
    global _loop_started
    should_start = False
    with _loop_lock:
        if not _loop_started:
            log.debug("Starting pyev event loop")
            _loop_started = True
            should_start = True

    if should_start:
        t = Thread(target=_run_loop, name="async_event_loop")
        t.daemon = True
        t.start()

    return should_start


def defunct_on_error(f):

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception, exc:
            self.defunct(exc)

    return wrapper


class PyevConnection(Connection):

    _buf = ""
    _total_reqd_bytes = 0
    _read_watcher = None
    _write_watcher = None
    _socket = None

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

        self.connected_event = Event()

        self._callbacks = {}
        self._push_watchers = defaultdict(set)
        self.deque = deque()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.host, self.port))
        self._socket.setblocking(0)

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

        self._read_watcher = pyev.Io(self._socket._sock, pyev.EV_READ, _loop, self.handle_read)
        self._write_watcher = pyev.Io(self._socket._sock, pyev.EV_WRITE, _loop, self.handle_write)
        with _loop_lock:
            self._read_watcher.start()
            self._write_watcher.start()

        self._send_options_message()

        # start the global event loop if needed
        if not _start_loop():
            # if the loop was already started, notify it
            with _loop_lock:
                _loop_notifier.send()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection to %s" % (self.host,))
        if self._read_watcher:
            self._read_watcher.stop()
        if self._write_watcher:
            self._write_watcher.stop()
        self._socket.close()
        with _loop_lock:
            _loop_notifier.send()

        # don't leave in-progress operations hanging
        if not self.is_defunct:
            self._error_all_callbacks(
                ConnectionException("Connection to %s was closed" % self.host))

    def __del__(self):
        self.close()

    def defunct(self, exc):
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

    def handle_write(self, watcher, revents):
        try:
            next_msg = self.deque.popleft()
        except IndexError:
            self._write_watcher.stop()
            return

        try:
            sent = self._socket.send(next_msg)
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
                self._write_watcher.stop()

    def handle_read(self, watcher, revents):
        try:
            buf = self._socket.recv(self.in_buffer_size)
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

            if not self._write_watcher.active:
                with _loop_lock:
                    self._write_watcher.start()
                    _loop_notifier.send()

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

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self.register_watcher(event_type, callback)
