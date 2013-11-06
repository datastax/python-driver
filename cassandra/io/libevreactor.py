from collections import defaultdict, deque
from functools import partial, wraps
import logging
import os
import socket
from threading import Event, Lock, Thread
import time
import traceback
import Queue

from cassandra import OperationTimedOut
from cassandra.connection import (Connection, ResponseWaiter, ConnectionShutdown,
                                  ConnectionBusy, NONBLOCKING,
                                  MAX_STREAM_PER_CONNECTION)
from cassandra.decoder import RegisterMessage
from cassandra.marshal import int32_unpack
import cassandra.io.libevwrapper as libev

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # ignore flake8 warning: # NOQA

try:
    import ssl
except ImportError:
    ssl = None # NOQA

log = logging.getLogger(__name__)

_loop = libev.Loop()
_loop_notifier = libev.Async(_loop)
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
            log.debug("Starting libev event loop")
            _loop_started = True
            should_start = True

    if should_start:
        t = Thread(target=_run_loop, name="event_loop")
        t.daemon = True
        t.start()

    return should_start


def defunct_on_error(f):

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception as exc:
            self.defunct(exc)

    return wrapper


class LibevConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes libev.
    """

    _total_reqd_bytes = 0
    _read_watcher = None
    _write_watcher = None
    _socket = None

    @classmethod
    def factory(cls, *args, **kwargs):
        timeout = kwargs.pop('timeout', 5.0)
        conn = cls(*args, **kwargs)
        conn.connected_event.wait(timeout)
        if conn.last_error:
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise OperationTimedOut("Timed out creating new connection")
        else:
            return conn

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.connected_event = Event()
        self._iobuf = StringIO()

        self._callbacks = {}
        self._push_watchers = defaultdict(set)
        self.deque = deque()
        self._deque_lock = Lock()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.ssl_options:
            if not ssl:
                raise Exception("This version of Python was not compiled with SSL support")
            self._socket = ssl.wrap_socket(self._socket, **self.ssl_options)
        self._socket.settimeout(1.0)  # TODO potentially make this value configurable
        self._socket.connect((self.host, self.port))
        self._socket.setblocking(0)

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

        self._read_watcher = libev.IO(self._socket._sock, libev.EV_READ, _loop, self.handle_read)
        self._write_watcher = libev.IO(self._socket._sock, libev.EV_WRITE, _loop, self.handle_write)
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

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        with _loop_lock:
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
                ConnectionShutdown("Connection to %s was closed" % self.host))

    def __del__(self):
        self.close()

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct:
                return
            self.is_defunct = True

        trace = traceback.format_exc(exc)
        if trace != "None":
            log.debug("Defuncting connection (%s) to %s: %s\n%s",
                      id(self), self.host, exc, traceback.format_exc(exc))
        else:
            log.debug("Defuncting connection (%s) to %s: %s", id(self), self.host, exc)

        self.last_error = exc
        self._error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def _error_all_callbacks(self, exc):
        new_exc = ConnectionShutdown(str(exc))
        for cb in self._callbacks.values():
            try:
                cb(new_exc)
            except Exception:
                log.warn("Ignoring unhandled exception while erroring callbacks for a "
                         "failed connection (%s) to host %s:",
                         id(self), self.host, exc_info=True)

    def handle_write(self, watcher, revents):
        if revents & libev.EV_ERROR:
            self.defunct(Exception("lbev reported an error"))
            return

        while True:
            try:
                with self._deque_lock:
                    next_msg = self.deque.popleft()
            except IndexError:
                with self._deque_lock:
                    if not self.deque:
                        with _loop_lock:
                            if self._write_watcher.is_active():
                                self._write_watcher.stop()
                return

            try:
                sent = self._socket.send(next_msg)
            except socket.error as err:
                if (err.args[0] in NONBLOCKING):
                    with self._deque_lock:
                        self.deque.appendleft(next_msg)
                    _loop_notifier.send()
                else:
                    self.defunct(err)
                return
            else:
                if sent < len(next_msg):
                    with self._deque_lock:
                        self.deque.appendleft(next_msg[sent:])
                    _loop_notifier.send()
                elif not self.deque:
                    with self._deque_lock:
                        if not self.deque:
                            with _loop_lock:
                                if self._write_watcher.is_active():
                                    self._write_watcher.stop()
                    return

    def handle_read(self, watcher, revents):
        if revents & libev.EV_ERROR:
            self.defunct(Exception("lbev reported an error"))
            return
        try:
            while True:
                buf = self._socket.recv(self.in_buffer_size)
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
                    # have enough for header, read body len from header
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
        else:
            log.debug("Connection %s closed by server", self)
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

        with self._deque_lock:
            self.deque.extend(chunks)

        with _loop_lock:
            if not self._write_watcher.is_active():
                self._write_watcher.start()
            _loop_notifier.send()

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

    def wait_for_response(self, msg, timeout=None):
        return self.wait_for_responses(msg, timeout=timeout)[0]

    def wait_for_responses(self, *msgs, **kwargs):
        timeout = kwargs.get('timeout')
        waiter = ResponseWaiter(len(msgs))

        # busy wait for sufficient space on the connection
        messages_sent = 0
        while True:
            needed = len(msgs) - messages_sent
            with self.lock:
                available = min(needed, MAX_STREAM_PER_CONNECTION - self.in_flight)
                self.in_flight += available

            for i in range(messages_sent, messages_sent + available):
                self.send_msg(msgs[i], partial(waiter.got_response, index=i), wait_for_id=True)
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
        finally:
            with self.lock:
                self.in_flight -= len(msgs)

    def register_watcher(self, event_type, callback):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=[event_type]))

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=type_callback_dict.keys()))
