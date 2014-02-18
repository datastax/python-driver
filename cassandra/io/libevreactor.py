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
    An implementation of :class:`.Connection` that uses libev for its event loop.
    """

    # class-level set of all connections; only replaced with a new copy
    # while holding _conn_set_lock, never modified in place
    _live_conns = set()
    # newly created connections that need their write/read watcher started
    _new_conns = set()
    # recently closed connections that need their write/read watcher stopped
    _closed_conns = set()
    _conn_set_lock = Lock()

    _write_watcher_is_active = False

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

    @classmethod
    def _connection_created(cls, conn):
        with cls._conn_set_lock:
            new_live_conns = cls._live_conns.copy()
            new_live_conns.add(conn)
            cls._live_conns = new_live_conns

            new_new_conns = cls._new_conns.copy()
            new_new_conns.add(conn)
            cls._new_conns = new_new_conns

    @classmethod
    def _connection_destroyed(cls, conn):
        with cls._conn_set_lock:
            new_live_conns = cls._live_conns.copy()
            new_live_conns.discard(conn)
            cls._live_conns = new_live_conns

            new_closed_conns = cls._closed_conns.copy()
            new_closed_conns.add(conn)
            cls._closed_conns = new_closed_conns

    @classmethod
    def loop_will_run(cls, prepare):
        changed = False
        for conn in cls._live_conns:
            if not conn.deque and conn._write_watcher_is_active:
                if conn._write_watcher:
                    conn._write_watcher.stop()
                conn._write_watcher_is_active = False
                changed = True
            elif conn.deque and not conn._write_watcher_is_active:
                conn._write_watcher.start()
                conn._write_watcher_is_active = True
                changed = True

        if cls._new_conns:
            with cls._conn_set_lock:
                to_start = cls._new_conns
                cls._new_conns = set()

            for conn in to_start:
                conn._read_watcher.start()

            changed = True

        if cls._closed_conns:
            with cls._conn_set_lock:
                to_stop = cls._closed_conns
                cls._closed_conns = set()

            for conn in to_stop:
                if conn._write_watcher:
                    conn._write_watcher.stop()
                if conn._read_watcher:
                    conn._read_watcher.stop()

            changed = True

        if changed:
            _loop_notifier.send()

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

        with _loop_lock:
            self._read_watcher = libev.IO(self._socket._sock, libev.EV_READ, _loop, self.handle_read)
            self._write_watcher = libev.IO(self._socket._sock, libev.EV_WRITE, _loop, self.handle_write)

        self._send_options_message()

        self.__class__._connection_created(self)

        # start the global event loop if needed
        _start_loop()
        _loop_notifier.send()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        self.__class__._connection_destroyed(self)
        _loop_notifier.send()
        self._socket.close()

        # don't leave in-progress operations hanging
        if not self.is_defunct:
            self._error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct or self.is_closed:
                return
            self.is_defunct = True

        trace = traceback.format_exc(exc)
        if trace != "None":
            log.debug("Defuncting connection (%s) to %s: %s\n%s",
                      id(self), self.host, exc, traceback.format_exc(exc))
        else:
            log.debug("Defuncting connection (%s) to %s: %s", id(self), self.host, exc)

        self.last_error = exc
        self.close()
        self._error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def _error_all_callbacks(self, exc):
        with self.lock:
            callbacks = self._callbacks
            self._callbacks = {}
        new_exc = ConnectionShutdown(str(exc))
        for cb in callbacks.values():
            try:
                cb(new_exc)
            except Exception:
                log.warn("Ignoring unhandled exception while erroring callbacks for a "
                         "failed connection (%s) to host %s:",
                         id(self), self.host, exc_info=True)

    def handle_write(self, watcher, revents, errno=None):
        if revents & libev.EV_ERROR:
            if errno:
                exc = IOError(errno, os.strerror(errno))
            else:
                exc = Exception("libev reported an error")

            self.defunct(exc)
            return

        while True:
            try:
                with self._deque_lock:
                    next_msg = self.deque.popleft()
            except IndexError:
                return

            try:
                sent = self._socket.send(next_msg)
            except socket.error as err:
                if (err.args[0] in NONBLOCKING):
                    with self._deque_lock:
                        self.deque.appendleft(next_msg)
                else:
                    self.defunct(err)
                return
            else:
                if sent < len(next_msg):
                    with self._deque_lock:
                        self.deque.appendleft(next_msg[sent:])

    def handle_read(self, watcher, revents, errno=None):
        if revents & libev.EV_ERROR:
            if errno:
                exc = IOError(errno, os.strerror(errno))
            else:
                exc = Exception("libev reported an error")

            self.defunct(exc)
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
        waiter = ResponseWaiter(self, len(msgs))

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
        except OperationTimedOut:
            raise
        except Exception, exc:
            self.defunct(exc)
            raise

    def register_watcher(self, event_type, callback):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=[event_type]))

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=type_callback_dict.keys()))


_preparer = libev.Prepare(_loop, LibevConnection.loop_will_run)
# prevent _preparer from keeping the loop from returning
_loop.unref()
_preparer.start()
