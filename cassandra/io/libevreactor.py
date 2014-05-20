# Copyright 2013-2014 DataStax, Inc.
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

import atexit
from collections import deque
from functools import partial
import logging
import os
import socket
from threading import Event, Lock, Thread

from six import BytesIO

from cassandra import OperationTimedOut
from cassandra.connection import Connection, ConnectionShutdown, NONBLOCKING
from cassandra.protocol import RegisterMessage
from cassandra.marshal import int32_unpack
try:
    import cassandra.io.libevwrapper as libev
except ImportError:
    raise ImportError(
        "The C extension needed to use libev was not found.  This "
        "probably means that you didn't have the required build dependencies "
        "when installing the driver.  See "
        "http://datastax.github.io/python-driver/installation.html#c-extensions "
        "for instructions on installing build dependencies and building "
        "the C extension.")


try:
    import ssl
except ImportError:
    ssl = None # NOQA

log = logging.getLogger(__name__)


class LibevConnection(Connection):
    """
    An implementation of :class:`.Connection` that uses libev for its event loop.
    """
    _loop = libev.Loop()
    _loop_notifier = libev.Async(_loop)
    _loop_notifier.start()

    # prevent _loop_notifier from keeping the loop from returning
    _loop.unref()

    _loop_started = None
    _loop_lock = Lock()
    _loop_shutdown = False

    @classmethod
    def _run_loop(cls):
        while True:
            end_condition = cls._loop.start()
            # there are still active watchers, no deadlock
            with cls._loop_lock:
                if not cls._loop_shutdown and (end_condition or cls._live_conns):
                    log.debug("Restarting event loop")
                    continue
                else:
                    # all Connections have been closed, no active watchers
                    log.debug("All Connections currently closed, event loop ended")
                    cls._loop_started = False
                    break

    @classmethod
    def _maybe_start_loop(cls):
        should_start = False
        with cls._loop_lock:
            if not cls._loop_started:
                log.debug("Starting libev event loop")
                cls._loop_started = True
                should_start = True

        if should_start:
            t = Thread(target=cls._run_loop, name="event_loop")
            t.daemon = True
            t.start()
            atexit.register(partial(cls._cleanup, t))

        return should_start

    @classmethod
    def _cleanup(cls, thread):
        cls._loop_shutdown = True
        log.debug("Waiting for event loop thread to join...")
        thread.join(timeout=1.0)
        if thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")

        log.debug("Event loop thread was joined")

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
            cls._loop_notifier.send()

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.connected_event = Event()
        self._iobuf = BytesIO()

        self._callbacks = {}
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

        with self._loop_lock:
            self._read_watcher = libev.IO(self._socket.fileno(), libev.EV_READ, self._loop, self.handle_read)
            self._write_watcher = libev.IO(self._socket.fileno(), libev.EV_WRITE, self._loop, self.handle_write)

        self._send_options_message()

        self.__class__._connection_created(self)

        # start the global event loop if needed
        self._maybe_start_loop()
        self._loop_notifier.send()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        self.__class__._connection_destroyed(self)
        self._loop_notifier.send()
        self._socket.close()

        # don't leave in-progress operations hanging
        if not self.is_defunct:
            self.error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))

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
            if ssl and isinstance(err, ssl.SSLError):
                if err.args[0] not in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
                    self.defunct(err)
                    return
            elif err.args[0] not in NONBLOCKING:
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
                        self._iobuf = BytesIO()
                        self._iobuf.write(leftover)

                        self._total_reqd_bytes = 0
                        self.process_msg(msg, body_len)
                    else:
                        self._total_reqd_bytes = body_len + 8
                        break
        else:
            log.debug("Connection %s closed by server", self)
            self.close()

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
            self._loop_notifier.send()

    def register_watcher(self, event_type, callback, register_timeout=None):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]), timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()), timeout=register_timeout)


_preparer = libev.Prepare(LibevConnection._loop, LibevConnection.loop_will_run)
# prevent _preparer from keeping the loop from returning
LibevConnection._loop.unref()
_preparer.start()
