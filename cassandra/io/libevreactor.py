# Copyright 2013-2016 DataStax, Inc.
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
import ssl
from threading import Lock, Thread
import time
import weakref

from six.moves import range

from cassandra.connection import (Connection, ConnectionShutdown,
                                  NONBLOCKING, Timer, TimerManager)
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


log = logging.getLogger(__name__)


def _cleanup(loop_weakref):
    try:
        loop = loop_weakref()
    except ReferenceError:
        return
    loop._cleanup()


class LibevLoop(object):

    def __init__(self):
        self._pid = os.getpid()
        self._loop = libev.Loop()
        self._notifier = libev.Async(self._loop)
        self._notifier.start()

        # prevent _notifier from keeping the loop from returning
        self._loop.unref()

        self._started = False
        self._shutdown = False
        self._lock = Lock()

        self._thread = None

        # set of all connections; only replaced with a new copy
        # while holding _conn_set_lock, never modified in place
        self._live_conns = set()
        # newly created connections that need their write/read watcher started
        self._new_conns = set()
        # recently closed connections that need their write/read watcher stopped
        self._closed_conns = set()
        self._conn_set_lock = Lock()

        self._preparer = libev.Prepare(self._loop, self._loop_will_run)
        # prevent _preparer from keeping the loop from returning
        self._loop.unref()
        self._preparer.start()

        self._timers = TimerManager()
        self._loop_timer = libev.Timer(self._loop, self._on_loop_timer)

        atexit.register(partial(_cleanup, weakref.ref(self)))

    def maybe_start(self):
        should_start = False
        with self._lock:
            if not self._started:
                log.debug("Starting libev event loop")
                self._started = True
                should_start = True

        if should_start:
            self._thread = Thread(target=self._run_loop, name="event_loop")
            self._thread.daemon = True
            self._thread.start()

        self._notifier.send()

    def _run_loop(self):
        while True:
            self._loop.start()
            # there are still active watchers, no deadlock
            with self._lock:
                if not self._shutdown and self._live_conns:
                    log.debug("Restarting event loop")
                    continue
                else:
                    # all Connections have been closed, no active watchers
                    log.debug("All Connections currently closed, event loop ended")
                    self._started = False
                    break

    def _cleanup(self):
        self._shutdown = True
        if not self._thread:
            return

        for conn in self._live_conns | self._new_conns | self._closed_conns:
            conn.close()
            map(lambda w: w.stop(), (w for w in (conn._write_watcher, conn._read_watcher) if w))

        self.notify()  # wake the timer watcher
        log.debug("Waiting for event loop thread to join...")
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")

        log.debug("Event loop thread was joined")

    def add_timer(self, timer):
        self._timers.add_timer(timer)
        self._notifier.send()  # wake up in case this timer is earlier

    def _update_timer(self):
        if not self._shutdown:
            next_end = self._timers.service_timeouts()
            if next_end:
                self._loop_timer.start(next_end - time.time())  # timer handles negative values
        else:
            self._loop_timer.stop()

    def _on_loop_timer(self):
        self._timers.service_timeouts()

    def notify(self):
        self._notifier.send()

    def connection_created(self, conn):
        with self._conn_set_lock:
            new_live_conns = self._live_conns.copy()
            new_live_conns.add(conn)
            self._live_conns = new_live_conns

            new_new_conns = self._new_conns.copy()
            new_new_conns.add(conn)
            self._new_conns = new_new_conns

    def connection_destroyed(self, conn):
        with self._conn_set_lock:
            new_live_conns = self._live_conns.copy()
            new_live_conns.discard(conn)
            self._live_conns = new_live_conns

            new_closed_conns = self._closed_conns.copy()
            new_closed_conns.add(conn)
            self._closed_conns = new_closed_conns

        self._notifier.send()

    def _loop_will_run(self, prepare):
        changed = False
        for conn in self._live_conns:
            if not conn.deque and conn._write_watcher_is_active:
                if conn._write_watcher:
                    conn._write_watcher.stop()
                conn._write_watcher_is_active = False
                changed = True
            elif conn.deque and not conn._write_watcher_is_active:
                conn._write_watcher.start()
                conn._write_watcher_is_active = True
                changed = True

        if self._new_conns:
            with self._conn_set_lock:
                to_start = self._new_conns
                self._new_conns = set()

            for conn in to_start:
                conn._read_watcher.start()

            changed = True

        if self._closed_conns:
            with self._conn_set_lock:
                to_stop = self._closed_conns
                self._closed_conns = set()

            for conn in to_stop:
                if conn._write_watcher:
                    conn._write_watcher.stop()
                    # clear reference cycles from IO callback
                    del conn._write_watcher
                if conn._read_watcher:
                    conn._read_watcher.stop()
                    # clear reference cycles from IO callback
                    del conn._read_watcher

            changed = True

        # TODO: update to do connection management, timer updates through dedicated async 'notifier' callbacks
        self._update_timer()

        if changed:
            self._notifier.send()


class LibevConnection(Connection):
    """
    An implementation of :class:`.Connection` that uses libev for its event loop.
    """
    _libevloop = None
    _write_watcher_is_active = False
    _read_watcher = None
    _write_watcher = None
    _socket = None

    @classmethod
    def initialize_reactor(cls):
        if not cls._libevloop:
            cls._libevloop = LibevLoop()
        else:
            if cls._libevloop._pid != os.getpid():
                log.debug("Detected fork, clearing and reinitializing reactor state")
                cls.handle_fork()
                cls._libevloop = LibevLoop()

    @classmethod
    def handle_fork(cls):
        if cls._libevloop:
            cls._libevloop._cleanup()
            cls._libevloop = None

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        cls._libevloop.add_timer(timer)
        return timer

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.deque = deque()
        self._deque_lock = Lock()
        self._connect_socket()
        self._socket.setblocking(0)

        with self._libevloop._lock:
            self._read_watcher = libev.IO(self._socket.fileno(), libev.EV_READ, self._libevloop._loop, self.handle_read)
            self._write_watcher = libev.IO(self._socket.fileno(), libev.EV_WRITE, self._libevloop._loop, self.handle_write)

        self._send_options_message()

        self._libevloop.connection_created(self)

        # start the global event loop if needed
        self._libevloop.maybe_start()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        self._libevloop.connection_destroyed(self)
        self._socket.close()
        log.debug("Closed socket to %s", self.host)

        # don't leave in-progress operations hanging
        if not self.is_defunct:
            self.error_all_requests(
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
            self.process_io_buffer()
        else:
            log.debug("Connection %s closed by server", self)
            self.close()

    def push(self, data):
        sabs = self.out_buffer_size
        if len(data) > sabs:
            chunks = []
            for i in range(0, len(data), sabs):
                chunks.append(data[i:i + sabs])
        else:
            chunks = [data]

        with self._deque_lock:
            self.deque.extend(chunks)
            self._libevloop.notify()
