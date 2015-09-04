# Copyright 2013-2015 DataStax, Inc.
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
import sys
from threading import Event, Lock, Thread
import time
import weakref

from six.moves import range

try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # noqa

import asyncore

try:
    import ssl
except ImportError:
    ssl = None  # NOQA

from cassandra.connection import (Connection, ConnectionShutdown,
                                  ConnectionException, NONBLOCKING,
                                  Timer, TimerManager)

log = logging.getLogger(__name__)


def _cleanup(loop_weakref):
    try:
        loop = loop_weakref()
    except ReferenceError:
        return

    loop._cleanup()


class AsyncoreLoop(object):


    def __init__(self):
        self._pid = os.getpid()
        self._loop_lock = Lock()
        self._started = False
        self._shutdown = False

        self._thread = None

        self._timers = TimerManager()

        atexit.register(partial(_cleanup, weakref.ref(self)))

    def maybe_start(self):
        should_start = False
        did_acquire = False
        try:
            did_acquire = self._loop_lock.acquire(False)
            if did_acquire and not self._started:
                self._started = True
                should_start = True
        finally:
            if did_acquire:
                self._loop_lock.release()

        if should_start:
            self._thread = Thread(target=self._run_loop, name="cassandra_driver_event_loop")
            self._thread.daemon = True
            self._thread.start()

    def _run_loop(self):
        log.debug("Starting asyncore event loop")
        with self._loop_lock:
            while not self._shutdown:
                try:
                    asyncore.loop(timeout=0.001, use_poll=True, count=100)
                    self._timers.service_timeouts()
                    if not asyncore.socket_map:
                        time.sleep(0.005)
                except Exception:
                    log.debug("Asyncore event loop stopped unexepectedly", exc_info=True)
                    break
            self._started = False

        log.debug("Asyncore event loop ended")

    def add_timer(self, timer):
        self._timers.add_timer(timer)

    def _cleanup(self):
        self._shutdown = True
        if not self._thread:
            return

        log.debug("Waiting for event loop thread to join...")
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")

        log.debug("Event loop thread was joined")


class AsyncoreConnection(Connection, asyncore.dispatcher):
    """
    An implementation of :class:`.Connection` that uses the ``asyncore``
    module in the Python standard library for its event loop.
    """

    _loop = None

    _writable = False
    _readable = False

    @classmethod
    def initialize_reactor(cls):
        if not cls._loop:
            cls._loop = AsyncoreLoop()
        else:
            current_pid = os.getpid()
            if cls._loop._pid != current_pid:
                log.debug("Detected fork, clearing and reinitializing reactor state")
                cls.handle_fork()
                cls._loop = AsyncoreLoop()

    @classmethod
    def handle_fork(cls):
        if cls._loop:
            cls._loop._cleanup()
            cls._loop = None

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        cls._loop.add_timer(timer)
        return timer

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)
        asyncore.dispatcher.__init__(self)

        self.deque = deque()
        self.deque_lock = Lock()

        self._connect_socket()
        asyncore.dispatcher.__init__(self, self._socket)

        self._writable = True
        self._readable = True

        self._send_options_message()

        # start the event loop if needed
        self._loop.maybe_start()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        self._writable = False
        self._readable = False
        asyncore.dispatcher.close(self)
        log.debug("Closed socket to %s", self.host)

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_error(self):
        self.defunct(sys.exc_info()[1])

    def handle_close(self):
        log.debug("Connection %s closed by server", self)
        self.close()

    def handle_write(self):
        while True:
            with self.deque_lock:
                try:
                    next_msg = self.deque.popleft()
                except IndexError:
                    self._writable = False
                    return

            try:
                sent = self.send(next_msg)
                self._readable = True
            except socket.error as err:
                if (err.args[0] in NONBLOCKING):
                    with self.deque_lock:
                        self.deque.appendleft(next_msg)
                else:
                    self.defunct(err)
                return
            else:
                if sent < len(next_msg):
                    with self.deque_lock:
                        self.deque.appendleft(next_msg[sent:])
                    if sent == 0:
                        return

    def handle_read(self):
        try:
            while True:
                buf = self.recv(self.in_buffer_size)
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
            if not self._requests and not self.is_control_connection:
                self._readable = False

    def push(self, data):
        sabs = self.out_buffer_size
        if len(data) > sabs:
            chunks = []
            for i in range(0, len(data), sabs):
                chunks.append(data[i:i + sabs])
        else:
            chunks = [data]

        with self.deque_lock:
            self.deque.extend(chunks)
            self._writable = True

    def writable(self):
        return self._writable

    def readable(self):
        return self._readable or (self.is_control_connection and not (self.is_defunct or self.is_closed))
