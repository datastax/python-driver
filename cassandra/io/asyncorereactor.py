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
import sys
from threading import Event, Lock, Thread

from six import BytesIO
from six.moves import range

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, EINVAL, EISCONN, errorcode
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # noqa

import asyncore

try:
    import ssl
except ImportError:
    ssl = None  # NOQA

from cassandra import OperationTimedOut
from cassandra.connection import (Connection, ConnectionShutdown,
                                  ConnectionException, NONBLOCKING)
from cassandra.protocol import RegisterMessage
from cassandra.marshal import int32_unpack

log = logging.getLogger(__name__)


class AsyncoreLoop(object):

    def __init__(self):
        self._loop_lock = Lock()
        self._started = False
        self._shutdown = False

        self._conns_lock = Lock()
        self._conns = WeakSet()

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
            thread = Thread(target=self._run_loop, name="cassandra_driver_event_loop")
            thread.daemon = True
            thread.start()
            atexit.register(partial(self._cleanup, thread))

    def _run_loop(self):
        log.debug("Starting asyncore event loop")
        with self._loop_lock:
            while True:
                try:
                    asyncore.loop(timeout=0.001, use_poll=True, count=1000)
                except Exception:
                    log.debug("Asyncore event loop stopped unexepectedly", exc_info=True)
                    break

                if self._shutdown:
                    break

                with self._conns_lock:
                    if len(self._conns) == 0:
                        break

            self._started = False

        log.debug("Asyncore event loop ended")

    def _cleanup(self, thread):
        self._shutdown = True
        log.debug("Waiting for event loop thread to join...")
        thread.join(timeout=1.0)
        if thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")

        log.debug("Event loop thread was joined")

    def connection_created(self, connection):
        with self._conns_lock:
            self._conns.add(connection)

    def connection_destroyed(self, connection):
        with self._conns_lock:
            self._conns.discard(connection)


class AsyncoreConnection(Connection, asyncore.dispatcher):
    """
    An implementation of :class:`.Connection` that uses the ``asyncore``
    module in the Python standard library for its event loop.
    """

    _loop = AsyncoreLoop()

    _total_reqd_bytes = 0
    _writable = False
    _readable = False

    @classmethod
    def factory(cls, *args, **kwargs):
        timeout = kwargs.pop('timeout', 5.0)
        conn = cls(*args, **kwargs)
        conn.connected_event.wait(timeout)
        if conn.last_error:
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise OperationTimedOut("Timed out creating connection")
        else:
            return conn

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)
        asyncore.dispatcher.__init__(self)

        self.connected_event = Event()
        self._iobuf = BytesIO()

        self._callbacks = {}
        self.deque = deque()
        self.deque_lock = Lock()

        self._loop.connection_created(self)

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((self.host, self.port))
        self.add_channel()

        if self.sockopts:
            for args in self.sockopts:
                self.socket.setsockopt(*args)

        self._writable = True
        self._readable = True

        # start the event loop if needed
        self._loop.maybe_start()

    def set_socket(self, sock):
        # Overrides the same method in asyncore. We deliberately
        # do not call add_channel() in this method so that we can call
        # it later, after connect() has completed.
        self.socket = sock
        self._fileno = sock.fileno()

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
            self.socket.setblocking(0)
            self.handle_connect_event()
        else:
            raise socket.error(err, errorcode[err])

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

        self._loop.connection_destroyed(self)

        if not self.is_defunct:
            self.error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_connect(self):
        self._send_options_message()

    def handle_error(self):
        self.defunct(sys.exc_info()[1])

    def handle_close(self):
        log.debug("connection (%s) to %s closed by server", id(self), self.host)
        self.close()

    def handle_write(self):
        while True:
            try:
                with self.deque_lock:
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

            if not self._callbacks and not self.is_control_connection:
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

    def register_watcher(self, event_type, callback, register_timeout=None):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]), timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()), timeout=register_timeout)
