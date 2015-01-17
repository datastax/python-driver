# Copyright 2014 Symantec Corporation
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

import collections
import eventlet
from eventlet.green import select
from eventlet.green import socket
from eventlet import queue
import errno
import functools
from six import moves
import threading

import logging
import os

import cassandra
from cassandra import connection as cassandra_connection
from cassandra import protocol as cassandra_protocol


log = logging.getLogger(__name__)


def is_timeout(err):
    return (
        err in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK) or
        (err == errno.EINVAL and os.name in ('nt', 'ce'))
    )


class EventletConnection(cassandra_connection.Connection):
    """
    An implementation of :class:`.Connection` that utilizes ``eventlet``.
    """

    _total_reqd_bytes = 0
    _read_watcher = None
    _write_watcher = None
    _socket = None

    @classmethod
    def initialize_reactor(cls):
        eventlet.monkey_patch()

    @classmethod
    def factory(cls, *args, **kwargs):
        timeout = kwargs.pop('timeout', 5.0)
        conn = cls(*args, **kwargs)
        conn.connected_event.wait(timeout)
        if conn.last_error:
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise cassandra.OperationTimedOut("Timed out creating connection")
        else:
            return conn

    def __init__(self, *args, **kwargs):
        cassandra_connection.Connection.__init__(self, *args, **kwargs)

        self.connected_event = threading.Event()
        self._write_queue = queue.Queue()

        self._callbacks = {}
        self._push_watchers = collections.defaultdict(set)

        sockerr = None
        addresses = socket.getaddrinfo(
            self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        for (af, socktype, proto, canonname, sockaddr) in addresses:
            try:
                self._socket = socket.socket(af, socktype, proto)
                self._socket.settimeout(1.0)
                self._socket.connect(sockaddr)
                sockerr = None
                break
            except socket.error as err:
                sockerr = err
        if sockerr:
            raise socket.error(
                sockerr.errno,
                "Tried connecting to %s. Last error: %s" % (
                    [a[4] for a in addresses], sockerr.strerror)
            )

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

        self._read_watcher = eventlet.spawn(lambda: self.handle_read())
        self._write_watcher = eventlet.spawn(lambda: self.handle_write())
        self._send_options_message()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s" % (id(self), self.host))

        cur_gthread = eventlet.getcurrent()

        if self._read_watcher and self._read_watcher != cur_gthread:
            self._read_watcher.kill()
        if self._write_watcher and self._write_watcher != cur_gthread:
            self._write_watcher.kill()
        if self._socket:
            self._socket.close()
        log.debug("Closed socket to %s" % (self.host,))

        if not self.is_defunct:
            self.error_all_callbacks(
                cassandra_connection.ConnectionShutdown(
                    "Connection to %s was closed" % self.host
                )
            )
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        while True:
            try:
                next_msg = self._write_queue.get()
                self._socket.sendall(next_msg)
            except socket.error as err:
                log.debug("Exception during socket send for %s: %s", self, err)
                self.defunct(err)
                return  # Leave the write loop

    def handle_read(self):
        run_select = functools.partial(select.select, (self._socket,), (), ())
        while True:
            try:
                run_select()
            except Exception as exc:
                if not self.is_closed:
                    log.debug("Exception during read select() for %s: %s",
                              self, exc)
                    self.defunct(exc)
                return

            try:
                buf = self._socket.recv(self.in_buffer_size)
                self._iobuf.write(buf)
            except socket.error as err:
                if not is_timeout(err):
                    log.debug("Exception during socket recv for %s: %s",
                              self, err)
                    self.defunct(err)
                    return  # leave the read loop

            if self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return

    def push(self, data):
        chunk_size = self.out_buffer_size
        for i in moves.xrange(0, len(data), chunk_size):
            self._write_queue.put(data[i:i + chunk_size])

    def register_watcher(self, event_type, callback, register_timeout=None):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            cassandra_protocol.RegisterMessage(event_list=[event_type]),
            timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            cassandra_protocol.RegisterMessage(
                event_list=type_callback_dict.keys()),
            timeout=register_timeout)
