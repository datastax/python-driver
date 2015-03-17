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
import gevent
from gevent import select, socket, ssl
from gevent.event import Event
from gevent.queue import Queue

from collections import defaultdict
from functools import partial
import logging
import os

from six.moves import xrange

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, EINVAL

from cassandra import OperationTimedOut
from cassandra.connection import Connection, ConnectionShutdown
from cassandra.protocol import RegisterMessage


log = logging.getLogger(__name__)


def is_timeout(err):
    return (
        err in (EINPROGRESS, EALREADY, EWOULDBLOCK) or
        (err == EINVAL and os.name in ('nt', 'ce'))
    )


class GeventConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes ``gevent``.
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
            raise OperationTimedOut("Timed out creating connection")
        else:
            return conn

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.connected_event = Event()
        self._write_queue = Queue()

        self._callbacks = {}
        self._push_watchers = defaultdict(set)

        sockerr = None
        addresses = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        for (af, socktype, proto, canonname, sockaddr) in addresses:
            try:
                self._socket = socket.socket(af, socktype, proto)
                if self.ssl_options:
                    self._socket = ssl.wrap_socket(self._socket, **self.ssl_options)
                self._socket.settimeout(1.0)
                self._socket.connect(sockaddr)
                sockerr = None
                break
            except socket.error as err:
                sockerr = err
        if sockerr:
            raise socket.error(sockerr.errno, "Tried connecting to %s. Last error: %s" % ([a[4] for a in addresses], sockerr.strerror))

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

        self._read_watcher = gevent.spawn(self.handle_read)
        self._write_watcher = gevent.spawn(self.handle_write)
        self._send_options_message()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s" % (id(self), self.host))
        if self._read_watcher:
            self._read_watcher.kill(block=False)
        if self._write_watcher:
            self._write_watcher.kill(block=False)
        if self._socket:
            self._socket.close()
        log.debug("Closed socket to %s" % (self.host,))

        if not self.is_defunct:
            self.error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        run_select = partial(select.select, (), (self._socket,), ())
        while True:
            try:
                next_msg = self._write_queue.get()
                run_select()
            except Exception as exc:
                if not self.is_closed:
                    log.debug("Exception during write select() for %s: %s", self, exc)
                    self.defunct(exc)
                return

            try:
                self._socket.sendall(next_msg)
            except socket.error as err:
                log.debug("Exception during socket sendall for %s: %s", self, err)
                self.defunct(err)
                return  # Leave the write loop

    def handle_read(self):
        run_select = partial(select.select, (self._socket,), (), ())
        while True:
            try:
                run_select()
            except Exception as exc:
                if not self.is_closed:
                    log.debug("Exception during read select() for %s: %s", self, exc)
                    self.defunct(exc)
                return

            try:
                while True:
                    buf = self._socket.recv(self.in_buffer_size)
                    self._iobuf.write(buf)
                    if len(buf) < self.in_buffer_size:
                        break
            except socket.error as err:
                if not is_timeout(err):
                    log.debug("Exception during socket recv for %s: %s", self, err)
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
        for i in xrange(0, len(data), chunk_size):
            self._write_queue.put(data[i:i + chunk_size])

    def register_watcher(self, event_type, callback, register_timeout=None):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]),
            timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()),
            timeout=register_timeout)
