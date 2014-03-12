import gevent
from gevent import select, socket
from gevent.event import Event
from gevent.queue import Queue

from collections import defaultdict
from functools import partial
import logging
import os

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # ignore flake8 warning: # NOQA

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, EINVAL

from cassandra import OperationTimedOut
from cassandra.connection import Connection, ConnectionShutdown
from cassandra.decoder import RegisterMessage
from cassandra.marshal import int32_unpack


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
        self._iobuf = StringIO()
        self._write_queue = Queue()

        self._callbacks = {}
        self._push_watchers = defaultdict(set)

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(1.0)
        self._socket.connect((self.host, self.port))

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

        self._read_watcher = gevent.spawn(lambda: self.handle_read())
        self._write_watcher = gevent.spawn(lambda: self.handle_write())
        self._send_options_message()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s" % (id(self), self.host))
        if self._read_watcher:
            self._read_watcher.kill()
        if self._write_watcher:
            self._write_watcher.kill()
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
                log.debug("Exception during read select() for %s: %s", self, exc)
                self.defunct(exc)
                return

            try:
                buf = self._socket.recv(self.in_buffer_size)
                self._iobuf.write(buf)
            except socket.error as err:
                if not is_timeout(err):
                    log.debug("Exception during socket recv for %s: %s", self, err)
                    self.defunct(err)
                    return  # leave the read loop

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
                log.debug("connection closed by server")
                self.close()
                return

    def push(self, data):
        chunk_size = self.out_buffer_size
        for i in xrange(0, len(data), chunk_size):
            self._write_queue.put(data[i:i + chunk_size])

    def register_watcher(self, event_type, callback):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=[event_type]))

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=type_callback_dict.keys()))
