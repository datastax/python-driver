import gevent
from gevent import select
from gevent import socket
from gevent.event import Event

from collections import defaultdict
from functools import partial
import logging
import os
import sys
import traceback

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # ignore flake8 warning: # NOQA

if 'gevent.monkey' in sys.modules:
    from gevent.queue import Queue
else:
    from Queue import Queue

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, EINVAL

from cassandra.connection import (Connection, ResponseWaiter, ConnectionShutdown,
                                  ConnectionBusy)
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
        conn = cls(*args, **kwargs)
        conn.connected_event.wait()
        if conn.last_error:
            raise conn.last_error
        else:
            return conn

    def __init__(self, *args, **kwargs):
        super(GeventConnection, self).__init__(*args, **kwargs)

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
        if self.is_closed:
            return
        self.is_closed = True

        log.debug("Closing connection to %s" % (self.host,))
        if self._read_watcher:
            self._read_watcher.kill()
        if self._write_watcher:
            self._write_watcher.kill()
        if self._socket:
            self._socket.close()
        log.debug("Closed socket to %s" % (self.host,))

        # don't leave in-progress operations hanging
        self.connected_event.set()
        if not self.is_defunct:
            self._error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))

    def __del__(self):
        try:
            self.close()
        except TypeError:
            pass

    def defunct(self, exc):
        if self.is_defunct:
            return
        self.is_defunct = True

        trace = traceback.format_exc(exc)
        if trace != "None":
            log.debug("Defuncting connection to %s: %s\n%s",
                      self.host, exc, traceback.format_exc(exc))
        else:
            log.debug("Defuncting connection to %s: %s", self.host, exc)

        self.last_error = exc
        self._error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def _error_all_callbacks(self, exc):
        new_exc = ConnectionShutdown(str(exc))
        for cb in self._callbacks.values():
            cb(new_exc)

    def handle_error(self):
        self.defunct(sys.exc_info()[1])

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        wlist = (self._socket,)

        while True:
            try:
                next_msg = self._write_queue.get()
                select.select((), wlist, ())
            except Exception as err:
                log.debug("Write loop: got error %s" % err)
                return

            try:
                self._socket.sendall(next_msg)
            except socket.error as err:
                log.debug("Write loop: got error, defuncting socket and exiting")
                self.defunct(err)
                return  # Leave the write loop

    def handle_read(self):
        rlist = (self._socket,)

        while True:
            try:
                select.select(rlist, (), ())
            except Exception as err:
                return

            try:
                buf = self._socket.recv(self.in_buffer_size)
            except socket.error as err:
                if not is_timeout(err):
                    self.defunct(err)
                    return  # leave the read loop

            if buf:
                self._iobuf.write(buf)
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
                        body_len_bytes = self._iobuf.read(4)
                        body_len = int32_unpack(body_len_bytes)

                        # seek to end to get length of current buffer
                        self._iobuf.seek(0, os.SEEK_END)
                        pos = self._iobuf.tell()

                        if pos - 8 >= body_len:
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

    def handle_pushed(self, response):
        for cb in self._push_watchers.get(response.event_type, []):
            try:
                cb(response.event_args)
            except Exception:
                log.exception("Pushed event handler errored, ignoring:")

    def push(self, data):
        chunk_size = self.out_buffer_size
        for i in xrange(0, len(data), chunk_size):
            self._write_queue.put(data[i:i+chunk_size])

    def send_msg(self, msg, cb):
        if self.is_defunct:
            raise ConnectionShutdown("Connection to %s is defunct" % self.host)
        elif self.is_closed:
            raise ConnectionShutdown("Connection to %s is closed" % self.host)

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
        self.wait_for_response(RegisterMessage(event_list=[event_type]))

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=type_callback_dict.keys()))
