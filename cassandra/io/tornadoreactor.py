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
"""
Module that implements an event loop based on Tornado ( https://tornado.com ).
"""
import logging
from threading import Thread, Lock
import tornado.ioloop

from cassandra.connection import Connection
from cassandra.connection import ConnectionShutdown
from cassandra.connection import Timer
from cassandra.connection import TimerManager


log = logging.getLogger(__name__)


class TornadoLoop(object):

    _lock = None
    _thread = None
    _timeout_task = None
    _timeout = None
    _loop = None

    def __init__(self):
        self._lock = Lock()
        self._timers = TimerManager()
        self._loop_timer_handle = None
        self._started = False
        self._running = False

    def maybe_start(self):
        should_start = False
        with self._lock:
            if not self._started:
                log.debug("Starting tornado event loop")
                self._started = True
                should_start = True

        if not should_start:
            return

        self._thread = Thread(
            target=self._run_loop, name="cassandra_tornado_event_loop"
        )
        self._thread.daemon = True
        self._thread.start()

    def _run_loop(self):
        while True:
            log.info("Creating new ioloop")
            loop = tornado.ioloop.IOLoop.current(False)
            if loop is None:
                loop = tornado.ioloop.IOLoop()
            self._loop = loop
            self._running = True
            self._loop.start()

    def wait_for_start(self):
        self.maybe_start()
        while not self._running:
            continue

    def remove_handler(self, socket):
        self._loop.add_callback(self._loop.remove_handler, socket)

    def add_handler(self, socket, handler):
        self._loop.add_callback(
            self._add_handler, handler, socket,
            tornado.ioloop.IOLoop.READ |
            # tornado.ioloop.IOLoop.WRITE |
            tornado.ioloop.IOLoop.ERROR
        )

    def add_timer(self, timer):
        self._timers.add_timer(timer)
        self._loop.add_callback(self._schedule_timeout)

    # These should only be called on the ioloop thread
    def _add_handler(self, handler, fd, events):
        self._loop.add_handler(fd, handler, events)

    def _schedule_timeout(self):
        next_timeout = self._timers.service_timeouts()
        if next_timeout is not None:
            # timer handles negative values
            self._loop.add_timeout(next_timeout, self._schedule_timeout)


class TornadoConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes the
    Tornado event loop.
    """

    _loop = None
    _socket = None

    @classmethod
    def initialize_reactor(cls):
        if not cls._loop:
            cls._loop = TornadoLoop()

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        cls._loop.add_timer(timer)
        return timer

    def __init__(self, *args, **kwargs):
        """
        Initialization method.

        Note that we can't call reactor methods directly here because
        it's not thread-safe, so we schedule the reactor/connection
        stuff to be run from the event loop thread when it gets the
        chance.
        """
        Connection.__init__(self, *args, **kwargs)
        self.is_closed = True
        self._setup_connection()

    def _setup_connection(self):
        self._connect_socket()
        self._socket.setblocking(0)
        self._fileno = self._socket.fileno()
        log.debug("Created connection with fd %s", self._fileno)
        self.is_closed = False
        self._loop.wait_for_start()
        self._loop.add_handler(self._socket, self.handle_events)
        self._send_options_message()

    def close(self):
        """
        Disconnect and error-out all requests.
        """
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        # We use stored fileno here as the socket is defunct and calling fileno
        # at this point on the object will result in an exception.
        try:
            self._loop.remove_handler(self._fileno)
            self._socket.close()
        except:
            # If the fileno represents a bad file descriptor then the
            # underlying tornado loop impl can result in an error. We don't
            # really care at this point, but we still need the fileno to be
            # unregistered in tornado and this achieves that
            pass

        self._socket = None
        log.debug("Closed socket to %s", self.host)

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_loop_exception(self, exc, *args):
        self.defunct(exc)

    def handle_events(self, fd, events):
        if events & tornado.ioloop.IOLoop.READ:
            self.handle_read()
        elif events & tornado.ioloop.IOLoop.ERROR:
            self.handle_error()

    def handle_error(self):
        """
        Process the incoming data buffer.
        """
        self.defunct(Exception("Error reading from socket"))

    def handle_read(self):
        """
        Process the incoming data buffer.
        """
        self._read_socket()

    def push(self, data):
        """
        This function is called when outgoing data should be queued
        for sending.
        """
        self._socket.send(data)
