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
"""
Module that implements an event loop based on twisted
( https://twistedmatrix.com ).
"""
import atexit
from functools import partial
import logging
from threading import Thread, Lock
import time
from twisted.internet import reactor, protocol
import weakref

from cassandra.connection import Connection, ConnectionShutdown, Timer, TimerManager


log = logging.getLogger(__name__)


def _cleanup(cleanup_weakref):
    try:
        cleanup_weakref()._cleanup()
    except ReferenceError:
        return


class TwistedConnectionProtocol(protocol.Protocol):
    """
    Twisted Protocol class for handling data received and connection
    made events.
    """

    def dataReceived(self, data):
        """
        Callback function that is called when data has been received
        on the connection.

        Reaches back to the Connection object and queues the data for
        processing.
        """
        self.transport.connector.factory.conn._iobuf.write(data)
        self.transport.connector.factory.conn.handle_read()

    def connectionMade(self):
        """
        Callback function that is called when a connection has succeeded.

        Reaches back to the Connection object and confirms that the connection
        is ready.
        """
        self.transport.connector.factory.conn.client_connection_made()

    def connectionLost(self, reason):
        # reason is a Failure instance
        self.transport.connector.factory.conn.defunct(reason.value)


class TwistedConnectionClientFactory(protocol.ClientFactory):

    def __init__(self, connection):
        # ClientFactory does not define __init__() in parent classes
        # and does not inherit from object.
        self.conn = connection

    def buildProtocol(self, addr):
        """
        Twisted function that defines which kind of protocol to use
        in the ClientFactory.
        """
        return TwistedConnectionProtocol()

    def clientConnectionFailed(self, connector, reason):
        """
        Overridden twisted callback which is called when the
        connection attempt fails.
        """
        log.debug("Connect failed: %s", reason)
        self.conn.defunct(reason.value)

    def clientConnectionLost(self, connector, reason):
        """
        Overridden twisted callback which is called when the
        connection goes away (cleanly or otherwise).

        It should be safe to call defunct() here instead of just close, because
        we can assume that if the connection was closed cleanly, there are no
        requests to error out. If this assumption turns out to be false, we
        can call close() instead of defunct() when "reason" is an appropriate
        type.
        """
        log.debug("Connect lost: %s", reason)
        self.conn.defunct(reason.value)


class TwistedLoop(object):

    _lock = None
    _thread = None
    _timeout_task = None
    _timeout = None

    def __init__(self):
        self._lock = Lock()
        self._timers = TimerManager()

    def maybe_start(self):
        with self._lock:
            if not reactor.running:
                self._thread = Thread(target=reactor.run,
                                      name="cassandra_driver_event_loop",
                                      kwargs={'installSignalHandlers': False})
                self._thread.daemon = True
                self._thread.start()
                atexit.register(partial(_cleanup, weakref.ref(self)))

    def _cleanup(self):
        if self._thread:
            reactor.callFromThread(reactor.stop)
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                log.warning("Event loop thread could not be joined, so "
                            "shutdown may not be clean. Please call "
                            "Cluster.shutdown() to avoid this.")
            log.debug("Event loop thread was joined")

    def add_timer(self, timer):
        self._timers.add_timer(timer)
        # callFromThread to schedule from the loop thread, where
        # the timeout task can safely be modified
        reactor.callFromThread(self._schedule_timeout, timer.end)

    def _schedule_timeout(self, next_timeout):
        if next_timeout:
            delay = max(next_timeout - time.time(), 0)
            if self._timeout_task and self._timeout_task.active():
                if next_timeout < self._timeout:
                    self._timeout_task.reset(delay)
                    self._timeout = next_timeout
            else:
                self._timeout_task = reactor.callLater(delay, self._on_loop_timer)
                self._timeout = next_timeout

    def _on_loop_timer(self):
        self._timers.service_timeouts()
        self._schedule_timeout(self._timers.next_timeout)


class TwistedConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes the
    Twisted event loop.
    """

    _loop = None

    @classmethod
    def initialize_reactor(cls):
        if not cls._loop:
            cls._loop = TwistedLoop()

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
        self.connector = None

        reactor.callFromThread(self.add_connection)
        self._loop.maybe_start()

    def add_connection(self):
        """
        Convenience function to connect and store the resulting
        connector.
        """
        self.connector = reactor.connectTCP(
            host=self.host, port=self.port,
            factory=TwistedConnectionClientFactory(self),
            timeout=self.connect_timeout)

    def client_connection_made(self):
        """
        Called by twisted protocol when a connection attempt has
        succeeded.
        """
        with self.lock:
            self.is_closed = False
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
        self.connector.disconnect()
        log.debug("Closed socket to %s", self.host)

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_read(self):
        """
        Process the incoming data buffer.
        """
        self.process_io_buffer()

    def push(self, data):
        """
        This function is called when outgoing data should be queued
        for sending.

        Note that we can't call transport.write() directly because
        it is not thread-safe, so we schedule it to run from within
        the event loop when it gets the chance.
        """
        reactor.callFromThread(self.connector.transport.write, data)
