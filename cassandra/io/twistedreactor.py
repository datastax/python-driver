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
"""
Module that implements an event loop based on twisted
( https://twistedmatrix.com ).
"""
from twisted.internet import reactor, protocol
from threading import Event, Thread
from functools import partial
import logging
import weakref
import atexit
import os

from io import BytesIO

from cassandra import OperationTimedOut
from cassandra.connection import Connection, ConnectionShutdown
from cassandra.protocol import RegisterMessage
from cassandra.marshal import int32_unpack


log = logging.getLogger(__name__)


def _cleanup(cleanup_weakref):
    try:
        cleanup_weakref()
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
        self.conn.close()

    def clientConnectionLost(self, connector, reason):
        """
        Overridden twisted callback which is called when the
        connection goes away (cleanly or otherwise).
        """
        log.debug("Connect lost: %s", reason)
        self.conn.close()


class TwistedConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes the
    Twisted event loop.
    """

    _total_reqd_bytes = 0

    @classmethod
    def factory(cls, *args, **kwargs):
        """
        A factory function which returns connections which have
        succeeded in connecting and are ready for service (or
        raises an exception otherwise).
        """
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
        """
        Initialization method.

        Note that we can't call reactor methods directly here because
        it's not thread-safe, so we schedule the reactor/connection
        stuff to be run from the event loop thread when it gets the
        chance.
        """
        Connection.__init__(self, *args, **kwargs)

        self.connected_event = Event()
        self._iobuf = BytesIO()
        self._thread = None
        self.is_closed = True
        self.connector = None

        self._callbacks = {}
        reactor.callFromThread(self.add_connection)

        if not reactor.running:  # XXX: might want a lock here?
            self._thread = Thread(target=reactor.run,
                                  name="cassandra_driver_event_loop",
                                  kwargs={'installSignalHandlers': False})
            self._thread.daemon = True
            self._thread.start()
            atexit.register(partial(_cleanup, weakref.ref(self)))

    def add_connection(self):
        """
        Convenience function to connect and store the resulting
        connector.
        """
        self.connector = reactor.connectTCP(
            host=self.host, port=self.port,
            factory=TwistedConnectionClientFactory(self))

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
        Disconnect and error-out all callbacks.
        """
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        self.connector.disconnect()
        log.debug("Closed socket to %s", self.host)

        if not self.is_defunct:
            self.error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_read(self):
        """
        Process the incoming data buffer.
        """
        while True:
            pos = self._iobuf.tell()
            if pos < 8 or (self._total_reqd_bytes > 0 and
                           pos < self._total_reqd_bytes):
                # we don't have a complete header yet or we
                # already saw a header, but we don't have a
                # complete message yet
                return
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
                    return

    def push(self, data):
        """
        This function is called when outgoing data should be queued
        for sending.

        Note that we can't call transport.write() directly because
        it is not thread-safe, so we schedule it to run from within
        the event loop when it gets the chance.
        """
        reactor.callFromThread(self.connector.transport.write, data)

    def _cleanup(self):
        if self._thread:
            reactor.callFromThread(reactor.stop)
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                log.warning("Event loop thread could not be joined, so "
                            "shutdown may not be clean. Please call "
                            "Cluster.shutdown() to avoid this.")
            log.debug("Event loop thread was joined")

    def register_watcher(self, event_type, callback, register_timeout=None):
        """
        Register a callback for a given event type.
        """
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]),
            timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        """
        Register multiple callback/event type pairs, expressed as a dict.
        """
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()),
            timeout=register_timeout)
