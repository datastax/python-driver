# Copyright 2013-2017 DataStax, Inc.
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
Connection pooling and host management.
"""

from functools import total_ordering
import logging
import time
from threading import Lock, RLock, Condition
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import AuthenticationFailed
from cassandra.connection import ConnectionException
from cassandra.policies import HostDistance

log = logging.getLogger(__name__)


class NoConnectionsAvailable(Exception):
    """
    All existing connections to a given host are busy, or there are
    no open connections.
    """
    pass


@total_ordering
class Host(object):
    """
    Represents a single Cassandra node.
    """

    address = None
    """
    The IP address of the node. This is the RPC address the driver uses when connecting to the node
    """

    broadcast_address = None
    """
    broadcast address configured for the node, *if available* ('peer' in system.peers table).
    This is not present in the ``system.local`` table for older versions of Cassandra. It is also not queried if
    :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    listen_address = None
    """
    listen address configured for the node, *if available*. This is only available in the ``system.local`` table for newer
    versions of Cassandra. It is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    Usually the same as ``broadcast_address`` unless configured differently in cassandra.yaml.
    """

    conviction_policy = None
    """
    A :class:`~.ConvictionPolicy` instance for determining when this node should
    be marked up or down.
    """

    is_up = None
    """
    :const:`True` if the node is considered up, :const:`False` if it is
    considered down, and :const:`None` if it is not known if the node is
    up or down.
    """

    release_version = None
    """
    release_version as queried from the control connection system tables
    """

    dse_version = None
    """
    dse_version as queried from the control connection system tables. Only populated when connecting to
    DSE with this property available. Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    dse_workload = None
    """
    DSE workload queried from the control connection system tables. Only populated when connecting to
    DSE with this property available. Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    _datacenter = None
    _rack = None
    _reconnection_handler = None
    lock = None

    _currently_handling_node_up = False

    def __init__(self, inet_address, conviction_policy_factory, datacenter=None, rack=None):
        if inet_address is None:
            raise ValueError("inet_address may not be None")
        if conviction_policy_factory is None:
            raise ValueError("conviction_policy_factory may not be None")

        self.address = inet_address
        self.conviction_policy = conviction_policy_factory(self)
        self.set_location_info(datacenter, rack)
        self.lock = RLock()

    @property
    def datacenter(self):
        """ The datacenter the node is in.  """
        return self._datacenter

    @property
    def rack(self):
        """ The rack the node is in.  """
        return self._rack

    def set_location_info(self, datacenter, rack):
        """
        Sets the datacenter and rack for this node. Intended for internal
        use (by the control connection, which periodically checks the
        ring topology) only.
        """
        self._datacenter = datacenter
        self._rack = rack

    def set_up(self):
        if not self.is_up:
            log.debug("Host %s is now marked up", self.address)
        self.conviction_policy.reset()
        self.is_up = True

    def set_down(self):
        self.is_up = False

    def signal_connection_failure(self, connection_exc):
        return self.conviction_policy.add_failure(connection_exc)

    def is_currently_reconnecting(self):
        return self._reconnection_handler is not None

    def get_and_set_reconnection_handler(self, new_handler):
        """
        Atomically replaces the reconnection handler for this
        host.  Intended for internal use only.
        """
        with self.lock:
            old = self._reconnection_handler
            self._reconnection_handler = new_handler
            return old

    def __eq__(self, other):
        return self.address == other.address

    def __hash__(self):
        return hash(self.address)

    def __lt__(self, other):
        return self.address < other.address

    def __str__(self):
        return str(self.address)

    def __repr__(self):
        dc = (" %s" % (self._datacenter,)) if self._datacenter else ""
        return "<%s: %s%s>" % (self.__class__.__name__, self.address, dc)


class _ReconnectionHandler(object):
    """
    Abstract class for attempting reconnections with a given
    schedule and scheduler.
    """

    _cancelled = False

    def __init__(self, scheduler, schedule, callback, *callback_args, **callback_kwargs):
        self.scheduler = scheduler
        self.schedule = schedule
        self.callback = callback
        self.callback_args = callback_args
        self.callback_kwargs = callback_kwargs

    def start(self):
        if self._cancelled:
            log.debug("Reconnection handler was cancelled before starting")
            return

        first_delay = next(self.schedule)
        self.scheduler.schedule(first_delay, self.run)

    def run(self):
        if self._cancelled:
            return

        conn = None
        try:
            conn = self.try_reconnect()
        except Exception as exc:
            try:
                next_delay = next(self.schedule)
            except StopIteration:
                # the schedule has been exhausted
                next_delay = None

            # call on_exception for logging purposes even if next_delay is None
            if self.on_exception(exc, next_delay):
                if next_delay is None:
                    log.warning(
                        "Will not continue to retry reconnection attempts "
                        "due to an exhausted retry schedule")
                else:
                    self.scheduler.schedule(next_delay, self.run)
        else:
            if not self._cancelled:
                self.on_reconnection(conn)
                self.callback(*(self.callback_args), **(self.callback_kwargs))
        finally:
            if conn:
                conn.close()

    def cancel(self):
        self._cancelled = True

    def try_reconnect(self):
        """
        Subclasses must implement this method.  It should attempt to
        open a new Connection and return it; if a failure occurs, an
        Exception should be raised.
        """
        raise NotImplementedError()

    def on_reconnection(self, connection):
        """
        Called when a new Connection is successfully opened.  Nothing is
        done by default.
        """
        pass

    def on_exception(self, exc, next_delay):
        """
        Called when an Exception is raised when trying to connect.
        `exc` is the Exception that was raised and `next_delay` is the
        number of seconds (as a float) that the handler will wait before
        attempting to connect again.

        Subclasses should return :const:`False` if no more attempts to
        connection should be made, :const:`True` otherwise.  The default
        behavior is to always retry unless the error is an
        :exc:`.AuthenticationFailed` instance.
        """
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            return True


class _HostReconnectionHandler(_ReconnectionHandler):

    def __init__(self, host, connection_factory, is_host_addition, on_add, on_up, *args, **kwargs):
        _ReconnectionHandler.__init__(self, *args, **kwargs)
        self.is_host_addition = is_host_addition
        self.on_add = on_add
        self.on_up = on_up
        self.host = host
        self.connection_factory = connection_factory

    def try_reconnect(self):
        return self.connection_factory()

    def on_reconnection(self, connection):
        log.info("Successful reconnection to %s, marking node up if it isn't already", self.host)
        if self.is_host_addition:
            self.on_add(self.host)
        else:
            self.on_up(self.host)

    def on_exception(self, exc, next_delay):
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            log.warning("Error attempting to reconnect to %s, scheduling retry in %s seconds: %s",
                        self.host, next_delay, exc)
            log.debug("Reconnection error details", exc_info=True)
            return True


class HostConnection(object):

    host = None
    host_distance = None
    is_shutdown = False
    shutdown_on_error = False

    _session = None
    _connection = None
    _lock = None
    _keyspace = None

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance
        self._session = weakref.proxy(session)
        self._lock = Lock()
        # this is used in conjunction with the connection streams. Not using the connection lock because the connection can be replaced in the lifetime of the pool.
        self._stream_available_condition = Condition(self._lock)
        self._is_replacing = False

        if host_distance == HostDistance.IGNORED:
            log.debug("Not opening connection to ignored host %s", self.host)
            return
        elif host_distance == HostDistance.REMOTE and not session.cluster.connect_to_remote_hosts:
            log.debug("Not opening connection to remote host %s", self.host)
            return

        log.debug("Initializing connection for host %s", self.host)
        self._connection = session.cluster.connection_factory(host.address)
        self._keyspace = session.keyspace
        if self._keyspace:
            self._connection.set_keyspace_blocking(self._keyspace)
        log.debug("Finished initializing connection for host %s", self.host)

    def borrow_connection(self, timeout):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        conn = self._connection
        if not conn:
            raise NoConnectionsAvailable()

        start = time.time()
        remaining = timeout
        while True:
            with conn.lock:
                if conn.in_flight <= conn.max_request_id:
                    conn.in_flight += 1
                    return conn, conn.get_request_id()
            if timeout is not None:
                remaining = timeout - time.time() + start
                if remaining < 0:
                    break
            with self._stream_available_condition:
                self._stream_available_condition.wait(remaining)

        raise NoConnectionsAvailable("All request IDs are currently in use")

    def return_connection(self, connection):
        with connection.lock:
            connection.in_flight -= 1
        with self._stream_available_condition:
            self._stream_available_condition.notify()

        if connection.is_defunct or connection.is_closed:
            if connection.signaled_error and not self.shutdown_on_error:
                return

            is_down = False
            if not connection.signaled_error:
                log.debug("Defunct or closed connection (%s) returned to pool, potentially "
                          "marking host %s as down", id(connection), self.host)
                is_down = self._session.cluster.signal_connection_failure(
                    self.host, connection.last_error, is_host_addition=False)
                connection.signaled_error = True

            if self.shutdown_on_error and not is_down:
                is_down = True
                self._session.cluster.on_down(self.host, is_host_addition=False)

            if is_down:
                self.shutdown()
            else:
                self._connection = None
                with self._lock:
                    if self._is_replacing:
                        return
                    self._is_replacing = True
                    self._session.submit(self._replace, connection)

    def _replace(self, connection):
        with self._lock:
            if self.is_shutdown:
                return

        log.debug("Replacing connection (%s) to %s", id(connection), self.host)
        try:
            conn = self._session.cluster.connection_factory(self.host.address)
            if self._keyspace:
                conn.set_keyspace_blocking(self._keyspace)
            self._connection = conn
        except Exception:
            log.warning("Failed reconnecting %s. Retrying." % (self.host.address,))
            self._session.submit(self._replace, connection)
        else:
            with self._lock:
                self._is_replacing = False
                self._stream_available_condition.notify()

    def shutdown(self):
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True
            self._stream_available_condition.notify_all()

        if self._connection:
            self._connection.close()
            self._connection = None

    def _set_keyspace_for_all_conns(self, keyspace, callback):
        if self.is_shutdown or not self._connection:
            return

        def connection_finished_setting_keyspace(conn, error):
            self.return_connection(conn)
            errors = [] if not error else [error]
            callback(self, errors)

        self._keyspace = keyspace
        self._connection.set_keyspace_async(keyspace, connection_finished_setting_keyspace)

    def get_connections(self):
        c = self._connection
        return [c] if c else []

    def get_state(self):
        connection = self._connection
        open_count = 1 if connection and not (connection.is_closed or connection.is_defunct) else 0
        in_flights = [connection.in_flight] if connection else []
        return {'shutdown': self.is_shutdown, 'open_count': open_count, 'in_flights': in_flights}

    @property
    def open_count(self):
        connection = self._connection
        return 1 if connection and not (connection.is_closed or connection.is_defunct) else 0
