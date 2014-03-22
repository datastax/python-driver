"""
Connection pooling and host management.
"""

import logging
import re
import socket
import time
from threading import RLock, Condition
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import AuthenticationFailed
from cassandra.connection import MAX_STREAM_PER_CONNECTION, ConnectionException

log = logging.getLogger(__name__)


class NoConnectionsAvailable(Exception):
    """
    All existing connections to a given host are busy, or there are
    no open connections.
    """
    pass


# example matches:
# 1.0.0
# 1.0.0-beta1
# 2.0-SNAPSHOT
version_re = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)(?:\.(?P<patch>\d+))?(?:-(?P<label>\w+))?")


class Host(object):
    """
    Represents a single Cassandra node.
    """

    address = None
    """
    The IP address or hostname of the node.
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

    version = None
    """
    A tuple representing the Cassandra version for this host.  This will
    remain as :const:`None` if the version is unknown.
    """

    _datacenter = None
    _rack = None
    _reconnection_handler = None
    lock = None

    _currently_handling_node_up = False
    _handle_node_up_condition = None

    def __init__(self, inet_address, conviction_policy_factory):
        if inet_address is None:
            raise ValueError("inet_address may not be None")
        if conviction_policy_factory is None:
            raise ValueError("conviction_policy_factory may not be None")

        self.address = inet_address
        self.conviction_policy = conviction_policy_factory(self)
        self.lock = RLock()
        self._handle_node_up_condition = Condition()

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

    def set_version(self, version_string):
        match = version_re.match(version_string)
        if match is not None:
            version = [int(match.group('major')), int(match.group('minor')), int(match.group('patch') or 0)]
            if match.group('label'):
                version.append(match.group('label'))
            self.version = tuple(version)

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
            return

        # TODO cancel previous reconnection handlers? That's probably the job
        # of whatever created this.

        first_delay = next(self.schedule)
        self.scheduler.schedule(first_delay, self.run)

    def run(self):
        if self._cancelled:
            return

        conn = None
        try:
            conn = self.try_reconnect()
        except Exception as exc:
            next_delay = self.schedule.next()
            if self.on_exception(exc, next_delay):
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
            log.warn("Error attempting to reconnect to %s, scheduling retry in %f seconds: %s",
                     self.host, next_delay, exc)
            log.debug("Reconnection error details", exc_info=True)
            return True


_MAX_SIMULTANEOUS_CREATION = 1
_MIN_TRASH_INTERVAL = 10


class HostConnectionPool(object):

    host = None
    host_distance = None

    is_shutdown = False
    open_count = 0
    _scheduled_for_creation = 0
    _next_trash_allowed_at = 0

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance

        self._session = weakref.proxy(session)
        self._lock = RLock()
        self._conn_available_condition = Condition()

        log.debug("Initializing new connection pool for host %s", self.host)
        core_conns = session.cluster.get_core_connections_per_host(host_distance)
        self._connections = [session.cluster.connection_factory(host.address)
                             for i in range(core_conns)]

        if session.keyspace:
            for conn in self._connections:
                conn.set_keyspace_blocking(session.keyspace)

        self._trash = set()
        self._next_trash_allowed_at = time.time()
        self.open_count = core_conns
        log.debug("Finished initializing new connection pool for host %s", self.host)

    def borrow_connection(self, timeout):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        conns = self._connections
        if not conns:
            # handled specially just for simpler code
            log.debug("Detected empty pool, opening core conns to %s", self.host)
            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            with self._lock:
                # we check the length of self._connections again
                # along with self._scheduled_for_creation while holding the lock
                # in case multiple threads hit this condition at the same time
                to_create = core_conns - (len(self._connections) + self._scheduled_for_creation)
                for i in range(to_create):
                    self._scheduled_for_creation += 1
                    self._session.submit(self._create_new_connection)

            # in_flight is incremented by wait_for_conn
            conn = self._wait_for_conn(timeout)
            return conn
        else:
            # note: it would be nice to push changes to these config settings
            # to pools instead of doing a new lookup on every
            # borrow_connection() call
            max_reqs = self._session.cluster.get_max_requests_per_connection(self.host_distance)
            max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)

            least_busy = min(conns, key=lambda c: c.in_flight)
            # to avoid another thread closing this connection while
            # trashing it (through the return_connection process), hold
            # the connection lock from this point until we've incremented
            # its in_flight count
            need_to_wait = False
            with least_busy.lock:

                if least_busy.in_flight >= MAX_STREAM_PER_CONNECTION:
                    # once we release the lock, wait for another connection
                    need_to_wait = True
                else:
                    least_busy.in_flight += 1

            if need_to_wait:
                # wait_for_conn will increment in_flight on the conn
                least_busy = self._wait_for_conn(timeout)

            # if we have too many requests on this connection but we still
            # have space to open a new connection against this host, go ahead
            # and schedule the creation of a new connection
            if least_busy.in_flight >= max_reqs and len(self._connections) < max_conns:
                self._maybe_spawn_new_connection()

            return least_busy

    def _maybe_spawn_new_connection(self):
        with self._lock:
            if self._scheduled_for_creation >= _MAX_SIMULTANEOUS_CREATION:
                return
            if self.open_count >= self._session.cluster.get_max_connections_per_host(self.host_distance):
                return
            self._scheduled_for_creation += 1

        log.debug("Submitting task for creation of new Connection to %s", self.host)
        self._session.submit(self._create_new_connection)

    def _create_new_connection(self):
        try:
            self._add_conn_if_under_max()
        except (ConnectionException, socket.error) as exc:
            log.warn("Failed to create new connection to %s: %s", self.host, exc)
        except Exception:
            log.exception("Unexpectedly failed to create new connection")
        finally:
            with self._lock:
                self._scheduled_for_creation -= 1

    def _add_conn_if_under_max(self):
        max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)
        with self._lock:
            if self.is_shutdown:
                return False

            if self.open_count >= max_conns:
                return False

            self.open_count += 1

        log.debug("Going to open new connection to host %s", self.host)
        try:
            conn = self._session.cluster.connection_factory(self.host.address)
            if self._session.keyspace:
                conn.set_keyspace_blocking(self._session.keyspace)
            self._next_trash_allowed_at = time.time() + _MIN_TRASH_INTERVAL
            with self._lock:
                new_connections = self._connections[:] + [conn]
                self._connections = new_connections
            log.debug("Added new connection (%s) to pool for host %s, signaling availablility",
                      id(conn), self.host)
            self._signal_available_conn()
            return True
        except (ConnectionException, socket.error) as exc:
            log.warn("Failed to add new connection to pool for host %s: %s", self.host, exc)
            with self._lock:
                self.open_count -= 1
            if self._session.cluster.signal_connection_failure(self.host, exc, is_host_addition=False):
                self.shutdown()
            return False
        except AuthenticationFailed:
            with self._lock:
                self.open_count -= 1
            return False

    def _await_available_conn(self, timeout):
        with self._conn_available_condition:
            self._conn_available_condition.wait(timeout)

    def _signal_available_conn(self):
        with self._conn_available_condition:
            self._conn_available_condition.notify()

    def _signal_all_available_conn(self):
        with self._conn_available_condition:
            self._conn_available_condition.notify_all()

    def _wait_for_conn(self, timeout):
        start = time.time()
        remaining = timeout

        while remaining > 0:
            # wait on our condition for the possibility that a connection
            # is useable
            self._await_available_conn(remaining)

            # self.shutdown() may trigger the above Condition
            if self.is_shutdown:
                raise ConnectionException("Pool is shutdown")

            conns = self._connections
            if conns:
                least_busy = min(conns, key=lambda c: c.in_flight)
                with least_busy.lock:
                    if least_busy.in_flight < MAX_STREAM_PER_CONNECTION:
                        least_busy.in_flight += 1
                        return least_busy

            remaining = timeout - (time.time() - start)

        raise NoConnectionsAvailable()

    def return_connection(self, connection):
        with connection.lock:
            connection.in_flight -= 1
            in_flight = connection.in_flight

        if connection.is_defunct or connection.is_closed:
            log.debug("Defunct or closed connection (%s) returned to pool, potentially "
                      "marking host %s as down", id(connection), self.host)
            is_down = self._session.cluster.signal_connection_failure(
                    self.host, connection.last_error, is_host_addition=False)
            if is_down:
                self.shutdown()
            else:
                self._replace(connection)
        else:
            if connection in self._trash:
                with connection.lock:
                    if connection.in_flight == 0:
                        with self._lock:
                            if connection in self._trash:
                                self._trash.remove(connection)
                        log.debug("Closing trashed connection (%s) to %s", id(connection), self.host)
                        connection.close()
                return

            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            min_reqs = self._session.cluster.get_min_requests_per_connection(self.host_distance)
            # we can use in_flight here without holding the connection lock
            # because the fact that in_flight dipped below the min at some
            # point is enough to start the trashing procedure
            if len(self._connections) > core_conns and in_flight <= min_reqs and \
                    time.time() >= self._next_trash_allowed_at:
                self._maybe_trash_connection(connection)
            else:
                self._signal_available_conn()

    def _maybe_trash_connection(self, connection):
        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        did_trash = False
        with self._lock:
            if connection not in self._connections:
                return

            if self.open_count > core_conns:
                did_trash = True
                self.open_count -= 1
                new_connections = self._connections[:]
                new_connections.remove(connection)
                self._connections = new_connections

                with connection.lock:
                    if connection.in_flight == 0:
                        log.debug("Skipping trash and closing unused connection (%s) to %s", id(connection), self.host)
                        connection.close()

                        # skip adding it to the trash if we're already closing it
                        return

                self._trash.add(connection)

        if did_trash:
            self._next_trash_allowed_at = time.time() + _MIN_TRASH_INTERVAL
            log.debug("Trashed connection (%s) to %s", id(connection), self.host)

    def _replace(self, connection):
        should_replace = False
        with self._lock:
            if connection in self._connections:
                new_connections = self._connections[:]
                new_connections.remove(connection)
                self._connections = new_connections
                self.open_count -= 1
                should_replace = True

        if should_replace:
            log.debug("Replacing connection (%s) to %s", id(connection), self.host)

            def close_and_replace():
                connection.close()
                self._add_conn_if_under_max()

            self._session.submit(close_and_replace)
        else:
            # just close it
            log.debug("Closing connection (%s) to %s", id(connection), self.host)
            connection.close()

    def shutdown(self):
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True

        self._signal_all_available_conn()
        for conn in self._connections:
            conn.close()
            self.open_count -= 1

        for conn in self._trash:
            conn.close()

    def ensure_core_connections(self):
        if self.is_shutdown:
            return

        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        with self._lock:
            to_create = core_conns - (len(self._connections) + self._scheduled_for_creation)
            for i in range(to_create):
                self._scheduled_for_creation += 1
                self._session.submit(self._create_new_connection)

    def _set_keyspace_for_all_conns(self, keyspace, callback):
        """
        Asynchronously sets the keyspace for all connections.  When all
        connections have been set, `callback` will be called with two
        arguments: this pool, and a list of any errors that occurred.
        """
        remaining_callbacks = set(self._connections)
        errors = []

        if not remaining_callbacks:
            callback(self, errors)
            return

        def connection_finished_setting_keyspace(conn, error):
            remaining_callbacks.remove(conn)
            if error:
                errors.append(error)

            if not remaining_callbacks:
                callback(self, errors)

        for conn in self._connections:
            conn.set_keyspace_async(keyspace, connection_finished_setting_keyspace)

    def get_state(self):
        in_flights = ", ".join([str(c.in_flight) for c in self._connections])
        return "shutdown: %s, open_count: %d, in_flights: %s" % (self.is_shutdown, self.open_count, in_flights)
