"""
Connection pooling and host management.
"""

import logging
import time
from threading import Lock, RLock, Condition
import traceback
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet

from cassandra import AuthenticationFailed
from cassandra.connection import MAX_STREAM_PER_CONNECTION, ConnectionException

log = logging.getLogger(__name__)


class NoConnectionsAvailable(Exception):
    """
    All existing connections to a given host are busy, or there are
    no open connections.
    """
    pass


class Host(object):
    """
    Represents a single Cassandra node.
    """

    address = None
    """
    The IP address or hostname of the node.
    """

    monitor = None
    """
    A :class:`.HealthMonitor` instance that tracks whether this node is
    up or down.
    """

    _datacenter = None
    _rack = None
    _reconnection_handler = None

    def __init__(self, inet_address, conviction_policy_factory):
        if inet_address is None:
            raise ValueError("inet_address may not be None")
        if conviction_policy_factory is None:
            raise ValueError("conviction_policy_factory may not be None")

        self.address = inet_address
        self.monitor = HealthMonitor(conviction_policy_factory(self))

        self._reconnection_lock = Lock()

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

    def get_and_set_reconnection_handler(self, new_handler):
        """
        Atomically replaces the reconnection handler for this
        host.  Intended for internal use only.
        """
        with self._reconnection_lock:
            old = self._reconnection_handler
            self._reconnection_handler = new_handler
            return old

    def __eq__(self, other):
        if not isinstance(other, Host):
            return False

        return self.address == other.address

    def __str__(self):
        return self.address

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

        first_delay = self.schedule.next()
        self.scheduler.schedule(first_delay, self.run)

    def run(self):
        if self._cancelled:
            self.callback(*(self.callback_args), **(self.callback_kwargs))

        try:
            self.on_reconnection(self.try_reconnect())
        except Exception, exc:
            next_delay = self.schedule.next()
            if self.on_exception(exc, next_delay):
                self.scheduler.schedule(next_delay, self.run)
        else:
            self.callback(*(self.callback_args), **(self.callback_kwargs))

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

    def __init__(self, host, connection_factory, *args, **kwargs):
        _ReconnectionHandler.__init__(self, *args, **kwargs)
        self.host = host
        self.connection_factory = connection_factory

    def try_reconnect(self):
        return self.connection_factory()

    def on_reconnection(self, connection):
        self.host.monitor.reset()

    def on_exception(self, exc, next_delay):
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            log.warn("Error attempting to reconnect to %s: %s", self.host, exc)
            log.debug(traceback.format_exc(exc))
            return True


class HealthMonitor(object):
    """
    Monitors whether a particular host is marked as up or down.
    This class is primarily intended for internal use, although
    applications may find it useful to check whether a given node
    is up or down.
    """

    is_up = True
    """
    A boolean representing the current state of the node.
    """

    def __init__(self, conviction_policy):
        self._conviction_policy = conviction_policy
        self._host = conviction_policy.host
        # self._listeners will hold, among other things, references to
        # Cluster objects.  To allow those to be GC'ed (and shutdown) even
        # though we've implemented __del__, use weak references.
        self._listeners = WeakSet()
        self._lock = RLock()

    def register(self, listener):
        with self._lock:
            self._listeners.add(listener)

    def unregister(self, listener):
        with self._lock:
            self._listeners.remove(listener)

    def set_up(self):
        if self.is_up:
            return

        self._conviction_policy.reset()
        log.info("Host %s is considered up", self._host)

        with self._lock:
            listeners = self._listeners.copy()

        for listener in listeners:
            listener.on_up(self._host)

        self.is_up = True

    def set_down(self):
        if not self.is_up:
            return

        self.is_up = False
        log.info("Host %s is considered down", self._host)

        with self._lock:
            listeners = self._listeners.copy()

        for listener in listeners:
            listener.on_down(self._host)

    def reset(self):
        return self.set_up()

    def signal_connection_failure(self, connection_exc):
        is_down = self._conviction_policy.add_failure(connection_exc)
        if is_down:
            self.set_down()
        return is_down


_MAX_SIMULTANEOUS_CREATION = 1


class HostConnectionPool(object):

    host = None
    host_distance = None

    is_shutdown = False
    open_count = 0
    _scheduled_for_creation = 0

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance

        self._session = weakref.proxy(session)
        self._lock = RLock()
        self._conn_available_condition = Condition()

        core_conns = session.cluster.get_core_connections_per_host(host_distance)
        self._connections = [session.cluster.connection_factory(host.address)
                             for i in range(core_conns)]
        self._trash = set()
        self.open_count = core_conns

    def borrow_connection(self, timeout):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        conns = self._connections
        if not conns:
            # handled specially just for simpler code
            log.debug("Detected empty pool, opening core conns to %s" % (self.host,))
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
            conn.set_keyspace(self._session.keyspace)
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
            with least_busy.lock:

                # if we have too many requests on this connection but we still
                # have space to open a new connection against this host, go ahead
                # and schedule the creation of a new connection
                if least_busy.in_flight >= max_reqs and len(self._connections) < max_conns:
                    self._maybe_spawn_new_connection()

                if least_busy.in_flight >= MAX_STREAM_PER_CONNECTION:
                    # once we release the lock, wait for another connection
                    need_to_wait = True
                else:
                    need_to_wait = False
                    least_busy.in_flight += 1

            if need_to_wait:
                # wait_for_conn will increment in_flight on the conn
                least_busy = self._wait_for_conn(timeout)

            least_busy.set_keyspace(self._session.keyspace)
            return least_busy

    def _maybe_spawn_new_connection(self):
        with self._lock:
            if self._scheduled_for_creation >= _MAX_SIMULTANEOUS_CREATION:
                return
            self._scheduled_for_creation += 1

        log.debug("Submitting task for creation of new Connection to %s" % (self.host,))
        self._session.submit(self._create_new_connection)

    def _create_new_connection(self):
        try:
            self._add_conn_if_under_max()
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

        try:
            conn = self._session.cluster.connection_factory(self.host.address)
            with self._lock:
                new_connections = self._connections[:] + [conn]
                self._connections = new_connections
            self._signal_available_conn()
            return True
        except ConnectionException, exc:
            log.exception("Failed to add new connection to pool for host %s" % (self.host,))
            with self._lock:
                self.open_count -= 1
            if self.host.monitor.signal_connection_failure(exc):
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
            is_down = self.host.monitor.signal_connection_failure(connection.last_error)
            if is_down:
                self.shutdown()
            else:
                self._replace(connection)
        else:
            if connection in self._trash:
                with connection.lock:
                    if in_flight == 0:
                        with self._lock:
                            self._trash.remove(connection)
                        connection.close()
                return

            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            min_reqs = self._session.cluster.get_min_requests_per_connection(self.host_distance)
            # we can use in_flight here without holding the connection lock
            # because the fact that in_flight dipped below the min at some
            # point is enough to start the trashing procedure
            if len(self._connections) > core_conns and in_flight <= min_reqs:
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
                        connection.close()

                        # skip adding it to the trash if we're already closing it
                        return

                self._trash.add(connection)

        if did_trash:
            log.debug("Trashed connection to %s" % (self.host,))

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
            log.debug("Replacing connection to %s" % (self.host,))

            def close_and_replace():
                connection.close()
                self._add_conn_if_under_max()

            self._session.submit(close_and_replace)
        else:
            # just close it
            log.debug("Closing connection to %s" % (self.host,))
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

        reconnector = self.host.get_and_set_reconnection_handler(None)
        if reconnector:
            reconnector.cancel()

    def ensure_core_connections(self):
        if self.is_shutdown:
            return

        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        with self._lock:
            to_create = core_conns - (len(self._connections) + self._scheduled_for_creation)
            for i in range(to_create):
                self._scheduled_for_creation += 1
                self._session.submit(self._create_new_connection)
