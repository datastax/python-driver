# TODO:
# - review locking, race conditions, deadlock
# - get values from proper config
# - proper threadpool submissions

import time
from threading import Lock, RLock, Condition

from connection import MAX_STREAM_PER_CONNECTION

class ConnectionException(Exception):
    pass


class BusyConnectionException(Exception):
    pass


class AuthenticationException(Exception):
    pass


class TimeoutException(Exception):
    pass


class Host(object):

    def __init__(self, inet_address, conviction_policy):
        if inet_address is None:
            raise ValueError("inet_address may not be None")
        if conviction_policy is None:
            raise ValueError("conviction_policy may not be None")

        self._datacenter = None
        self._rack = None

        self.address = inet_address
        self.monitor = HealthMonitor(conviction_policy(self))

        self._reconnection_handler = None
        self._reconnection_lock = Lock()

    def set_location_info(self, datacenter, rack):
        self._datacenter = datacenter
        self._rack = rack

    def get_and_set_reconnection_handler(self, new_handler):
        with self._reconnection_lock:
            if self._reconnection_handler:
                return self._reconnection_handler
            else:
                self._reconnection_handler = new_handler
                return None

    def __eq__(self, other):
        if not isinstance(other, Host):
            return False

        return self.address == other.address

    def __str__(self):
        return self.address


class _ReconnectionHandler(object):

    def __init__(self, scheduler, schedule, callback, *callback_args, **callback_kwargs):
        self.scheduler = scheduler
        self.schedule = schedule
        self.callback = callback
        self.callback_args = callback_args
        self.callback_kwargs = callback_kwargs
        self._cancelled

    def start(self):
        if self._cancelled:
            return

        first_delay = self.schedule.get_next_delay()
        self.scheduler.schedule(first_delay, self.run)
        # TODO cancel previous

    def run(self):
        if self._cancelled:
            return

        # TODO wait for readyForNext?
        self.on_reconnection(self.try_reconnect())
        self.callback(*self.callback_args, **self.callback_kwargs)

    def cancel(self):
        self._cancelled = True

    def reschedule(self, delay):
        if self._cancelled:
            return

        try:
            self.scheduler.schedule(delay, self.run)
        except Exception, exc:
            next_delay = self.schedule.get_next_delay()
            if self.on_exception(exc, next_delay):
                self.reschedule(next_delay)

    def try_reconnect(self):
        raise NotImplemented()

    def on_reconnection(self, connection):
        raise NotImplemented()

    def on_exception(self, exc, next_delay):
        if isinstance(exc, AuthenticationException):
            return False
        else:
            return True


class _HostReconnectionHandler(_ReconnectionHandler):

    def __init__(self, host, connection_factory, *args, **kwargs):
        _ReconnectionHandler.__init__(self, *args, **kwargs)
        self.host = host
        self.connection_factory = connection_factory

    def try_reconnect(self):
        return self.connection_factory.open(self.host)

    def on_reconnection(self, connection):
        self.host.monitor.reset()

    def on_exception(self, exc, next_delay):
        # TODO only overridden to add logging, so add logging
        if isinstance(exc, AuthenticationException):
            return False
        else:
            return True


class HealthMonitor(object):

    is_up = True

    def __init__(self, conviction_policy):
        self._conviction_policy = conviction_policy
        self._host = conviction_policy.host
        self._listeners = set()
        self._lock = RLock()

    def register(self, listener):
        with self._lock:
            self._listeners.add(listener)

    def unregister(self, listener):
        with self._lock:
            self._listeners.remove(listener)

    def set_down(self):
        self.is_up = False

        with self._lock:
            listeners = self._listeners.copy()

        for listener in listeners:
            listener.on_down(self._host)

    def reset(self):
        self._conviction_policy.reset()

        with self._lock:
            listeners = self._listeners.copy()

        for listener in listeners:
            listener.on_up(self._host)

        self.is_up = True

    def signal_connection_failure(self, connection_exc):
        is_down = self._conviction_policy.add_failure(connection_exc)
        if is_down:
            self.set_down()
        return is_down


class HostConnectionPool(object):

    host = None
    host_distance = None

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance

        self._session = session
        self._is_shutdown = False
        self._lock = RLock()
        self._conn_available_condition = Condition()

        # TODO potentially use threading.Queue for this
        core_conns = session.cluster.get_core_connections_per_host(host_distance)
        self._connections = [session.connection_factory.open(host)
                             for i in range(core_conns)]
        self._trash = set()
        self._open_count = len(self._connections)
        self._scheduled_for_creation = 0

    def borrow_connection(self, timeout):
        with self._lock:
            if self._is_shutdown:
                raise ConnectionException("Pool is shutdown", self.host)

        if not self._connections:
            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            for i in range(core_conns):
                with self._lock:
                    self._scheduled_for_creation += 1
                    self._session.submit(self._create_new_connection)

                conn = self._wait_for_conn(timeout)
                conn.set_keyspace()  # TODO need to get keyspace from pool
                return conn
        else:
            least_busy = min(self._connections, key=lambda c: c.in_flight)
            max_reqs = self._session.cluster.get_max_requests_per_connection(self.host_distance)
            max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)

            # if we have too many requests on this connection but we still
            # have space to open a new connection against this host
            if least_busy.in_flight >= max_reqs and len(self._connections) < max_conns:
                self.maybe_spawn_new_connection()

            while True:
                need_to_wait = False
                with least_busy._lock:
                    if least_busy.in_flight >= MAX_STREAM_PER_CONNECTION:
                        need_to_wait = True
                    else:
                        least_busy.in_flight += 1
                        break

                if need_to_wait:
                    least_busy = self._wait_for_conn(timeout)
                    break

            least_busy.set_keyspace()  # TODO get keyspace from pool state
            return least_busy

    def _create_new_connection(self):
        self._add_conn_if_under_max()
        with self._lock:
            self._scheduled_for_creation -= 1

    def _add_conn_if_under_max(self):
        max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)
        with self._lock:
            if self._is_shutdown:
                return False

            if self._open_count >= max_conns:
                return False

            self._open_count += 1

        try:
            conn = self._session.connection_factory.open(self.host)
            with self._lock:
                self._connections.append(conn)
        except ConnectionException:
            with self._lock:
                self._open_count -= 1
            return False
        except AuthenticationException:
            with self._lock:
                self._open_count -= 1
            return False

    def _await_available_conn(self, timeout):
        with self._available_conn_condition:
            self._available_conn_condition.wait(timeout)

    def _signal_available_conn(self):
        with self._available_conn_condition:
            self._available_conn_condition.notify()

    def _signal_all_available_conn(self):
        with self._available_conn_condition:
            self._available_conn_condition.notify_all()

    def _wait_for_conn(self, timeout):
        start = time.time()
        remaining = timeout

        while True:
            self._await_available_conn(remaining)

            if self._is_shutdown:
                raise ConnectionException("Pool is shutdown")

            least_busy = min(self._connections, key=lambda c: c.in_flight)
            while True:
                with least_busy._lock:
                    if least_busy.in_flight >= MAX_STREAM_PER_CONNECTION:
                        break

                    least_busy.in_flight += 1
                    return least_busy

            remaining = timeout - (time.time() - start)
            if remaining <= 0:
                raise TimeoutException()

    def return_connection(self, conn):
        with conn._lock:
            conn.in_flight -= 1
            in_flight = conn.in_flight

        if conn.is_defunct:
            is_down = self.host.monitor.signal_connection_failure(conn.last_exception)
            if is_down:
                self.shutdown()
            else:
                self.replace(conn)
        else:
            with self._lock:
                if conn in self._trash and in_flight == 0:
                    self._trash.remove(conn)
                    conn.close()
                    return

            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            min_reqs = self._session.cluster.get_min_requests_per_connection(self.host_distance)
            if len(self._connections) > core_conns and in_flight <= min_reqs:
                self._trash_connection(conn)
            else:
                self._signal_available_conn()

    def _trash_connection(self, conn):
        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        with self._lock:
            if self._open_count <= core_conns:
                return False

            self._open_count -= 1

            self._connections.remove(conn)
            with conn._lock:
                if conn.in_flight == 0:
                    conn.close()
                else:
                    self._trash.add(conn)

            return True

    def _replace(self, conn):
        with self._lock:
            self._connections.remove(conn)

        def close_and_replace():
            conn.close()
            self._add_conn_if_under_max()

        self._session.submit(close_and_replace)

    def _close(self, conn):
        self._session.submit(conn.close)

    @property
    def is_shutdown(self):
        return self._is_shutdown

    @property
    def open_count(self):
        return self._open_count

    def shutdown(self):
        with self._lock:
            if self._is_shutdown:
                return
            else:
                self._is_shutdown = True

        self._signal_all_available_conn()
        for conn in self._connections:
            conn.close()
            self._open_count -= 1

    def ensure_core_connections(self):
        if self._is_shutdown:
            return

        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        for i in range(core_conns - self._open_count):
            with self._lock:
                self._scheduled_for_creation += 1
                self._session.submit(self._create_new_connection)
