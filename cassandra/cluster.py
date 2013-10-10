"""
This module houses the main classes you will interact with,
:class:`.Cluster` and :class:`.Session`.
"""

from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import time
from threading import Lock, RLock, Thread, Event
import Queue
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from functools import partial
from itertools import groupby

from cassandra import ConsistencyLevel, AuthenticationFailed
from cassandra.connection import ConnectionException, ConnectionShutdown
from cassandra.decoder import (QueryMessage, ResultMessage,
                               ErrorMessage, ReadTimeoutErrorMessage,
                               WriteTimeoutErrorMessage,
                               UnavailableErrorMessage,
                               OverloadedErrorMessage,
                               PrepareMessage, ExecuteMessage,
                               PreparedQueryNotFound,
                               IsBootstrappingErrorMessage, named_tuple_factory,
                               dict_factory)
from cassandra.metadata import Metadata
from cassandra.metrics import Metrics
from cassandra.policies import (RoundRobinPolicy, SimpleConvictionPolicy,
                                ExponentialReconnectionPolicy, HostDistance,
                                RetryPolicy)
from cassandra.query import (SimpleStatement, PreparedStatement, BoundStatement,
                             bind_params, QueryTrace, Statement)
from cassandra.pool import (_ReconnectionHandler, _HostReconnectionHandler,
                            HostConnectionPool)

# libev is all around faster, so we want to try and default to using that when we can
try:
    from cassandra.io.libevreactor import LibevConnection as DefaultConnection
except ImportError:
    from cassandra.io.asyncorereactor import AsyncoreConnection as DefaultConnection  # NOQA

# Forces load of utf8 encoding module to avoid deadlock that occurs
# if code that is being imported tries to import the module in a seperate
# thread.
# See http://bugs.python.org/issue10923
"".encode('utf8')

log = logging.getLogger(__name__)


DEFAULT_MIN_REQUESTS = 5
DEFAULT_MAX_REQUESTS = 100

DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST = 2
DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST = 8

DEFAULT_MIN_CONNECTIONS_PER_REMOTE_HOST = 1
DEFAULT_MAX_CONNECTIONS_PER_REMOTE_HOST = 2


class NoHostAvailable(Exception):
    """
    Raised when an operation is attempted but all connections are
    busy, defunct, closed, or resulted in errors when used.
    """

    errors = None
    """
    A map of the form ``{ip: exception}`` which details the particular
    Exception that was caught for each host the operation was attempted
    against.
    """

    def __init__(self, message, errors):
        Exception.__init__(self, message, errors)
        self.errors = errors


class Cluster(object):
    """
    The main class to use when interacting with a Cassandra cluster.
    Typically, one instance of this class will be created for each
    separate Cassandra cluster that your application interacts with.

    Example usage::

        >>> from cassandra.cluster import Cluster
        >>> cluster = Cluster(['192.168.1.1', '192.168.1.2'])
        >>> session = cluster.connect()
        >>> session.execute("CREATE KEYSPACE ...")
        >>> ...
        >>> cluster.shutdown()

    """

    port = 9042
    """
    The server-side port to open connections to. Defaults to 9042.
    """

    compression = True
    """
    Whether or not compression should be enabled when possible. Defaults to
    :const:`True` and attempts to use snappy compression.
    """

    auth_provider = None
    """
    An optional function that accepts one argument, the IP address of a node,
    and returns a dict of credentials for that node.
    """

    load_balancing_policy = RoundRobinPolicy()
    """
    An instance of :class:`.policies.LoadBalancingPolicy` or
    one of its subclasses.  Defaults to :class:`~.RoundRobinPolicy`.
    """

    reconnection_policy = ExponentialReconnectionPolicy(1.0, 600.0)
    """
    An instance of :class:`.policies.ReconnectionPolicy`. Defaults to an instance
    of :class:`.ExponentialReconnectionPolicy` with a base delay of one second and
    a max delay of ten minutes.
    """

    default_retry_policy = RetryPolicy()
    """
    A default :class:`.policies.RetryPolicy` instance to use for all
    :class:`.Statement` objects which do not have a :attr:`~.Statement.retry_policy`
    explicitly set.
    """

    conviction_policy_factory = SimpleConvictionPolicy
    """
    A factory function which creates instances of
    :class:`.policies.ConvictionPolicy`.  Defaults to
    :class:`.policies.SimpleConvictionPolicy`.
    """

    metrics_enabled = False
    """
    Whether or not metric collection is enabled.
    """

    metrics = None
    """
    An instance of :class:`.metrics.Metrics` if :attr:`.metrics_enabled` is
    :const:`True`, else :const:`None`.
    """

    ssl_options = None
    """
    A optional dict which will be used as kwargs for ``ssl.wrap_socket()``
    when new sockets are created.  This should be used when client encryption
    is enabled in Cassandra.

    By default, a ``ca_certs`` value should be supplied (the value should be
    a string pointing to the location of the CA certs file), and you probably
    want to specify ``ssl_version`` as ``ssl.PROTOCOL_TLSv1`` to match
    Cassandra's default protocol.
    """

    sockopts = None
    """
    An optional list of tuples which will be used as arguments to
    ``socket.setsockopt()`` for all created sockets.
    """

    max_schema_agreement_wait = 10
    """
    The maximum duration (in seconds) that the driver will wait for schema
    agreement across the cluster. Defaults to ten seconds.
    """

    metadata = None
    """
    An instance of :class:`cassandra.metadata.Metadata`.
    """

    connection_class = DefaultConnection
    """
    This determines what event loop system will be used for managing
    I/O with Cassandra.  These are the current options:

    * :class:`cassandra.io.asyncorereactor.AsyncoreConnection`
    * :class:`cassandra.io.libevreactor.LibevConnection`

    By default, ``AsyncoreConnection`` will be used, which uses
    the ``asyncore`` module in the Python standard library.  The
    performance is slightly worse than with ``libev``, but it is
    supported on a wider range of systems.

    If ``libev`` is installed, ``LibevConnection`` will be used instead.
    """

    sessions = None
    control_connection = None
    scheduler = None
    executor = None
    _is_shutdown = False
    _is_setup = False
    _prepared_statements = None

    def __init__(self,
                 contact_points=("127.0.0.1",),
                 port=9042,
                 compression=True,
                 auth_provider=None,
                 load_balancing_policy=None,
                 reconnection_policy=None,
                 default_retry_policy=None,
                 conviction_policy_factory=None,
                 metrics_enabled=False,
                 connection_class=None,
                 ssl_options=None,
                 sockopts=None,
                 cql_version=None,
                 executor_threads=2,
                 max_schema_agreement_wait=10):
        """
        Any of the mutable Cluster attributes may be set as keyword arguments
        to the constructor.
        """
        if 'gevent.monkey' in sys.modules:
            raise Exception(
                "gevent monkey-patching detected. This driver does not currently "
                "support gevent, and monkey patching will break the driver "
                "completely. You can track progress towards adding gevent "
                "support here: https://datastax-oss.atlassian.net/browse/PYTHON-7.")

        self.contact_points = contact_points
        self.port = port
        self.compression = compression

        if auth_provider is not None:
            if not callable(auth_provider):
                raise ValueError("auth_provider must be callable")
            self.auth_provider = auth_provider

        if load_balancing_policy is not None:
            self.load_balancing_policy = load_balancing_policy

        if reconnection_policy is not None:
            self.reconnection_policy = reconnection_policy

        if default_retry_policy is not None:
            self.default_retry_policy = default_retry_policy

        if conviction_policy_factory is not None:
            if not callable(conviction_policy_factory):
                raise ValueError("conviction_policy_factory must be callable")
            self.conviction_policy_factory = conviction_policy_factory

        if connection_class is not None:
            self.connection_class = connection_class

        self.metrics_enabled = metrics_enabled
        self.ssl_options = ssl_options
        self.sockopts = sockopts
        self.cql_version = cql_version
        self.max_schema_agreement_wait = max_schema_agreement_wait

        # let Session objects be GC'ed (and shutdown) when the user no longer
        # holds a reference. Normally the cycle detector would handle this,
        # but implementing __del__ prevents that.
        self.sessions = WeakSet()
        self.metadata = Metadata(self)
        self.control_connection = None
        self._prepared_statements = {}

        self._min_requests_per_connection = {
            HostDistance.LOCAL: DEFAULT_MIN_REQUESTS,
            HostDistance.REMOTE: DEFAULT_MIN_REQUESTS
        }

        self._max_requests_per_connection = {
            HostDistance.LOCAL: DEFAULT_MAX_REQUESTS,
            HostDistance.REMOTE: DEFAULT_MAX_REQUESTS
        }

        self._core_connections_per_host = {
            HostDistance.LOCAL: DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST,
            HostDistance.REMOTE: DEFAULT_MIN_CONNECTIONS_PER_REMOTE_HOST
        }

        self._max_connections_per_host = {
            HostDistance.LOCAL: DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST,
            HostDistance.REMOTE: DEFAULT_MAX_CONNECTIONS_PER_REMOTE_HOST
        }

        self.executor = ThreadPoolExecutor(max_workers=executor_threads)
        self.scheduler = _Scheduler(self.executor)

        self._lock = RLock()

        if self.metrics_enabled:
            self.metrics = Metrics(weakref.proxy(self))

        self.control_connection = ControlConnection(self)
        for address in contact_points:
            self.add_host(address, signal=True)

    def get_min_requests_per_connection(self, host_distance):
        return self._min_requests_per_connection[host_distance]

    def set_min_requests_per_connection(self, host_distance, min_requests):
        self._min_requests_per_connection[host_distance] = min_requests

    def get_max_requests_per_connection(self, host_distance):
        return self._max_requests_per_connection[host_distance]

    def set_max_requests_per_connection(self, host_distance, max_requests):
        self._max_requests_per_connection[host_distance] = max_requests

    def get_core_connections_per_host(self, host_distance):
        return self._core_connections_per_host[host_distance]

    def set_core_connections_per_host(self, host_distance, core_connections):
        old = self._core_connections_per_host[host_distance]
        self._core_connections_per_host[host_distance] = core_connections
        if old < core_connections:
            self.ensure_core_connections()

    def get_max_connections_per_host(self, host_distance):
        return self._max_connections_per_host[host_distance]

    def set_max_connections_per_host(self, host_distance, max_connections):
        self._max_connections_per_host[host_distance] = max_connections

    def connection_factory(self, address, *args, **kwargs):
        """
        Called to create a new connection with proper configuration.
        Intended for internal use only.
        """
        kwargs = self._make_connection_kwargs(address, kwargs)
        return self.connection_class.factory(address, *args, **kwargs)

    def _make_connection_factory(self, host, *args, **kwargs):
        kwargs = self._make_connection_kwargs(host.address, kwargs)
        return partial(self.connection_class.factory, host.address, *args, **kwargs)

    def _make_connection_kwargs(self, address, kwargs_dict):
        if self.auth_provider:
            kwargs_dict['credentials'] = self.auth_provider(address)

        kwargs_dict['port'] = self.port
        kwargs_dict['compression'] = self.compression
        kwargs_dict['sockopts'] = self.sockopts
        kwargs_dict['ssl_options'] = self.ssl_options
        kwargs_dict['cql_version'] = self.cql_version

        return kwargs_dict

    def connect(self, keyspace=None):
        """
        Creates and returns a new :class:`~.Session` object.  If `keyspace`
        is specified, that keyspace will be the default keyspace for
        operations on the ``Session``.
        """
        with self._lock:
            if self._is_shutdown:
                raise Exception("Cluster is already shut down")

            if not self._is_setup:
                self.load_balancing_policy.populate(
                    weakref.proxy(self), self.metadata.all_hosts())
                self._is_setup = True

            if self.control_connection:
                try:
                    self.control_connection.connect()
                    log.debug("Control connection created")
                except Exception:
                    log.exception("Control connection failed to connect, "
                                  "shutting down Cluster:")
                    self.shutdown()
                    raise

        session = self._new_session()
        if keyspace:
            session.set_keyspace(keyspace)
        return session

    def shutdown(self):
        """
        Closes all sessions and connection associated with this Cluster.
        Once shutdown, a Cluster should not be used for any purpose.
        """
        with self._lock:
            if self._is_shutdown:
                raise Exception("The Cluster was already shutdown")
            else:
                self._is_shutdown = True

        if self.scheduler:
            self.scheduler.shutdown()

        if self.control_connection:
            self.control_connection.shutdown()

        if self.sessions:
            for session in self.sessions:
                session.shutdown()

        if self.executor:
            self.executor.shutdown()

    def __del__(self):
        # we don't use shutdown() because we want to avoid shutting down
        # Sessions while they are still being used (in case there are no
        # longer any references to this Cluster object, but there are
        # still references to the Session object)
        if not self._is_shutdown:
            if self.scheduler:
                self.scheduler.shutdown()
            if self.control_connection:
                self.control_connection.shutdown()
            if self.executor:
                self.executor.shutdown(wait=False)

    def _new_session(self):
        session = Session(self, self.metadata.all_hosts())
        self.sessions.add(session)
        return session

    def on_up(self, host):
        """
        Called when a host is marked up by its :class:`~.HealthMonitor`.
        Intended for internal use only.
        """
        reconnector = host.get_and_set_reconnection_handler(None)
        if reconnector:
            reconnector.cancel()

        self._prepare_all_queries(host)

        self.control_connection.on_up(host)
        for session in self.sessions:
            session.on_up(host)

    def on_down(self, host):
        """
        Called when a host is marked down by its :class:`~.HealthMonitor`.
        Intended for internal use only.
        """
        self.control_connection.on_down(host)
        for session in self.sessions:
            session.on_down(host)

        schedule = self.reconnection_policy.new_schedule()

        # in order to not hold references to this Cluster open and prevent
        # proper shutdown when the program ends, we'll just make a closure
        # of the current Cluster attributes to create new Connections with
        conn_factory = self._make_connection_factory(host)

        reconnector = _HostReconnectionHandler(
            host, conn_factory, self.scheduler, schedule,
            host.get_and_set_reconnection_handler, new_handler=None)

        old_reconnector = host.get_and_set_reconnection_handler(reconnector)
        if old_reconnector:
            old_reconnector.cancel()

        reconnector.start()

    def add_host(self, address, signal):
        """
        Called when adding initial contact points and when the control
        connection subsequently discovers a new node.  Intended for internal
        use only.
        """
        log.info("Now considering host %s for new connections", address)
        new_host = self.metadata.add_host(address)
        if new_host and signal:
            self._prepare_all_queries(new_host)
            self.control_connection.on_add(new_host)
            for session in self.sessions:  # TODO need to copy/lock?
                session.on_add(new_host)

        return new_host

    def remove_host(self, host):
        """
        Called when the control connection observes that a node has left the
        ring.  Intended for internal use only.
        """
        log.info("Host %s will no longer be considered for new connections", host)
        if host and self.metadata.remove_host(host):
            self.control_connection.on_remove(host)
            for session in self.sessions:
                session.on_remove(host)

    def ensure_core_connections(self):
        """
        If any host has fewer than the configured number of core connections
        open, attempt to open connections until that number is met.
        """
        for session in self.sessions:
            for pool in session._pools.values():
                pool.ensure_core_connections()

    def submit_schema_refresh(self, keyspace=None, table=None):
        """
        Schedule a refresh of the internal representation of the current
        schema for this cluster.  If `keyspace` is specified, only that
        keyspace will be refreshed, and likewise for `table`.
        """
        return self.executor.submit(
            self.control_connection.refresh_schema, keyspace, table)

    def _prepare_all_queries(self, host):
        if not self._prepared_statements:
            return

        log.debug("Preparing all known prepared statements against host %s" % (host,))
        try:
            connection = self.connection_factory(host.address)
            try:
                self.control_connection.wait_for_schema_agreement(connection)
            except Exception:
                pass

            statements = self._prepared_statements.values()
            for keyspace, ks_statements in groupby(statements, lambda s: s.keyspace):
                if keyspace is not None:
                    connection.set_keyspace(keyspace)

                # note: we could potentially prepare some of these in parallel,
                # but at the same time, we don't want to put too much load on
                # the server at once
                for statement in ks_statements:
                    message = PrepareMessage(query=statement.query_string)
                    try:
                        response = connection.wait_for_response(message)
                        if (not isinstance(response, ResultMessage) or
                            response.kind != ResultMessage.KIND_PREPARED):
                            log.debug("Got unexpected response when preparing "
                                      "statement on host %s: %r" % (host, response))
                    except Exception:
                        log.exception("Error trying to prepare statement on "
                                      "host %s" % (host,))

        except Exception:
            # log and ignore
            log.exception("Error trying to prepare all statements on host %s" % (host,))

    def prepare_on_all_sessions(self, query_id, prepared_statement, excluded_host):
        self._prepared_statements[query_id] = prepared_statement
        for session in self.sessions:
            session.prepare_on_all_hosts(prepared_statement.query_string, excluded_host)


class Session(object):
    """
    A collection of connection pools for each host in the cluster.
    Instances of this class should not be created directly, only
    using :meth:`.Cluster.connect()`.

    Queries and statements can be executed through ``Session`` instances
    using the :meth:`~.Session.execute()` and :meth:`~.Session.execute_async()`
    method.

    Example usage::

        >>> session = cluster.connect()
        >>> session.set_keyspace("mykeyspace")
        >>> session.execute("SELECT * FROM mycf")

    """

    cluster = None
    hosts = None
    keyspace = None
    is_shutdown = False

    row_factory = staticmethod(named_tuple_factory)
    """
    The format to return row results in.  By default, each
    returned row will be a named tuple.  You can alternatively
    use any of the following:

      - :func:`cassandra.decoder.tuple_factory`
      - :func:`cassandra.decoder.named_tuple_factory`
      - :func:`cassandra.decoder.dict_factory`
      - :func:`cassandra.decoder.ordered_dict_factory`

    """

    _lock = None
    _pools = None
    _load_balancer = None
    _metrics = None

    def __init__(self, cluster, hosts):
        self.cluster = cluster
        self.hosts = hosts

        self._lock = RLock()
        self._pools = {}
        self._load_balancer = cluster.load_balancing_policy
        self._metrics = cluster.metrics

        for host in hosts:
            self.add_host(host)

    def execute(self, query, parameters=None, trace=False):
        """
        Execute the given query and synchronously wait for the response.

        If an error is encountered while executing the query, an Exception
        will be raised.

        `query` may be a query string or an instance of :class:`cassandra.query.Statement`.

        `parameters` may be a sequence or dict of parameters to bind.  If a
        sequence is used, ``%s`` should be used the placeholder for each
        argument.  If a dict is used, ``%(name)s`` style placeholders must
        be used.

        If `trace` is set to :const:`True`, an attempt will be made to
        fetch the trace details and attach them to the `query`'s
        :attr:`~.Statement.trace` attribute in the form of a :class:`.QueryTrace`
        instance.  This requires that `query` be a :class:`.Statement` subclass
        instance and not just a string.  If there is an error fetching the
        trace details, the :attr:`~.Statement.trace` attribute will be left as
        :const:`None`.
        """
        if trace and not isinstance(query, Statement):
            raise TypeError(
                "The query argument must be an instance of a subclass of "
                "cassandra.query.Statement when trace=True")

        future = self.execute_async(query, parameters, trace)
        try:
            result = future.result()
        finally:
            if trace:
                try:
                    query.trace = future.get_query_trace()
                except Exception:
                    log.exception("Unable to fetch query trace:")

        return result

    def execute_async(self, query, parameters=None, trace=False):
        """
        Execute the given query and return a :class:`~.ResponseFuture` object
        which callbacks may be attached to for asynchronous response
        delivery.  You may also call :meth:`~.ResponseFuture.result()`
        on the ``ResponseFuture`` to syncronously block for results at
        any time.

        If `trace` is set to :const:`True`, you may call
        :meth:`.ResponseFuture.get_query_trace()` after the request
        completes to retrieve a :class:`.QueryTrace` instance.

        Example usage::

            >>> session = cluster.connect()
            >>> future = session.execute_async("SELECT * FROM mycf")

            >>> def log_results(results):
            ...     for row in results:
            ...         log.info("Results: %s", row)

            >>> def log_error(exc):
            >>>     log.error("Operation failed: %s", exc)

            >>> future.add_callbacks(log_results, log_error)

        Async execution with blocking wait for results::

            >>> future = session.execute_async("SELECT * FROM mycf")
            >>> # do other stuff...

            >>> try:
            ...     results = future.result()
            ... except Exception:
            ...     log.exception("Operation failed:")

        """
        if isinstance(query, basestring):
            query = SimpleStatement(query)
        elif isinstance(query, PreparedStatement):
            query = query.bind(parameters)

        if isinstance(query, BoundStatement):
            message = ExecuteMessage(
                query_id=query.prepared_statement.query_id,
                query_params=query.values,
                consistency_level=query.consistency_level)
        else:
            query_string = query.query_string
            if parameters:
                query_string = bind_params(query.query_string, parameters)
            message = QueryMessage(query=query_string, consistency_level=query.consistency_level)

        if trace:
            message.tracing = True

        future = ResponseFuture(self, message, query, metrics=self._metrics)
        future.send_request()
        return future

    def prepare(self, query):
        """
        Prepares a query string, returing a :class:`~cassandra.query.PreparedStatement`
        instance which can be used as follows::

            >>> session = cluster.connect("mykeyspace")
            >>> query = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
            >>> prepared = session.prepare(query)
            >>> session.execute(prepared.bind((user.id, user.name, user.age)))

        """
        message = PrepareMessage(query=query)
        future = ResponseFuture(self, message, query=None)
        try:
            future.send_request()
            query_id, column_metadata = future.result()
        except Exception:
            log.exception("Error preparing query:")
            raise

        prepared_statement = PreparedStatement.from_message(
            query_id, column_metadata, self.cluster.metadata, query, self.keyspace)

        host = future._current_host
        try:
            self.cluster.prepare_on_all_sessions(query_id, prepared_statement, host)
        except Exception:
            log.exception("Error preparing query on all hosts:")

        return prepared_statement

    def prepare_on_all_hosts(self, query, excluded_host):
        """
        Prepare the given query on all hosts, excluding ``excluded_host``.
        Intended for internal use only.
        """
        for host, pool in self._pools.items():
            if host != excluded_host:
                future = ResponseFuture(self, PrepareMessage(query=query), None)

                # we don't care about errors preparing against specific hosts,
                # since we can always prepare them as needed when the prepared
                # statement is used.  Just log errors and continue on.
                try:
                    request_id = future._query(host)
                except Exception:
                    log.exception("Error preparing query for host %s:" % (host,))
                    continue

                if request_id is None:
                    # the error has already been logged by ResponsFuture
                    log.debug("Failed to prepare query for host %s" % (host,))
                    continue

                try:
                    future.result()
                except Exception:
                    log.exception("Error preparing query for host %s:" % (host,))

    def shutdown(self):
        """
        Close all connections.  ``Session`` instances should not be used
        for any purpose after being shutdown.
        """
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True

        for pool in self._pools.values():
            pool.shutdown()

    def __del__(self):
        try:
            self.shutdown()
            del self.cluster
        except TypeError:
            pass

    def add_host(self, host):
        """ Internal """
        distance = self._load_balancer.distance(host)
        if distance == HostDistance.IGNORED:
            return self._pools.get(host)
        else:
            try:
                new_pool = HostConnectionPool(host, distance, self)
            except AuthenticationFailed as auth_exc:
                conn_exc = ConnectionException(str(auth_exc), host=host)
                host.monitor.signal_connection_failure(conn_exc)
                return self._pools.get(host)
            except Exception as conn_exc:
                host.monitor.signal_connection_failure(conn_exc)
                return self._pools.get(host)

            previous = self._pools.get(host)
            self._pools[host] = new_pool
            return previous

    def on_up(self, host):
        """
        Called by the parent Cluster instance when a host's :class:`HealthMonitor`
        marks it up.  Only intended for internal use.
        """
        previous_pool = self.add_host(host)
        self._load_balancer.on_up(host)
        if previous_pool:
            previous_pool.shutdown()

    def on_down(self, host):
        """
        Called by the parent Cluster instance when a host's :class:`HealthMonitor`
        marks it down.  Only intended for internal use.
        """
        self._load_balancer.on_down(host)
        pool = self._pools.pop(host, None)
        if pool:
            pool.shutdown()

        for host in self.cluster.metadata.all_hosts():
            if not host.monitor.is_up:
                continue

            distance = self._load_balancer.distance(host)
            if distance != HostDistance.IGNORED:
                pool = self._pools.get(host)
                if not pool:
                    self.add_host(host)
                else:
                    pool.host_distance = distance

    def on_add(self, host):
        """ Internal """
        previous_pool = self.add_host(host)
        self._load_balancer.on_add(host)
        if previous_pool:
            previous_pool.shutdown()

    def on_remove(self, host):
        """ Internal """
        self._load_balancer.on_remove(host)
        pool = self._pools.pop(host)
        if pool:
            pool.shutdown()

    def set_keyspace(self, keyspace):
        """
        Set the default keyspace for all queries made through this Session.
        This operation blocks until complete.
        """
        self.execute('USE "%s"' % (keyspace,))

    def submit(self, fn, *args, **kwargs):
        """ Internal """
        return self.cluster.executor.submit(fn, *args, **kwargs)


class _ControlReconnectionHandler(_ReconnectionHandler):
    """
    Internal
    """

    def __init__(self, control_connection, *args, **kwargs):
        _ReconnectionHandler.__init__(self, *args, **kwargs)
        self.control_connection = weakref.proxy(control_connection)

    def try_reconnect(self):
        # we'll either get back a new Connection or a NoHostAvailable
        return self.control_connection._reconnect_internal()

    def on_reconnection(self, connection):
        self.control_connection._set_new_connection(connection)

    def on_exception(self, exc, next_delay):
        # TODO only overridden to add logging, so add logging
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            log.debug("Error trying to reconnect control connection: %r" % (exc,))
            return True


class ControlConnection(object):
    """
    Internal
    """

    _SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces"
    _SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies"
    _SELECT_COLUMNS = "SELECT * FROM system.schema_columns"

    _SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers"
    _SELECT_LOCAL = "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'"

    _SELECT_SCHEMA_PEERS = "SELECT rpc_address, schema_version FROM system.peers"
    _SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'"

    # for testing purposes
    _time = time

    def __init__(self, cluster):
        # use a weak reference to allow the Cluster instance to be GC'ed (and
        # shutdown) since implementing __del__ disables the cycle detector
        self._cluster = weakref.proxy(cluster)
        self._balancing_policy = cluster.load_balancing_policy
        self._balancing_policy.populate(cluster, [])
        self._reconnection_policy = cluster.reconnection_policy
        self._connection = None

        self._lock = RLock()
        self._schema_agreement_lock = Lock()

        self._reconnection_handler = None
        self._reconnection_lock = RLock()

        self._is_shutdown = False

    def connect(self):
        if self._is_shutdown:
            return

        self._set_new_connection(self._reconnect_internal())

    def _set_new_connection(self, conn):
        """
        Replace existing connection (if there is one) and close it.
        """
        with self._lock:
            old = self._connection
            self._connection = conn

        if old:
            old.close()

    def _reconnect_internal(self):
        """
        Tries to connect to each host in the query plan until one succeeds
        or every attempt fails. If successful, a new Connection will be
        returned.  Otherwise, :exc:`NoHostAvailable` will be raised
        with an "errors" arg that is a dict mapping host addresses
        to the exception that was raised when an attempt was made to open
        a connection to that host.
        """
        errors = {}
        for host in self._balancing_policy.make_query_plan():
            try:
                return self._try_connect(host)
            except ConnectionException as exc:
                errors[host.address] = exc
                host.monitor.signal_connection_failure(exc)
                log.warn("[control connection] Error connecting to %s:", host, exc_info=True)
            except Exception as exc:
                errors[host.address] = exc
                log.warn("[control connection] Error connecting to %s:", host, exc_info=True)

        raise NoHostAvailable("Unable to connect to any servers", errors)

    def _try_connect(self, host):
        """
        Creates a new Connection, registers for pushed events, and refreshes
        node/token and schema metadata.
        """
        log.debug("[control connection] Opening new connection to %s", host)
        connection = self._cluster.connection_factory(host.address)

        log.debug("[control connection] Established new connection to %s, "
                  "registering watchers and refreshing schema and topology",
                  host)
        try:
            connection.register_watchers({
                "TOPOLOGY_CHANGE": self._handle_topology_change,
                "STATUS_CHANGE": self._handle_status_change,
                "SCHEMA_CHANGE": self._handle_schema_change
            })

            self._refresh_node_list_and_token_map(connection)
            self._refresh_schema(connection)
        except Exception:
            connection.close()
            raise

        return connection

    def reconnect(self):
        if self._is_shutdown:
            return

        self._cluster.executor.submit(self._reconnect)

    def _reconnect(self):
        log.debug("[control connection] Attempting to reconnect")
        try:
            self._set_new_connection(self._reconnect_internal())
        except NoHostAvailable:
            # make a retry schedule (which includes backoff)
            schedule = self._reconnection_policy.new_schedule()

            with self._reconnection_lock:

                # cancel existing reconnection attempts
                if self._reconnection_handler:
                    self._reconnection_handler.cancel()

                # when a connection is successfully made, _set_new_connection
                # will be called with the new connection and then our
                # _reconnection_handler will be cleared out
                self._reconnection_handler = _ControlReconnectionHandler(
                    self, self._cluster.scheduler, schedule,
                    self._get_and_set_reconnection_handler,
                    new_handler=None)
                self._reconnection_handler.start()
        except Exception:
            log.debug("[control connection] error reconnecting", exc_info=True)
            raise

    def _get_and_set_reconnection_handler(self, new_handler):
        """
        Called by the _ControlReconnectionHandler when a new connection
        is successfully created.  Clears out the _reconnection_handler on
        this ControlConnection.
        """
        with self._reconnection_lock:
            old = self._reconnection_handler
            self._reconnection_handler = new_handler
            return old

    def shutdown(self):
        with self._lock:
            if self._is_shutdown:
                return
            else:
                self._is_shutdown = True

        # stop trying to reconnect (if we are)
        if self._reconnection_handler:
            self._reconnection_handler.cancel()

        if self._connection:
            self._connection.close()

    def refresh_schema(self, keyspace=None, table=None):
        try:
            if self._connection:
                self._refresh_schema(self._connection, keyspace, table)
        except Exception:
            log.debug("[control connection] Error refreshing schema", exc_info=True)
            self._signal_error()

    def _refresh_schema(self, connection, keyspace=None, table=None):
        self.wait_for_schema_agreement(connection)

        where_clause = ""
        if keyspace:
            where_clause = " WHERE keyspace_name = '%s'" % (keyspace,)
            if table:
                where_clause += " AND columnfamily_name = '%s'" % (table,)

        cl = ConsistencyLevel.ONE
        if table:
            ks_query = None
        else:
            ks_query = QueryMessage(query=self._SELECT_KEYSPACES + where_clause, consistency_level=cl)
        cf_query = QueryMessage(query=self._SELECT_COLUMN_FAMILIES + where_clause, consistency_level=cl)
        col_query = QueryMessage(query=self._SELECT_COLUMNS + where_clause, consistency_level=cl)

        if ks_query:
            ks_result, cf_result, col_result = connection.wait_for_responses(ks_query, cf_query, col_query)
            ks_result = dict_factory(*ks_result.results)
            cf_result = dict_factory(*cf_result.results)
            col_result = dict_factory(*col_result.results)
        else:
            ks_result = None
            cf_result, col_result = connection.wait_for_responses(cf_query, col_query)
            cf_result = dict_factory(*cf_result.results)
            col_result = dict_factory(*col_result.results)

        self._cluster.metadata.rebuild_schema(keyspace, table, ks_result, cf_result, col_result)

    def refresh_node_list_and_token_map(self):
        try:
            if self._connection:
                self._refresh_node_list_and_token_map(self._connection)
        except Exception:
            log.debug("[control connection] Error refreshing node list and token map", exc_info=True)
            self._signal_error()

    def _refresh_node_list_and_token_map(self, connection):
        log.debug("[control connection] Refreshing node list and token map")
        cl = ConsistencyLevel.ONE
        peers_query = QueryMessage(query=self._SELECT_PEERS, consistency_level=cl)
        local_query = QueryMessage(query=self._SELECT_LOCAL, consistency_level=cl)
        peers_result, local_result = connection.wait_for_responses(peers_query, local_query)
        peers_result = dict_factory(*peers_result.results)

        partitioner = None
        token_map = {}

        if local_result.results:
            local_rows = dict_factory(*(local_result.results))
            local_row = local_rows[0]
            cluster_name = local_row["cluster_name"]
            self._cluster.metadata.cluster_name = cluster_name

            host = self._cluster.metadata.get_host(connection.host)
            if host:
                host.set_location_info(local_row["data_center"], local_row["rack"])

            partitioner = local_row.get("partitioner")
            tokens = local_row.get("tokens")
            if partitioner and tokens:
                token_map[host] = tokens

        found_hosts = set()
        for row in peers_result:
            addr = row.get("rpc_address")

            # TODO handle ipv6 equivalent
            if not addr or addr == "0.0.0.0":
                addr = row.get("peer")

            found_hosts.add(addr)

            host = self._cluster.metadata.get_host(addr)
            if host is None:
                log.debug("[control connection] Found new host to connect to: %s" % (addr,))
                host = self._cluster.add_host(addr, signal=True)
            host.set_location_info(row.get("data_center"), row.get("rack"))

            tokens = row.get("tokens")
            if partitioner and tokens:
                token_map[host] = tokens

        for old_host in self._cluster.metadata.all_hosts():
            if old_host.address != connection.host and \
                    old_host.address not in found_hosts:
                self._cluster.remove_host(old_host)

        if partitioner:
            self._cluster.metadata.rebuild_token_map(partitioner, token_map)

    def _handle_topology_change(self, event):
        change_type = event["change_type"]
        addr, port = event["address"]
        if change_type == "NEW_NODE":
            self._cluster.scheduler.schedule(1, self._cluster.add_host, addr, signal=True)
        elif change_type == "REMOVED_NODE":
            host = self._cluster.metadata.get_host(addr)
            self._cluster.scheduler.schedule(0, self._cluster.remove_host, host)
        elif change_type == "MOVED_NODE":
            self._cluster.scheduler.schedule(1, self.refresh_node_list_and_token_map)

    def _handle_status_change(self, event):
        change_type = event["change_type"]
        addr, port = event["address"]
        host = self._cluster.metadata.get_host(addr)
        if change_type == "UP":
            if host is None:
                # this is the first time we've seen the node
                self._cluster.scheduler.schedule(1, self._cluster.add_host, addr, signal=True)
            else:
                self._cluster.scheduler.schedule(1, host.monitor.set_up)
        elif change_type == "DOWN":
            # Note that there is a slight risk we can receive the event late and thus
            # mark the host down even though we already had reconnected successfully.
            # But it is unlikely, and don't have too much consequence since we'll try reconnecting
            # right away, so we favor the detection to make the Host.is_up more accurate.
            if host is not None:
                self._cluster.scheduler.schedule(1, host.monitor.set_down)

    def _handle_schema_change(self, event):
        keyspace = event['keyspace'] or None
        table = event['table'] or None
        if event['change_type'] in ("CREATED", "DROPPED"):
            keyspace = keyspace if table else None
            self._cluster.executor.submit(self.refresh_schema, keyspace)
        elif event['change_type'] == "UPDATED":
            self._cluster.executor.submit(self.refresh_schema, keyspace, table)

    def wait_for_schema_agreement(self, connection=None):
        # Each schema change typically generates two schema refreshes, one
        # from the response type and one from the pushed notification. Holding
        # a lock is just a simple way to cut down on the number of schema queries
        # we'll make.
        with self._schema_agreement_lock:
            log.debug("[control connection] Waiting for schema agreement")
            if not connection:
                connection = self._connection

            start = self._time.time()
            elapsed = 0
            cl = ConsistencyLevel.ONE
            while elapsed < self._cluster.max_schema_agreement_wait:
                peers_query = QueryMessage(query=self._SELECT_SCHEMA_PEERS, consistency_level=cl)
                local_query = QueryMessage(query=self._SELECT_SCHEMA_LOCAL, consistency_level=cl)
                peers_result, local_result = connection.wait_for_responses(peers_query, local_query)
                peers_result = dict_factory(*peers_result.results)

                versions = set()
                if local_result.results:
                    local_row = dict_factory(*local_result.results)[0]
                    if local_row.get("schema_version"):
                        versions.add(local_row.get("schema_version"))

                for row in peers_result:
                    if not row.get("rpc_address") or not row.get("schema_version"):
                        continue

                    rpc = row.get("rpc_address")
                    if rpc == "0.0.0.0":  # TODO ipv6 check
                        rpc = row.get("peer")

                    peer = self._cluster.metadata.get_host(rpc)
                    if peer and peer.monitor.is_up:
                        versions.add(row.get("schema_version"))

                if len(versions) == 1:
                    return True

                log.debug("[control connection] Schemas mismatched, trying again")
                self._time.sleep(0.2)
                elapsed = self._time.time() - start

            return False

    def _signal_error(self):
        # try just signaling the host monitor, as this will trigger a reconnect
        # as part of marking the host down
        if self._connection and self._connection.is_defunct:
            host = self._cluster.metadata.get_host(self._connection.host)
            # host may be None if it's already been removed, but that indicates
            # that errors have already been reported, so we're fine
            if host:
                host.monitor.signal_connection_failure(self._connection.last_error)
                return

        # if the connection is not defunct or the host already left, reconnect
        # manually
        self.reconnect()

    @property
    def is_open(self):
        conn = self._connection
        return bool(conn and conn.is_open)

    def on_up(self, host):
        log.debug("[control connection] Host %s is considered up" % (host,))
        self._balancing_policy.on_up(host)

    def on_down(self, host):
        log.debug("[control connection] Host %s is considered down" % (host,))
        self._balancing_policy.on_down(host)

        conn = self._connection
        if conn and conn.host == host.address and \
                self._reconnection_handler is None:
            self.reconnect()

    def on_add(self, host):
        log.debug("[control connection] Adding host %r and refreshing topology" % (host,))
        self._balancing_policy.on_add(host)
        self.refresh_node_list_and_token_map()

    def on_remove(self, host):
        log.debug("[control connection] Removing host %r and refreshing topology" % (host,))
        self._balancing_policy.on_remove(host)
        self.refresh_node_list_and_token_map()


class _Scheduler(object):

    _scheduled = None
    _executor = None
    is_shutdown = False

    def __init__(self, executor):
        self._scheduled = Queue.PriorityQueue()
        self._executor = executor

        t = Thread(target=self.run, name="Task Scheduler")
        t.daemon = True
        t.start()

    def shutdown(self):
        try:
            log.debug("Shutting down Cluster Scheduler")
        except AttributeError:
            # this can happen on interpreter shutdown
            pass
        self.is_shutdown = True

    def schedule(self, delay, fn, *args, **kwargs):
        if not self.is_shutdown:
            run_at = time.time() + delay
            self._scheduled.put_nowait((run_at, (fn, args, kwargs)))
        else:
            log.debug("Ignoring scheduled function after shutdown: %r" % fn)

    def run(self):
        while True:
            if self.is_shutdown:
                return

            try:
                while True:
                    run_at, task = self._scheduled.get(block=True, timeout=None)
                    if self.is_shutdown:
                        log.debug("Not executing scheduled task due to Scheduler shutdown")
                        return
                    if run_at <= time.time():
                        fn, args, kwargs = task
                        self._executor.submit(fn, *args, **kwargs)
                    else:
                        self._scheduled.put_nowait((run_at, task))
                        break
            except Queue.Empty:
                pass

            time.sleep(0.1)


def refresh_schema_and_set_result(keyspace, table, control_conn, response_future):
    try:
        control_conn.refresh_schema(keyspace, table)
    except Exception:
        log.exception("Exception refreshing schema in response to schema change:")
    finally:
        response_future._set_final_result(None)


_NO_RESULT_YET = object()


class ResponseFuture(object):
    """
    An asynchronous response delivery mechanism that is returned from calls
    to :meth:`.Session.execute_async()`.

    There are two ways for results to be delivered:
     - Synchronously, by calling :meth:`.result()`
     - Asynchronously, by attaching callback and errback functions via
       :meth:`.add_callback()`, :meth:`.add_errback()`, and
       :meth:`.add_callbacks()`.
    """
    session = None
    row_factory = None
    message = None
    query = None

    _req_id = None
    _final_result = _NO_RESULT_YET
    _final_exception = None
    _query_trace = None
    _callback = None
    _errback = None
    _current_host = None
    _current_pool = None
    _connection = None
    _query_retries = 0
    _start_time = None
    _metrics = None

    def __init__(self, session, message, query, metrics=None):
        self.session = session
        self.row_factory = session.row_factory
        self.message = message
        self.query = query
        self._metrics = metrics
        if metrics is not None:
            self._start_time = time.time()

        # convert the list/generator/etc to an iterator so that subsequent
        # calls to send_request (which retries may do) will resume where
        # they last left off
        self.query_plan = iter(session._load_balancer.make_query_plan(
            session.keyspace, query))

        self._event = Event()
        self._errors = {}

    def __del__(self):
        try:
            del self.session
        except AttributeError:
            pass

    def send_request(self):
        """ Internal """
        # query_plan is an iterator, so this will resume where we last left
        # off if send_request() is called multiple times
        for host in self.query_plan:
            req_id = self._query(host)
            if req_id is not None:
                self._req_id = req_id
                return

        self._final_exception = NoHostAvailable(
            "Unable to complete the operation against any hosts", self._errors)

    def _query(self, host):
        pool = self.session._pools.get(host)
        if not pool or pool.is_shutdown:
            self._errors[host] = ConnectionException("Pool is shutdown")
            return None

        connection = None
        try:
            # TODO get connectTimeout from cluster settings
            connection = pool.borrow_connection(timeout=2.0)
            request_id = connection.send_msg(self.message, cb=self._set_result)
            self._current_host = host
            self._current_pool = pool
            self._connection = connection
            return request_id
        except Exception as exc:
            log.debug("Error querying host %s", host, exc_info=True)
            self._errors[host] = exc
            if connection:
                pool.return_connection(connection)
            return None

    def _set_result(self, response):
        try:
            if self._current_pool and self._connection:
                self._current_pool.return_connection(self._connection)

            trace_id = getattr(response, 'trace_id', None)
            if trace_id:
                self._query_trace = QueryTrace(trace_id, self.session)

            if isinstance(response, ResultMessage):
                if response.kind == ResultMessage.KIND_SET_KEYSPACE:
                    session = getattr(self, 'session', None)
                    if session:
                        session.keyspace = response.results
                    self._set_final_result(None)
                elif response.kind == ResultMessage.KIND_SCHEMA_CHANGE:
                    # refresh the schema before responding, but do it in another
                    # thread instead of the event loop thread
                    self.session.submit(
                        refresh_schema_and_set_result,
                        response.results['keyspace'],
                        response.results['table'],
                        self.session.cluster.control_connection,
                        self)
                else:
                    results = getattr(response, 'results', None)
                    if results is not None and response.kind == ResultMessage.KIND_ROWS:
                        results = self.row_factory(*results)
                    self._set_final_result(results)
            elif isinstance(response, ErrorMessage):
                retry_policy = None
                if self.query:
                    retry_policy = self.query.retry_policy
                if not retry_policy:
                    retry_policy = self.session.cluster.default_retry_policy

                if isinstance(response, ReadTimeoutErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_read_timeout()
                    retry = retry_policy.on_read_timeout(
                        self.query, retry_num=self._query_retries, **response.info)
                elif isinstance(response, WriteTimeoutErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_write_timeout()
                    retry = retry_policy.on_write_timeout(
                        self.query, retry_num=self._query_retries, **response.info)
                elif isinstance(response, UnavailableErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_unavailable()
                    retry = retry_policy.on_unavailable(
                        self.query, retry_num=self._query_retries, **response.info)
                elif isinstance(response, OverloadedErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_other_error()
                    # need to retry against a different host here
                    log.warn("Host %s is overloaded, retrying against a different "
                             "host" % (self._current_host))
                    self._retry(reuse_connection=False, consistency_level=None)
                    return
                elif isinstance(response, IsBootstrappingErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_other_error()
                    # need to retry against a different host here
                    self._retry(reuse_connection=False, consistency_level=None)
                    return
                elif isinstance(response, PreparedQueryNotFound):
                    query_id = response.info
                    try:
                        prepared_statement = self.session.cluster._prepared_statements[query_id]
                    except KeyError:
                        log.error("Tried to execute unknown prepared statement %s" % (query_id.encode('hex'),))
                        self._set_final_exception(response)
                        return

                    current_keyspace = self._connection.keyspace
                    prepared_keyspace = prepared_statement.keyspace
                    if current_keyspace != prepared_keyspace:
                        self._set_final_exception(
                            ValueError("The Session's current keyspace (%s) does "
                                       "not match the keyspace the statement was "
                                       "prepared with (%s)" %
                                       (current_keyspace, prepared_keyspace)))
                        return

                    prepare_message = PrepareMessage(query=prepared_statement.query_string)
                    # since this might block, run on the executor to avoid hanging
                    # the event loop thread
                    self.session.submit(self._connection.send_msg,
                                        prepare_message,
                                        cb=self._execute_after_prepare)
                    return
                else:
                    if hasattr(response, 'to_exception'):
                        self._set_final_exception(response.to_exception())
                    else:
                        self._set_final_exception(response)
                    return

                retry_type, consistency = retry
                if retry_type is RetryPolicy.RETRY:
                    self._query_retries += 1
                    self._retry(reuse_connection=True, consistency_level=consistency)
                elif retry_type is RetryPolicy.RETHROW:
                    self._set_final_exception(response.to_exception())
                else:  # IGNORE
                    if self._metrics is not None:
                        self._metrics.on_ignore()
                    self._set_final_result(None)
            elif isinstance(response, ConnectionException):
                if self._metrics is not None:
                    self._metrics.on_connection_error()
                if not isinstance(response, ConnectionShutdown):
                    self._connection.defunct(response)
                self._retry(reuse_connection=False, consistency_level=None)
            elif isinstance(response, Exception):
                if hasattr(response, 'to_exception'):
                    self._set_final_exception(response.to_exception())
                else:
                    self._set_final_exception(response)
            else:
                # we got some other kind of response message
                msg = "Got unexpected message: %r" % (response,)
                exc = ConnectionException(msg, self._current_host)
                self._connection.defunct(exc)
                self._set_final_exception(exc)
        except Exception as exc:
            # almost certainly caused by a bug, but we need to set something here
            log.exception("Unexpected exception while handling result in ResponseFuture:")
            self._set_final_exception(exc)

    def _execute_after_prepare(self, response):
        """
        Handle the response to our attempt to prepare a statement.
        If it succeeded, run the original query again against the same host.
        """
        if isinstance(response, ResultMessage):
            if response.kind == ResultMessage.KIND_PREPARED:
                # use self._query to re-use the same host and
                # at the same time properly borrow the connection
                self._query(self._current_host)
            else:
                self._set_final_exception(ConnectionException(
                    "Got unexpected response when preparing statement "
                    "on host %s: %s" % (self._host, response)))
        elif isinstance(response, ErrorMessage):
            self._set_final_exception(response)
        else:
            self._set_final_exception(ConnectionException(
                "Got unexpected response type when preparing "
                "statement on host %s: %s" % (self._host, response)))

    def _set_final_result(self, response):
        if self._metrics is not None:
            self._metrics.request_timer.addValue(time.time() - self._start_time)
        if hasattr(self, 'session'):
            try:
                del self.session  # clear reference cycles
            except AttributeError:
                pass
        self._final_result = response
        self._event.set()
        if self._callback:
            fn, args, kwargs = self._callback
            fn(response, *args, **kwargs)

    def _set_final_exception(self, response):
        if self._metrics is not None:
            self._metrics.request_timer.addValue(time.time() - self._start_time)
        try:
            del self.session  # clear reference cycles
        except AttributeError:
            pass
        self._final_exception = response
        self._event.set()
        if self._errback:
            fn, args, kwargs = self._errback
            fn(response, *args, **kwargs)

    def _retry(self, reuse_connection, consistency_level):
        if self._final_exception:
            # the connection probably broke while we were waiting
            # to retry the operation
            return

        if self._metrics is not None:
            self._metrics.on_retry()
        if consistency_level is not None:
            self.message.consistency_level = consistency_level

        # don't retry on the event loop thread
        self.session.submit(self._retry_task, reuse_connection)

    def _retry_task(self, reuse_connection):
        if self._final_exception:
            # the connection probably broke while we were waiting
            # to retry the operation
            return

        if reuse_connection and self._query(self._current_host):
            return

        # otherwise, move onto another host
        self.send_request()

    def result(self):
        """
        Return the final result or raise an Exception if errors were
        encountered.  If the final result or error has not been set
        yet, this method will block until that time.

        Example usage::

            >>> future = session.execute_async("SELECT * FROM mycf")
            >>> # do other stuff...

            >>> try:
            ...     rows = future.result()
            ...     for row in rows:
            ...         ... # process results
            ... except Exception:
            ...     log.exception("Operation failed:")

        """
        if self._final_result is not _NO_RESULT_YET:
            return self._final_result
        elif self._final_exception:
            raise self._final_exception
        else:
            self._event.wait()
            if self._final_result is not _NO_RESULT_YET:
                return self._final_result
            elif self._final_exception:
                raise self._final_exception
            else:
                assert False  # shouldn't get here

    def get_query_trace(self):
        """
        Returns the :class:`~.query.QueryTrace` instance representing a trace
        of the last attempt for this operation, or :const:`None` if tracing was
        not enabled for this query.  Note that this may raise an exception if
        there are problems retrieving the trace details from Cassandra.
        """
        if not self._query_trace:
            return None

        self._query_trace.populate()
        return self._query_trace

    def add_callback(self, fn, *args, **kwargs):
        """
        Attaches a callback function to be called when the final results arrive.

        By default, `fn` will be called with the results as the first and only
        argument.  If `*args` or `**kwargs` are supplied, they will be passed
        through as additional positional or keyword arguments to `fn`.

        If an error is hit while executing the operation, a callback attached
        here will not be called.  Use :meth:`.add_errback()` or :meth:`add_callbacks()`
        if you wish to handle that case.

        If the final result has already been seen when this method is called,
        the callback will be called immediately (before this method returns).

        Usage example::

            >>> session = cluster.connect("mykeyspace")

            >>> def handle_results(rows, start_time, should_log=False):
            ...     if should_log:
            ...         log.info("Total time: %f", time.time() - start_time)
            ...     ...

            >>> future = session.execute_async("SELECT * FROM users")
            >>> future.add_callback(handle_results, time.time(), should_log=True)

        """
        if self._final_result is not _NO_RESULT_YET:
            fn(self._final_result, *args, **kwargs)
        else:
            self._callback = (fn, args, kwargs)
        return self

    def add_errback(self, fn, *args, **kwargs):
        """
        Like :meth:`.add_callback()`, but handles error cases.
        An Exception instance will be passed as the first positional argument
        to `fn`.
        """
        if self._final_exception:
            fn(self._final_exception, *args, **kwargs)
        else:
            self._errback = (fn, args, kwargs)
        return self

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        """
        A convenient combination of :meth:`.add_callback()` and
        :meth:`.add_errback()`.

        Example usage::

            >>> session = cluster.connect()
            >>> query = "SELECT * FROM mycf"
            >>> future = session.execute_async(query)

            >>> def log_results(results, level='debug'):
            ...     for row in results:
            ...         log.log(level, "Result: %s", row)

            >>> def log_error(exc, query):
            ...     log.error("Query '%s' failed: %s", query, exc)

            >>> future.add_callbacks(
            ...     callback=log_results, callback_kwargs={'level': 'info'},
            ...     errback=log_error, errback_args=(query,))

        """
        self.add_callback(callback, *callback_args, **(callback_kwargs or {}))
        self.add_errback(errback, *errback_args, **(errback_kwargs or {}))

    def __str__(self):
        query = self.query.query_string
        return "<ResponseFuture: query='%s' request_id=%s result=%s exception=%s host=%s>" \
               % (query, self._req_id, self._final_result, self._final_exception, self._current_host)
