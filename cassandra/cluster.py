from futures import ThreadPoolExecutor
import logging
import time
from threading import RLock, Thread, Event
import Queue
import weakref
from functools import partial

from cassandra import ConsistencyLevel
from cassandra.connection import Connection, ConnectionException
from cassandra.decoder import (QueryMessage, ResultMessage,
                               ErrorMessage, ReadTimeoutErrorMessage,
                               WriteTimeoutErrorMessage,
                               UnavailableErrorMessage,
                               OverloadedErrorMessage,
                               IsBootstrappingErrorMessage, named_tuple_factory,
                               dict_factory)
from cassandra.metadata import Metadata
from cassandra.policies import (RoundRobinPolicy, SimpleConvictionPolicy,
                                ExponentialReconnectionPolicy, HostDistance,
                                RetryPolicy)
from cassandra.query import SimpleStatement, bind_params
from cassandra.pool import (AuthenticationException, _ReconnectionHandler,
                            _HostReconnectionHandler, HostConnectionPool)


log = logging.getLogger(__name__)


DEFAULT_MIN_REQUESTS = 25
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
    ``True`` and attempts to use snappy compression.
    """

    auth_provider = None
    """
    An optional function that accepts one argument, the IP address of a node,
    and returns a dict of credentials for that node.
    """

    load_balancing_policy_factory = RoundRobinPolicy
    """
    A factory function which creates instances of subclasses of
    :cls:`policies.LoadBalancingPolicy`.  Defaults to
    :cls:`policies.RoundRobinPolicy`.
    """

    reconnection_policy = ExponentialReconnectionPolicy(1.0, 600.0)
    """
    An instance of :cls:`policies.ReconnectionPolicy`. Defaults to an instance
    of :cls:`ExponentialReconnectionPolicy` with a base delay of one second and
    a max delay of ten minutes.
    """

    retry_policy_factory = RetryPolicy
    """
    A factory function which creates instances of
    :cls:`policies.RetryPolicy`.  Defaults to
    :cls:`policies.RetryPolicy`.
    """

    conviction_policy_factory = SimpleConvictionPolicy
    """
    A factory function which creates instances of
    :cls:`policies.ConvictionPolicy`.  Defaults to
    :cls:`policies.SimpleConvictionPolicy`.
    """

    metrics_enabled = False
    """
    Whether or not metric collection is enabled.
    """

    sockopts = None
    """
    An optional list of tuples which will be used as *args to
    ``socket.setsockopt()`` for all created sockets.
    """

    max_schema_agreement_wait = 10
    """
    The maximum duration (in seconds) that the driver will wait for schema
    agreement across the cluster. Defaults to ten seconds.
    """

    metadata = None
    """
    An instance of :cls:`cassandra.metadata.Metadata`.
    """

    sessions = None
    control_connection = None
    scheduler = None
    executor = None
    _is_shutdown = False

    def __init__(self,
                 contact_points=("127.0.0.1",),
                 port=9042,
                 compression=True,
                 auth_provider=None,
                 load_balancing_policy_factory=None,
                 reconnection_policy=None,
                 retry_policy_factory=None,
                 conviction_policy_factory=None,
                 metrics_enabled=False,
                 sockopts=None,
                 executor_threads=2,
                 max_schema_agreement_wait=10):

        self.contact_points = contact_points
        self.port = port
        self.compression = compression

        if auth_provider is not None:
            if not callable(auth_provider):
                raise ValueError("auth_provider must be callable")
            self.auth_provider = auth_provider

        if load_balancing_policy_factory is not None:
            if not callable(load_balancing_policy_factory):
                raise ValueError("load_balancing_policy_factory must be callable")
            self.load_balancing_policy_factory = load_balancing_policy_factory

        if reconnection_policy is not None:
            self.reconnection_policy = reconnection_policy

        if retry_policy_factory is not None:
            if not callable(retry_policy_factory):
                raise ValueError("retry_policy_factory must be callable")
            self.retry_policy_factory = retry_policy_factory

        if conviction_policy_factory is not None:
            if not callable(conviction_policy_factory):
                raise ValueError("conviction_policy_factory must be callable")
            self.conviction_policy_factory = conviction_policy_factory

        self.metrics_enabled = metrics_enabled
        self.sockopts = sockopts
        self.max_schema_agreement_wait = max_schema_agreement_wait

        # let Session objects be GC'ed (and shutdown) when the user no longer
        # holds a reference. Normally the cycle detector would handle this,
        # but implementing __del__ prevents that.
        self.sessions = weakref.WeakSet()
        self.metadata = Metadata(self)
        self.control_connection = None

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

        for address in contact_points:
            self.add_host(address, signal=False)

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

    def _connection_factory(self, host, *args, **kwargs):
        if self.auth_provider:
            kwargs['credentials'] = self.auth_provider(host)

        kwargs['compression'] = self.compression
        kwargs['sockopts'] = self.sockopts

        return Connection.factory(host, *args, **kwargs)

    def _make_connection_factory(self, host, *args, **kwargs):
        if self.auth_provider:
            kwargs['credentials'] = self.auth_provider(host)

        kwargs['compression'] = self.compression
        kwargs['sockopts'] = self.sockopts

        return partial(Connection.factory, host, *args, **kwargs)

    def connect(self, keyspace=None):
        """
        Creates and returns a new :cls:`~.Session` object.  If `keyspace`
        is specified, that keyspace will be the default keyspace for
        operations on the ``Session``.
        """
        with self._lock:
            if self._is_shutdown:
                raise Exception("Cluster is already shut down")

            if not self.control_connection:
                self.control_connection = ControlConnection(self)
                try:
                    self.control_connection.connect()
                except:
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
                return
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
        """ Internal method """
        reconnector = host.get_and_set_reconnection_handler(None)
        if reconnector:
            reconnector.cancel()

        self.prepare_all_queries(host)

        self.control_connection.on_up(host)
        for session in self.sessions:
            session.on_up(host)

    def on_down(self, host):
        """ Internal method """
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

    def on_add(self, host):
        """ Internal method """
        self.prepare_all_queries(host)
        self.control_connection.on_add(host)
        for session in self.sessions:  # TODO need to copy/lock?
            session.on_add(host)

    def on_remove(self, host):
        """ Internal method """
        self.control_connection.on_remove(host)
        for session in self.sessions:
            session.on_remove(host)

    def add_host(self, address, signal):
        """ Internal method """
        new_host = self.metadata.add_host(address)
        if new_host and signal:
            self.on_add(new_host)
        return new_host

    def remove_host(self, host):
        """ Internal method """
        if host and self.metadata.remove_host(host):
            self.on_remove(host)

    def ensure_core_connections(self):
        """
        If any host has fewer than the configured number of core connections
        open, attempt to open connections until that number is met.
        """
        for session in self.session:
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

    def prepare_all_queries(self, host):
        pass


class Session(object):
    """
    A collection of connection pools for each host in the cluster.
    Instances of this class should not be created directly, only
    using :meth:`~.Cluster.connect()`.

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

    def __init__(self, cluster, hosts):
        self.cluster = cluster
        self.hosts = hosts

        self._lock = RLock()
        self._pools = {}
        self._load_balancer = cluster.load_balancing_policy_factory()
        self._load_balancer.populate(weakref.proxy(cluster), hosts)

        for host in hosts:
            self.add_host(host)

    def execute(self, query, parameters=None):
        """
        Execute the given query and synchronously wait for the response.

        If an error is encountered while executing the query, an Exception
        will be raised.

        `query` may be a query string or an instance of :cls:`cassandra.query.Query`.

        `parameters` may be a sequence or dict of parameters to bind.  If a
        sequence is used, '%s' should be used the placeholder for each
        argument.  If a dict is used, '%(name)s' style placeholders must
        be used.
        """
        return self.execute_async(query, parameters).deliver()

    def execute_async(self, query, parameters=None):
        """
        Execute the given query and return a :cls:`~.ResponseFuture` object
        which callbacks may be attached to for asynchronous response
        delivery.  You may also call :meth:`~.ResponseFuture.deliver()`
        on the ``ResponseFuture`` to syncronously block for results at
        any time.

        Example usage::

            >>> session = cluster.connect()
            >>> future = session.execute_async("SELECT * FROM mycf")
            >>>
            >>> def print_results(results):
            ...     for row in results:
            ...         print row
            >>>
            >>> def handle_error(exc):
            >>>     print exc
            >>>
            >>> future.addCallbacks(print_results, handle_error)

        Async execution with blocking wait for results::

            >>> future = session.execute_async("SELECT * FROM mycf")
            >>>
            >>> # do other stuff...
            >>>
            >>> try:
            ...     results = future.deliver()
            ... except:
            ...     print "operation failed!"

        """
        if isinstance(query, basestring):
            query = SimpleStatement(query)

        # TODO bound statements need to be handled differently
        query_string = query.query_string
        if parameters:
            query_string = bind_params(query.query_string, parameters)
        message = QueryMessage(query=query_string, consistency_level=query.consistency_level)

        if query.tracing_enabled:
            # TODO enable tracing on the message
            pass

        future = ResponseFuture(self, message, query)
        future.send_request()
        return future

    def prepare(self, query):
        raise NotImplemented

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
        self.shutdown()
        del self.cluster

    def add_host(self, host):
        """ Internal """
        distance = self._load_balancer.distance(host)
        if distance == HostDistance.IGNORED:
            return self._pools.get(host)
        else:
            try:
                new_pool = HostConnectionPool(host, distance, self)
            except AuthenticationException, auth_exc:
                conn_exc = ConnectionException(str(auth_exc), host=host)
                host.monitor.signal_connection_failure(conn_exc)
                return self._pools.get(host)
            except ConnectionException, conn_exc:
                host.monitor.signal_connection_failure(conn_exc)
                return self._pools.get(host)

            previous = self._pools.get(host)
            self._pools[host] = new_pool
            return previous

    def on_up(self, host):
        """ Internal """
        previous_pool = self.add_host(host)
        self._load_balancer.on_up(host)
        if previous_pool:
            previous_pool.shutdown()

    def on_down(self, host):
        """ Internal """
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
        if isinstance(exc, AuthenticationException):
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
        self._balancing_policy = RoundRobinPolicy()
        self._balancing_policy.populate(cluster, cluster.metadata.all_hosts())
        self._reconnection_policy = ExponentialReconnectionPolicy(2, 300)
        self._connection = None

        self._lock = RLock()

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
            except ConnectionException, exc:
                errors[host.address] = exc
                host.monitor.signal_connection_failure(exc)
                log.exception("Error reconnecting control connection:")
            except Exception, exc:
                errors[host.address] = exc
                log.exception("Error reconnecting control connection:")

        raise NoHostAvailable("Unable to connect to any servers", errors)

    def _try_connect(self, host):
        """
        Creates a new Connection, registers for pushed events, and refreshes
        node/token and schema metadata.
        """
        connection = self._cluster._connection_factory(host.address)

        try:
            connection.register_watchers({
                "TOPOLOGY_CHANGE": self._handle_topology_change,
                "STATUS_CHANGE": self._handle_status_change,
                "SCHEMA_CHANGE": self._handle_schema_change
            })

            self._refresh_node_list_and_token_map(connection)
            self._refresh_schema(connection)
        except:
            connection.close()
            raise

        return connection

    def reconnect(self):
        if self._is_shutdown:
            return

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
        except:
            log.exception("[control connection] Error refreshing schema:")
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
        except:
            log.exception("[control connection] Error refreshing node list and token map:")
            self._signal_error()

    def _refresh_node_list_and_token_map(self, connection):
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
        self._balancing_policy.on_up(host)

    def on_down(self, host):
        self._balancing_policy.on_down(host)

        conn = self._connection
        if conn and conn.host == host.address and \
                self._reconnection_handler is None:
            self.reconnect()

    def on_add(self, host):
        self._balancing_policy.on_add(host)
        self.refresh_node_list_and_token_map()

    def on_remove(self, host):
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
        log.debug("Shutting down Cluster Scheduler")
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
    except:
        log.exception("Exception refreshing schema in response to schema change:")
    finally:
        response_future._set_final_result(None)


_NO_RESULT_YET = object()


class ResponseFuture(object):
    """
    An asynchronous response delivery mechanism that is returned from calls
    to :meth:`~.Session.execute_async()`.

    There are two ways for results to be delivered:
     - Asynchronously, by attaching callback and errback functions
     - Synchronously, by calling :meth:`~.ResponseFuture.deliver()`

    Callback and errback example::

        >>> session = cluster.connect()
        >>> future = session.execute_async("SELECT * FROM mycf")
        >>>
        >>> def print_results(results):
        ...     for row in results:
        ...         print row
        >>>
        >>> def handle_error(exc):
        >>>     print exc
        >>>
        >>> future.addCallbacks(print_results, handle_error)

    Example of using ``deliver()`` to synchronously wait for results::

        >>> future = session.execute_async("SELECT * FROM mycf")
        >>>
        >>> # do other stuff...
        >>>
        >>> try:
        ...     results = future.deliver()
        ... except:
        ...     print "operation failed!"

    """

    def __init__(self, session, message, query):
        self.session = session
        self.row_factory = session.row_factory
        self.message = message
        self.query = query

        # convert the list/generator/etc to an iterator so that subsequent
        # calls to send_request (which retries may do) will resume where
        # they last left off
        self.query_plan = iter(session._load_balancer.make_query_plan(query))

        self._req_id = None
        self._final_result = _NO_RESULT_YET
        self._final_exception = None
        self._current_host = self._current_pool = self._connection = None
        self._event = Event()
        self._errors = {}
        self._query_retries = 0
        self._callback = self._errback = None

    def __del__(self):
        if hasattr(self, 'session'):
            del self.session

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
        except Exception, exc:
            self._errors[host] = exc
            if connection:
                pool.return_connection(connection)
            return None

    def _set_result(self, response):
        if self._current_pool:
            self._current_pool.return_connection(self._connection)

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
            retry_policy = self.query.retry_policy
            if not retry_policy:
                retry_policy = self.session.cluster.retry_policy_factory()

            if isinstance(response, ReadTimeoutErrorMessage):
                retry = retry_policy.on_read_timeout(
                    self.query, retry_num=self._query_retries, **response.info)
            elif isinstance(response, WriteTimeoutErrorMessage):
                retry = retry_policy.on_write_timeout(
                    self.query, retry_num=self._query_retries, **response.info)
            elif isinstance(response, UnavailableErrorMessage):
                retry = retry_policy.on_unavailable(
                    self.query, retry_num=self._query_retries, **response.info)
            elif isinstance(response, OverloadedErrorMessage):
                # need to retry against a different host here
                log.warn("Host %s is overloaded, retrying against a different "
                         "host" % (self._current_host))
                self._retry(reuse_connection=False, consistency_level=None)
                return
            elif isinstance(response, IsBootstrappingErrorMessage):
                # need to retry against a different host here
                self._retry(reuse_connection=False, consistency_level=None)
                return
            # TODO need to define the PreparedQueryNotFound class
            # elif isinstance(response, PreparedQueryNotFound):
            #     pass
            else:
                self._set_final_exception(response)
                return

            retry_type, consistency = retry
            if retry_type is RetryPolicy.RETRY:
                self._query_retries += 1
                self._retry(reuse_connection=True, consistency_level=consistency)
            elif retry_type is RetryPolicy.RETHROW:
                self._set_final_exception(response)
            else:  # IGNORE
                self._set_final_result(None)
        elif isinstance(response, Exception):
            self._set_final_exception(response)
        else:
            # we got some other kind of response message
            msg = "Got unexpected message: %r" % (response,)
            exc = ConnectionException(msg, self._current_host)
            self._connection.defunct(exc)
            self._set_final_exception(exc)

    def _set_final_result(self, response):
        if hasattr(self, 'session'):
            del self.session  # clear reference cycles
        self._final_result = response
        self._event.set()
        if self._callback:
            fn, args, kwargs = self._callback
            fn(response, *args, **kwargs)

    def _set_final_exception(self, response):
        if hasattr(self, 'session'):
            del self.session  # clear reference cycles
        self._final_exception = response
        self._event.set()
        if self._errback:
            fn, args, kwargs = self._errback
            fn(response, *args, **kwargs)

    def _retry(self, reuse_connection, consistency_level):
        if consistency_level is not None:
            self.message.consistency_level = consistency_level

        # don't retry on the event loop thread
        self.session.submit(self._retry_task, reuse_connection)

    def _retry_task(self, reuse_connection):
        if reuse_connection and self._query(self._current_host):
            return

        # otherwise, move onto another host
        self.send_request()

    def deliver(self):
        """
        Return the final result or raise an Exception if errors were
        encountered.  If the final result or error has not been set
        yet, this method will block until that time.
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

    def add_callback(self, fn, *args, **kwargs):
        """
        Attaches a callback function to be called when the final results arrive.

        By default, `fn` will be called with the results as the first and only
        argument.  If `*args` or `**kwargs` are supplied, they will be passed
        through as additional positional or keyword arguments to `fn`.

        If an error is hit while executing the operation, a callback attached
        here will not be called.  Use ``add_errback`` or ``add_callbacks``
        if you wish to handle that case.

        If the final result has already been seen when this method is called,
        the callback will be called immediately (before this method returns).
        """
        if self._final_result is not _NO_RESULT_YET:
            fn(self._final_result, *args, **kwargs)
        else:
            self._callback = (fn, args, kwargs)
        return self

    def add_errback(self, fn, *args, **kwargs):
        """
        Like :meth:`~.ResultFuture.add_callback()`, but handles error cases.
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
        A convenient combination of :meth:`~.ResultFuture.add_callback()` and
        :meth:`~.ResultFuture.add_errback()``.
        """
        self.add_callback(callback, *callback_args, **(callback_kwargs or {}))
        self.add_errback(errback, *errback_args, **(errback_kwargs or {}))
