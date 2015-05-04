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
This module houses the main classes you will interact with,
:class:`.Cluster` and :class:`.Session`.
"""
from __future__ import absolute_import

import atexit
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import logging
from random import random
import socket
import sys
import time
from threading import Lock, RLock, Thread, Event

import six
from six.moves import range
from six.moves import queue as Queue

import weakref
from weakref import WeakValueDictionary
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from functools import partial, wraps
from itertools import groupby

from cassandra import (ConsistencyLevel, AuthenticationFailed,
                       InvalidRequest, OperationTimedOut,
                       UnsupportedOperation, Unauthorized)
from cassandra.connection import (ConnectionException, ConnectionShutdown,
                                  ConnectionHeartbeat)
from cassandra.cqltypes import UserType
from cassandra.encoder import Encoder
from cassandra.protocol import (QueryMessage, ResultMessage,
                                ErrorMessage, ReadTimeoutErrorMessage,
                                WriteTimeoutErrorMessage,
                                UnavailableErrorMessage,
                                OverloadedErrorMessage,
                                PrepareMessage, ExecuteMessage,
                                PreparedQueryNotFound,
                                IsBootstrappingErrorMessage,
                                BatchMessage, RESULT_KIND_PREPARED,
                                RESULT_KIND_SET_KEYSPACE, RESULT_KIND_ROWS,
                                RESULT_KIND_SCHEMA_CHANGE)
from cassandra.metadata import Metadata, protect_name
from cassandra.policies import (RoundRobinPolicy, SimpleConvictionPolicy,
                                ExponentialReconnectionPolicy, HostDistance,
                                RetryPolicy)
from cassandra.pool import (Host, _ReconnectionHandler, _HostReconnectionHandler,
                            HostConnectionPool, HostConnection,
                            NoConnectionsAvailable)
from cassandra.query import (SimpleStatement, PreparedStatement, BoundStatement,
                             BatchStatement, bind_params, QueryTrace, Statement,
                             named_tuple_factory, dict_factory, FETCH_SIZE_UNSET)

def _is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('socket')

# default to gevent when we are monkey patched with gevent, eventlet when
# monkey patched with eventlet, otherwise if libev is available, use that as
# the default because it's fastest. Otherwise, use asyncore.
if 'gevent.monkey' in sys.modules:
    from cassandra.io.geventreactor import GeventConnection as DefaultConnection
elif _is_eventlet_monkey_patched():
    from cassandra.io.eventletreactor import EventletConnection as DefaultConnection
else:
    try:
        from cassandra.io.libevreactor import LibevConnection as DefaultConnection  # NOQA
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


_NOT_SET = object()


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


def _future_completed(future):
    """ Helper for run_in_executor() """
    exc = future.exception()
    if exc:
        log.debug("Failed to run task on executor", exc_info=exc)


def run_in_executor(f):
    """
    A decorator to run the given method in the ThreadPoolExecutor.
    """

    @wraps(f)
    def new_f(self, *args, **kwargs):

        if self.is_shutdown:
            return
        try:
            future = self.executor.submit(f, self, *args, **kwargs)
            future.add_done_callback(_future_completed)
        except Exception:
            log.exception("Failed to submit task to executor")

    return new_f


def _shutdown_cluster(cluster):
    if cluster and not cluster.is_shutdown:
        cluster.shutdown()


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

    contact_points = ['127.0.0.1']
    """
    The list of contact points to try connecting for cluster discovery.

    Defaults to loopback interface.

    Note: When using :class:`.DCAwareLoadBalancingPolicy` with no explicit
    local_dc set, the DC is chosen from an arbitrary host in contact_points.
    In this case, contact_points should contain only nodes from a single,
    local DC.
    """

    port = 9042
    """
    The server-side port to open connections to. Defaults to 9042.
    """

    cql_version = None
    """
    If a specific version of CQL should be used, this may be set to that
    string version.  Otherwise, the highest CQL version supported by the
    server will be automatically used.
    """

    protocol_version = 2
    """
    The version of the native protocol to use.

    Version 2 of the native protocol adds support for lightweight transactions,
    batch operations, and automatic query paging. The v2 protocol is
    supported by Cassandra 2.0+.

    Version 3 of the native protocol adds support for protocol-level
    client-side timestamps (see :attr:`.Session.use_client_timestamp`),
    serial consistency levels for :class:`~.BatchStatement`, and an
    improved connection pool.

    The following table describes the native protocol versions that
    are supported by each version of Cassandra:

    +-------------------+-------------------+
    | Cassandra Version | Protocol Versions |
    +===================+===================+
    | 1.2               | 1                 |
    +-------------------+-------------------+
    | 2.0               | 1, 2              |
    +-------------------+-------------------+
    | 2.1               | 1, 2, 3           |
    +-------------------+-------------------+
    """

    compression = True
    """
    Controls compression for communications between the driver and Cassandra.
    If left as the default of :const:`True`, either lz4 or snappy compression
    may be used, depending on what is supported by both the driver
    and Cassandra.  If both are fully supported, lz4 will be preferred.

    You may also set this to 'snappy' or 'lz4' to request that specific
    compression type.

    Setting this to :const:`False` disables compression.
    """

    _auth_provider = None
    _auth_provider_callable = None

    @property
    def auth_provider(self):
        """
        When :attr:`~.Cluster.protocol_version` is 2 or higher, this should
        be an instance of a subclass of :class:`~cassandra.auth.AuthProvider`,
        such as :class:`~.PlainTextAuthProvider`.

        When :attr:`~.Cluster.protocol_version` is 1, this should be
        a function that accepts one argument, the IP address of a node,
        and returns a dict of credentials for that node.

        When not using authentication, this should be left as :const:`None`.
        """
        return self._auth_provider

    @auth_provider.setter  # noqa
    def auth_provider(self, value):
        if not value:
            self._auth_provider = value
            return

        try:
            self._auth_provider_callable = value.new_authenticator
        except AttributeError:
            if self.protocol_version > 1:
                raise TypeError("auth_provider must implement the cassandra.auth.AuthProvider "
                                "interface when protocol_version >= 2")
            elif not callable(value):
                raise TypeError("auth_provider must be callable when protocol_version == 1")
            self._auth_provider_callable = value

        self._auth_provider = value

    load_balancing_policy = None
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

    connect_to_remote_hosts = True
    """
    If left as :const:`True`, hosts that are considered :attr:`~.HostDistance.REMOTE`
    by the :attr:`~.Cluster.load_balancing_policy` will have a connection
    opened to them.  Otherwise, they will not have a connection opened to them.

    .. versionadded:: 2.1.0
    """

    metrics_enabled = False
    """
    Whether or not metric collection is enabled.  If enabled, :attr:`.metrics`
    will be an instance of :class:`~cassandra.metrics.Metrics`.
    """

    metrics = None
    """
    An instance of :class:`cassandra.metrics.Metrics` if :attr:`.metrics_enabled` is
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
    If set <= 0, the driver will bypass schema agreement waits altogether.
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
    * :class:`cassandra.io.geventreactor.GeventConnection` (requires monkey-patching)
    * :class:`cassandra.io.twistedreactor.TwistedConnection`

    By default, ``AsyncoreConnection`` will be used, which uses
    the ``asyncore`` module in the Python standard library.  The
    performance is slightly worse than with ``libev``, but it is
    supported on a wider range of systems.

    If ``libev`` is installed, ``LibevConnection`` will be used instead.

    If gevent monkey-patching of the standard library is detected,
    GeventConnection will be used automatically.
    """

    control_connection_timeout = 2.0
    """
    A timeout, in seconds, for queries made by the control connection, such
    as querying the current schema and information about nodes in the cluster.
    If set to :const:`None`, there will be no timeout for these queries.
    """

    idle_heartbeat_interval = 30
    """
    Interval, in seconds, on which to heartbeat idle connections. This helps
    keep connections open through network devices that expire idle connections.
    It also helps discover bad connections early in low-traffic scenarios.
    Setting to zero disables heartbeats.
    """

    schema_event_refresh_window = 2
    """
    Window, in seconds, within which a schema component will be refreshed after
    receiving a schema_change event.

    The driver delays a random amount of time in the range [0.0, window)
    before executing the refresh. This serves two purposes:

    1.) Spread the refresh for deployments with large fanout from C* to client tier,
    preventing a 'thundering herd' problem with many clients refreshing simultaneously.

    2.) Remove redundant refreshes. Redundant events arriving within the delay period
    are discarded, and only one refresh is executed.

    Setting this to zero will execute refreshes immediately.

    Setting this negative will disable schema refreshes in response to push events
    (refreshes will still occur in response to schema change responses to DDL statements
    executed by Sessions of this Cluster).
    """

    topology_event_refresh_window = 10
    """
    Window, in seconds, within which the node and token list will be refreshed after
    receiving a topology_change event.

    Setting this to zero will execute refreshes immediately.

    Setting this negative will disable node refreshes in response to push events
    (refreshes will still occur in response to new nodes observed on "UP" events).

    See :attr:`.schema_event_refresh_window` for discussion of rationale
    """

    connect_timeout = 5
    """
    Timeout, in seconds, for creating new connections.

    This timeout covers the entire connection negotiation, including TCP
    establishment, options passing, and authentication.
    """

    sessions = None
    control_connection = None
    scheduler = None
    executor = None
    is_shutdown = False
    _is_setup = False
    _prepared_statements = None
    _prepared_statement_lock = None
    _idle_heartbeat = None

    _user_types = None
    """
    A map of {keyspace: {type_name: UserType}}
    """

    _listeners = None
    _listener_lock = None

    def __init__(self,
                 contact_points=["127.0.0.1"],
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
                 protocol_version=2,
                 executor_threads=2,
                 max_schema_agreement_wait=10,
                 control_connection_timeout=2.0,
                 idle_heartbeat_interval=30,
                 schema_event_refresh_window=2,
                 topology_event_refresh_window=10,
                 connect_timeout=5):
        """
        Any of the mutable Cluster attributes may be set as keyword arguments
        to the constructor.
        """
        if contact_points is not None:
            if isinstance(contact_points, six.string_types):
                raise TypeError("contact_points should not be a string, it should be a sequence (e.g. list) of strings")

            self.contact_points = contact_points

        self.port = port
        self.compression = compression
        self.protocol_version = protocol_version
        self.auth_provider = auth_provider

        if load_balancing_policy is not None:
            if isinstance(load_balancing_policy, type):
                raise TypeError("load_balancing_policy should not be a class, it should be an instance of that class")

            self.load_balancing_policy = load_balancing_policy
        else:
            self.load_balancing_policy = RoundRobinPolicy()

        if reconnection_policy is not None:
            if isinstance(reconnection_policy, type):
                raise TypeError("reconnection_policy should not be a class, it should be an instance of that class")

            self.reconnection_policy = reconnection_policy

        if default_retry_policy is not None:
            if isinstance(default_retry_policy, type):
                raise TypeError("default_retry_policy should not be a class, it should be an instance of that class")

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
        self.control_connection_timeout = control_connection_timeout
        self.idle_heartbeat_interval = idle_heartbeat_interval
        self.schema_event_refresh_window = schema_event_refresh_window
        self.topology_event_refresh_window = topology_event_refresh_window
        self.connect_timeout = connect_timeout

        self._listeners = set()
        self._listener_lock = Lock()

        # let Session objects be GC'ed (and shutdown) when the user no longer
        # holds a reference.
        self.sessions = WeakSet()
        self.metadata = Metadata()
        self.control_connection = None
        self._prepared_statements = WeakValueDictionary()
        self._prepared_statement_lock = Lock()

        self._user_types = defaultdict(dict)

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
            from cassandra.metrics import Metrics
            self.metrics = Metrics(weakref.proxy(self))

        self.control_connection = ControlConnection(
            self, self.control_connection_timeout,
            self.schema_event_refresh_window, self.topology_event_refresh_window)

    def register_user_type(self, keyspace, user_type, klass):
        """
        Registers a class to use to represent a particular user-defined type.
        Query parameters for this user-defined type will be assumed to be
        instances of `klass`.  Result sets for this user-defined type will
        be instances of `klass`.  If no class is registered for a user-defined
        type, a namedtuple will be used for result sets, and non-prepared
        statements may not encode parameters for this type correctly.

        `keyspace` is the name of the keyspace that the UDT is defined in.

        `user_type` is the string name of the UDT to register the mapping
        for.

        `klass` should be a class with attributes whose names match the
        fields of the user-defined type.  The constructor must accepts kwargs
        for each of the fields in the UDT.

        This method should only be called after the type has been created
        within Cassandra.

        Example::

            cluster = Cluster(protocol_version=3)
            session = cluster.connect()
            session.set_keyspace('mykeyspace')
            session.execute("CREATE TYPE address (street text, zipcode int)")
            session.execute("CREATE TABLE users (id int PRIMARY KEY, location address)")

            # create a class to map to the "address" UDT
            class Address(object):

                def __init__(self, street, zipcode):
                    self.street = street
                    self.zipcode = zipcode

            cluster.register_user_type('mykeyspace', 'address', Address)

            # insert a row using an instance of Address
            session.execute("INSERT INTO users (id, location) VALUES (%s, %s)",
                            (0, Address("123 Main St.", 78723)))

            # results will include Address instances
            results = session.execute("SELECT * FROM users")
            row = results[0]
            print row.id, row.location.street, row.location.zipcode

        """
        self._user_types[keyspace][user_type] = klass
        for session in self.sessions:
            session.user_type_registered(keyspace, user_type, klass)
        UserType.evict_udt_class(keyspace, user_type)

    def get_min_requests_per_connection(self, host_distance):
        return self._min_requests_per_connection[host_distance]

    def set_min_requests_per_connection(self, host_distance, min_requests):
        if self.protocol_version >= 3:
            raise UnsupportedOperation(
                "Cluster.set_min_requests_per_connection() only has an effect "
                "when using protocol_version 1 or 2.")
        self._min_requests_per_connection[host_distance] = min_requests

    def get_max_requests_per_connection(self, host_distance):
        return self._max_requests_per_connection[host_distance]

    def set_max_requests_per_connection(self, host_distance, max_requests):
        if self.protocol_version >= 3:
            raise UnsupportedOperation(
                "Cluster.set_max_requests_per_connection() only has an effect "
                "when using protocol_version 1 or 2.")
        self._max_requests_per_connection[host_distance] = max_requests

    def get_core_connections_per_host(self, host_distance):
        """
        Gets the minimum number of connections per Session that will be opened
        for each host with :class:`~.HostDistance` equal to `host_distance`.
        The default is 2 for :attr:`~HostDistance.LOCAL` and 1 for
        :attr:`~HostDistance.REMOTE`.

        This property is ignored if :attr:`~.Cluster.protocol_version` is
        3 or higher.
        """
        return self._core_connections_per_host[host_distance]

    def set_core_connections_per_host(self, host_distance, core_connections):
        """
        Sets the minimum number of connections per Session that will be opened
        for each host with :class:`~.HostDistance` equal to `host_distance`.
        The default is 2 for :attr:`~HostDistance.LOCAL` and 1 for
        :attr:`~HostDistance.REMOTE`.

        If :attr:`~.Cluster.protocol_version` is set to 3 or higher, this
        is not supported (there is always one connection per host, unless
        the host is remote and :attr:`connect_to_remote_hosts` is :const:`False`)
        and using this will result in an :exc:`~.UnsupporteOperation`.
        """
        if self.protocol_version >= 3:
            raise UnsupportedOperation(
                "Cluster.set_core_connections_per_host() only has an effect "
                "when using protocol_version 1 or 2.")
        old = self._core_connections_per_host[host_distance]
        self._core_connections_per_host[host_distance] = core_connections
        if old < core_connections:
            self._ensure_core_connections()

    def get_max_connections_per_host(self, host_distance):
        """
        Gets the maximum number of connections per Session that will be opened
        for each host with :class:`~.HostDistance` equal to `host_distance`.
        The default is 8 for :attr:`~HostDistance.LOCAL` and 2 for
        :attr:`~HostDistance.REMOTE`.

        This property is ignored if :attr:`~.Cluster.protocol_version` is
        3 or higher.
        """
        return self._max_connections_per_host[host_distance]

    def set_max_connections_per_host(self, host_distance, max_connections):
        """
        Sets the maximum number of connections per Session that will be opened
        for each host with :class:`~.HostDistance` equal to `host_distance`.
        The default is 2 for :attr:`~HostDistance.LOCAL` and 1 for
        :attr:`~HostDistance.REMOTE`.

        If :attr:`~.Cluster.protocol_version` is set to 3 or higher, this
        is not supported (there is always one connection per host, unless
        the host is remote and :attr:`connect_to_remote_hosts` is :const:`False`)
        and using this will result in an :exc:`~.UnsupporteOperation`.
        """
        if self.protocol_version >= 3:
            raise UnsupportedOperation(
                "Cluster.set_max_connections_per_host() only has an effect "
                "when using protocol_version 1 or 2.")
        self._max_connections_per_host[host_distance] = max_connections

    def connection_factory(self, address, *args, **kwargs):
        """
        Called to create a new connection with proper configuration.
        Intended for internal use only.
        """
        kwargs = self._make_connection_kwargs(address, kwargs)
        return self.connection_class.factory(address, self.connect_timeout, *args, **kwargs)

    def _make_connection_factory(self, host, *args, **kwargs):
        kwargs = self._make_connection_kwargs(host.address, kwargs)
        return partial(self.connection_class.factory, host.address, self.connect_timeout, *args, **kwargs)

    def _make_connection_kwargs(self, address, kwargs_dict):
        if self._auth_provider_callable:
            kwargs_dict['authenticator'] = self._auth_provider_callable(address)

        kwargs_dict['port'] = self.port
        kwargs_dict['compression'] = self.compression
        kwargs_dict['sockopts'] = self.sockopts
        kwargs_dict['ssl_options'] = self.ssl_options
        kwargs_dict['cql_version'] = self.cql_version
        kwargs_dict['protocol_version'] = self.protocol_version
        kwargs_dict['user_type_map'] = self._user_types

        return kwargs_dict

    def connect(self, keyspace=None):
        """
        Creates and returns a new :class:`~.Session` object.  If `keyspace`
        is specified, that keyspace will be the default keyspace for
        operations on the ``Session``.
        """
        with self._lock:
            if self.is_shutdown:
                raise Exception("Cluster is already shut down")

            if not self._is_setup:
                log.debug("Connecting to cluster, contact points: %s; protocol version: %s",
                          self.contact_points, self.protocol_version)
                self.connection_class.initialize_reactor()
                atexit.register(partial(_shutdown_cluster, self))
                for address in self.contact_points:
                    host, new = self.add_host(address, signal=False)
                    if new:
                        host.set_up()
                        for listener in self.listeners:
                            listener.on_add(host)

                self.load_balancing_policy.populate(
                    weakref.proxy(self), self.metadata.all_hosts())

                try:
                    self.control_connection.connect()
                    log.debug("Control connection created")
                except Exception:
                    log.exception("Control connection failed to connect, "
                                  "shutting down Cluster:")
                    self.shutdown()
                    raise

                self.load_balancing_policy.check_supported()

                if self.idle_heartbeat_interval:
                    self._idle_heartbeat = ConnectionHeartbeat(self.idle_heartbeat_interval, self.get_connection_holders)
                self._is_setup = True

        session = self._new_session()
        if keyspace:
            session.set_keyspace(keyspace)
        return session

    def get_connection_holders(self):
        holders = []
        for s in self.sessions:
            holders.extend(s.get_pools())
        holders.append(self.control_connection)
        return holders

    def shutdown(self):
        """
        Closes all sessions and connection associated with this Cluster.
        To ensure all connections are properly closed, **you should always
        call shutdown() on a Cluster instance when you are done with it**.

        Once shutdown, a Cluster should not be used for any purpose.
        """
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True

        if self._idle_heartbeat:
            self._idle_heartbeat.stop()

        self.scheduler.shutdown()

        self.control_connection.shutdown()

        for session in self.sessions:
            session.shutdown()

        self.executor.shutdown()

    def _new_session(self):
        session = Session(self, self.metadata.all_hosts())
        for keyspace, type_map in six.iteritems(self._user_types):
            for udt_name, klass in six.iteritems(type_map):
                session.user_type_registered(keyspace, udt_name, klass)
        self.sessions.add(session)
        return session

    def _cleanup_failed_on_up_handling(self, host):
        self.load_balancing_policy.on_down(host)
        self.control_connection.on_down(host)
        for session in self.sessions:
            session.remove_pool(host)

        self._start_reconnector(host, is_host_addition=False)

    def _on_up_future_completed(self, host, futures, results, lock, finished_future):
        with lock:
            futures.discard(finished_future)

            try:
                results.append(finished_future.result())
            except Exception as exc:
                results.append(exc)

            if futures:
                return

        try:
            # all futures have completed at this point
            for exc in [f for f in results if isinstance(f, Exception)]:
                log.error("Unexpected failure while marking node %s up:", host, exc_info=exc)
                self._cleanup_failed_on_up_handling(host)
                return

            if not all(results):
                log.debug("Connection pool could not be created, not marking node %s up", host)
                self._cleanup_failed_on_up_handling(host)
                return

            log.info("Connection pools established for node %s", host)
            # mark the host as up and notify all listeners
            host.set_up()
            for listener in self.listeners:
                listener.on_up(host)
        finally:
            with host.lock:
                host._currently_handling_node_up = False

        # see if there are any pools to add or remove now that the host is marked up
        for session in self.sessions:
            session.update_created_pools()

    def on_up(self, host):
        """
        Intended for internal use only.
        """
        if self.is_shutdown:
            return

        log.debug("Waiting to acquire lock for handling up status of node %s", host)
        with host.lock:
            if host._currently_handling_node_up:
                log.debug("Another thread is already handling up status of node %s", host)
                return

            if host.is_up:
                log.debug("Host %s was already marked up", host)
                return

            host._currently_handling_node_up = True
        log.debug("Starting to handle up status of node %s", host)

        have_future = False
        futures = set()
        try:
            log.info("Host %s may be up; will prepare queries and open connection pool", host)

            reconnector = host.get_and_set_reconnection_handler(None)
            if reconnector:
                log.debug("Now that host %s is up, cancelling the reconnection handler", host)
                reconnector.cancel()

            self._prepare_all_queries(host)
            log.debug("Done preparing all queries for host %s, ", host)

            for session in self.sessions:
                session.remove_pool(host)

            log.debug("Signalling to load balancing policy that host %s is up", host)
            self.load_balancing_policy.on_up(host)

            log.debug("Signalling to control connection that host %s is up", host)
            self.control_connection.on_up(host)

            log.debug("Attempting to open new connection pools for host %s", host)
            futures_lock = Lock()
            futures_results = []
            callback = partial(self._on_up_future_completed, host, futures, futures_results, futures_lock)
            for session in self.sessions:
                future = session.add_or_renew_pool(host, is_host_addition=False)
                if future is not None:
                    have_future = True
                    future.add_done_callback(callback)
                    futures.add(future)
        except Exception:
            log.exception("Unexpected failure handling node %s being marked up:", host)
            for future in futures:
                future.cancel()

            self._cleanup_failed_on_up_handling(host)

            with host.lock:
                host._currently_handling_node_up = False
            raise
        else:
            if not have_future:
                with host.lock:
                    host._currently_handling_node_up = False

        # for testing purposes
        return futures

    def _start_reconnector(self, host, is_host_addition):
        if self.load_balancing_policy.distance(host) == HostDistance.IGNORED:
            return

        schedule = self.reconnection_policy.new_schedule()

        # in order to not hold references to this Cluster open and prevent
        # proper shutdown when the program ends, we'll just make a closure
        # of the current Cluster attributes to create new Connections with
        conn_factory = self._make_connection_factory(host)

        reconnector = _HostReconnectionHandler(
            host, conn_factory, is_host_addition, self.on_add, self.on_up,
            self.scheduler, schedule, host.get_and_set_reconnection_handler,
            new_handler=None)

        old_reconnector = host.get_and_set_reconnection_handler(reconnector)
        if old_reconnector:
            log.debug("Old host reconnector found for %s, cancelling", host)
            old_reconnector.cancel()

        log.debug("Starting reconnector for host %s", host)
        reconnector.start()

    @run_in_executor
    def on_down(self, host, is_host_addition, expect_host_to_be_down=False):
        """
        Intended for internal use only.
        """
        if self.is_shutdown:
            return

        with host.lock:
            if (not host.is_up and not expect_host_to_be_down) or host.is_currently_reconnecting():
                return

            host.set_down()

        log.warning("Host %s has been marked down", host)

        self.load_balancing_policy.on_down(host)
        self.control_connection.on_down(host)
        for session in self.sessions:
            session.on_down(host)

        for listener in self.listeners:
            listener.on_down(host)

        self._start_reconnector(host, is_host_addition)

    def on_add(self, host, refresh_nodes=True):
        if self.is_shutdown:
            return

        log.debug("Handling new host %r and notifying listeners", host)

        distance = self.load_balancing_policy.distance(host)
        if distance != HostDistance.IGNORED:
            self._prepare_all_queries(host)
            log.debug("Done preparing queries for new host %r", host)

        self.load_balancing_policy.on_add(host)
        self.control_connection.on_add(host, refresh_nodes)

        if distance == HostDistance.IGNORED:
            log.debug("Not adding connection pool for new host %r because the "
                      "load balancing policy has marked it as IGNORED", host)
            self._finalize_add(host)
            return

        futures_lock = Lock()
        futures_results = []
        futures = set()

        def future_completed(future):
            with futures_lock:
                futures.discard(future)

                try:
                    futures_results.append(future.result())
                except Exception as exc:
                    futures_results.append(exc)

                if futures:
                    return

            log.debug('All futures have completed for added host %s', host)

            for exc in [f for f in futures_results if isinstance(f, Exception)]:
                log.error("Unexpected failure while adding node %s, will not mark up:", host, exc_info=exc)
                return

            if not all(futures_results):
                log.warning("Connection pool could not be created, not marking node %s up", host)
                return

            self._finalize_add(host)

        have_future = False
        for session in self.sessions:
            future = session.add_or_renew_pool(host, is_host_addition=True)
            if future is not None:
                have_future = True
                futures.add(future)
                future.add_done_callback(future_completed)

        if not have_future:
            self._finalize_add(host)

    def _finalize_add(self, host):
        # mark the host as up and notify all listeners
        host.set_up()
        for listener in self.listeners:
            listener.on_add(host)

        # see if there are any pools to add or remove now that the host is marked up
        for session in self.sessions:
            session.update_created_pools()

    def on_remove(self, host):
        if self.is_shutdown:
            return

        log.debug("Removing host %s", host)
        host.set_down()
        self.load_balancing_policy.on_remove(host)
        for session in self.sessions:
            session.on_remove(host)
        for listener in self.listeners:
            listener.on_remove(host)
        self.control_connection.on_remove(host)

    def signal_connection_failure(self, host, connection_exc, is_host_addition, expect_host_to_be_down=False):
        is_down = host.signal_connection_failure(connection_exc)
        if is_down:
            self.on_down(host, is_host_addition, expect_host_to_be_down)
        return is_down

    def add_host(self, address, datacenter=None, rack=None, signal=True, refresh_nodes=True):
        """
        Called when adding initial contact points and when the control
        connection subsequently discovers a new node.
        Returns a Host instance, and a flag indicating whether it was new in
        the metadata.
        Intended for internal use only.
        """
        host, new = self.metadata.add_or_return_host(Host(address, self.conviction_policy_factory, datacenter, rack))
        if new and signal:
            log.info("New Cassandra host %r discovered", host)
            self.on_add(host, refresh_nodes)

        return host, new

    def remove_host(self, host):
        """
        Called when the control connection observes that a node has left the
        ring.  Intended for internal use only.
        """
        if host and self.metadata.remove_host(host):
            log.info("Cassandra host %s removed", host)
            self.on_remove(host)

    def register_listener(self, listener):
        """
        Adds a :class:`cassandra.policies.HostStateListener` subclass instance to
        the list of listeners to be notified when a host is added, removed,
        marked up, or marked down.
        """
        with self._listener_lock:
            self._listeners.add(listener)

    def unregister_listener(self, listener):
        """ Removes a registered listener. """
        with self._listener_lock:
            self._listeners.remove(listener)

    @property
    def listeners(self):
        with self._listener_lock:
            return self._listeners.copy()

    def _ensure_core_connections(self):
        """
        If any host has fewer than the configured number of core connections
        open, attempt to open connections until that number is met.
        """
        for session in self.sessions:
            for pool in session._pools.values():
                pool.ensure_core_connections()

    def refresh_schema(self, keyspace=None, table=None, usertype=None, max_schema_agreement_wait=None):
        """
        Synchronously refresh the schema metadata.

        By default, the timeout for this operation is governed by :attr:`~.Cluster.max_schema_agreement_wait`
        and :attr:`~.Cluster.control_connection_timeout`.

        Passing max_schema_agreement_wait here overrides :attr:`~.Cluster.max_schema_agreement_wait`.

        Setting max_schema_agreement_wait <= 0 will bypass schema agreement and refresh schema immediately.

        An Exception is raised if schema refresh fails for any reason.
        """
        if not self.control_connection.refresh_schema(keyspace, table, usertype, max_schema_agreement_wait):
            raise Exception("Schema was not refreshed. See log for details.")

    def submit_schema_refresh(self, keyspace=None, table=None, usertype=None):
        """
        Schedule a refresh of the internal representation of the current
        schema for this cluster.  If `keyspace` is specified, only that
        keyspace will be refreshed, and likewise for `table`.
        """
        return self.executor.submit(
            self.control_connection.refresh_schema, keyspace, table, usertype)

    def refresh_nodes(self):
        """
        Synchronously refresh the node list and token metadata

        An Exception is raised if node refresh fails for any reason.
        """
        if not self.control_connection.refresh_node_list_and_token_map():
            raise Exception("Node list was not refreshed. See log for details.")

    def set_meta_refresh_enabled(self, enabled):
        """
        Sets a flag to enable (True) or disable (False) all metadata refresh queries.
        This applies to both schema and node topology.

        Disabling this is useful to minimize refreshes during multiple changes.

        Meta refresh must be enabled for the driver to become aware of any cluster
        topology changes or schema updates.
        """
        self.control_connection.set_meta_refresh_enabled(bool(enabled))

    def _prepare_all_queries(self, host):
        if not self._prepared_statements:
            return

        log.debug("Preparing all known prepared statements against host %s", host)
        connection = None
        try:
            connection = self.connection_factory(host.address)
            try:
                self.control_connection.wait_for_schema_agreement(connection)
            except Exception:
                log.debug("Error waiting for schema agreement before preparing statements against host %s", host, exc_info=True)

            statements = self._prepared_statements.values()
            for keyspace, ks_statements in groupby(statements, lambda s: s.keyspace):
                if keyspace is not None:
                    connection.set_keyspace_blocking(keyspace)

                # prepare 10 statements at a time
                ks_statements = list(ks_statements)
                chunks = []
                for i in range(0, len(ks_statements), 10):
                    chunks.append(ks_statements[i:i + 10])

                for ks_chunk in chunks:
                    messages = [PrepareMessage(query=s.query_string) for s in ks_chunk]
                    # TODO: make this timeout configurable somehow?
                    responses = connection.wait_for_responses(*messages, timeout=5.0)
                    for response in responses:
                        if (not isinstance(response, ResultMessage) or
                                response.kind != RESULT_KIND_PREPARED):
                            log.debug("Got unexpected response when preparing "
                                      "statement on host %s: %r", host, response)

            log.debug("Done preparing all known prepared statements against host %s", host)
        except OperationTimedOut as timeout:
            log.warning("Timed out trying to prepare all statements on host %s: %s", host, timeout)
        except (ConnectionException, socket.error) as exc:
            log.warning("Error trying to prepare all statements on host %s: %r", host, exc)
        except Exception:
            log.exception("Error trying to prepare all statements on host %s", host)
        finally:
            if connection:
                connection.close()

    def prepare_on_all_sessions(self, query_id, prepared_statement, excluded_host):
        with self._prepared_statement_lock:
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
    methods.

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

      - :func:`cassandra.query.tuple_factory` - return a result row as a tuple
      - :func:`cassandra.query.named_tuple_factory` - return a result row as a named tuple
      - :func:`cassandra.query.dict_factory` - return a result row as a dict
      - :func:`cassandra.query.ordered_dict_factory` - return a result row as an OrderedDict

    """

    default_timeout = 10.0
    """
    A default timeout, measured in seconds, for queries executed through
    :meth:`.execute()` or :meth:`.execute_async()`.  This default may be
    overridden with the `timeout` parameter for either of those methods
    or the `timeout` parameter for :meth:`.ResponseFuture.result()`.

    Setting this to :const:`None` will cause no timeouts to be set by default.

    Please see :meth:`.ResponseFuture.result` for details on the scope and
    effect of this timeout.

    .. versionadded:: 2.0.0
    """

    default_consistency_level = ConsistencyLevel.ONE
    """
    The default :class:`~ConsistencyLevel` for operations executed through
    this session.  This default may be overridden by setting the
    :attr:`~.Statement.consistency_level` on individual statements.

    .. versionadded:: 1.2.0
    """

    max_trace_wait = 2.0
    """
    The maximum amount of time (in seconds) the driver will wait for trace
    details to be populated server-side for a query before giving up.
    If the `trace` parameter for :meth:`~.execute()` or :meth:`~.execute_async()`
    is :const:`True`, the driver will repeatedly attempt to fetch trace
    details for the query (using exponential backoff) until this limit is
    hit.  If the limit is passed, an error will be logged and the
    :attr:`.Statement.trace` will be left as :const:`None`. """

    default_fetch_size = 5000
    """
    By default, this many rows will be fetched at a time. Setting
    this to :const:`None` will disable automatic paging for large query
    results.  The fetch size can be also specified per-query through
    :attr:`.Statement.fetch_size`.

    This only takes effect when protocol version 2 or higher is used.
    See :attr:`.Cluster.protocol_version` for details.

    .. versionadded:: 2.0.0
    """

    use_client_timestamp = True
    """
    When using protocol version 3 or higher, write timestamps may be supplied
    client-side at the protocol level.  (Normally they are generated
    server-side by the coordinator node.)  Note that timestamps specified
    within a CQL query will override this timestamp.

    .. versionadded:: 2.1.0
    """

    encoder = None
    """
    A :class:`~cassandra.encoder.Encoder` instance that will be used when
    formatting query parameters for non-prepared statements.  This is not used
    for prepared statements (because prepared statements give the driver more
    information about what CQL types are expected, allowing it to accept a
    wider range of python types).

    The encoder uses a mapping from python types to encoder methods (for
    specific CQL types).  This mapping can be be modified by users as they see
    fit.  Methods of :class:`~cassandra.encoder.Encoder` should be used for mapping
    values if possible, because they take precautions to avoid injections and
    properly sanitize data.

    Example::

        cluster = Cluster()
        session = cluster.connect("mykeyspace")
        session.encoder.mapping[tuple] = session.encoder.cql_encode_tuple

        session.execute("CREATE TABLE mytable (k int PRIMARY KEY, col tuple<int, ascii>)")
        session.execute("INSERT INTO mytable (k, col) VALUES (%s, %s)", [0, (123, 'abc')])

    .. versionadded:: 2.1.0
    """

    _lock = None
    _pools = None
    _load_balancer = None
    _metrics = None
    _protocol_version = None

    def __init__(self, cluster, hosts):
        self.cluster = cluster
        self.hosts = hosts

        self._lock = RLock()
        self._pools = {}
        self._load_balancer = cluster.load_balancing_policy
        self._metrics = cluster.metrics
        self._protocol_version = self.cluster.protocol_version

        self.encoder = Encoder()

        # create connection pools in parallel
        futures = []
        for host in hosts:
            future = self.add_or_renew_pool(host, is_host_addition=False)
            if future is not None:
                futures.append(future)

        for future in futures:
            future.result()

    def execute(self, query, parameters=None, timeout=_NOT_SET, trace=False):
        """
        Execute the given query and synchronously wait for the response.

        If an error is encountered while executing the query, an Exception
        will be raised.

        `query` may be a query string or an instance of :class:`cassandra.query.Statement`.

        `parameters` may be a sequence or dict of parameters to bind.  If a
        sequence is used, ``%s`` should be used the placeholder for each
        argument.  If a dict is used, ``%(name)s`` style placeholders must
        be used.

        `timeout` should specify a floating-point timeout (in seconds) after
        which an :exc:`.OperationTimedOut` exception will be raised if the query
        has not completed.  If not set, the timeout defaults to
        :attr:`~.Session.default_timeout`.  If set to :const:`None`, there is
        no timeout. Please see :meth:`.ResponseFuture.result` for details on
        the scope and effect of this timeout.

        If `trace` is set to :const:`True`, an attempt will be made to
        fetch the trace details and attach them to the `query`'s
        :attr:`~.Statement.trace` attribute in the form of a :class:`.QueryTrace`
        instance.  This requires that `query` be a :class:`.Statement` subclass
        instance and not just a string.  If there is an error fetching the
        trace details, the :attr:`~.Statement.trace` attribute will be left as
        :const:`None`.
        """
        if timeout is _NOT_SET:
            timeout = self.default_timeout

        if trace and not isinstance(query, Statement):
            raise TypeError(
                "The query argument must be an instance of a subclass of "
                "cassandra.query.Statement when trace=True")

        future = self.execute_async(query, parameters, trace)
        try:
            result = future.result(timeout)
        finally:
            if trace:
                try:
                    query.trace = future.get_query_trace(self.max_trace_wait)
                except Exception:
                    log.exception("Unable to fetch query trace:")

        return result

    def execute_async(self, query, parameters=None, trace=False):
        """
        Execute the given query and return a :class:`~.ResponseFuture` object
        which callbacks may be attached to for asynchronous response
        delivery.  You may also call :meth:`~.ResponseFuture.result()`
        on the :class:`.ResponseFuture` to syncronously block for results at
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
        future = self._create_response_future(query, parameters, trace)
        future.send_request()
        return future

    def _create_response_future(self, query, parameters, trace):
        """ Returns the ResponseFuture before calling send_request() on it """

        prepared_statement = None

        if isinstance(query, six.string_types):
            query = SimpleStatement(query)
        elif isinstance(query, PreparedStatement):
            query = query.bind(parameters)

        cl = query.consistency_level if query.consistency_level is not None else self.default_consistency_level
        fetch_size = query.fetch_size
        if fetch_size is FETCH_SIZE_UNSET and self._protocol_version >= 2:
            fetch_size = self.default_fetch_size
        elif self._protocol_version == 1:
            fetch_size = None

        if self._protocol_version >= 3 and self.use_client_timestamp:
            timestamp = int(time.time() * 1e6)
        else:
            timestamp = None

        if isinstance(query, SimpleStatement):
            query_string = query.query_string
            if six.PY2 and isinstance(query_string, six.text_type):
                query_string = query_string.encode('utf-8')
            if parameters:
                query_string = bind_params(query_string, parameters, self.encoder)
            message = QueryMessage(
                query_string, cl, query.serial_consistency_level,
                fetch_size, timestamp=timestamp)
        elif isinstance(query, BoundStatement):
            message = ExecuteMessage(
                query.prepared_statement.query_id, query.values, cl,
                query.serial_consistency_level, fetch_size,
                timestamp=timestamp)
            prepared_statement = query.prepared_statement
        elif isinstance(query, BatchStatement):
            if self._protocol_version < 2:
                raise UnsupportedOperation(
                    "BatchStatement execution is only supported with protocol version "
                    "2 or higher (supported in Cassandra 2.0 and higher).  Consider "
                    "setting Cluster.protocol_version to 2 to support this operation.")
            message = BatchMessage(
                query.batch_type, query._statements_and_parameters, cl,
                query.serial_consistency_level, timestamp)

        if trace:
            message.tracing = True

        return ResponseFuture(
            self, message, query, self.default_timeout, metrics=self._metrics,
            prepared_statement=prepared_statement)

    def prepare(self, query):
        """
        Prepares a query string, returing a :class:`~cassandra.query.PreparedStatement`
        instance which can be used as follows::

            >>> session = cluster.connect("mykeyspace")
            >>> query = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
            >>> prepared = session.prepare(query)
            >>> session.execute(prepared, (user.id, user.name, user.age))

        Or you may bind values to the prepared statement ahead of time::

            >>> prepared = session.prepare(query)
            >>> bound_stmt = prepared.bind((user.id, user.name, user.age))
            >>> session.execute(bound_stmt)

        Of course, prepared statements may (and should) be reused::

            >>> prepared = session.prepare(query)
            >>> for user in users:
            ...     bound = prepared.bind((user.id, user.name, user.age))
            ...     session.execute(bound)

        **Important**: PreparedStatements should be prepared only once.
        Preparing the same query more than once will likely affect performance.
        """
        message = PrepareMessage(query=query)
        future = ResponseFuture(self, message, query=None)
        try:
            future.send_request()
            query_id, column_metadata = future.result(self.default_timeout)
        except Exception:
            log.exception("Error preparing query:")
            raise

        prepared_statement = PreparedStatement.from_message(
            query_id, column_metadata, self.cluster.metadata, query, self.keyspace,
            self._protocol_version)

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
        futures = []
        for host in self._pools.keys():
            if host != excluded_host and host.is_up:
                future = ResponseFuture(self, PrepareMessage(query=query), None)

                # we don't care about errors preparing against specific hosts,
                # since we can always prepare them as needed when the prepared
                # statement is used.  Just log errors and continue on.
                try:
                    request_id = future._query(host)
                except Exception:
                    log.exception("Error preparing query for host %s:", host)
                    continue

                if request_id is None:
                    # the error has already been logged by ResponsFuture
                    log.debug("Failed to prepare query for host %s: %r",
                              host, future._errors.get(host))
                    continue

                futures.append((host, future))

        for host, future in futures:
            try:
                future.result(self.default_timeout)
            except Exception:
                log.exception("Error preparing query for host %s:", host)

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

    def add_or_renew_pool(self, host, is_host_addition):
        """
        For internal use only.
        """
        distance = self._load_balancer.distance(host)
        if distance == HostDistance.IGNORED:
            return None

        def run_add_or_renew_pool():
            try:
                if self._protocol_version >= 3:
                    new_pool = HostConnection(host, distance, self)
                else:
                    new_pool = HostConnectionPool(host, distance, self)
            except AuthenticationFailed as auth_exc:
                conn_exc = ConnectionException(str(auth_exc), host=host)
                self.cluster.signal_connection_failure(host, conn_exc, is_host_addition)
                return False
            except Exception as conn_exc:
                log.warning("Failed to create connection pool for new host %s:",
                            host, exc_info=conn_exc)
                # the host itself will still be marked down, so we need to pass
                # a special flag to make sure the reconnector is created
                self.cluster.signal_connection_failure(
                    host, conn_exc, is_host_addition, expect_host_to_be_down=True)
                return False

            previous = self._pools.get(host)
            self._pools[host] = new_pool
            log.debug("Added pool for host %s to session", host)
            if previous:
                previous.shutdown()

            return True

        return self.submit(run_add_or_renew_pool)

    def remove_pool(self, host):
        pool = self._pools.pop(host, None)
        if pool:
            log.debug("Removed connection pool for %r", host)
            return self.submit(pool.shutdown)
        else:
            return None

    def update_created_pools(self):
        """
        When the set of live nodes change, the loadbalancer will change its
        mind on host distances. It might change it on the node that came/left
        but also on other nodes (for instance, if a node dies, another
        previously ignored node may be now considered).

        This method ensures that all hosts for which a pool should exist
        have one, and hosts that shouldn't don't.

        For internal use only.
        """
        for host in self.cluster.metadata.all_hosts():
            distance = self._load_balancer.distance(host)
            pool = self._pools.get(host)

            if not pool or pool.is_shutdown:
                if distance != HostDistance.IGNORED and host.is_up:
                    self.add_or_renew_pool(host, False)
            elif distance != pool.host_distance:
                # the distance has changed
                if distance == HostDistance.IGNORED:
                    self.remove_pool(host)
                else:
                    pool.host_distance = distance

    def on_down(self, host):
        """
        Called by the parent Cluster instance when a node is marked down.
        Only intended for internal use.
        """
        future = self.remove_pool(host)
        if future:
            future.add_done_callback(lambda f: self.update_created_pools())

    def on_remove(self, host):
        """ Internal """
        self.on_down(host)

    def set_keyspace(self, keyspace):
        """
        Set the default keyspace for all queries made through this Session.
        This operation blocks until complete.
        """
        self.execute('USE %s' % (protect_name(keyspace),))

    def _set_keyspace_for_all_pools(self, keyspace, callback):
        """
        Asynchronously sets the keyspace on all pools.  When all
        pools have set all of their connections, `callback` will be
        called with a dictionary of all errors that occurred, keyed
        by the `Host` that they occurred against.
        """
        self.keyspace = keyspace

        remaining_callbacks = set(self._pools.values())
        errors = {}

        if not remaining_callbacks:
            callback(errors)
            return

        def pool_finished_setting_keyspace(pool, host_errors):
            remaining_callbacks.remove(pool)
            if host_errors:
                errors[pool.host] = host_errors

            if not remaining_callbacks:
                callback(host_errors)

        for pool in self._pools.values():
            pool._set_keyspace_for_all_conns(keyspace, pool_finished_setting_keyspace)

    def user_type_registered(self, keyspace, user_type, klass):
        """
        Called by the parent Cluster instance when the user registers a new
        mapping from a user-defined type to a class.  Intended for internal
        use only.
        """
        try:
            ks_meta = self.cluster.metadata.keyspaces[keyspace]
        except KeyError:
            raise UserTypeDoesNotExist(
                'Keyspace %s does not exist or has not been discovered by the driver' % (keyspace,))

        try:
            type_meta = ks_meta.user_types[user_type]
        except KeyError:
            raise UserTypeDoesNotExist(
                'User type %s does not exist in keyspace %s' % (user_type, keyspace))

        def encode(val):
            return '{ %s }' % ' , '.join('%s : %s' % (
                field_name,
                self.encoder.cql_encode_all_types(getattr(val, field_name, None))
            ) for field_name in type_meta.field_names)

        self.encoder.mapping[klass] = encode

    def submit(self, fn, *args, **kwargs):
        """ Internal """
        if not self.is_shutdown:
            return self.cluster.executor.submit(fn, *args, **kwargs)

    def get_pool_state(self):
        return dict((host, pool.get_state()) for host, pool in self._pools.items())

    def get_pools(self):
        return self._pools.values()


class UserTypeDoesNotExist(Exception):
    """
    An attempt was made to use a user-defined type that does not exist.

    .. versionadded:: 2.1.0
    """
    pass


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
            log.debug("Error trying to reconnect control connection: %r", exc)
            return True


def _watch_callback(obj_weakref, method_name, *args, **kwargs):
    """
    A callback handler for the ControlConnection that tolerates
    weak references.
    """
    obj = obj_weakref()
    if obj is None:
        return
    getattr(obj, method_name)(*args, **kwargs)


def _clear_watcher(conn, expiring_weakref):
    """
    Called when the ControlConnection object is about to be finalized.
    This clears watchers on the underlying Connection object.
    """
    try:
        conn.control_conn_disposed()
    except ReferenceError:
        pass


class ControlConnection(object):
    """
    Internal
    """

    _SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces"
    _SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies"
    _SELECT_COLUMNS = "SELECT * FROM system.schema_columns"
    _SELECT_USERTYPES = "SELECT * FROM system.schema_usertypes"
    _SELECT_TRIGGERS = "SELECT * FROM system.schema_triggers"

    _SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address, schema_version FROM system.peers"
    _SELECT_LOCAL = "SELECT cluster_name, data_center, rack, tokens, partitioner, schema_version FROM system.local WHERE key='local'"

    _SELECT_SCHEMA_PEERS = "SELECT peer, rpc_address, schema_version FROM system.peers"
    _SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'"

    _is_shutdown = False
    _timeout = None
    _protocol_version = None

    _schema_event_refresh_window = None
    _topology_event_refresh_window = None

    _meta_refresh_enabled = True

    # for testing purposes
    _time = time

    def __init__(self, cluster, timeout,
                 schema_event_refresh_window,
                 topology_event_refresh_window):
        # use a weak reference to allow the Cluster instance to be GC'ed (and
        # shutdown) since implementing __del__ disables the cycle detector
        self._cluster = weakref.proxy(cluster)
        self._connection = None
        self._timeout = timeout

        self._schema_event_refresh_window = schema_event_refresh_window
        self._topology_event_refresh_window = topology_event_refresh_window

        self._lock = RLock()
        self._schema_agreement_lock = Lock()

        self._reconnection_handler = None
        self._reconnection_lock = RLock()

    def connect(self):
        if self._is_shutdown:
            return

        self._protocol_version = self._cluster.protocol_version
        self._set_new_connection(self._reconnect_internal())

    def _set_new_connection(self, conn):
        """
        Replace existing connection (if there is one) and close it.
        """
        with self._lock:
            old = self._connection
            self._connection = conn

        if old:
            log.debug("[control connection] Closing old connection %r, replacing with %r", old, conn)
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
        for host in self._cluster.load_balancing_policy.make_query_plan():
            try:
                return self._try_connect(host)
            except ConnectionException as exc:
                errors[host.address] = exc
                log.warning("[control connection] Error connecting to %s:", host, exc_info=True)
                self._cluster.signal_connection_failure(host, exc, is_host_addition=False)
            except Exception as exc:
                errors[host.address] = exc
                log.warning("[control connection] Error connecting to %s:", host, exc_info=True)

        raise NoHostAvailable("Unable to connect to any servers", errors)

    def _try_connect(self, host):
        """
        Creates a new Connection, registers for pushed events, and refreshes
        node/token and schema metadata.
        """
        log.debug("[control connection] Opening new connection to %s", host)
        connection = self._cluster.connection_factory(host.address, is_control_connection=True)

        log.debug("[control connection] Established new connection %r, "
                  "registering watchers and refreshing schema and topology",
                  connection)

        # use weak references in both directions
        # _clear_watcher will be called when this ControlConnection is about to be finalized
        # _watch_callback will get the actual callback from the Connection and relay it to
        # this object (after a dereferencing a weakref)
        self_weakref = weakref.ref(self, callback=partial(_clear_watcher, weakref.proxy(connection)))
        try:
            connection.register_watchers({
                "TOPOLOGY_CHANGE": partial(_watch_callback, self_weakref, '_handle_topology_change'),
                "STATUS_CHANGE": partial(_watch_callback, self_weakref, '_handle_status_change'),
                "SCHEMA_CHANGE": partial(_watch_callback, self_weakref, '_handle_schema_change')
            }, register_timeout=self._timeout)

            peers_query = QueryMessage(query=self._SELECT_PEERS, consistency_level=ConsistencyLevel.ONE)
            local_query = QueryMessage(query=self._SELECT_LOCAL, consistency_level=ConsistencyLevel.ONE)
            shared_results = connection.wait_for_responses(
                peers_query, local_query, timeout=self._timeout)

            self._refresh_node_list_and_token_map(connection, preloaded_results=shared_results)
            self._refresh_schema(connection, preloaded_results=shared_results)
            if not self._cluster.metadata.keyspaces:
                log.warning("[control connection] No schema built on connect; retrying without wait for schema agreement")
                self._refresh_schema(connection, preloaded_results=shared_results, schema_agreement_wait=0)
        except Exception:
            connection.close()
            raise

        return connection

    def reconnect(self):
        if self._is_shutdown:
            return

        self._submit(self._reconnect)

    def _reconnect(self):
        log.debug("[control connection] Attempting to reconnect")
        try:
            self._set_new_connection(self._reconnect_internal())
        except NoHostAvailable:
            # make a retry schedule (which includes backoff)
            schedule = self.cluster.reconnection_policy.new_schedule()

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

    def _submit(self, *args, **kwargs):
        try:
            if not self._cluster.is_shutdown:
                return self._cluster.executor.submit(*args, **kwargs)
        except ReferenceError:
            pass
        return None

    def shutdown(self):
        with self._lock:
            if self._is_shutdown:
                return
            else:
                self._is_shutdown = True

        log.debug("Shutting down control connection")
        # stop trying to reconnect (if we are)
        if self._reconnection_handler:
            self._reconnection_handler.cancel()

        if self._connection:
            self._connection.close()
            del self._connection

    def refresh_schema(self, keyspace=None, table=None, usertype=None,
                       schema_agreement_wait=None):
        if not self._meta_refresh_enabled:
            log.debug("[control connection] Skipping schema refresh because meta refresh is disabled")
            return False

        try:
            if self._connection:
                return self._refresh_schema(self._connection, keyspace, table, usertype,
                                            schema_agreement_wait=schema_agreement_wait)
        except ReferenceError:
            pass  # our weak reference to the Cluster is no good
        except Exception:
            log.debug("[control connection] Error refreshing schema", exc_info=True)
            self._signal_error()
        return False

    def _refresh_schema(self, connection, keyspace=None, table=None, usertype=None,
                        preloaded_results=None, schema_agreement_wait=None):
        if self._cluster.is_shutdown:
            return False

        assert table is None or usertype is None

        agreed = self.wait_for_schema_agreement(connection,
                                                preloaded_results=preloaded_results,
                                                wait_time=schema_agreement_wait)
        if not agreed:
            log.debug("Skipping schema refresh due to lack of schema agreement")
            return False

        cl = ConsistencyLevel.ONE
        if table:
            def _handle_results(success, result):
                if success:
                    return dict_factory(*result.results) if result else {}
                else:
                    raise result

            # a particular table changed
            where_clause = " WHERE keyspace_name = '%s' AND columnfamily_name = '%s'" % (keyspace, table)
            cf_query = QueryMessage(query=self._SELECT_COLUMN_FAMILIES + where_clause, consistency_level=cl)
            col_query = QueryMessage(query=self._SELECT_COLUMNS + where_clause, consistency_level=cl)
            triggers_query = QueryMessage(query=self._SELECT_TRIGGERS + where_clause, consistency_level=cl)
            (cf_success, cf_result), (col_success, col_result), (triggers_success, triggers_result) \
                = connection.wait_for_responses(cf_query, col_query, triggers_query, timeout=self._timeout, fail_on_error=False)

            log.debug("[control connection] Fetched table info for %s.%s, rebuilding metadata", keyspace, table)
            cf_result = _handle_results(cf_success, cf_result)
            col_result = _handle_results(col_success, col_result)

            # handle the triggers table not existing in Cassandra 1.2
            if not triggers_success and isinstance(triggers_result, InvalidRequest):
                triggers_result = {}
            else:
                triggers_result = _handle_results(triggers_success, triggers_result)

            self._cluster.metadata.table_changed(keyspace, table, cf_result, col_result, triggers_result)
        elif usertype:
            # user defined types within this keyspace changed
            where_clause = " WHERE keyspace_name = '%s' AND type_name = '%s'" % (keyspace, usertype)
            types_query = QueryMessage(query=self._SELECT_USERTYPES + where_clause, consistency_level=cl)
            types_result = connection.wait_for_response(types_query)
            log.debug("[control connection] Fetched user type info for %s.%s, rebuilding metadata", keyspace, usertype)
            types_result = dict_factory(*types_result.results) if types_result.results else {}
            self._cluster.metadata.usertype_changed(keyspace, usertype, types_result)
        elif keyspace:
            # only the keyspace itself changed (such as replication settings)
            where_clause = " WHERE keyspace_name = '%s'" % (keyspace,)
            ks_query = QueryMessage(query=self._SELECT_KEYSPACES + where_clause, consistency_level=cl)
            ks_result = connection.wait_for_response(ks_query)
            log.debug("[control connection] Fetched keyspace info for %s, rebuilding metadata", keyspace)
            ks_result = dict_factory(*ks_result.results) if ks_result.results else {}
            self._cluster.metadata.keyspace_changed(keyspace, ks_result)
        else:
            # build everything from scratch
            queries = [
                QueryMessage(query=self._SELECT_KEYSPACES, consistency_level=cl),
                QueryMessage(query=self._SELECT_COLUMN_FAMILIES, consistency_level=cl),
                QueryMessage(query=self._SELECT_COLUMNS, consistency_level=cl),
                QueryMessage(query=self._SELECT_USERTYPES, consistency_level=cl),
                QueryMessage(query=self._SELECT_TRIGGERS, consistency_level=cl)
            ]

            responses = connection.wait_for_responses(*queries, timeout=self._timeout, fail_on_error=False)
            (ks_success, ks_result), (cf_success, cf_result), \
                (col_success, col_result), (types_success, types_result), \
                (trigger_success, triggers_result) = responses

            if ks_success:
                ks_result = dict_factory(*ks_result.results)
            else:
                raise ks_result

            if cf_success:
                cf_result = dict_factory(*cf_result.results)
            else:
                raise cf_result

            if col_success:
                col_result = dict_factory(*col_result.results)
            else:
                raise col_result

            # if we're connected to Cassandra < 2.0, the trigges table will not exist
            if trigger_success:
                triggers_result = dict_factory(*triggers_result.results)
            else:
                if isinstance(triggers_result, InvalidRequest):
                    log.debug("[control connection] triggers table not found")
                    triggers_result = {}
                elif isinstance(triggers_result, Unauthorized):
                    log.warning("[control connection] this version of Cassandra does not allow access to schema_triggers metadata with authorization enabled (CASSANDRA-7967); "
                                "The driver will operate normally, but will not reflect triggers in the local metadata model, or schema strings.")
                    triggers_result = {}
                else:
                    raise triggers_result

            # if we're connected to Cassandra < 2.1, the usertypes table will not exist
            if types_success:
                types_result = dict_factory(*types_result.results) if types_result.results else {}
            else:
                if isinstance(types_result, InvalidRequest):
                    log.debug("[control connection] user types table not found")
                    types_result = {}
                else:
                    raise types_result

            log.debug("[control connection] Fetched schema, rebuilding metadata")
            self._cluster.metadata.rebuild_schema(ks_result, types_result, cf_result, col_result, triggers_result)
        return True

    def refresh_node_list_and_token_map(self, force_token_rebuild=False):
        if not self._meta_refresh_enabled:
            log.debug("[control connection] Skipping node list refresh because meta refresh is disabled")
            return False

        try:
            if self._connection:
                self._refresh_node_list_and_token_map(self._connection, force_token_rebuild=force_token_rebuild)
                return True
        except ReferenceError:
            pass  # our weak reference to the Cluster is no good
        except Exception:
            log.debug("[control connection] Error refreshing node list and token map", exc_info=True)
            self._signal_error()
        return False

    def _refresh_node_list_and_token_map(self, connection, preloaded_results=None,
                                         force_token_rebuild=False):

        if preloaded_results:
            log.debug("[control connection] Refreshing node list and token map using preloaded results")
            peers_result = preloaded_results[0]
            local_result = preloaded_results[1]
        else:
            log.debug("[control connection] Refreshing node list and token map")
            cl = ConsistencyLevel.ONE
            peers_query = QueryMessage(query=self._SELECT_PEERS, consistency_level=cl)
            local_query = QueryMessage(query=self._SELECT_LOCAL, consistency_level=cl)
            peers_result, local_result = connection.wait_for_responses(
                peers_query, local_query, timeout=self._timeout)

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
                datacenter = local_row.get("data_center")
                rack = local_row.get("rack")
                self._update_location_info(host, datacenter, rack)

            partitioner = local_row.get("partitioner")
            tokens = local_row.get("tokens")
            if partitioner and tokens:
                token_map[host] = tokens

        # Check metadata.partitioner to see if we haven't built anything yet. If
        # every node in the cluster was in the contact points, we won't discover
        # any new nodes, so we need this additional check.  (See PYTHON-90)
        should_rebuild_token_map = force_token_rebuild or self._cluster.metadata.partitioner is None
        found_hosts = set()
        for row in peers_result:
            addr = row.get("rpc_address")

            if not addr or addr in ["0.0.0.0", "::"]:
                addr = row.get("peer")

            tokens = row.get("tokens")
            if not tokens:
                log.warning("Excluding host (%s) with no tokens in system.peers table of %s." % (addr, connection.host))
                continue

            found_hosts.add(addr)

            host = self._cluster.metadata.get_host(addr)
            datacenter = row.get("data_center")
            rack = row.get("rack")
            if host is None:
                log.debug("[control connection] Found new host to connect to: %s", addr)
                host, _ = self._cluster.add_host(addr, datacenter, rack, signal=True, refresh_nodes=False)
                should_rebuild_token_map = True
            else:
                should_rebuild_token_map |= self._update_location_info(host, datacenter, rack)

            if partitioner and tokens:
                token_map[host] = tokens

        for old_host in self._cluster.metadata.all_hosts():
            if old_host.address != connection.host and old_host.address not in found_hosts:
                should_rebuild_token_map = True
                if old_host.address not in self._cluster.contact_points:
                    log.debug("[control connection] Found host that has been removed: %r", old_host)
                    self._cluster.remove_host(old_host)

        log.debug("[control connection] Finished fetching ring info")
        if partitioner and should_rebuild_token_map:
            log.debug("[control connection] Rebuilding token map due to topology changes")
            self._cluster.metadata.rebuild_token_map(partitioner, token_map)

    def _update_location_info(self, host, datacenter, rack):
        if host.datacenter == datacenter and host.rack == rack:
            return False

        # If the dc/rack information changes, we need to update the load balancing policy.
        # For that, we remove and re-add the node against the policy. Not the most elegant, and assumes
        # that the policy will update correctly, but in practice this should work.
        self._cluster.load_balancing_policy.on_down(host)
        host.set_location_info(datacenter, rack)
        self._cluster.load_balancing_policy.on_up(host)
        return True

    def _handle_topology_change(self, event):
        change_type = event["change_type"]
        addr, port = event["address"]
        if change_type == "NEW_NODE" or change_type == "MOVED_NODE":
            if self._topology_event_refresh_window >= 0:
                delay = random() * self._topology_event_refresh_window
                self._cluster.scheduler.schedule_unique(delay, self.refresh_node_list_and_token_map)
        elif change_type == "REMOVED_NODE":
            host = self._cluster.metadata.get_host(addr)
            self._cluster.scheduler.schedule_unique(0, self._cluster.remove_host, host)

    def _handle_status_change(self, event):
        change_type = event["change_type"]
        addr, port = event["address"]
        host = self._cluster.metadata.get_host(addr)
        if change_type == "UP":
            delay = 1 + random() * 0.5  # randomness to avoid thundering herd problem on events
            if host is None:
                # this is the first time we've seen the node
                self._cluster.scheduler.schedule_unique(delay, self.refresh_node_list_and_token_map)
            else:
                self._cluster.scheduler.schedule_unique(delay, self._cluster.on_up, host)
        elif change_type == "DOWN":
            # Note that there is a slight risk we can receive the event late and thus
            # mark the host down even though we already had reconnected successfully.
            # But it is unlikely, and don't have too much consequence since we'll try reconnecting
            # right away, so we favor the detection to make the Host.is_up more accurate.
            if host is not None:
                # this will be run by the scheduler
                self._cluster.on_down(host, is_host_addition=False)

    def _handle_schema_change(self, event):
        if self._schema_event_refresh_window < 0:
            return

        keyspace = event.get('keyspace')
        table = event.get('table')
        usertype = event.get('type')
        delay = random() * self._schema_event_refresh_window
        self._cluster.scheduler.schedule_unique(delay, self.refresh_schema, keyspace, table, usertype)

    def wait_for_schema_agreement(self, connection=None, preloaded_results=None, wait_time=None):

        total_timeout = wait_time if wait_time is not None else self._cluster.max_schema_agreement_wait
        if total_timeout <= 0:
            return True

        # Each schema change typically generates two schema refreshes, one
        # from the response type and one from the pushed notification. Holding
        # a lock is just a simple way to cut down on the number of schema queries
        # we'll make.
        with self._schema_agreement_lock:
            if self._is_shutdown:
                return

            if not connection:
                connection = self._connection

            if preloaded_results:
                log.debug("[control connection] Attempting to use preloaded results for schema agreement")

                peers_result = preloaded_results[0]
                local_result = preloaded_results[1]
                schema_mismatches = self._get_schema_mismatches(peers_result, local_result, connection.host)
                if schema_mismatches is None:
                    return True

            log.debug("[control connection] Waiting for schema agreement")
            start = self._time.time()
            elapsed = 0
            cl = ConsistencyLevel.ONE
            schema_mismatches = None
            while elapsed < total_timeout:
                peers_query = QueryMessage(query=self._SELECT_SCHEMA_PEERS, consistency_level=cl)
                local_query = QueryMessage(query=self._SELECT_SCHEMA_LOCAL, consistency_level=cl)
                try:
                    timeout = min(self._timeout, total_timeout - elapsed)
                    peers_result, local_result = connection.wait_for_responses(
                        peers_query, local_query, timeout=timeout)
                except OperationTimedOut as timeout:
                    log.debug("[control connection] Timed out waiting for "
                              "response during schema agreement check: %s", timeout)
                    elapsed = self._time.time() - start
                    continue
                except ConnectionShutdown:
                    if self._is_shutdown:
                        log.debug("[control connection] Aborting wait for schema match due to shutdown")
                        return None
                    else:
                        raise

                schema_mismatches = self._get_schema_mismatches(peers_result, local_result, connection.host)
                if schema_mismatches is None:
                    return True

                log.debug("[control connection] Schemas mismatched, trying again")
                self._time.sleep(0.2)
                elapsed = self._time.time() - start

            log.warning("Node %s is reporting a schema disagreement: %s",
                        connection.host, schema_mismatches)
            return False

    def _get_schema_mismatches(self, peers_result, local_result, local_address):
        peers_result = dict_factory(*peers_result.results)

        versions = defaultdict(set)
        if local_result.results:
            local_row = dict_factory(*local_result.results)
            if local_row:
                local_row = local_row[0]
                if local_row.get("schema_version"):
                    versions[local_row.get("schema_version")].add(local_address)

        for row in peers_result:
            schema_ver = row.get('schema_version')
            if not schema_ver:
                continue

            addr = row.get("rpc_address")
            if not addr or addr in ["0.0.0.0", "::"]:
                addr = row.get("peer")

            peer = self._cluster.metadata.get_host(addr)
            if peer and peer.is_up:
                versions[schema_ver].add(addr)

        if len(versions) == 1:
            log.debug("[control connection] Schemas match")
            return None

        return dict((version, list(nodes)) for version, nodes in six.iteritems(versions))

    def _signal_error(self):
        # try just signaling the cluster, as this will trigger a reconnect
        # as part of marking the host down
        if self._connection and self._connection.is_defunct:
            host = self._cluster.metadata.get_host(self._connection.host)
            # host may be None if it's already been removed, but that indicates
            # that errors have already been reported, so we're fine
            if host:
                self._cluster.signal_connection_failure(
                    host, self._connection.last_error, is_host_addition=False)
                return

        # if the connection is not defunct or the host already left, reconnect
        # manually
        self.reconnect()

    def on_up(self, host):
        pass

    def on_down(self, host):

        conn = self._connection
        if conn and conn.host == host.address and \
                self._reconnection_handler is None:
            log.debug("[control connection] Control connection host (%s) is "
                      "considered down, starting reconnection", host)
            # this will result in a task being submitted to the executor to reconnect
            self.reconnect()

    def on_add(self, host, refresh_nodes=True):
        if refresh_nodes:
            self.refresh_node_list_and_token_map(force_token_rebuild=True)

    def on_remove(self, host):
        self.refresh_node_list_and_token_map(force_token_rebuild=True)

    def get_connections(self):
        c = getattr(self, '_connection', None)
        return [c] if c else []

    def return_connection(self, connection):
        if connection is self._connection and (connection.is_defunct or connection.is_closed):
            self.reconnect()

    def set_meta_refresh_enabled(self, enabled):
        self._meta_refresh_enabled = enabled


def _stop_scheduler(scheduler, thread):
    try:
        if not scheduler.is_shutdown:
            scheduler.shutdown()
    except ReferenceError:
        pass

    thread.join()


class _Scheduler(object):

    _queue = None
    _scheduled_tasks = None
    _executor = None
    is_shutdown = False

    def __init__(self, executor):
        self._queue = Queue.PriorityQueue()
        self._scheduled_tasks = set()
        self._executor = executor

        t = Thread(target=self.run, name="Task Scheduler")
        t.daemon = True
        t.start()

        # although this runs on a daemonized thread, we prefer to stop
        # it gracefully to avoid random errors during interpreter shutdown
        atexit.register(partial(_stop_scheduler, weakref.proxy(self), t))

    def shutdown(self):
        try:
            log.debug("Shutting down Cluster Scheduler")
        except AttributeError:
            # this can happen on interpreter shutdown
            pass
        self.is_shutdown = True
        self._queue.put_nowait((0, None))

    def schedule(self, delay, fn, *args):
        self._insert_task(delay, (fn, args))

    def schedule_unique(self, delay, fn, *args):
        task = (fn, args)
        if task not in self._scheduled_tasks:
            self._insert_task(delay, task)
        else:
            log.debug("Ignoring schedule_unique for already-scheduled task: %r", task)

    def _insert_task(self, delay, task):
        if not self.is_shutdown:
            run_at = time.time() + delay
            self._scheduled_tasks.add(task)
            self._queue.put_nowait((run_at, task))
        else:
            log.debug("Ignoring scheduled task after shutdown: %r", task)

    def run(self):
        while True:
            if self.is_shutdown:
                return

            try:
                while True:
                    run_at, task = self._queue.get(block=True, timeout=None)
                    if self.is_shutdown:
                        log.debug("Not executing scheduled task due to Scheduler shutdown")
                        return
                    if run_at <= time.time():
                        self._scheduled_tasks.remove(task)
                        fn, args = task
                        future = self._executor.submit(fn, *args)
                        future.add_done_callback(self._log_if_failed)
                    else:
                        self._queue.put_nowait((run_at, task))
                        break
            except Queue.Empty:
                pass

            time.sleep(0.1)

    def _log_if_failed(self, future):
        exc = future.exception()
        if exc:
            log.warning(
                "An internally scheduled tasked failed with an unhandled exception:",
                exc_info=exc)


def refresh_schema_and_set_result(keyspace, table, usertype, control_conn, response_future):
    try:
        if control_conn._meta_refresh_enabled:
            log.debug("Refreshing schema in response to schema change. Keyspace: %s; Table: %s, Type: %s",
                      keyspace, table, usertype)
            control_conn._refresh_schema(response_future._connection, keyspace, table, usertype)
        else:
            log.debug("Skipping schema refresh in response to schema change because meta refresh is disabled; "
                      "Keyspace: %s; Table: %s, Type: %s", keyspace, table, usertype)
    except Exception:
        log.exception("Exception refreshing schema in response to schema change:")
        response_future.session.submit(
            control_conn.refresh_schema, keyspace, table, usertype)
    finally:
        response_future._set_final_result(None)


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

    query = None
    """
    The :class:`~.Statement` instance that is being executed through this
    :class:`.ResponseFuture`.
    """

    session = None
    row_factory = None
    message = None
    default_timeout = None

    _req_id = None
    _final_result = _NOT_SET
    _final_exception = None
    _query_trace = None
    _callbacks = None
    _errbacks = None
    _current_host = None
    _current_pool = None
    _connection = None
    _query_retries = 0
    _start_time = None
    _metrics = None
    _paging_state = None

    def __init__(self, session, message, query, default_timeout=None, metrics=None, prepared_statement=None):
        self.session = session
        self.row_factory = session.row_factory
        self.message = message
        self.query = query
        self.default_timeout = default_timeout
        self._metrics = metrics
        self.prepared_statement = prepared_statement
        self._callback_lock = Lock()
        if metrics is not None:
            self._start_time = time.time()
        self._make_query_plan()
        self._event = Event()
        self._errors = {}
        self._callbacks = []
        self._errbacks = []

    def _make_query_plan(self):
        # convert the list/generator/etc to an iterator so that subsequent
        # calls to send_request (which retries may do) will resume where
        # they last left off
        self.query_plan = iter(self.session._load_balancer.make_query_plan(
            self.session.keyspace, self.query))

    def send_request(self):
        """ Internal """
        # query_plan is an iterator, so this will resume where we last left
        # off if send_request() is called multiple times
        for host in self.query_plan:
            req_id = self._query(host)
            if req_id is not None:
                self._req_id = req_id
                return

        self._set_final_exception(NoHostAvailable(
            "Unable to complete the operation against any hosts", self._errors))

    def _query(self, host, message=None, cb=None):
        if message is None:
            message = self.message

        if cb is None:
            cb = self._set_result

        pool = self.session._pools.get(host)
        if not pool:
            self._errors[host] = ConnectionException("Host has been marked down or removed")
            return None
        elif pool.is_shutdown:
            self._errors[host] = ConnectionException("Pool is shutdown")
            return None

        self._current_host = host
        self._current_pool = pool

        connection = None
        try:
            # TODO get connectTimeout from cluster settings
            connection, request_id = pool.borrow_connection(timeout=2.0)
            self._connection = connection
            connection.send_msg(message, request_id, cb=cb)
            return request_id
        except NoConnectionsAvailable as exc:
            log.debug("All connections for host %s are at capacity, moving to the next host", host)
            self._errors[host] = exc
            return None
        except Exception as exc:
            log.debug("Error querying host %s", host, exc_info=True)
            self._errors[host] = exc
            if self._metrics is not None:
                self._metrics.on_connection_error()
            if connection:
                pool.return_connection(connection)
            return None

    @property
    def has_more_pages(self):
        """
        Returns :const:`True` if there are more pages left in the
        query results, :const:`False` otherwise.  This should only
        be checked after the first page has been returned.

        .. versionadded:: 2.0.0
        """
        return self._paging_state is not None

    def start_fetching_next_page(self):
        """
        If there are more pages left in the query result, this asynchronously
        starts fetching the next page.  If there are no pages left, :exc:`.QueryExhausted`
        is raised.  Also see :attr:`.has_more_pages`.

        This should only be called after the first page has been returned.

        .. versionadded:: 2.0.0
        """
        if not self._paging_state:
            raise QueryExhausted()

        self._make_query_plan()
        self.message.paging_state = self._paging_state
        self._event.clear()
        self._final_result = _NOT_SET
        self._final_exception = None
        self.send_request()

    def _reprepare(self, prepare_message):
        cb = partial(self.session.submit, self._execute_after_prepare)
        request_id = self._query(self._current_host, prepare_message, cb=cb)
        if request_id is None:
            # try to submit the original prepared statement on some other host
            self.send_request()

    def _set_result(self, response):
        try:
            if self._current_pool and self._connection:
                self._current_pool.return_connection(self._connection)

            trace_id = getattr(response, 'trace_id', None)
            if trace_id:
                self._query_trace = QueryTrace(trace_id, self.session)

            if isinstance(response, ResultMessage):
                if response.kind == RESULT_KIND_SET_KEYSPACE:
                    session = getattr(self, 'session', None)
                    # since we're running on the event loop thread, we need to
                    # use a non-blocking method for setting the keyspace on
                    # all connections in this session, otherwise the event
                    # loop thread will deadlock waiting for keyspaces to be
                    # set.  This uses a callback chain which ends with
                    # self._set_keyspace_completed() being called in the
                    # event loop thread.
                    if session:
                        session._set_keyspace_for_all_pools(
                            response.results, self._set_keyspace_completed)
                elif response.kind == RESULT_KIND_SCHEMA_CHANGE:
                    # refresh the schema before responding, but do it in another
                    # thread instead of the event loop thread
                    self.session.submit(
                        refresh_schema_and_set_result,
                        response.results['keyspace'],
                        response.results.get('table'),
                        response.results.get('type'),
                        self.session.cluster.control_connection,
                        self)
                else:
                    results = getattr(response, 'results', None)
                    if results is not None and response.kind == RESULT_KIND_ROWS:
                        self._paging_state = response.paging_state
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
                    log.warning("Host %s is overloaded, retrying against a different "
                                "host", self._current_host)
                    self._retry(reuse_connection=False, consistency_level=None)
                    return
                elif isinstance(response, IsBootstrappingErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_other_error()
                    # need to retry against a different host here
                    self._retry(reuse_connection=False, consistency_level=None)
                    return
                elif isinstance(response, PreparedQueryNotFound):
                    if self.prepared_statement:
                        query_id = self.prepared_statement.query_id
                        assert query_id == response.info, \
                            "Got different query ID in server response (%s) than we " \
                            "had before (%s)" % (response.info, query_id)
                    else:
                        query_id = response.info

                    try:
                        prepared_statement = self.session.cluster._prepared_statements[query_id]
                    except KeyError:
                        if not self.prepared_statement:
                            log.error("Tried to execute unknown prepared statement: id=%s",
                                      query_id.encode('hex'))
                            self._set_final_exception(response)
                            return
                        else:
                            prepared_statement = self.prepared_statement
                            self.session.cluster._prepared_statements[query_id] = prepared_statement

                    current_keyspace = self._connection.keyspace
                    prepared_keyspace = prepared_statement.keyspace
                    if prepared_keyspace and current_keyspace != prepared_keyspace:
                        self._set_final_exception(
                            ValueError("The Session's current keyspace (%s) does "
                                       "not match the keyspace the statement was "
                                       "prepared with (%s)" %
                                       (current_keyspace, prepared_keyspace)))
                        return

                    log.debug("Re-preparing unrecognized prepared statement against host %s: %s",
                              self._current_host, prepared_statement.query_string)
                    prepare_message = PrepareMessage(query=prepared_statement.query_string)
                    # since this might block, run on the executor to avoid hanging
                    # the event loop thread
                    self.session.submit(self._reprepare, prepare_message)
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

    def _set_keyspace_completed(self, errors):
        if not errors:
            self._set_final_result(None)
        else:
            self._set_final_exception(ConnectionException(
                "Failed to set keyspace on all hosts: %s" % (errors,)))

    def _execute_after_prepare(self, response):
        """
        Handle the response to our attempt to prepare a statement.
        If it succeeded, run the original query again against the same host.
        """
        if self._current_pool and self._connection:
            self._current_pool.return_connection(self._connection)

        if self._final_exception:
            return

        if isinstance(response, ResultMessage):
            if response.kind == RESULT_KIND_PREPARED:
                # use self._query to re-use the same host and
                # at the same time properly borrow the connection
                request_id = self._query(self._current_host)
                if request_id is None:
                    # this host errored out, move on to the next
                    self.send_request()
            else:
                self._set_final_exception(ConnectionException(
                    "Got unexpected response when preparing statement "
                    "on host %s: %s" % (self._current_host, response)))
        elif isinstance(response, ErrorMessage):
            self._set_final_exception(response)
        elif isinstance(response, ConnectionException):
            log.debug("Connection error when preparing statement on host %s: %s",
                      self._current_host, response)
            # try again on a different host, preparing again if necessary
            self._errors[self._current_host] = response
            self.send_request()
        else:
            self._set_final_exception(ConnectionException(
                "Got unexpected response type when preparing "
                "statement on host %s: %s" % (self._current_host, response)))

    def _set_final_result(self, response):
        if self._metrics is not None:
            self._metrics.request_timer.addValue(time.time() - self._start_time)

        with self._callback_lock:
            self._final_result = response

        self._event.set()

        # apply each callback
        for callback in self._callbacks:
            fn, args, kwargs = callback
            fn(response, *args, **kwargs)

    def _set_final_exception(self, response):
        if self._metrics is not None:
            self._metrics.request_timer.addValue(time.time() - self._start_time)

        with self._callback_lock:
            self._final_exception = response
        self._event.set()

        for errback in self._errbacks:
            fn, args, kwargs = errback
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

        if reuse_connection and self._query(self._current_host) is not None:
            return

        # otherwise, move onto another host
        self.send_request()

    def result(self, timeout=_NOT_SET):
        """
        Return the final result or raise an Exception if errors were
        encountered.  If the final result or error has not been set
        yet, this method will block until that time.

        You may set a timeout (in seconds) with the `timeout` parameter.
        By default, the :attr:`~.default_timeout` for the :class:`.Session`
        this was created through will be used for the timeout on this
        operation.

        This timeout applies to the entire request, including any retries
        (decided internally by the :class:`.policies.RetryPolicy` used with
        the request).

        If the timeout is exceeded, an :exc:`cassandra.OperationTimedOut` will be raised.
        This is a client-side timeout. For more information
        about server-side coordinator timeouts, see :class:`.policies.RetryPolicy`.

        **Important**: This timeout currently has no effect on callbacks registered
        on a :class:`~.ResponseFuture` through :meth:`.ResponseFuture.add_callback` or
        :meth:`.ResponseFuture.add_errback`; even if a query exceeds this default
        timeout, neither the registered callback or errback will be called.

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
        if timeout is _NOT_SET:
            timeout = self.default_timeout

        if self._final_result is not _NOT_SET:
            if self._paging_state is None:
                return self._final_result
            else:
                return PagedResult(self, self._final_result, timeout)
        elif self._final_exception:
            raise self._final_exception
        else:
            self._event.wait(timeout=timeout)
            if self._final_result is not _NOT_SET:
                if self._paging_state is None:
                    return self._final_result
                else:
                    return PagedResult(self, self._final_result, timeout)
            elif self._final_exception:
                raise self._final_exception
            else:
                raise OperationTimedOut(errors=self._errors, last_host=self._current_host)

    def get_query_trace(self, max_wait=None):
        """
        Returns the :class:`~.query.QueryTrace` instance representing a trace
        of the last attempt for this operation, or :const:`None` if tracing was
        not enabled for this query.  Note that this may raise an exception if
        there are problems retrieving the trace details from Cassandra. If the
        trace is not available after `max_wait` seconds,
        :exc:`cassandra.query.TraceUnavailable` will be raised.
        """
        if not self._query_trace:
            return None

        self._query_trace.populate(max_wait)
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

        **Important**: if the callback you attach results in an exception being
        raised, **the exception will be ignored**, so please ensure your
        callback handles all error cases that you care about.

        Usage example::

            >>> session = cluster.connect("mykeyspace")

            >>> def handle_results(rows, start_time, should_log=False):
            ...     if should_log:
            ...         log.info("Total time: %f", time.time() - start_time)
            ...     ...

            >>> future = session.execute_async("SELECT * FROM users")
            >>> future.add_callback(handle_results, time.time(), should_log=True)

        """
        run_now = False
        with self._callback_lock:
            if self._final_result is not _NOT_SET:
                run_now = True
            else:
                self._callbacks.append((fn, args, kwargs))
        if run_now:
            fn(self._final_result, *args, **kwargs)
        return self

    def add_errback(self, fn, *args, **kwargs):
        """
        Like :meth:`.add_callback()`, but handles error cases.
        An Exception instance will be passed as the first positional argument
        to `fn`.
        """
        run_now = False
        with self._callback_lock:
            if self._final_exception:
                run_now = True
            else:
                self._errbacks.append((fn, args, kwargs))
        if run_now:
            fn(self._final_exception, *args, **kwargs)
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

    def clear_callbacks(self):
        with self._callback_lock:
            self._callback = []
            self._errback = []

    def __str__(self):
        result = "(no result yet)" if self._final_result is _NOT_SET else self._final_result
        return "<ResponseFuture: query='%s' request_id=%s result=%s exception=%s host=%s>" \
               % (self.query, self._req_id, result, self._final_exception, self._current_host)
    __repr__ = __str__


class QueryExhausted(Exception):
    """
    Raised when :meth:`.ResponseFuture.start_fetching_next_page()` is called and
    there are no more pages.  You can check :attr:`.ResponseFuture.has_more_pages`
    before calling to avoid this.

    .. versionadded:: 2.0.0
    """
    pass


class PagedResult(object):
    """
    An iterator over the rows from a paged query result.  Whenever the number
    of result rows for a query exceed the :attr:`~.query.Statement.fetch_size`
    (or :attr:`~.Session.default_fetch_size`, if not set) an instance of this
    class will be returned.

    You can treat this as a normal iterator over rows::

        >>> from cassandra.query import SimpleStatement
        >>> statement = SimpleStatement("SELECT * FROM users", fetch_size=10)
        >>> for user_row in session.execute(statement):
        ...     process_user(user_row)

    Whenever there are no more rows in the current page, the next page will
    be fetched transparently.  However, note that it *is* possible for
    an :class:`Exception` to be raised while fetching the next page, just
    like you might see on a normal call to ``session.execute()``.

    .. versionadded: 2.0.0
    """

    response_future = None

    def __init__(self, response_future, initial_response, timeout=_NOT_SET):
        self.response_future = response_future
        self.current_response = iter(initial_response)
        self.timeout = timeout

    def __iter__(self):
        return self

    def next(self):
        try:
            return next(self.current_response)
        except StopIteration:
            if not self.response_future.has_more_pages:
                raise

        self.response_future.start_fetching_next_page()
        result = self.response_future.result(self.timeout)
        if self.response_future.has_more_pages:
            self.current_response = result.current_response
        else:
            self.current_response = iter(result)

        return next(self.current_response)

    __next__ = next
