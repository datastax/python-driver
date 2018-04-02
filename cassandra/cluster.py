# Copyright DataStax, Inc.
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
from collections import defaultdict, Mapping
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait as wait_futures
from copy import copy
from functools import partial, wraps
from itertools import groupby, count
import logging
from warnings import warn
from random import random
import six
from six.moves import filter, range, queue as Queue
import socket
import sys
import time
from threading import Lock, RLock, Thread, Event

import weakref
from weakref import WeakValueDictionary
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import (ConsistencyLevel, AuthenticationFailed,
                       OperationTimedOut, UnsupportedOperation,
                       SchemaTargetType, DriverException, ProtocolVersion)
from cassandra.connection import (ConnectionException, ConnectionShutdown,
                                  ConnectionHeartbeat, ProtocolVersionUnsupported)
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
                                RESULT_KIND_SCHEMA_CHANGE, ProtocolHandler)
from cassandra.metadata import Metadata, protect_name, murmur3
from cassandra.policies import (TokenAwarePolicy, DCAwareRoundRobinPolicy, SimpleConvictionPolicy,
                                ExponentialReconnectionPolicy, HostDistance,
                                RetryPolicy, IdentityTranslator, NoSpeculativeExecutionPlan,
                                NoSpeculativeExecutionPolicy)
from cassandra.pool import (Host, _ReconnectionHandler, _HostReconnectionHandler,
                            HostConnectionPool, HostConnection,
                            NoConnectionsAvailable)
from cassandra.query import (SimpleStatement, PreparedStatement, BoundStatement,
                             BatchStatement, bind_params, QueryTrace, TraceUnavailable,
                             named_tuple_factory, dict_factory, tuple_factory, FETCH_SIZE_UNSET)
from cassandra.timestamps import MonotonicTimestampGenerator


def _is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('socket')


def _is_gevent_monkey_patched():
    if 'gevent.monkey' not in sys.modules:
        return False
    import gevent.socket
    return socket.socket is gevent.socket.socket

# default to gevent when we are monkey patched with gevent, eventlet when
# monkey patched with eventlet, otherwise if libev is available, use that as
# the default because it's fastest. Otherwise, use asyncore.
if _is_gevent_monkey_patched():
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


_clusters_for_shutdown = set()


def _register_cluster_shutdown(cluster):
    _clusters_for_shutdown.add(cluster)


def _discard_cluster_shutdown(cluster):
    _clusters_for_shutdown.discard(cluster)


def _shutdown_clusters():
    clusters = _clusters_for_shutdown.copy()  # copy because shutdown modifies the global set "discard"
    for cluster in clusters:
        cluster.shutdown()

atexit.register(_shutdown_clusters)


def default_lbp_factory():
    if murmur3 is not None:
        return TokenAwarePolicy(DCAwareRoundRobinPolicy())
    return DCAwareRoundRobinPolicy()


class ExecutionProfile(object):
    load_balancing_policy = None
    """
    An instance of :class:`.policies.LoadBalancingPolicy` or one of its subclasses.

    Used in determining host distance for establishing connections, and routing requests.

    Defaults to ``TokenAwarePolicy(DCAwareRoundRobinPolicy())`` if not specified
    """

    retry_policy = None
    """
    An instance of :class:`.policies.RetryPolicy` instance used when :class:`.Statement` objects do not have a
    :attr:`~.Statement.retry_policy` explicitly set.

    Defaults to :class:`.RetryPolicy` if not specified
    """

    consistency_level = ConsistencyLevel.LOCAL_ONE
    """
    :class:`.ConsistencyLevel` used when not specified on a :class:`.Statement`.
    """

    serial_consistency_level = None
    """
    Serial :class:`.ConsistencyLevel` used when not specified on a :class:`.Statement` (for LWT conditional statements).
    """

    request_timeout = 10.0
    """
    Request timeout used when not overridden in :meth:`.Session.execute`
    """

    row_factory = staticmethod(tuple_factory)
    """
    A callable to format results, accepting ``(colnames, rows)`` where ``colnames`` is a list of column names, and
    ``rows`` is a list of tuples, with each tuple representing a row of parsed values.

    Some example implementations:

    - :func:`cassandra.query.tuple_factory` - return a result row as a tuple
    - :func:`cassandra.query.named_tuple_factory` - return a result row as a named tuple
    - :func:`cassandra.query.dict_factory` - return a result row as a dict
    - :func:`cassandra.query.ordered_dict_factory` - return a result row as an OrderedDict
    """

    speculative_execution_policy = None
    """
    An instance of :class:`.policies.SpeculativeExecutionPolicy`

    Defaults to :class:`.NoSpeculativeExecutionPolicy` if not specified
    """

    # indicates if lbp was set explicitly or uses default values
    _load_balancing_policy_explicit = False

    def __init__(self, load_balancing_policy=_NOT_SET, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=10.0, row_factory=named_tuple_factory, speculative_execution_policy=None):

        if load_balancing_policy is _NOT_SET:
            self._load_balancing_policy_explicit = False
            self.load_balancing_policy = default_lbp_factory()
        else:
            self._load_balancing_policy_explicit = True
            self.load_balancing_policy = load_balancing_policy
        self.retry_policy = retry_policy or RetryPolicy()
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.request_timeout = request_timeout
        self.row_factory = row_factory
        self.speculative_execution_policy = speculative_execution_policy or NoSpeculativeExecutionPolicy()


class ProfileManager(object):

    def __init__(self):
        self.profiles = dict()

    def _profiles_without_explicit_lbps(self):
        names = (profile_name for
                 profile_name, profile in self.profiles.items()
                 if not profile._load_balancing_policy_explicit)
        return tuple(
            'EXEC_PROFILE_DEFAULT' if n is EXEC_PROFILE_DEFAULT else n
            for n in names
        )

    def distance(self, host):
        distances = set(p.load_balancing_policy.distance(host) for p in self.profiles.values())
        return HostDistance.LOCAL if HostDistance.LOCAL in distances else \
            HostDistance.REMOTE if HostDistance.REMOTE in distances else \
            HostDistance.IGNORED

    def populate(self, cluster, hosts):
        for p in self.profiles.values():
            p.load_balancing_policy.populate(cluster, hosts)

    def check_supported(self):
        for p in self.profiles.values():
            p.load_balancing_policy.check_supported()

    def on_up(self, host):
        for p in self.profiles.values():
            p.load_balancing_policy.on_up(host)

    def on_down(self, host):
        for p in self.profiles.values():
            p.load_balancing_policy.on_down(host)

    def on_add(self, host):
        for p in self.profiles.values():
            p.load_balancing_policy.on_add(host)

    def on_remove(self, host):
        for p in self.profiles.values():
            p.load_balancing_policy.on_remove(host)

    @property
    def default(self):
        """
        internal-only; no checks are done because this entry is populated on cluster init
        """
        return self.profiles[EXEC_PROFILE_DEFAULT]


EXEC_PROFILE_DEFAULT = object()
"""
Key for the ``Cluster`` default execution profile, used when no other profile is selected in
``Session.execute(execution_profile)``.

Use this as the key in ``Cluster(execution_profiles)`` to override the default profile.
"""


class _ConfigMode(object):
    UNCOMMITTED = 0
    LEGACY = 1
    PROFILES = 2


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

    ``Cluster`` and ``Session`` also provide context management functions
    which implicitly handle shutdown when leaving scope.
    """

    contact_points = ['127.0.0.1']
    """
    The list of contact points to try connecting for cluster discovery.

    Defaults to loopback interface.

    Note: When using :class:`.DCAwareLoadBalancingPolicy` with no explicit
    local_dc set (as is the default), the DC is chosen from an arbitrary
    host in contact_points. In this case, contact_points should contain
    only nodes from a single, local DC.

    Note: In the next major version, if you specify contact points, you will
    also be required to also explicitly specify a load-balancing policy. This
    change will help prevent cases where users had hard-to-debug issues
    surrounding unintuitive default load-balancing policy behavior.
    """
    # tracks if contact_points was set explicitly or with default values
    _contact_points_explicit = None

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

    protocol_version = ProtocolVersion.V4
    """
    The maximum version of the native protocol to use.

    See :class:`.ProtocolVersion` for more information about versions.

    If not set in the constructor, the driver will automatically downgrade
    version based on a negotiation with the server, but it is most efficient
    to set this to the maximum supported by your version of Cassandra.
    Setting this will also prevent conflicting versions negotiated if your
    cluster is upgraded.

    """

    allow_beta_protocol_version = False

    no_compact = False

    """
    Setting true injects a flag in all messages that makes the server accept and use "beta" protocol version.
    Used for testing new protocol features incrementally before the new version is complete.
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

    _load_balancing_policy = None
    @property
    def load_balancing_policy(self):
        """
        An instance of :class:`.policies.LoadBalancingPolicy` or
        one of its subclasses.

        .. versionchanged:: 2.6.0

        Defaults to :class:`~.TokenAwarePolicy` (:class:`~.DCAwareRoundRobinPolicy`).
        when using CPython (where the murmur3 extension is available). :class:`~.DCAwareRoundRobinPolicy`
        otherwise. Default local DC will be chosen from contact points.

        **Please see** :class:`~.DCAwareRoundRobinPolicy` **for a discussion on default behavior with respect to
        DC locality and remote nodes.**
        """
        return self._load_balancing_policy

    @load_balancing_policy.setter
    def load_balancing_policy(self, lbp):
        if self._config_mode == _ConfigMode.PROFILES:
            raise ValueError("Cannot set Cluster.load_balancing_policy while using Configuration Profiles. Set this in a profile instead.")
        self._load_balancing_policy = lbp
        self._config_mode = _ConfigMode.LEGACY

    @property
    def _default_load_balancing_policy(self):
        return self.profile_manager.default.load_balancing_policy

    reconnection_policy = ExponentialReconnectionPolicy(1.0, 600.0)
    """
    An instance of :class:`.policies.ReconnectionPolicy`. Defaults to an instance
    of :class:`.ExponentialReconnectionPolicy` with a base delay of one second and
    a max delay of ten minutes.
    """

    _default_retry_policy = RetryPolicy()
    @property
    def default_retry_policy(self):
        """
        A default :class:`.policies.RetryPolicy` instance to use for all
        :class:`.Statement` objects which do not have a :attr:`~.Statement.retry_policy`
        explicitly set.
        """
        return self._default_retry_policy

    @default_retry_policy.setter
    def default_retry_policy(self, policy):
        if self._config_mode == _ConfigMode.PROFILES:
            raise ValueError("Cannot set Cluster.default_retry_policy while using Configuration Profiles. Set this in a profile instead.")
        self._default_retry_policy = policy
        self._config_mode = _ConfigMode.LEGACY

    conviction_policy_factory = SimpleConvictionPolicy
    """
    A factory function which creates instances of
    :class:`.policies.ConvictionPolicy`.  Defaults to
    :class:`.policies.SimpleConvictionPolicy`.
    """

    address_translator = IdentityTranslator()
    """
    :class:`.policies.AddressTranslator` instance to be used in translating server node addresses
    to driver connection addresses.
    """

    connect_to_remote_hosts = True
    """
    If left as :const:`True`, hosts that are considered :attr:`~.HostDistance.REMOTE`
    by the :attr:`~.Cluster.load_balancing_policy` will have a connection
    opened to them.  Otherwise, they will not have a connection opened to them.

    Note that the default load balancing policy ignores remote hosts by default.

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

    .. versionchanged:: 3.3.0

    In addition to ``wrap_socket`` kwargs, clients may also specify ``'check_hostname': True`` to verify the cert hostname
    as outlined in RFC 2818 and RFC 6125. Note that this requires the certificate to be transferred, so
    should almost always require the option ``'cert_reqs': ssl.CERT_REQUIRED``. Note also that this functionality was not built into
    Python standard library until (2.7.9, 3.2). To enable this mechanism in earlier versions, patch ``ssl.match_hostname``
    with a custom or `back-ported function <https://pypi.python.org/pypi/backports.ssl_match_hostname>`_.
    """

    sockopts = None
    """
    An optional list of tuples which will be used as arguments to
    ``socket.setsockopt()`` for all created sockets.

    Note: some drivers find setting TCPNODELAY beneficial in the context of
    their execution model. It was not found generally beneficial for this driver.
    To try with your own workload, set ``sockopts = [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]``
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
    * :class:`cassandra.io.eventletreactor.EventletConnection` (requires monkey-patching - see doc for details)
    * :class:`cassandra.io.geventreactor.GeventConnection` (requires monkey-patching - see doc for details)
    * :class:`cassandra.io.twistedreactor.TwistedConnection`
    * EXPERIMENTAL: :class:`cassandra.io.asyncioreactor.AsyncioConnection`

    By default, ``AsyncoreConnection`` will be used, which uses
    the ``asyncore`` module in the Python standard library.

    If ``libev`` is installed, ``LibevConnection`` will be used instead.

    If ``gevent`` or ``eventlet`` monkey-patching is detected, the corresponding
    connection class will be used automatically.

    ``AsyncioConnection``, which uses the ``asyncio`` module in the Python
    standard library, is also available, but currently experimental. Note that
    it requires ``asyncio`` features that were only introduced in the 3.4 line
    in 3.4.6, and in the 3.5 line in 3.5.1.
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

    idle_heartbeat_timeout = 30
    """
    Timeout, in seconds, on which the heartbeat wait for idle connection responses.
    Lowering this value can help to discover bad connections earlier.
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

    Setting this negative will disable node refreshes in response to push events.

    See :attr:`.schema_event_refresh_window` for discussion of rationale
    """

    status_event_refresh_window = 2
    """
    Window, in seconds, within which the driver will start the reconnect after
    receiving a status_change event.

    Setting this to zero will connect immediately.

    This is primarily used to avoid 'thundering herd' in deployments with large fanout from cluster to clients.
    When nodes come up, clients attempt to reprepare prepared statements (depending on :attr:`.reprepare_on_up`), and
    establish connection pools. This can cause a rush of connections and queries if not mitigated with this factor.
    """

    prepare_on_all_hosts = True
    """
    Specifies whether statements should be prepared on all hosts, or just one.

    This can reasonably be disabled on long-running applications with numerous clients preparing statements on startup,
    where a randomized initial condition of the load balancing policy can be expected to distribute prepares from
    different clients across the cluster.
    """

    reprepare_on_up = True
    """
    Specifies whether all known prepared statements should be prepared on a node when it comes up.

    May be used to avoid overwhelming a node on return, or if it is supposed that the node was only marked down due to
    network. If statements are not reprepared, they are prepared on the first execution, causing
    an extra roundtrip for one or more client requests.
    """

    connect_timeout = 5
    """
    Timeout, in seconds, for creating new connections.

    This timeout covers the entire connection negotiation, including TCP
    establishment, options passing, and authentication.
    """

    timestamp_generator = None
    """
    An object, shared between all sessions created by this cluster instance,
    that generates timestamps when client-side timestamp generation is enabled.
    By default, each :class:`Cluster` uses a new
    :class:`~.MonotonicTimestampGenerator`.

    Applications can set this value for custom timestamp behavior. See the
    documentation for :meth:`Session.timestamp_generator`.
    """

    @property
    def schema_metadata_enabled(self):
        """
        Flag indicating whether internal schema metadata is updated.

        When disabled, the driver does not populate Cluster.metadata.keyspaces on connect, or on schema change events. This
        can be used to speed initial connection, and reduce load on client and server during operation. Turning this off
        gives away token aware request routing, and programmatic inspection of the metadata model.
        """
        return self.control_connection._schema_meta_enabled

    @schema_metadata_enabled.setter
    def schema_metadata_enabled(self, enabled):
        self.control_connection._schema_meta_enabled = bool(enabled)

    @property
    def token_metadata_enabled(self):
        """
        Flag indicating whether internal token metadata is updated.

        When disabled, the driver does not query node token information on connect, or on topology change events. This
        can be used to speed initial connection, and reduce load on client and server during operation. It is most useful
        in large clusters using vnodes, where the token map can be expensive to compute. Turning this off
        gives away token aware request routing, and programmatic inspection of the token ring.
        """
        return self.control_connection._token_meta_enabled

    @token_metadata_enabled.setter
    def token_metadata_enabled(self, enabled):
        self.control_connection._token_meta_enabled = bool(enabled)

    profile_manager = None
    _config_mode = _ConfigMode.UNCOMMITTED

    sessions = None
    control_connection = None
    scheduler = None
    executor = None
    is_shutdown = False
    _is_setup = False
    _prepared_statements = None
    _prepared_statement_lock = None
    _idle_heartbeat = None
    _protocol_version_explicit = False
    _discount_down_events = True

    _user_types = None
    """
    A map of {keyspace: {type_name: UserType}}
    """

    _listeners = None
    _listener_lock = None

    def __init__(self,
                 contact_points=_NOT_SET,
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
                 protocol_version=_NOT_SET,
                 executor_threads=2,
                 max_schema_agreement_wait=10,
                 control_connection_timeout=2.0,
                 idle_heartbeat_interval=30,
                 schema_event_refresh_window=2,
                 topology_event_refresh_window=10,
                 connect_timeout=5,
                 schema_metadata_enabled=True,
                 token_metadata_enabled=True,
                 address_translator=None,
                 status_event_refresh_window=2,
                 prepare_on_all_hosts=True,
                 reprepare_on_up=True,
                 execution_profiles=None,
                 allow_beta_protocol_version=False,
                 timestamp_generator=None,
                 idle_heartbeat_timeout=30,
                 no_compact=False):
        """
        ``executor_threads`` defines the number of threads in a pool for handling asynchronous tasks such as
        extablishing connection pools or refreshing metadata.

        Any of the mutable Cluster attributes may be set as keyword arguments to the constructor.
        """
        if contact_points is not None:
            if contact_points is _NOT_SET:
                self._contact_points_explicit = False
                contact_points = ['127.0.0.1']
            else:
                self._contact_points_explicit = True

            if isinstance(contact_points, six.string_types):
                raise TypeError("contact_points should not be a string, it should be a sequence (e.g. list) of strings")

            if None in contact_points:
                raise ValueError("contact_points should not contain None (it can resolve to localhost)")
            self.contact_points = contact_points

        self.port = port

        self.contact_points_resolved = [endpoint[4][0] for a in self.contact_points
                                        for endpoint in socket.getaddrinfo(a, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM)]

        self.compression = compression

        if protocol_version is not _NOT_SET:
            self.protocol_version = protocol_version
            self._protocol_version_explicit = True
        self.allow_beta_protocol_version = allow_beta_protocol_version

        self.no_compact = no_compact

        self.auth_provider = auth_provider

        if load_balancing_policy is not None:
            if isinstance(load_balancing_policy, type):
                raise TypeError("load_balancing_policy should not be a class, it should be an instance of that class")
            self.load_balancing_policy = load_balancing_policy
        else:
            self._load_balancing_policy = default_lbp_factory()  # set internal attribute to avoid committing to legacy config mode

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

        if address_translator is not None:
            if isinstance(address_translator, type):
                raise TypeError("address_translator should not be a class, it should be an instance of that class")
            self.address_translator = address_translator

        if connection_class is not None:
            self.connection_class = connection_class

        if timestamp_generator is not None:
            if not callable(timestamp_generator):
                raise ValueError("timestamp_generator must be callable")
            self.timestamp_generator = timestamp_generator
        else:
            self.timestamp_generator = MonotonicTimestampGenerator()

        self.profile_manager = ProfileManager()
        self.profile_manager.profiles[EXEC_PROFILE_DEFAULT] = ExecutionProfile(self.load_balancing_policy,
                                                                               self.default_retry_policy,
                                                                               Session._default_consistency_level,
                                                                               Session._default_serial_consistency_level,
                                                                               Session._default_timeout,
                                                                               Session._row_factory)
        # legacy mode if either of these is not default
        if load_balancing_policy or default_retry_policy:
            if execution_profiles:
                raise ValueError("Clusters constructed with execution_profiles should not specify legacy parameters "
                                 "load_balancing_policy or default_retry_policy. Configure this in a profile instead.")

            self._config_mode = _ConfigMode.LEGACY
            warn("Legacy execution parameters will be removed in 4.0. Consider using "
                 "execution profiles.", DeprecationWarning)

        else:
            if execution_profiles:
                self.profile_manager.profiles.update(execution_profiles)
                self._config_mode = _ConfigMode.PROFILES

        if self._contact_points_explicit:
            if self._config_mode is _ConfigMode.PROFILES:
                default_lbp_profiles = self.profile_manager._profiles_without_explicit_lbps()
                if default_lbp_profiles:
                    log.warning(
                        'Cluster.__init__ called with contact_points '
                        'specified, but load-balancing policies are not '
                        'specified in some ExecutionProfiles. In the next '
                        'major version, this will raise an error; please '
                        'specify a load-balancing policy. '
                        '(contact_points = {cp}, '
                        'EPs without explicit LBPs = {eps})'
                        ''.format(cp=contact_points, eps=default_lbp_profiles))
            else:
                if load_balancing_policy is None:
                    log.warning(
                        'Cluster.__init__ called with contact_points '
                        'specified, but no load_balancing_policy. In the next '
                        'major version, this will raise an error; please '
                        'specify a load-balancing policy. '
                        '(contact_points = {cp}, lbp = {lbp})'
                        ''.format(cp=contact_points, lbp=load_balancing_policy))

        self.metrics_enabled = metrics_enabled
        self.ssl_options = ssl_options
        self.sockopts = sockopts
        self.cql_version = cql_version
        self.max_schema_agreement_wait = max_schema_agreement_wait
        self.control_connection_timeout = control_connection_timeout
        self.idle_heartbeat_interval = idle_heartbeat_interval
        self.idle_heartbeat_timeout = idle_heartbeat_timeout
        self.schema_event_refresh_window = schema_event_refresh_window
        self.topology_event_refresh_window = topology_event_refresh_window
        self.status_event_refresh_window = status_event_refresh_window
        self.connect_timeout = connect_timeout
        self.prepare_on_all_hosts = prepare_on_all_hosts
        self.reprepare_on_up = reprepare_on_up

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
            self.schema_event_refresh_window, self.topology_event_refresh_window,
            self.status_event_refresh_window,
            schema_metadata_enabled, token_metadata_enabled)

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
        if self.protocol_version < 3:
            log.warning("User Type serialization is only supported in native protocol version 3+ (%d in use). "
                        "CQL encoding for simple statements will still work, but named tuples will "
                        "be returned when reading type %s.%s.", self.protocol_version, keyspace, user_type)

        self._user_types[keyspace][user_type] = klass
        for session in tuple(self.sessions):
            session.user_type_registered(keyspace, user_type, klass)
        UserType.evict_udt_class(keyspace, user_type)

    def add_execution_profile(self, name, profile, pool_wait_timeout=5):
        """
        Adds an :class:`.ExecutionProfile` to the cluster. This makes it available for use by ``name`` in :meth:`.Session.execute`
        and :meth:`.Session.execute_async`. This method will raise if the profile already exists.

        Normally profiles will be injected at cluster initialization via ``Cluster(execution_profiles)``. This method
        provides a way of adding them dynamically.

        Adding a new profile updates the connection pools according to the specified ``load_balancing_policy``. By default,
        this method will wait up to five seconds for the pool creation to complete, so the profile can be used immediately
        upon return. This behavior can be controlled using ``pool_wait_timeout`` (see
        `concurrent.futures.wait <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait>`_
        for timeout semantics).
        """
        if not isinstance(profile, ExecutionProfile):
            raise TypeError("profile must be an instance of ExecutionProfile")
        if self._config_mode == _ConfigMode.LEGACY:
            raise ValueError("Cannot add execution profiles when legacy parameters are set explicitly.")
        if name in self.profile_manager.profiles:
            raise ValueError("Profile %s already exists")
        contact_points_but_no_lbp = (
            self._contact_points_explicit and not
            profile._load_balancing_policy_explicit)
        if contact_points_but_no_lbp:
            log.warning(
                'Tried to add an ExecutionProfile with name {name}. '
                '{self} was explicitly configured with contact_points, but '
                '{ep} was not explicitly configured with a '
                'load_balancing_policy. In the next major version, trying to '
                'add an ExecutionProfile without an explicitly configured LBP '
                'to a cluster with explicitly configured contact_points will '
                'raise an exception; please specify a load-balancing policy '
                'in the ExecutionProfile.'
                ''.format(name=repr(name), self=self, ep=profile))

        self.profile_manager.profiles[name] = profile
        profile.load_balancing_policy.populate(self, self.metadata.all_hosts())
        # on_up after populate allows things like DCA LBP to choose default local dc
        for host in filter(lambda h: h.is_up, self.metadata.all_hosts()):
            profile.load_balancing_policy.on_up(host)
        futures = set()
        for session in tuple(self.sessions):
            futures.update(session.update_created_pools())
        _, not_done = wait_futures(futures, pool_wait_timeout)
        if not_done:
            raise OperationTimedOut("Failed to create all new connection pools in the %ss timeout.")


    def get_min_requests_per_connection(self, host_distance):
        return self._min_requests_per_connection[host_distance]

    def set_min_requests_per_connection(self, host_distance, min_requests):
        """
        Sets a threshold for concurrent requests per connection, below which
        connections will be considered for disposal (down to core connections;
        see :meth:`~Cluster.set_core_connections_per_host`).

        Pertains to connection pool management in protocol versions {1,2}.
        """
        if self.protocol_version >= 3:
            raise UnsupportedOperation(
                "Cluster.set_min_requests_per_connection() only has an effect "
                "when using protocol_version 1 or 2.")
        if min_requests < 0 or min_requests > 126 or \
           min_requests >= self._max_requests_per_connection[host_distance]:
            raise ValueError("min_requests must be 0-126 and less than the max_requests for this host_distance (%d)" %
                             (self._min_requests_per_connection[host_distance],))
        self._min_requests_per_connection[host_distance] = min_requests

    def get_max_requests_per_connection(self, host_distance):
        return self._max_requests_per_connection[host_distance]

    def set_max_requests_per_connection(self, host_distance, max_requests):
        """
        Sets a threshold for concurrent requests per connection, above which new
        connections will be created to a host (up to max connections;
        see :meth:`~Cluster.set_max_connections_per_host`).

        Pertains to connection pool management in protocol versions {1,2}.
        """
        if self.protocol_version >= 3:
            raise UnsupportedOperation(
                "Cluster.set_max_requests_per_connection() only has an effect "
                "when using protocol_version 1 or 2.")
        if max_requests < 1 or max_requests > 127 or \
           max_requests <= self._min_requests_per_connection[host_distance]:
            raise ValueError("max_requests must be 1-127 and greater than the min_requests for this host_distance (%d)" %
                             (self._min_requests_per_connection[host_distance],))
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

        Protocol version 1 and 2 are limited in the number of concurrent
        requests they can send per connection. The driver implements connection
        pooling to support higher levels of concurrency.

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
            kwargs_dict.setdefault('authenticator', self._auth_provider_callable(address))

        kwargs_dict.setdefault('port', self.port)
        kwargs_dict.setdefault('compression', self.compression)
        kwargs_dict.setdefault('sockopts', self.sockopts)
        kwargs_dict.setdefault('ssl_options', self.ssl_options)
        kwargs_dict.setdefault('cql_version', self.cql_version)
        kwargs_dict.setdefault('protocol_version', self.protocol_version)
        kwargs_dict.setdefault('user_type_map', self._user_types)
        kwargs_dict.setdefault('allow_beta_protocol_version', self.allow_beta_protocol_version)
        kwargs_dict.setdefault('no_compact', self.no_compact)

        return kwargs_dict

    def protocol_downgrade(self, host_addr, previous_version):
        if self._protocol_version_explicit:
            raise DriverException("ProtocolError returned from server while using explicitly set client protocol_version %d" % (previous_version,))

        new_version = ProtocolVersion.get_lower_supported(previous_version)
        if new_version < ProtocolVersion.MIN_SUPPORTED:
            raise DriverException(
                "Cannot downgrade protocol version below minimum supported version: %d" % (ProtocolVersion.MIN_SUPPORTED,))

        log.warning("Downgrading core protocol version from %d to %d for %s. "
                    "To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. "
                    "http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version", self.protocol_version, new_version, host_addr)
        self.protocol_version = new_version

    def connect(self, keyspace=None, wait_for_all_pools=False):
        """
        Creates and returns a new :class:`~.Session` object.  If `keyspace`
        is specified, that keyspace will be the default keyspace for
        operations on the ``Session``.
        """
        with self._lock:
            if self.is_shutdown:
                raise DriverException("Cluster is already shut down")

            if not self._is_setup:
                log.debug("Connecting to cluster, contact points: %s; protocol version: %s",
                          self.contact_points, self.protocol_version)
                self.connection_class.initialize_reactor()
                _register_cluster_shutdown(self)
                for address in self.contact_points_resolved:
                    host, new = self.add_host(address, signal=False)
                    if new:
                        host.set_up()
                        for listener in self.listeners:
                            listener.on_add(host)

                self.profile_manager.populate(
                    weakref.proxy(self), self.metadata.all_hosts())
                self.load_balancing_policy.populate(
                    weakref.proxy(self), self.metadata.all_hosts()
                )

                try:
                    self.control_connection.connect()

                    # we set all contact points up for connecting, but we won't infer state after this
                    for address in self.contact_points_resolved:
                        h = self.metadata.get_host(address)
                        if h and self.profile_manager.distance(h) == HostDistance.IGNORED:
                            h.is_up = None

                    log.debug("Control connection created")
                except Exception:
                    log.exception("Control connection failed to connect, "
                                  "shutting down Cluster:")
                    self.shutdown()
                    raise

                self.profile_manager.check_supported()  # todo: rename this method

                if self.idle_heartbeat_interval:
                    self._idle_heartbeat = ConnectionHeartbeat(
                        self.idle_heartbeat_interval,
                        self.get_connection_holders,
                        timeout=self.idle_heartbeat_timeout
                    )
                self._is_setup = True

        session = self._new_session(keyspace)
        if wait_for_all_pools:
            wait_futures(session._initial_connect_futures)
        return session

    def get_connection_holders(self):
        holders = []
        for s in tuple(self.sessions):
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

        for session in tuple(self.sessions):
            session.shutdown()

        self.executor.shutdown()

        _discard_cluster_shutdown(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    def _new_session(self, keyspace):
        session = Session(self, self.metadata.all_hosts(), keyspace)
        self._session_register_user_types(session)
        self.sessions.add(session)
        return session

    def _session_register_user_types(self, session):
        for keyspace, type_map in six.iteritems(self._user_types):
            for udt_name, klass in six.iteritems(type_map):
                session.user_type_registered(keyspace, udt_name, klass)

    def _cleanup_failed_on_up_handling(self, host):
        self.profile_manager.on_down(host)
        self.control_connection.on_down(host)
        for session in tuple(self.sessions):
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
        for session in tuple(self.sessions):
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

            if self.profile_manager.distance(host) != HostDistance.IGNORED:
                self._prepare_all_queries(host)
                log.debug("Done preparing all queries for host %s, ", host)

            for session in tuple(self.sessions):
                session.remove_pool(host)

            log.debug("Signalling to load balancing policies that host %s is up", host)
            self.profile_manager.on_up(host)

            log.debug("Signalling to control connection that host %s is up", host)
            self.control_connection.on_up(host)

            log.debug("Attempting to open new connection pools for host %s", host)
            futures_lock = Lock()
            futures_results = []
            callback = partial(self._on_up_future_completed, host, futures, futures_results, futures_lock)
            for session in tuple(self.sessions):
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
                    host.set_up()
                    host._currently_handling_node_up = False

        # for testing purposes
        return futures

    def _start_reconnector(self, host, is_host_addition):
        if self.profile_manager.distance(host) == HostDistance.IGNORED:
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
            was_up = host.is_up

            # ignore down signals if we have open pools to the host
            # this is to avoid closing pools when a control connection host became isolated
            if self._discount_down_events and self.profile_manager.distance(host) != HostDistance.IGNORED:
                connected = False
                for session in tuple(self.sessions):
                    pool_states = session.get_pool_state()
                    pool_state = pool_states.get(host)
                    if pool_state:
                        connected |= pool_state['open_count'] > 0
                if connected:
                    return

            host.set_down()
            if (not was_up and not expect_host_to_be_down) or host.is_currently_reconnecting():
                return

        log.warning("Host %s has been marked down", host)

        self.profile_manager.on_down(host)
        self.control_connection.on_down(host)
        for session in tuple(self.sessions):
            session.on_down(host)

        for listener in self.listeners:
            listener.on_down(host)

        self._start_reconnector(host, is_host_addition)

    def on_add(self, host, refresh_nodes=True):
        if self.is_shutdown:
            return

        log.debug("Handling new host %r and notifying listeners", host)

        distance = self.profile_manager.distance(host)
        if distance != HostDistance.IGNORED:
            self._prepare_all_queries(host)
            log.debug("Done preparing queries for new host %r", host)

        self.profile_manager.on_add(host)
        self.control_connection.on_add(host, refresh_nodes)

        if distance == HostDistance.IGNORED:
            log.debug("Not adding connection pool for new host %r because the "
                      "load balancing policy has marked it as IGNORED", host)
            self._finalize_add(host, set_up=False)
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
        for session in tuple(self.sessions):
            future = session.add_or_renew_pool(host, is_host_addition=True)
            if future is not None:
                have_future = True
                futures.add(future)
                future.add_done_callback(future_completed)

        if not have_future:
            self._finalize_add(host)

    def _finalize_add(self, host, set_up=True):
        if set_up:
            host.set_up()

        for listener in self.listeners:
            listener.on_add(host)

        # see if there are any pools to add or remove now that the host is marked up
        for session in tuple(self.sessions):
            session.update_created_pools()

    def on_remove(self, host):
        if self.is_shutdown:
            return

        log.debug("Removing host %s", host)
        host.set_down()
        self.profile_manager.on_remove(host)
        for session in tuple(self.sessions):
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
        for session in tuple(self.sessions):
            for pool in tuple(session._pools.values()):
                pool.ensure_core_connections()

    @staticmethod
    def _validate_refresh_schema(keyspace, table, usertype, function, aggregate):
        if any((table, usertype, function, aggregate)):
            if not keyspace:
                raise ValueError("keyspace is required to refresh specific sub-entity {table, usertype, function, aggregate}")
            if sum(1 for e in (table, usertype, function) if e) > 1:
                raise ValueError("{table, usertype, function, aggregate} are mutually exclusive")

    @staticmethod
    def _target_type_from_refresh_args(keyspace, table, usertype, function, aggregate):
        if aggregate:
            return SchemaTargetType.AGGREGATE
        elif function:
            return SchemaTargetType.FUNCTION
        elif usertype:
            return SchemaTargetType.TYPE
        elif table:
            return SchemaTargetType.TABLE
        elif keyspace:
            return SchemaTargetType.KEYSPACE
        return None

    def get_control_connection_host(self):
        """
        Returns the control connection host metadata.
        """
        connection = self.control_connection._connection
        host = connection.host if connection else None
        return self.metadata.get_host(host) if host else None

    def refresh_schema_metadata(self, max_schema_agreement_wait=None):
        """
        Synchronously refresh all schema metadata.

        By default, the timeout for this operation is governed by :attr:`~.Cluster.max_schema_agreement_wait`
        and :attr:`~.Cluster.control_connection_timeout`.

        Passing max_schema_agreement_wait here overrides :attr:`~.Cluster.max_schema_agreement_wait`.

        Setting max_schema_agreement_wait <= 0 will bypass schema agreement and refresh schema immediately.

        An Exception is raised if schema refresh fails for any reason.
        """
        if not self.control_connection.refresh_schema(schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("Schema metadata was not refreshed. See log for details.")

    def refresh_keyspace_metadata(self, keyspace, max_schema_agreement_wait=None):
        """
        Synchronously refresh keyspace metadata. This applies to keyspace-level information such as replication
        and durability settings. It does not refresh tables, types, etc. contained in the keyspace.

        See :meth:`~.Cluster.refresh_schema_metadata` for description of ``max_schema_agreement_wait`` behavior
        """
        if not self.control_connection.refresh_schema(target_type=SchemaTargetType.KEYSPACE, keyspace=keyspace,
                                                      schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("Keyspace metadata was not refreshed. See log for details.")

    def refresh_table_metadata(self, keyspace, table, max_schema_agreement_wait=None):
        """
        Synchronously refresh table metadata. This applies to a table, and any triggers or indexes attached
        to the table.

        See :meth:`~.Cluster.refresh_schema_metadata` for description of ``max_schema_agreement_wait`` behavior
        """
        if not self.control_connection.refresh_schema(target_type=SchemaTargetType.TABLE, keyspace=keyspace, table=table,
                                                      schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("Table metadata was not refreshed. See log for details.")

    def refresh_materialized_view_metadata(self, keyspace, view, max_schema_agreement_wait=None):
        """
        Synchronously refresh materialized view metadata.

        See :meth:`~.Cluster.refresh_schema_metadata` for description of ``max_schema_agreement_wait`` behavior
        """
        if not self.control_connection.refresh_schema(target_type=SchemaTargetType.TABLE, keyspace=keyspace, table=view,
                                                      schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("View metadata was not refreshed. See log for details.")

    def refresh_user_type_metadata(self, keyspace, user_type, max_schema_agreement_wait=None):
        """
        Synchronously refresh user defined type metadata.

        See :meth:`~.Cluster.refresh_schema_metadata` for description of ``max_schema_agreement_wait`` behavior
        """
        if not self.control_connection.refresh_schema(target_type=SchemaTargetType.TYPE, keyspace=keyspace, type=user_type,
                                                      schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("User Type metadata was not refreshed. See log for details.")

    def refresh_user_function_metadata(self, keyspace, function, max_schema_agreement_wait=None):
        """
        Synchronously refresh user defined function metadata.

        ``function`` is a :class:`cassandra.UserFunctionDescriptor`.

        See :meth:`~.Cluster.refresh_schema_metadata` for description of ``max_schema_agreement_wait`` behavior
        """
        if not self.control_connection.refresh_schema(target_type=SchemaTargetType.FUNCTION, keyspace=keyspace, function=function,
                                                      schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("User Function metadata was not refreshed. See log for details.")

    def refresh_user_aggregate_metadata(self, keyspace, aggregate, max_schema_agreement_wait=None):
        """
        Synchronously refresh user defined aggregate metadata.

        ``aggregate`` is a :class:`cassandra.UserAggregateDescriptor`.

        See :meth:`~.Cluster.refresh_schema_metadata` for description of ``max_schema_agreement_wait`` behavior
        """
        if not self.control_connection.refresh_schema(target_type=SchemaTargetType.AGGREGATE, keyspace=keyspace, aggregate=aggregate,
                                                      schema_agreement_wait=max_schema_agreement_wait, force=True):
            raise DriverException("User Aggregate metadata was not refreshed. See log for details.")

    def refresh_nodes(self, force_token_rebuild=False):
        """
        Synchronously refresh the node list and token metadata

        `force_token_rebuild` can be used to rebuild the token map metadata, even if no new nodes are discovered.

        An Exception is raised if node refresh fails for any reason.
        """
        if not self.control_connection.refresh_node_list_and_token_map(force_token_rebuild):
            raise DriverException("Node list was not refreshed. See log for details.")

    def set_meta_refresh_enabled(self, enabled):
        """
        *Deprecated:* set :attr:`~.Cluster.schema_metadata_enabled` :attr:`~.Cluster.token_metadata_enabled` instead

        Sets a flag to enable (True) or disable (False) all metadata refresh queries.
        This applies to both schema and node topology.

        Disabling this is useful to minimize refreshes during multiple changes.

        Meta refresh must be enabled for the driver to become aware of any cluster
        topology changes or schema updates.
        """
        warn("Cluster.set_meta_refresh_enabled is deprecated and will be removed in 4.0. Set "
             "Cluster.schema_metadata_enabled and Cluster.token_metadata_enabled instead.", DeprecationWarning)
        self.schema_metadata_enabled = enabled
        self.token_metadata_enabled = enabled

    @classmethod
    def _send_chunks(cls, connection, host, chunks, set_keyspace=False):
        for ks_chunk in chunks:
            messages = [PrepareMessage(query=s.query_string,
                                       keyspace=s.keyspace if set_keyspace else None)
                        for s in ks_chunk]
            # TODO: make this timeout configurable somehow?
            responses = connection.wait_for_responses(*messages, timeout=5.0, fail_on_error=False)
            for success, response in responses:
                if not success:
                    log.debug("Got unexpected response when preparing "
                              "statement on host %s: %r", host, response)

    def _prepare_all_queries(self, host):
        if not self._prepared_statements or not self.reprepare_on_up:
            return

        log.debug("Preparing all known prepared statements against host %s", host)
        connection = None
        try:
            connection = self.connection_factory(host.address)
            statements = self._prepared_statements.values()
            if ProtocolVersion.uses_keyspace_flag(self.protocol_version):
                # V5 protocol and higher, no need to set the keyspace
                chunks = []
                for i in range(0, len(statements), 10):
                    chunks.append(statements[i:i + 10])
                    self._send_chunks(connection, host, chunks, True)
            else:
                for keyspace, ks_statements in groupby(statements, lambda s: s.keyspace):
                    if keyspace is not None:
                        connection.set_keyspace_blocking(keyspace)

                    # prepare 10 statements at a time
                    ks_statements = list(ks_statements)
                    chunks = []
                    for i in range(0, len(ks_statements), 10):
                        chunks.append(ks_statements[i:i + 10])
                    self._send_chunks(connection, host, chunks)

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

    def add_prepared(self, query_id, prepared_statement):
        with self._prepared_statement_lock:
            self._prepared_statements[query_id] = prepared_statement


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

    _row_factory = staticmethod(named_tuple_factory)
    @property
    def row_factory(self):
        """
        The format to return row results in.  By default, each
        returned row will be a named tuple.  You can alternatively
        use any of the following:

        - :func:`cassandra.query.tuple_factory` - return a result row as a tuple
        - :func:`cassandra.query.named_tuple_factory` - return a result row as a named tuple
        - :func:`cassandra.query.dict_factory` - return a result row as a dict
        - :func:`cassandra.query.ordered_dict_factory` - return a result row as an OrderedDict

        """
        return self._row_factory

    @row_factory.setter
    def row_factory(self, rf):
        self._validate_set_legacy_config('row_factory', rf)

    _default_timeout = 10.0

    @property
    def default_timeout(self):
        """
        A default timeout, measured in seconds, for queries executed through
        :meth:`.execute()` or :meth:`.execute_async()`.  This default may be
        overridden with the `timeout` parameter for either of those methods.

        Setting this to :const:`None` will cause no timeouts to be set by default.

        Please see :meth:`.ResponseFuture.result` for details on the scope and
        effect of this timeout.

        .. versionadded:: 2.0.0
        """
        return self._default_timeout

    @default_timeout.setter
    def default_timeout(self, timeout):
        self._validate_set_legacy_config('default_timeout', timeout)

    _default_consistency_level = ConsistencyLevel.LOCAL_ONE

    @property
    def default_consistency_level(self):
        """
        *Deprecated:* use execution profiles instead
        The default :class:`~ConsistencyLevel` for operations executed through
        this session.  This default may be overridden by setting the
        :attr:`~.Statement.consistency_level` on individual statements.

        .. versionadded:: 1.2.0

        .. versionchanged:: 3.0.0

            default changed from ONE to LOCAL_ONE
        """
        return self._default_consistency_level

    @default_consistency_level.setter
    def default_consistency_level(self, cl):
        """
        *Deprecated:* use execution profiles instead
        """
        warn("Setting the consistency level at the session level will be removed in 4.0. Consider using "
             "execution profiles and setting the desired consitency level to the EXEC_PROFILE_DEFAULT profile."
             , DeprecationWarning)
        self._validate_set_legacy_config('default_consistency_level', cl)

    _default_serial_consistency_level = None

    @property
    def default_serial_consistency_level(self):
        """
        The default :class:`~ConsistencyLevel` for serial phase of  conditional updates executed through
        this session.  This default may be overridden by setting the
        :attr:`~.Statement.serial_consistency_level` on individual statements.

        Only valid for ``protocol_version >= 2``.
        """
        return self._default_serial_consistency_level

    @default_serial_consistency_level.setter
    def default_serial_consistency_level(self, cl):
        self._validate_set_legacy_config('default_serial_consistency_level', cl)

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

    timestamp_generator = None
    """
    When :attr:`use_client_timestamp` is set, sessions call this object and use
    the result as the timestamp.  (Note that timestamps specified within a CQL
    query will override this timestamp.)  By default, a new
    :class:`~.MonotonicTimestampGenerator` is created for
    each :class:`Cluster` instance.

    Applications can set this value for custom timestamp behavior.  For
    example, an application could share a timestamp generator across
    :class:`Cluster` objects to guarantee that the application will use unique,
    increasing timestamps across clusters, or set it to to ``lambda:
    int(time.time() * 1e6)`` if losing records over clock inconsistencies is
    acceptable for the application. Custom :attr:`timestamp_generator` s should
    be callable, and calling them should return an integer representing microseconds
    since some point in time, typically UNIX epoch.

    .. versionadded:: 3.8.0
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

    client_protocol_handler = ProtocolHandler
    """
    Specifies a protocol handler that will be used for client-initiated requests (i.e. no
    internal driver requests). This can be used to override or extend features such as
    message or type ser/des.

    The default pure python implementation is :class:`cassandra.protocol.ProtocolHandler`.

    When compiled with Cython, there are also built-in faster alternatives. See :ref:`faster_deser`
    """

    _lock = None
    _pools = None
    _profile_manager = None
    _metrics = None
    _request_init_callbacks = None

    def __init__(self, cluster, hosts, keyspace=None):
        self.cluster = cluster
        self.hosts = hosts
        self.keyspace = keyspace

        self._lock = RLock()
        self._pools = {}
        self._profile_manager = cluster.profile_manager
        self._metrics = cluster.metrics
        self._request_init_callbacks = []
        self._protocol_version = self.cluster.protocol_version

        self.encoder = Encoder()

        # create connection pools in parallel
        self._initial_connect_futures = set()
        for host in hosts:
            future = self.add_or_renew_pool(host, is_host_addition=False)
            if future:
                self._initial_connect_futures.add(future)

        futures = wait_futures(self._initial_connect_futures, return_when=FIRST_COMPLETED)
        while futures.not_done and not any(f.result() for f in futures.done):
            futures = wait_futures(futures.not_done, return_when=FIRST_COMPLETED)

        if not any(f.result() for f in self._initial_connect_futures):
            msg = "Unable to connect to any servers"
            if self.keyspace:
                msg += " using keyspace '%s'" % self.keyspace
            raise NoHostAvailable(msg, [h.address for h in hosts])

    def execute(self, query, parameters=None, timeout=_NOT_SET, trace=False, custom_payload=None, execution_profile=EXEC_PROFILE_DEFAULT, paging_state=None):
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

        If `trace` is set to :const:`True`, the query will be sent with tracing enabled.
        The trace details can be obtained using the returned :class:`.ResultSet` object.

        `custom_payload` is a :ref:`custom_payload` dict to be passed to the server.
        If `query` is a Statement with its own custom_payload. The message payload
        will be a union of the two, with the values specified here taking precedence.

        `execution_profile` is the execution profile to use for this request. It can be a key to a profile configured
        via :meth:`Cluster.add_execution_profile` or an instance (from :meth:`Session.execution_profile_clone_update`,
        for example

        `paging_state` is an optional paging state, reused from a previous :class:`ResultSet`.
        """
        return self.execute_async(query, parameters, trace, custom_payload, timeout, execution_profile, paging_state).result()

    def execute_async(self, query, parameters=None, trace=False, custom_payload=None, timeout=_NOT_SET, execution_profile=EXEC_PROFILE_DEFAULT, paging_state=None):
        """
        Execute the given query and return a :class:`~.ResponseFuture` object
        which callbacks may be attached to for asynchronous response
        delivery.  You may also call :meth:`~.ResponseFuture.result()`
        on the :class:`.ResponseFuture` to synchronously block for results at
        any time.

        See :meth:`Session.execute` for parameter definitions.

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
        future = self._create_response_future(query, parameters, trace, custom_payload, timeout, execution_profile, paging_state)
        future._protocol_handler = self.client_protocol_handler
        self._on_request(future)
        future.send_request()
        return future

    def _create_response_future(self, query, parameters, trace, custom_payload, timeout, execution_profile=EXEC_PROFILE_DEFAULT, paging_state=None):
        """ Returns the ResponseFuture before calling send_request() on it """

        prepared_statement = None

        if isinstance(query, six.string_types):
            query = SimpleStatement(query)
        elif isinstance(query, PreparedStatement):
            query = query.bind(parameters)

        if self.cluster._config_mode == _ConfigMode.LEGACY:
            if execution_profile is not EXEC_PROFILE_DEFAULT:
                raise ValueError("Cannot specify execution_profile while using legacy parameters.")

            if timeout is _NOT_SET:
                timeout = self.default_timeout

            cl = query.consistency_level if query.consistency_level is not None else self.default_consistency_level
            serial_cl = query.serial_consistency_level if query.serial_consistency_level is not None else self.default_serial_consistency_level

            retry_policy = query.retry_policy or self.cluster.default_retry_policy
            row_factory = self.row_factory
            load_balancing_policy = self.cluster.load_balancing_policy
            spec_exec_policy = None
        else:
            execution_profile = self._get_execution_profile(execution_profile)

            if timeout is _NOT_SET:
                timeout = execution_profile.request_timeout

            cl = query.consistency_level if query.consistency_level is not None else execution_profile.consistency_level
            serial_cl = query.serial_consistency_level if query.serial_consistency_level is not None else execution_profile.serial_consistency_level

            retry_policy = query.retry_policy or execution_profile.retry_policy
            row_factory = execution_profile.row_factory
            load_balancing_policy = execution_profile.load_balancing_policy
            spec_exec_policy = execution_profile.speculative_execution_policy


        fetch_size = query.fetch_size
        if fetch_size is FETCH_SIZE_UNSET and self._protocol_version >= 2:
            fetch_size = self.default_fetch_size
        elif self._protocol_version == 1:
            fetch_size = None

        start_time = time.time()
        if self._protocol_version >= 3 and self.use_client_timestamp:
            timestamp = self.cluster.timestamp_generator()
        else:
            timestamp = None

        if isinstance(query, SimpleStatement):
            query_string = query.query_string
            statement_keyspace = query.keyspace if ProtocolVersion.uses_keyspace_flag(self._protocol_version) else None
            if parameters:
                query_string = bind_params(query_string, parameters, self.encoder)
            message = QueryMessage(
                query_string, cl, serial_cl,
                fetch_size, timestamp=timestamp,
                keyspace=statement_keyspace)
        elif isinstance(query, BoundStatement):
            prepared_statement = query.prepared_statement
            message = ExecuteMessage(
                prepared_statement.query_id, query.values, cl,
                serial_cl, fetch_size,
                timestamp=timestamp, skip_meta=bool(prepared_statement.result_metadata),
                result_metadata_id=prepared_statement.result_metadata_id)
        elif isinstance(query, BatchStatement):
            if self._protocol_version < 2:
                raise UnsupportedOperation(
                    "BatchStatement execution is only supported with protocol version "
                    "2 or higher (supported in Cassandra 2.0 and higher).  Consider "
                    "setting Cluster.protocol_version to 2 to support this operation.")
            statement_keyspace = query.keyspace if ProtocolVersion.uses_keyspace_flag(self._protocol_version) else None
            message = BatchMessage(
                query.batch_type, query._statements_and_parameters, cl,
                serial_cl, timestamp, statement_keyspace)

        message.tracing = trace

        message.update_custom_payload(query.custom_payload)
        message.update_custom_payload(custom_payload)
        message.allow_beta_protocol_version = self.cluster.allow_beta_protocol_version
        message.paging_state = paging_state

        spec_exec_plan = spec_exec_policy.new_plan(query.keyspace or self.keyspace, query) if query.is_idempotent and spec_exec_policy else None
        return ResponseFuture(
            self, message, query, timeout, metrics=self._metrics,
            prepared_statement=prepared_statement, retry_policy=retry_policy, row_factory=row_factory,
            load_balancer=load_balancing_policy, start_time=start_time, speculative_execution_plan=spec_exec_plan)

    def _get_execution_profile(self, ep):
        profiles = self.cluster.profile_manager.profiles
        try:
            return ep if isinstance(ep, ExecutionProfile) else profiles[ep]
        except KeyError:
            raise ValueError("Invalid execution_profile: '%s'; valid profiles are %s" % (ep, profiles.keys()))

    def execution_profile_clone_update(self, ep, **kwargs):
        """
        Returns a clone of the ``ep`` profile.  ``kwargs`` can be specified to update attributes
        of the returned profile.

        This is a shallow clone, so any objects referenced by the profile are shared. This means Load Balancing Policy
        is maintained by inclusion in the active profiles. It also means updating any other rich objects will be seen
        by the active profile. In cases where this is not desirable, be sure to replace the instance instead of manipulating
        the shared object.
        """
        clone = copy(self._get_execution_profile(ep))
        for attr, value in kwargs.items():
            setattr(clone, attr, value)
        return clone

    def add_request_init_listener(self, fn, *args, **kwargs):
        """
        Adds a callback with arguments to be called when any request is created.

        It will be invoked as `fn(response_future, *args, **kwargs)` after each client request is created,
        and before the request is sent\*. This can be used to create extensions by adding result callbacks to the
        response future.

        \* where `response_future` is the :class:`.ResponseFuture` for the request.

        Note that the init callback is done on the client thread creating the request, so you may need to consider
        synchronization if you have multiple threads. Any callbacks added to the response future will be executed
        on the event loop thread, so the normal advice about minimizing cycles and avoiding blocking apply (see Note in
        :meth:`.ResponseFuture.add_callbacks`.

        See `this example <https://github.com/datastax/python-driver/blob/master/examples/request_init_listener.py>`_ in the
        source tree for an example.
        """
        self._request_init_callbacks.append((fn, args, kwargs))

    def remove_request_init_listener(self, fn, *args, **kwargs):
        """
        Removes a callback and arguments from the list.

        See :meth:`.Session.add_request_init_listener`.
        """
        self._request_init_callbacks.remove((fn, args, kwargs))

    def _on_request(self, response_future):
        for fn, args, kwargs in self._request_init_callbacks:
            fn(response_future, *args, **kwargs)

    def prepare(self, query, custom_payload=None, keyspace=None):
        """
        Prepares a query string, returning a :class:`~cassandra.query.PreparedStatement`
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

        Alternatively, if :attr:`~.Cluster.protocol_version` is 5 or higher
        (requires Cassandra 4.0+), the keyspace can be specified as a
        parameter. This will allow you to avoid specifying the keyspace in the
        query without specifying a keyspace in :meth:`~.Cluster.connect`. It
        even will let you prepare and use statements against a keyspace other
        than the one originally specified on connection:

            >>> analyticskeyspace_prepared = session.prepare(
            ...     "INSERT INTO user_activity id, last_activity VALUES (?, ?)",
            ...     keyspace="analyticskeyspace")  # note the different keyspace

        **Important**: PreparedStatements should be prepared only once.
        Preparing the same query more than once will likely affect performance.

        `custom_payload` is a key value map to be passed along with the prepare
        message. See :ref:`custom_payload`.
        """
        message = PrepareMessage(query=query, keyspace=keyspace)
        future = ResponseFuture(self, message, query=None, timeout=self.default_timeout)
        try:
            future.send_request()
            query_id, bind_metadata, pk_indexes, result_metadata, result_metadata_id = future.result()
        except Exception:
            log.exception("Error preparing query:")
            raise

        prepared_keyspace = keyspace if keyspace else None
        prepared_statement = PreparedStatement.from_message(
            query_id, bind_metadata, pk_indexes, self.cluster.metadata, query, self.keyspace,
            self._protocol_version, result_metadata, result_metadata_id)
        prepared_statement.custom_payload = future.custom_payload

        self.cluster.add_prepared(query_id, prepared_statement)

        if self.cluster.prepare_on_all_hosts:
            host = future._current_host
            try:
                self.prepare_on_all_hosts(prepared_statement.query_string, host, prepared_keyspace)
            except Exception:
                log.exception("Error preparing query on all hosts:")

        return prepared_statement

    def prepare_on_all_hosts(self, query, excluded_host, keyspace=None):
        """
        Prepare the given query on all hosts, excluding ``excluded_host``.
        Intended for internal use only.
        """
        futures = []
        for host in tuple(self._pools.keys()):
            if host != excluded_host and host.is_up:
                future = ResponseFuture(self, PrepareMessage(query=query, keyspace=keyspace),
                                            None, self.default_timeout)

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
                future.result()
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

        # PYTHON-673. If shutdown was called shortly after session init, avoid
        # a race by cancelling any initial connection attempts haven't started,
        # then blocking on any that have.
        for future in self._initial_connect_futures:
            future.cancel()
        wait_futures(self._initial_connect_futures)

        for pool in tuple(self._pools.values()):
            pool.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    def __del__(self):
        try:
            # Ensure all connections are closed, in case the Session object is deleted by the GC
            self.shutdown()
        except:
            # Ignore all errors. Shutdown errors can be caught by the user
            # when cluster.shutdown() is called explicitly.
            pass

    def add_or_renew_pool(self, host, is_host_addition):
        """
        For internal use only.
        """
        distance = self._profile_manager.distance(host)
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
            with self._lock:
                while new_pool._keyspace != self.keyspace:
                    self._lock.release()
                    set_keyspace_event = Event()
                    errors_returned = []

                    def callback(pool, errors):
                        errors_returned.extend(errors)
                        set_keyspace_event.set()

                    new_pool._set_keyspace_for_all_conns(self.keyspace, callback)
                    set_keyspace_event.wait(self.cluster.connect_timeout)
                    if not set_keyspace_event.is_set() or errors_returned:
                        log.warning("Failed setting keyspace for pool after keyspace changed during connect: %s", errors_returned)
                        self.cluster.on_down(host, is_host_addition)
                        new_pool.shutdown()
                        self._lock.acquire()
                        return False
                    self._lock.acquire()
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
        futures = set()
        for host in self.cluster.metadata.all_hosts():
            distance = self._profile_manager.distance(host)
            pool = self._pools.get(host)
            future = None
            if not pool or pool.is_shutdown:
                # we don't eagerly set is_up on previously ignored hosts. None is included here
                # to allow us to attempt connections to hosts that have gone from ignored to something
                # else.
                if distance != HostDistance.IGNORED and host.is_up in (True, None):
                    future = self.add_or_renew_pool(host, False)
            elif distance != pool.host_distance:
                # the distance has changed
                if distance == HostDistance.IGNORED:
                    future = self.remove_pool(host)
                else:
                    pool.host_distance = distance
            if future:
                futures.add(future)
        return futures

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
        with self._lock:
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

        for pool in tuple(self._pools.values()):
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

        field_names = type_meta.field_names
        if six.PY2:
            # go from unicode to string to avoid decode errors from implicit
            # decode when formatting non-ascii values
            field_names = [fn.encode('utf-8') for fn in field_names]

        def encode(val):
            return '{ %s }' % ' , '.join('%s : %s' % (
                field_name,
                self.encoder.cql_encode_all_types(getattr(val, field_name, None))
            ) for field_name in field_names)

        self.encoder.mapping[klass] = encode

    def submit(self, fn, *args, **kwargs):
        """ Internal """
        if not self.is_shutdown:
            return self.cluster.executor.submit(fn, *args, **kwargs)

    def get_pool_state(self):
        return dict((host, pool.get_state()) for host, pool in tuple(self._pools.items()))

    def get_pools(self):
        return self._pools.values()

    def _validate_set_legacy_config(self, attr_name, value):
        if self.cluster._config_mode == _ConfigMode.PROFILES:
            raise ValueError("Cannot set Session.%s while using Configuration Profiles. Set this in a profile instead." % (attr_name,))
        setattr(self, '_' + attr_name, value)
        self.cluster._config_mode = _ConfigMode.LEGACY


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

    _SELECT_PEERS = "SELECT * FROM system.peers"
    _SELECT_PEERS_NO_TOKENS = "SELECT peer, data_center, rack, rpc_address, release_version, schema_version FROM system.peers"
    _SELECT_LOCAL = "SELECT * FROM system.local WHERE key='local'"
    _SELECT_LOCAL_NO_TOKENS = "SELECT cluster_name, data_center, rack, partitioner, release_version, schema_version FROM system.local WHERE key='local'"

    _SELECT_SCHEMA_PEERS = "SELECT peer, rpc_address, schema_version FROM system.peers"
    _SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'"

    _is_shutdown = False
    _timeout = None
    _protocol_version = None

    _schema_event_refresh_window = None
    _topology_event_refresh_window = None
    _status_event_refresh_window = None

    _schema_meta_enabled = True
    _token_meta_enabled = True

    # for testing purposes
    _time = time

    def __init__(self, cluster, timeout,
                 schema_event_refresh_window,
                 topology_event_refresh_window,
                 status_event_refresh_window,
                 schema_meta_enabled=True,
                 token_meta_enabled=True):
        # use a weak reference to allow the Cluster instance to be GC'ed (and
        # shutdown) since implementing __del__ disables the cycle detector
        self._cluster = weakref.proxy(cluster)
        self._connection = None
        self._timeout = timeout

        self._schema_event_refresh_window = schema_event_refresh_window
        self._topology_event_refresh_window = topology_event_refresh_window
        self._status_event_refresh_window = status_event_refresh_window
        self._schema_meta_enabled = schema_meta_enabled
        self._token_meta_enabled = token_meta_enabled

        self._lock = RLock()
        self._schema_agreement_lock = Lock()

        self._reconnection_handler = None
        self._reconnection_lock = RLock()

        self._event_schedule_times = {}

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
        lbp = (
            self._cluster.load_balancing_policy
            if self._cluster._config_mode == _ConfigMode.LEGACY else
            self._cluster._default_load_balancing_policy
        )

        for host in lbp.make_query_plan():
            try:
                return self._try_connect(host)
            except ConnectionException as exc:
                errors[host.address] = exc
                log.warning("[control connection] Error connecting to %s:", host, exc_info=True)
                self._cluster.signal_connection_failure(host, exc, is_host_addition=False)
            except Exception as exc:
                errors[host.address] = exc
                log.warning("[control connection] Error connecting to %s:", host, exc_info=True)
            if self._is_shutdown:
                raise DriverException("[control connection] Reconnection in progress during shutdown")

        raise NoHostAvailable("Unable to connect to any servers", errors)

    def _try_connect(self, host):
        """
        Creates a new Connection, registers for pushed events, and refreshes
        node/token and schema metadata.
        """
        log.debug("[control connection] Opening new connection to %s", host)

        while True:
            try:
                connection = self._cluster.connection_factory(host.address, is_control_connection=True)
                if self._is_shutdown:
                    connection.close()
                    raise DriverException("Reconnecting during shutdown")
                break
            except ProtocolVersionUnsupported as e:
                self._cluster.protocol_downgrade(host.address, e.startup_version)

        log.debug("[control connection] Established new connection %r, "
                  "registering watchers and refreshing schema and topology",
                  connection)

        # use weak references in both directions
        # _clear_watcher will be called when this ControlConnection is about to be finalized
        # _watch_callback will get the actual callback from the Connection and relay it to
        # this object (after a dereferencing a weakref)
        self_weakref = weakref.ref(self, partial(_clear_watcher, weakref.proxy(connection)))
        try:
            connection.register_watchers({
                "TOPOLOGY_CHANGE": partial(_watch_callback, self_weakref, '_handle_topology_change'),
                "STATUS_CHANGE": partial(_watch_callback, self_weakref, '_handle_status_change'),
                "SCHEMA_CHANGE": partial(_watch_callback, self_weakref, '_handle_schema_change')
            }, register_timeout=self._timeout)

            sel_peers = self._SELECT_PEERS if self._token_meta_enabled else self._SELECT_PEERS_NO_TOKENS
            sel_local = self._SELECT_LOCAL if self._token_meta_enabled else self._SELECT_LOCAL_NO_TOKENS
            peers_query = QueryMessage(query=sel_peers, consistency_level=ConsistencyLevel.ONE)
            local_query = QueryMessage(query=sel_local, consistency_level=ConsistencyLevel.ONE)
            shared_results = connection.wait_for_responses(
                peers_query, local_query, timeout=self._timeout)

            self._refresh_node_list_and_token_map(connection, preloaded_results=shared_results)
            self._refresh_schema(connection, preloaded_results=shared_results, schema_agreement_wait=-1)
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
            schedule = self._cluster.reconnection_policy.new_schedule()

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
        # stop trying to reconnect (if we are)
        with self._reconnection_lock:
            if self._reconnection_handler:
                self._reconnection_handler.cancel()

        with self._lock:
            if self._is_shutdown:
                return
            else:
                self._is_shutdown = True

            log.debug("Shutting down control connection")
            if self._connection:
                self._connection.close()
                self._connection = None

    def refresh_schema(self, force=False, **kwargs):
        try:
            if self._connection:
                return self._refresh_schema(self._connection, force=force, **kwargs)
        except ReferenceError:
            pass  # our weak reference to the Cluster is no good
        except Exception:
            log.debug("[control connection] Error refreshing schema", exc_info=True)
            self._signal_error()
        return False

    def _refresh_schema(self, connection, preloaded_results=None, schema_agreement_wait=None, force=False, **kwargs):
        if self._cluster.is_shutdown:
            return False

        agreed = self.wait_for_schema_agreement(connection,
                                                preloaded_results=preloaded_results,
                                                wait_time=schema_agreement_wait)

        if not self._schema_meta_enabled and not force:
            log.debug("[control connection] Skipping schema refresh because schema metadata is disabled")
            return False

        if not agreed:
            log.debug("Skipping schema refresh due to lack of schema agreement")
            return False

        self._cluster.metadata.refresh(connection, self._timeout, **kwargs)

        return True

    def refresh_node_list_and_token_map(self, force_token_rebuild=False):
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
            cl = ConsistencyLevel.ONE
            if not self._token_meta_enabled:
                log.debug("[control connection] Refreshing node list without token map")
                sel_peers = self._SELECT_PEERS_NO_TOKENS
                sel_local = self._SELECT_LOCAL_NO_TOKENS
            else:
                log.debug("[control connection] Refreshing node list and token map")
                sel_peers = self._SELECT_PEERS
                sel_local = self._SELECT_LOCAL
            peers_query = QueryMessage(query=sel_peers, consistency_level=cl)
            local_query = QueryMessage(query=sel_local, consistency_level=cl)
            peers_result, local_result = connection.wait_for_responses(
                peers_query, local_query, timeout=self._timeout)

        peers_result = dict_factory(*peers_result.results)

        partitioner = None
        token_map = {}

        found_hosts = set()
        if local_result.results:
            found_hosts.add(connection.host)
            local_rows = dict_factory(*(local_result.results))
            local_row = local_rows[0]
            cluster_name = local_row["cluster_name"]
            self._cluster.metadata.cluster_name = cluster_name

            partitioner = local_row.get("partitioner")
            tokens = local_row.get("tokens")

            host = self._cluster.metadata.get_host(connection.host)
            if host:
                datacenter = local_row.get("data_center")
                rack = local_row.get("rack")
                self._update_location_info(host, datacenter, rack)
                host.listen_address = local_row.get("listen_address")
                host.broadcast_address = local_row.get("broadcast_address")
                host.release_version = local_row.get("release_version")
                host.dse_version = local_row.get("dse_version")
                host.dse_workload = local_row.get("workload")

                if partitioner and tokens:
                    token_map[host] = tokens

        # Check metadata.partitioner to see if we haven't built anything yet. If
        # every node in the cluster was in the contact points, we won't discover
        # any new nodes, so we need this additional check.  (See PYTHON-90)
        should_rebuild_token_map = force_token_rebuild or self._cluster.metadata.partitioner is None
        for row in peers_result:
            addr = self._rpc_from_peer_row(row)

            tokens = row.get("tokens", None)
            if 'tokens' in row and not tokens:  # it was selected, but empty
                log.warning("Excluding host (%s) with no tokens in system.peers table of %s." % (addr, connection.host))
                continue
            if addr in found_hosts:
                log.warning("Found multiple hosts with the same rpc_address (%s). Excluding peer %s", addr, row.get("peer"))
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

            host.broadcast_address = row.get("peer")
            host.release_version = row.get("release_version")
            host.dse_version = row.get("dse_version")
            host.dse_workload = row.get("workload")

            if partitioner and tokens:
                token_map[host] = tokens

        for old_host in self._cluster.metadata.all_hosts():
            if old_host.address != connection.host and old_host.address not in found_hosts:
                should_rebuild_token_map = True
                log.debug("[control connection] Removing host not found in peers metadata: %r", old_host)
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
        self._cluster.profile_manager.on_down(host)
        host.set_location_info(datacenter, rack)
        self._cluster.profile_manager.on_up(host)
        return True

    def _delay_for_event_type(self, event_type, delay_window):
        # this serves to order processing correlated events (received within the window)
        # the window and randomization still have the desired effect of skew across client instances
        next_time = self._event_schedule_times.get(event_type, 0)
        now = self._time.time()
        if now <= next_time:
            this_time = next_time + 0.01
            delay = this_time - now
        else:
            delay = random() * delay_window
            this_time = now + delay
        self._event_schedule_times[event_type] = this_time
        return delay

    def _refresh_nodes_if_not_up(self, addr):
        """
        Used to mitigate refreshes for nodes that are already known.
        Some versions of the server send superfluous NEW_NODE messages in addition to UP events.
        """
        host = self._cluster.metadata.get_host(addr)
        if not host or not host.is_up:
            self.refresh_node_list_and_token_map()

    def _handle_topology_change(self, event):
        change_type = event["change_type"]
        addr = self._translate_address(event["address"][0])
        if change_type == "NEW_NODE" or change_type == "MOVED_NODE":
            if self._topology_event_refresh_window >= 0:
                delay = self._delay_for_event_type('topology_change', self._topology_event_refresh_window)
                self._cluster.scheduler.schedule_unique(delay, self._refresh_nodes_if_not_up, addr)
        elif change_type == "REMOVED_NODE":
            host = self._cluster.metadata.get_host(addr)
            self._cluster.scheduler.schedule_unique(0, self._cluster.remove_host, host)

    def _handle_status_change(self, event):
        change_type = event["change_type"]
        addr = self._translate_address(event["address"][0])
        host = self._cluster.metadata.get_host(addr)
        if change_type == "UP":
            delay = self._delay_for_event_type('status_change', self._status_event_refresh_window)
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

    def _translate_address(self, addr):
        return self._cluster.address_translator.translate(addr)

    def _handle_schema_change(self, event):
        if self._schema_event_refresh_window < 0:
            return
        delay = self._delay_for_event_type('schema_change', self._schema_event_refresh_window)
        self._cluster.scheduler.schedule_unique(delay, self.refresh_schema, **event)

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
            local_row = dict_factory(*local_result.results)[0]
            if local_row.get("schema_version"):
                versions[local_row.get("schema_version")].add(local_address)

        for row in peers_result:
            schema_ver = row.get('schema_version')
            if not schema_ver:
                continue
            addr = self._rpc_from_peer_row(row)
            peer = self._cluster.metadata.get_host(addr)
            if peer and peer.is_up is not False:
                versions[schema_ver].add(addr)

        if len(versions) == 1:
            log.debug("[control connection] Schemas match")
            return None

        return dict((version, list(nodes)) for version, nodes in six.iteritems(versions))

    def _rpc_from_peer_row(self, row):
        addr = row.get("rpc_address")
        if not addr or addr in ["0.0.0.0", "::"]:
            addr = row.get("peer")
        return self._translate_address(addr)

    def _signal_error(self):
        with self._lock:
            if self._is_shutdown:
                return

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
        c = self._connection
        if c and c.host == host.address:
            log.debug("[control connection] Control connection host (%s) is being removed. Reconnecting", host)
            # refresh will be done on reconnect
            self.reconnect()
        else:
            self.refresh_node_list_and_token_map(force_token_rebuild=True)

    def get_connections(self):
        c = getattr(self, '_connection', None)
        return [c] if c else []

    def return_connection(self, connection):
        if connection is self._connection and (connection.is_defunct or connection.is_closed):
            self.reconnect()


def _stop_scheduler(scheduler, thread):
    try:
        if not scheduler.is_shutdown:
            scheduler.shutdown()
    except ReferenceError:
        pass

    thread.join()


class _Scheduler(Thread):

    _queue = None
    _scheduled_tasks = None
    _executor = None
    is_shutdown = False

    def __init__(self, executor):
        self._queue = Queue.PriorityQueue()
        self._scheduled_tasks = set()
        self._count = count()
        self._executor = executor

        Thread.__init__(self, name="Task Scheduler")
        self.daemon = True
        self.start()

    def shutdown(self):
        try:
            log.debug("Shutting down Cluster Scheduler")
        except AttributeError:
            # this can happen on interpreter shutdown
            pass
        self.is_shutdown = True
        self._queue.put_nowait((0, 0, None))
        self.join()

    def schedule(self, delay, fn, *args, **kwargs):
        self._insert_task(delay, (fn, args, tuple(kwargs.items())))

    def schedule_unique(self, delay, fn, *args, **kwargs):
        task = (fn, args, tuple(kwargs.items()))
        if task not in self._scheduled_tasks:
            self._insert_task(delay, task)
        else:
            log.debug("Ignoring schedule_unique for already-scheduled task: %r", task)

    def _insert_task(self, delay, task):
        if not self.is_shutdown:
            run_at = time.time() + delay
            self._scheduled_tasks.add(task)
            self._queue.put_nowait((run_at, next(self._count), task))
        else:
            log.debug("Ignoring scheduled task after shutdown: %r", task)

    def run(self):
        while True:
            if self.is_shutdown:
                return

            try:
                while True:
                    run_at, i, task = self._queue.get(block=True, timeout=None)
                    if self.is_shutdown:
                        if task:
                            log.debug("Not executing scheduled task due to Scheduler shutdown")
                        return
                    if run_at <= time.time():
                        self._scheduled_tasks.discard(task)
                        fn, args, kwargs = task
                        kwargs = dict(kwargs)
                        future = self._executor.submit(fn, *args, **kwargs)
                        future.add_done_callback(self._log_if_failed)
                    else:
                        self._queue.put_nowait((run_at, i, task))
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


def refresh_schema_and_set_result(control_conn, response_future, connection, **kwargs):
    try:
        log.debug("Refreshing schema in response to schema change. "
                  "%s", kwargs)
        response_future.is_schema_agreed = control_conn._refresh_schema(connection, **kwargs)
    except Exception:
        log.exception("Exception refreshing schema in response to schema change:")
        response_future.session.submit(control_conn.refresh_schema, **kwargs)
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

    is_schema_agreed = True
    """
    For DDL requests, this may be set ``False`` if the schema agreement poll after the response fails.

    Always ``True`` for non-DDL requests.
    """

    request_encoded_size = None
    """
    Size of the request message sent
    """

    coordinator_host = None
    """
    The host from which we recieved a response
    """

    attempted_hosts = None
    """
    A list of hosts tried, including all speculative executions, retries, and pages
    """

    session = None
    row_factory = None
    message = None
    default_timeout = None

    _retry_policy = None
    _profile_manager = None

    _req_id = None
    _final_result = _NOT_SET
    _col_names = None
    _col_types = None
    _final_exception = None
    _query_traces = None
    _callbacks = None
    _errbacks = None
    _current_host = None
    _connection = None
    _query_retries = 0
    _start_time = None
    _metrics = None
    _paging_state = None
    _custom_payload = None
    _warnings = None
    _timer = None
    _protocol_handler = ProtocolHandler
    _spec_execution_plan = NoSpeculativeExecutionPlan()

    _warned_timeout = False

    def __init__(self, session, message, query, timeout, metrics=None, prepared_statement=None,
                 retry_policy=RetryPolicy(), row_factory=None, load_balancer=None, start_time=None, speculative_execution_plan=None):
        self.session = session
        # TODO: normalize handling of retry policy and row factory
        self.row_factory = row_factory or session.row_factory
        self._load_balancer = load_balancer or session.cluster._default_load_balancing_policy
        self.message = message
        self.query = query
        self.timeout = timeout
        self._retry_policy = retry_policy
        self._metrics = metrics
        self.prepared_statement = prepared_statement
        self._callback_lock = Lock()
        self._start_time = start_time or time.time()
        self._spec_execution_plan = speculative_execution_plan or self._spec_execution_plan
        self._make_query_plan()
        self._event = Event()
        self._errors = {}
        self._callbacks = []
        self._errbacks = []
        self.attempted_hosts = []
        self._start_timer()

    @property
    def _time_remaining(self):
        if self.timeout is None:
            return None
        return (self._start_time + self.timeout) - time.time()

    def _start_timer(self):
        if self._timer is None:
            spec_delay = self._spec_execution_plan.next_execution(self._current_host)
            if spec_delay >= 0:
                if self._time_remaining is None or self._time_remaining > spec_delay:
                    self._timer = self.session.cluster.connection_class.create_timer(spec_delay, self._on_speculative_execute)
                    return
            if self._time_remaining is not None:
                self._timer = self.session.cluster.connection_class.create_timer(self._time_remaining, self._on_timeout)

    def _cancel_timer(self):
        if self._timer:
            self._timer.cancel()

    def _on_timeout(self, _attempts=0):
        """
        Called when the request associated with this ResponseFuture times out.

        This function may reschedule itself. The ``_attempts`` parameter tracks
        the number of times this has happened. This parameter should only be
        set in those cases, where ``_on_timeout`` reschedules itself.
        """
        # PYTHON-853: for short timeouts, we sometimes race with our __init__
        if self._connection is None and _attempts < 3:
            self._timer = self.session.cluster.connection_class.create_timer(
                0.01,
                partial(self._on_timeout, _attempts=_attempts + 1)
            )
            return

        if self._connection is not None:
            try:
                self._connection._requests.pop(self._req_id)
            # This prevents the race condition of the
            # event loop thread just receiving the waited message
            # If it arrives after this, it will be ignored
            except KeyError:
                return

            pool = self.session._pools.get(self._current_host)
            if pool and not pool.is_shutdown:
                with self._connection.lock:
                    self._connection.request_ids.append(self._req_id)

                pool.return_connection(self._connection)

        errors = self._errors
        if not errors:
            if self.is_schema_agreed:
                key = self._current_host.address if self._current_host else 'no host queried before timeout'
                errors = {key: "Client request timeout. See Session.execute[_async](timeout)"}
            else:
                connection = self.session.cluster.control_connection._connection
                host = connection.host if connection else 'unknown'
                errors = {host: "Request timed out while waiting for schema agreement. See Session.execute[_async](timeout) and Cluster.max_schema_agreement_wait."}

        self._set_final_exception(OperationTimedOut(errors, self._current_host))

    def _on_speculative_execute(self):
        self._timer = None
        if not self._event.is_set():

            # PYTHON-836, the speculative queries must be after
            # the query is sent from the main thread, otherwise the
            # query from the main thread may raise NoHostAvailable
            # if the _query_plan has been exhausted by the specualtive queries.
            # This also prevents a race condition accessing the iterator.
            # We reschedule this call until the main thread has succeeded
            # making a query
            if not self.attempted_hosts:
                self._timer = self.session.cluster.connection_class.create_timer(0.01, self._on_speculative_execute)
                return

            if self._time_remaining is not None:
                if self._time_remaining <= 0:
                    self._on_timeout()
                    return
            self.send_request(error_no_hosts=False)
            self._start_timer()


    def _make_query_plan(self):
        # convert the list/generator/etc to an iterator so that subsequent
        # calls to send_request (which retries may do) will resume where
        # they last left off
        self.query_plan = iter(self._load_balancer.make_query_plan(self.session.keyspace, self.query))

    def send_request(self, error_no_hosts=True):
        """ Internal """
        # query_plan is an iterator, so this will resume where we last left
        # off if send_request() is called multiple times
        for host in self.query_plan:
            req_id = self._query(host)
            if req_id is not None:
                self._req_id = req_id
                return True
            if self.timeout is not None and time.time() - self._start_time > self.timeout:
                self._on_timeout()
                return True

        if error_no_hosts:
            self._set_final_exception(NoHostAvailable(
                "Unable to complete the operation against any hosts", self._errors))
        return False

    def _query(self, host, message=None, cb=None):
        if message is None:
            message = self.message

        pool = self.session._pools.get(host)
        if not pool:
            self._errors[host] = ConnectionException("Host has been marked down or removed")
            return None
        elif pool.is_shutdown:
            self._errors[host] = ConnectionException("Pool is shutdown")
            return None

        self._current_host = host

        connection = None
        try:
            # TODO get connectTimeout from cluster settings
            connection, request_id = pool.borrow_connection(timeout=2.0)
            self._connection = connection
            result_meta = self.prepared_statement.result_metadata if self.prepared_statement else []

            if cb is None:
                cb = partial(self._set_result, host, connection, pool)

            self.request_encoded_size = connection.send_msg(message, request_id, cb=cb,
                                                            encoder=self._protocol_handler.encode_message,
                                                            decoder=self._protocol_handler.decode_message,
                                                            result_metadata=result_meta)
            self.attempted_hosts.append(host)
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

    @property
    def warnings(self):
        """
        Warnings returned from the server, if any. This will only be
        set for protocol_version 4+.

        Warnings may be returned for such things as oversized batches,
        or too many tombstones in slice queries.

        Ensure the future is complete before trying to access this property
        (call :meth:`.result()`, or after callback is invoked).
        Otherwise it may throw if the response has not been received.
        """
        # TODO: When timers are introduced, just make this wait
        if not self._event.is_set():
            raise DriverException("warnings cannot be retrieved before ResponseFuture is finalized")
        return self._warnings

    @property
    def custom_payload(self):
        """
        The custom payload returned from the server, if any. This will only be
        set by Cassandra servers implementing a custom QueryHandler, and only
        for protocol_version 4+.

        Ensure the future is complete before trying to access this property
        (call :meth:`.result()`, or after callback is invoked).
        Otherwise it may throw if the response has not been received.

        :return: :ref:`custom_payload`.
        """
        # TODO: When timers are introduced, just make this wait
        if not self._event.is_set():
            raise DriverException("custom_payload cannot be retrieved before ResponseFuture is finalized")
        return self._custom_payload

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
        self._start_timer()
        self.send_request()

    def _reprepare(self, prepare_message, host, connection, pool):
        cb = partial(self.session.submit, self._execute_after_prepare, host, connection, pool)
        request_id = self._query(host, prepare_message, cb=cb)
        if request_id is None:
            # try to submit the original prepared statement on some other host
            self.send_request()

    def _set_result(self, host, connection, pool, response):
        try:
            self.coordinator_host = host
            if pool:
                pool.return_connection(connection)

            trace_id = getattr(response, 'trace_id', None)
            if trace_id:
                if not self._query_traces:
                    self._query_traces = []
                self._query_traces.append(QueryTrace(trace_id, self.session))

            self._warnings = getattr(response, 'warnings', None)
            self._custom_payload = getattr(response, 'custom_payload', None)

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
                    self.is_schema_agreed = False
                    self.session.submit(
                        refresh_schema_and_set_result,
                        self.session.cluster.control_connection,
                        self, connection, **response.results)
                else:
                    results = getattr(response, 'results', None)
                    if results is not None and response.kind == RESULT_KIND_ROWS:
                        self._paging_state = response.paging_state
                        self._col_types = response.col_types
                        self._col_names = results[0]
                        results = self.row_factory(*results)
                    self._set_final_result(results)
            elif isinstance(response, ErrorMessage):
                retry_policy = self._retry_policy

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
                                "host", host)
                    self._retry(reuse_connection=False, consistency_level=None, host=host)
                    return
                elif isinstance(response, IsBootstrappingErrorMessage):
                    if self._metrics is not None:
                        self._metrics.on_other_error()
                    # need to retry against a different host here
                    self._retry(reuse_connection=False, consistency_level=None, host=host)
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
                    if not ProtocolVersion.uses_keyspace_flag(self.session.cluster.protocol_version) \
                            and prepared_keyspace  and current_keyspace != prepared_keyspace:
                        self._set_final_exception(
                            ValueError("The Session's current keyspace (%s) does "
                                       "not match the keyspace the statement was "
                                       "prepared with (%s)" %
                                       (current_keyspace, prepared_keyspace)))
                        return

                    log.debug("Re-preparing unrecognized prepared statement against host %s: %s",
                              host, prepared_statement.query_string)
                    prepared_keyspace = prepared_statement.keyspace \
                        if ProtocolVersion.uses_keyspace_flag(self.session.cluster.protocol_version) else None
                    prepare_message = PrepareMessage(query=prepared_statement.query_string,
                                                     keyspace=prepared_keyspace)
                    # since this might block, run on the executor to avoid hanging
                    # the event loop thread
                    self.session.submit(self._reprepare, prepare_message, host, connection, pool)
                    return
                else:
                    if hasattr(response, 'to_exception'):
                        self._set_final_exception(response.to_exception())
                    else:
                        self._set_final_exception(response)
                    return

                retry_type, consistency = retry
                if retry_type in (RetryPolicy.RETRY, RetryPolicy.RETRY_NEXT_HOST):
                    self._query_retries += 1
                    reuse = retry_type == RetryPolicy.RETRY
                    self._retry(reuse, consistency, host)
                elif retry_type is RetryPolicy.RETHROW:
                    self._set_final_exception(response.to_exception())
                else:  # IGNORE
                    if self._metrics is not None:
                        self._metrics.on_ignore()
                    self._set_final_result(None)
                self._errors[host] = response.to_exception()
            elif isinstance(response, ConnectionException):
                if self._metrics is not None:
                    self._metrics.on_connection_error()
                if not isinstance(response, ConnectionShutdown):
                    self._connection.defunct(response)
                self._retry(reuse_connection=False, consistency_level=None, host=host)
            elif isinstance(response, Exception):
                if hasattr(response, 'to_exception'):
                    self._set_final_exception(response.to_exception())
                else:
                    self._set_final_exception(response)
            else:
                # we got some other kind of response message
                msg = "Got unexpected message: %r" % (response,)
                exc = ConnectionException(msg, host)
                self._cancel_timer()
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

    def _execute_after_prepare(self, host, connection, pool, response):
        """
        Handle the response to our attempt to prepare a statement.
        If it succeeded, run the original query again against the same host.
        """
        if pool:
            pool.return_connection(connection)

        if self._final_exception:
            return

        if isinstance(response, ResultMessage):
            if response.kind == RESULT_KIND_PREPARED:
                if self.prepared_statement:
                    # result metadata is the only thing that could have
                    # changed from an alter
                    (_, _, _,
                     self.prepared_statement.result_metadata,
                     new_metadata_id) = response.results
                    if new_metadata_id is not None:
                        self.prepared_statement.result_metadata_id = new_metadata_id

                # use self._query to re-use the same host and
                # at the same time properly borrow the connection
                request_id = self._query(host)
                if request_id is None:
                    # this host errored out, move on to the next
                    self.send_request()
            else:
                self._set_final_exception(ConnectionException(
                    "Got unexpected response when preparing statement "
                    "on host %s: %s" % (host, response)))
        elif isinstance(response, ErrorMessage):
            if hasattr(response, 'to_exception'):
                self._set_final_exception(response.to_exception())
            else:
                self._set_final_exception(response)
        elif isinstance(response, ConnectionException):
            log.debug("Connection error when preparing statement on host %s: %s",
                      host, response)
            # try again on a different host, preparing again if necessary
            self._errors[host] = response
            self.send_request()
        else:
            self._set_final_exception(ConnectionException(
                "Got unexpected response type when preparing "
                "statement on host %s: %s" % (host, response)))

    def _set_final_result(self, response):
        self._cancel_timer()
        if self._metrics is not None:
            self._metrics.request_timer.addValue(time.time() - self._start_time)

        with self._callback_lock:
            self._final_result = response
            # save off current callbacks inside lock for execution outside it
            # -- prevents case where _final_result is set, then a callback is
            # added and executed on the spot, then executed again as a
            # registered callback
            to_call = tuple(
                partial(fn, response, *args, **kwargs)
                for (fn, args, kwargs) in self._callbacks
            )

        self._event.set()

        # apply each callback
        for callback_partial in to_call:
            callback_partial()

    def _set_final_exception(self, response):
        self._cancel_timer()
        if self._metrics is not None:
            self._metrics.request_timer.addValue(time.time() - self._start_time)

        with self._callback_lock:
            self._final_exception = response
            # save off current errbacks inside lock for execution outside it --
            # prevents case where _final_exception is set, then an errback is
            # added and executed on the spot, then executed again as a
            # registered errback
            to_call = tuple(
                partial(fn, response, *args, **kwargs)
                for (fn, args, kwargs) in self._errbacks
            )
        self._event.set()

        # apply each callback
        for callback_partial in to_call:
            callback_partial()

    def _retry(self, reuse_connection, consistency_level, host):
        if self._final_exception:
            # the connection probably broke while we were waiting
            # to retry the operation
            return

        if self._metrics is not None:
            self._metrics.on_retry()
        if consistency_level is not None:
            self.message.consistency_level = consistency_level

        # don't retry on the event loop thread
        self.session.submit(self._retry_task, reuse_connection, host)

    def _retry_task(self, reuse_connection, host):
        if self._final_exception:
            # the connection probably broke while we were waiting
            # to retry the operation
            return

        if reuse_connection and self._query(host) is not None:
            return

        # otherwise, move onto another host
        self.send_request()

    def result(self):
        """
        Return the final result or raise an Exception if errors were
        encountered.  If the final result or error has not been set
        yet, this method will block until it is set, or the timeout
        set for the request expires.

        Timeout is specified in the Session request execution functions.
        If the timeout is exceeded, an :exc:`cassandra.OperationTimedOut` will be raised.
        This is a client-side timeout. For more information
        about server-side coordinator timeouts, see :class:`.policies.RetryPolicy`.

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
        self._event.wait()
        if self._final_result is not _NOT_SET:
            return ResultSet(self, self._final_result)
        else:
            raise self._final_exception

    def get_query_trace_ids(self):
        """
        Returns the trace session ids for this future, if tracing was enabled (does not fetch trace data).
        """
        return [trace.trace_id for trace in self._query_traces]

    def get_query_trace(self, max_wait=None, query_cl=ConsistencyLevel.LOCAL_ONE):
        """
        Fetches and returns the query trace of the last response, or `None` if tracing was
        not enabled.

        Note that this may raise an exception if there are problems retrieving the trace
        details from Cassandra. If the trace is not available after `max_wait`,
        :exc:`cassandra.query.TraceUnavailable` will be raised.

        If the ResponseFuture is not done (async execution) and you try to retrieve the trace,
        :exc:`cassandra.query.TraceUnavailable` will be raised.

        `query_cl` is the consistency level used to poll the trace tables.
        """
        if self._final_result is _NOT_SET and self._final_exception is None:
            raise TraceUnavailable(
                "Trace information was not available. The ResponseFuture is not done.")

        if self._query_traces:
            return self._get_query_trace(len(self._query_traces) - 1, max_wait, query_cl)

    def get_all_query_traces(self, max_wait_per=None, query_cl=ConsistencyLevel.LOCAL_ONE):
        """
        Fetches and returns the query traces for all query pages, if tracing was enabled.

        See note in :meth:`~.get_query_trace` regarding possible exceptions.
        """
        if self._query_traces:
            return [self._get_query_trace(i, max_wait_per, query_cl) for i in range(len(self._query_traces))]
        return []

    def _get_query_trace(self, i, max_wait, query_cl):
        trace = self._query_traces[i]
        if not trace.events:
            trace.populate(max_wait=max_wait, query_cl=query_cl)
        return trace

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

        Note: in the case that the result is not available when the callback is added,
        the callback is executed by IO event thread. This means that the callback
        should not block or attempt further synchronous requests, because no further
        IO will be processed until the callback returns.

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
            # Always add fn to self._callbacks, even when we're about to
            # execute it, to prevent races with functions like
            # start_fetching_next_page that reset _final_result
            self._callbacks.append((fn, args, kwargs))
            if self._final_result is not _NOT_SET:
                run_now = True
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
            # Always add fn to self._errbacks, even when we're about to execute
            # it, to prevent races with functions like start_fetching_next_page
            # that reset _final_exception
            self._errbacks.append((fn, args, kwargs))
            if self._final_exception:
                run_now = True
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
            self._callbacks = []
            self._errbacks = []

    def __str__(self):
        result = "(no result yet)" if self._final_result is _NOT_SET else self._final_result
        return "<ResponseFuture: query='%s' request_id=%s result=%s exception=%s coordinator_host=%s>" \
               % (self.query, self._req_id, result, self._final_exception, self.coordinator_host)
    __repr__ = __str__


class QueryExhausted(Exception):
    """
    Raised when :meth:`.ResponseFuture.start_fetching_next_page()` is called and
    there are no more pages.  You can check :attr:`.ResponseFuture.has_more_pages`
    before calling to avoid this.

    .. versionadded:: 2.0.0
    """
    pass


class ResultSet(object):
    """
    An iterator over the rows from a query result. Also supplies basic equality
    and indexing methods for backward-compatability. These methods materialize
    the entire result set (loading all pages), and should only be used if the
    total result size is understood. Warnings are emitted when paged results
    are materialized in this fashion.

    You can treat this as a normal iterator over rows::

        >>> from cassandra.query import SimpleStatement
        >>> statement = SimpleStatement("SELECT * FROM users", fetch_size=10)
        >>> for user_row in session.execute(statement):
        ...     process_user(user_row)

    Whenever there are no more rows in the current page, the next page will
    be fetched transparently.  However, note that it *is* possible for
    an :class:`Exception` to be raised while fetching the next page, just
    like you might see on a normal call to ``session.execute()``.
    """

    def __init__(self, response_future, initial_response):
        self.response_future = response_future
        self.column_names = response_future._col_names
        self.column_types = response_future._col_types
        self._set_current_rows(initial_response)
        self._page_iter = None
        self._list_mode = False

    @property
    def has_more_pages(self):
        """
        True if the last response indicated more pages; False otherwise
        """
        return self.response_future.has_more_pages

    @property
    def current_rows(self):
        """
        The list of current page rows. May be empty if the result was empty,
        or this is the last page.
        """
        return self._current_rows or []

    def one(self):
        """
        Return a single row of the results or None if empty. This is basically
        a shortcut to `result_set.current_rows[0]` and should only be used when
        you know a query returns a single row. Consider using an iterator if the
        ResultSet contains more than one row.
        """
        return self._current_rows[0] if self._current_rows else None

    def __iter__(self):
        if self._list_mode:
            return iter(self._current_rows)
        self._page_iter = iter(self._current_rows)
        return self

    def next(self):
        try:
            return next(self._page_iter)
        except StopIteration:
            if not self.response_future.has_more_pages:
                if not self._list_mode:
                    self._current_rows = []
                raise

        self.fetch_next_page()
        self._page_iter = iter(self._current_rows)

        return next(self._page_iter)

    __next__ = next

    def fetch_next_page(self):
        """
        Manually, synchronously fetch the next page. Supplied for manually retrieving pages
        and inspecting :meth:`~.current_page`. It is not necessary to call this when iterating
        through results; paging happens implicitly in iteration.
        """
        if self.response_future.has_more_pages:
            self.response_future.start_fetching_next_page()
            result = self.response_future.result()
            self._current_rows = result._current_rows  # ResultSet has already _set_current_rows to the appropriate form
        else:
            self._current_rows = []

    def _set_current_rows(self, result):
        if isinstance(result, Mapping):
            self._current_rows = [result] if result else []
            return
        try:
            iter(result)  # can't check directly for generator types because cython generators are different
            self._current_rows = result
        except TypeError:
            self._current_rows = [result] if result else []

    def _fetch_all(self):
        self._current_rows = list(self)
        self._page_iter = None

    def _enter_list_mode(self, operator):
        if self._list_mode:
            return
        if self._page_iter:
            raise RuntimeError("Cannot use %s when results have been iterated." % operator)
        if self.response_future.has_more_pages:
            log.warning("Using %s on paged results causes entire result set to be materialized.", operator)
        self._fetch_all()  # done regardless of paging status in case the row factory produces a generator
        self._list_mode = True

    def __eq__(self, other):
        self._enter_list_mode("equality operator")
        return self._current_rows == other

    def __getitem__(self, i):
        if i == 0:
            warn("ResultSet indexing support will be removed in 4.0. Consider using "
                 "ResultSet.one() to get a single row.", DeprecationWarning)
        self._enter_list_mode("index operator")
        return self._current_rows[i]

    def __nonzero__(self):
        return bool(self._current_rows)

    __bool__ = __nonzero__

    def get_query_trace(self, max_wait_sec=None):
        """
        Gets the last query trace from the associated future.
        See :meth:`.ResponseFuture.get_query_trace` for details.
        """
        return self.response_future.get_query_trace(max_wait_sec)

    def get_all_query_traces(self, max_wait_sec_per=None):
        """
        Gets all query traces from the associated future.
        See :meth:`.ResponseFuture.get_all_query_traces` for details.
        """
        return self.response_future.get_all_query_traces(max_wait_sec_per)

    @property
    def was_applied(self):
        """
        For LWT results, returns whether the transaction was applied.

        Result is indeterminate if called on a result that was not an LWT request or on
        a :class:`.query.BatchStatement` containing LWT. In the latter case either all the batch
        succeeds or fails.

        Only valid when one of the of the internal row factories is in use.
        """
        if self.response_future.row_factory not in (named_tuple_factory, dict_factory, tuple_factory):
            raise RuntimeError("Cannot determine LWT result with row factory %s" % (self.response_future.row_factory,))

        is_batch_statement = isinstance(self.response_future.query, BatchStatement)
        if is_batch_statement and (not self.column_names or self.column_names[0] != "[applied]"):
            raise RuntimeError("No LWT were present in the BatchStatement")

        if not is_batch_statement and len(self.current_rows) != 1:
            raise RuntimeError("LWT result should have exactly one row. This has %d." % (len(self.current_rows)))

        row = self.current_rows[0]
        if isinstance(row, tuple):
            return row[0]
        else:
            return row['[applied]']

    @property
    def paging_state(self):
        """
        Server paging state of the query. Can be `None` if the query was not paged.

        The driver treats paging state as opaque, but it may contain primary key data, so applications may want to
        avoid sending this to untrusted parties.
        """
        return self.response_future._paging_state
