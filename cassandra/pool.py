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
Connection pooling and host management.
"""
from concurrent.futures import Future
from functools import total_ordering
import logging
import socket
import time
import random
import copy
import uuid
from threading import Lock, RLock, Condition
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import AuthenticationFailed
from cassandra.connection import ConnectionException, EndPoint, DefaultEndPoint
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

    endpoint = None
    """
    The :class:`~.connection.EndPoint` to connect to the node.
    """

    broadcast_address = None
    """
    broadcast address configured for the node, *if available*:

    'system.local.broadcast_address' or 'system.peers.peer' (Cassandra 2-3)
    'system.local.broadcast_address' or 'system.peers_v2.peer' (Cassandra 4)

    This is not present in the ``system.local`` table for older versions of Cassandra. It
    is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    broadcast_port = None
    """
    broadcast port configured for the node, *if available*:

    'system.local.broadcast_port' or 'system.peers_v2.peer_port' (Cassandra 4)

    It is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    broadcast_rpc_address = None
    """
    The broadcast rpc address of the node:

    'system.local.rpc_address' or 'system.peers.rpc_address' (Cassandra 3)
    'system.local.rpc_address' or 'system.peers.native_transport_address (DSE  6+)'
    'system.local.rpc_address' or 'system.peers_v2.native_address (Cassandra 4)'
    """

    broadcast_rpc_port = None
    """
    The broadcast rpc port of the node, *if available*:
    
    'system.local.rpc_port' or 'system.peers.native_transport_port' (DSE 6+)
    'system.local.rpc_port' or 'system.peers_v2.native_port' (Cassandra 4)
    """

    listen_address = None
    """
    listen address configured for the node, *if available*:

    'system.local.listen_address'

    This is only available in the ``system.local`` table for newer versions of Cassandra. It is also not
    queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``. Usually the same as ``broadcast_address``
    unless configured differently in cassandra.yaml.
    """

    listen_port = None
    """
    listen port configured for the node, *if available*:

    'system.local.listen_port'

    This is only available in the ``system.local`` table for newer versions of Cassandra. It is also not
    queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
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

    host_id = None
    """
    The unique identifier of the cassandra node
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
    This is a legacy attribute that does not portray multiple workloads in a uniform fashion.
    See also :attr:`~.Host.dse_workloads`.
    """

    dse_workloads = None
    """
    DSE workloads set, queried from the control connection system tables. Only populated when connecting to
    DSE with this property available (added in DSE 5.1).
    Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    _datacenter = None
    _rack = None
    _reconnection_handler = None
    lock = None

    _currently_handling_node_up = False

    sharding_info = None

    def __init__(self, endpoint, conviction_policy_factory, datacenter=None, rack=None, host_id=None):
        if endpoint is None:
            raise ValueError("endpoint may not be None")
        if conviction_policy_factory is None:
            raise ValueError("conviction_policy_factory may not be None")

        self.endpoint = endpoint if isinstance(endpoint, EndPoint) else DefaultEndPoint(endpoint)
        self.conviction_policy = conviction_policy_factory(self)
        if not host_id:
            host_id = uuid.uuid4()
        self.host_id = host_id
        self.set_location_info(datacenter, rack)
        self.lock = RLock()

    @property
    def address(self):
        """
        The IP address of the endpoint. This is the RPC address the driver uses when connecting to the node.
        """
        # backward compatibility
        return self.endpoint.address

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
            log.debug("Host %s is now marked up", self.endpoint)
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
        if isinstance(other, Host):
            return self.endpoint == other.endpoint
        else:  # TODO Backward compatibility, remove next major
            return self.endpoint.address == other

    def __hash__(self):
        return hash(self.endpoint)

    def __lt__(self, other):
        return self.endpoint < other.endpoint

    def __str__(self):
        return str(self.endpoint)

    def __repr__(self):
        dc = (" %s" % (self._datacenter,)) if self._datacenter else ""
        return "<%s: %s%s>" % (self.__class__.__name__, self.endpoint, dc)


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
    """
    When using v3 of the native protocol, this is used instead of a connection
    pool per host (HostConnectionPool) due to the increased in-flight capacity
    of individual connections.
    """

    host = None
    host_distance = None
    is_shutdown = False
    shutdown_on_error = False

    _session = None
    _lock = None
    _keyspace = None

    # If the number of excess connections exceeds the number of shards times
    # the number below, all excess connections will be closed.
    max_excess_connections_per_shard_multiplier = 3

    tablets_routing_v1 = False

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance
        self._session = weakref.proxy(session)
        self._lock = Lock()
        # this is used in conjunction with the connection streams. Not using the connection lock because the connection can be replaced in the lifetime of the pool.
        self._stream_available_condition = Condition(Lock())
        self._is_replacing = False
        self._connecting = set()
        self._connections = {}
        # A pool of additional connections which are not used but affect how Scylla
        # assigns shards to them. Scylla tends to assign the shard which has
        # the lowest number of connections. If connections are not distributed
        # evenly at the moment, we might need to open several dummy connections
        # to other shards before Scylla returns a connection to the shards we are
        # interested in.
        # After we get at least one connection for each shard, we can close
        # the additional connections.
        self._excess_connections = set()
        # Contains connections which shouldn't be used anymore
        # and are waiting until all requests time out or complete
        # so that we can dispose of them.
        self._trash = set()
        self._shard_connections_futures = []
        self.advanced_shardaware_block_until = 0

        if host_distance == HostDistance.IGNORED:
            log.debug("Not opening connection to ignored host %s", self.host)
            return
        elif host_distance == HostDistance.REMOTE and not session.cluster.connect_to_remote_hosts:
            log.debug("Not opening connection to remote host %s", self.host)
            return

        log.debug("Initializing connection for host %s", self.host)
        first_connection = session.cluster.connection_factory(self.host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
        log.debug("First connection created to %s for shard_id=%i", self.host, first_connection.features.shard_id)
        self._connections[first_connection.features.shard_id] = first_connection
        self._keyspace = session.keyspace

        if self._keyspace:
            first_connection.set_keyspace_blocking(self._keyspace)
        if first_connection.features.sharding_info and not self._session.cluster.shard_aware_options.disable:
            self.host.sharding_info = first_connection.features.sharding_info
            self._open_connections_for_all_shards(first_connection.features.shard_id)
        self.tablets_routing_v1 = first_connection.features.tablets_routing_v1

        log.debug("Finished initializing connection for host %s", self.host)

    def _get_connection_for_routing_key(self, routing_key=None, keyspace=None, table=None):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        if not self._connections:
            raise NoConnectionsAvailable()

        shard_id = None
        if not self._session.cluster.shard_aware_options.disable and self.host.sharding_info and routing_key:
            t = self._session.cluster.metadata.token_map.token_class.from_key(routing_key)
            
            shard_id = None
            if self.tablets_routing_v1 and table is not None:
                if keyspace is None:
                    keyspace = self._keyspace

                tablet = self._session.cluster.metadata._tablets.get_tablet_for_key(keyspace, table, t)

                if tablet is not None:
                    for replica in tablet.replicas:
                        if replica[0] == self.host.host_id:
                            shard_id = replica[1]
                            break

            if shard_id is None:
                shard_id = self.host.sharding_info.shard_id_from_token(t.value)

        conn = self._connections.get(shard_id)

        # missing shard aware connection to shard_id, let's schedule an
        # optimistic try to connect to it
        if shard_id is not None:
            if conn:
                log.debug(
                    "Using connection to shard_id=%i on host %s for routing_key=%s",
                    shard_id,
                    self.host,
                    routing_key
                )
                if conn.orphaned_threshold_reached and shard_id not in self._connecting:
                    # The connection has met its orphaned stream ID limit
                    # and needs to be replaced. Start opening a connection
                    # to the same shard and replace when it is opened.
                    self._connecting.add(shard_id)
                    self._session.submit(self._open_connection_to_missing_shard, shard_id)
                    log.debug(
                        "Connection to shard_id=%i reached orphaned stream limit, replacing on host %s (%s/%i)",
                        shard_id,
                        self.host,
                        len(self._connections.keys()),
                        self.host.sharding_info.shards_count
                    )
            elif shard_id not in self._connecting:
                # rate controlled optimistic attempt to connect to a missing shard
                self._connecting.add(shard_id)
                self._session.submit(self._open_connection_to_missing_shard, shard_id)
                log.debug(
                    "Trying to connect to missing shard_id=%i on host %s (%s/%i)",
                    shard_id,
                    self.host,
                    len(self._connections.keys()),
                    self.host.sharding_info.shards_count
                )

        if conn and not conn.is_closed:
            return conn
        active_connections = [conn for conn in list(self._connections.values()) if not conn.is_closed]
        if active_connections:
            return random.choice(active_connections)
        return random.choice(list(self._connections.values()))

    def borrow_connection(self, timeout, routing_key=None, keyspace=None, table=None):
        conn = self._get_connection_for_routing_key(routing_key, keyspace, table)
        start = time.time()
        remaining = timeout
        last_retry = False
        while True:
            if conn.is_closed:
                # The connection might have been closed in the meantime - if so, try again
                conn = self._get_connection_for_routing_key(routing_key, keyspace, table)
            with conn.lock:
                if (not conn.is_closed or last_retry) and conn.in_flight < conn.max_request_id:
                    # On last retry we ignore connection status, since it is better to return closed connection than
                    #  raise Exception
                    conn.in_flight += 1
                    return conn, conn.get_request_id()
            if timeout is not None:
                remaining = timeout - time.time() + start
                if remaining < 0:
                    # When timeout reached we try to get connection last time and break if it fails
                    if last_retry:
                        break
                    last_retry = True
                    continue
            with self._stream_available_condition:
                if conn.orphaned_threshold_reached and conn.is_closed:
                    conn = self._get_connection()
                else:
                    self._stream_available_condition.wait(remaining)

        raise NoConnectionsAvailable("All request IDs are currently in use")

    def return_connection(self, connection, stream_was_orphaned=False):
        if not stream_was_orphaned:
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
                is_down = self.host.signal_connection_failure(connection.last_error)
                connection.signaled_error = True

            if self.shutdown_on_error and not is_down:
                is_down = True

            if is_down:
                self.shutdown()
                self._session.cluster.on_down(self.host, is_host_addition=False)
            else:
                connection.close()
                with self._lock:
                    if self.is_shutdown:
                        return
                    self._connections.pop(connection.features.shard_id, None)
                    if self._is_replacing:
                        return
                    self._is_replacing = True
                    self._session.submit(self._replace, connection)
        else:
            if connection in self._trash:
                with connection.lock:
                    if connection.in_flight == len(connection.orphaned_request_ids):
                        with self._lock:
                            if connection in self._trash:
                                self._trash.remove(connection)
                                log.debug("Closing trashed connection (%s) to %s", id(connection), self.host)
                                connection.close()
                return

    def on_orphaned_stream_released(self):
        """
        Called when a response for an orphaned stream (timed out on the client
        side) was received.
        """
        with self._stream_available_condition:
            self._stream_available_condition.notify()

    def _replace(self, connection):
        with self._lock:
            if self.is_shutdown:
                return

            log.debug("Replacing connection (%s) to %s", id(connection), self.host)
            try:
                if connection.features.shard_id in self._connections.keys():
                    del self._connections[connection.features.shard_id]
                if self.host.sharding_info and not self._session.cluster.shard_aware_options.disable:
                    self._connecting.add(connection.features.shard_id)
                    self._session.submit(self._open_connection_to_missing_shard, connection.features.shard_id)
                else:
                    connection = self._session.cluster.connection_factory(self.host.endpoint,
                                                                          on_orphaned_stream_released=self.on_orphaned_stream_released)
                    if self._keyspace:
                        connection.set_keyspace_blocking(self._keyspace)
                    self._connections[connection.features.shard_id] = connection
            except Exception:
                log.warning("Failed reconnecting %s. Retrying." % (self.host.endpoint,))
                self._session.submit(self._replace, connection)
            else:
                self._is_replacing = False
                with self._stream_available_condition:
                    self._stream_available_condition.notify()

    def shutdown(self):
        log.debug("Shutting down connections to %s", self.host)
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True
            with self._stream_available_condition:
                self._stream_available_condition.notify_all()

            for future in self._shard_connections_futures:
                future.cancel()

            connections_to_close = self._connections.copy()
            self._connections.clear()

        # connection.close can call pool.return_connection, which will
        #  obtain self._lock via self._stream_available_condition.
        # So, it never should be called within self._lock context
        for connection in connections_to_close.values():
            log.debug("Closing connection (%s) to %s", id(connection), self.host)
            connection.close()

        self._close_excess_connections()

        trash_conns = None
        with self._lock:
            if self._trash:
                trash_conns = self._trash
                self._trash = set()

        if trash_conns:
            for conn in trash_conns:
                conn.close()

    def _close_excess_connections(self):
        with self._lock:
            if not self._excess_connections:
                return
            conns = self._excess_connections.copy()
            self._excess_connections.clear()

        for c in conns:
            log.debug("Closing excess connection (%s) to %s", id(c), self.host)
            c.close()

    def disable_advanced_shard_aware(self, secs):
        log.warning("disabling advanced_shard_aware for %i seconds, could be that this client is behind NAT?", secs)
        self.advanced_shardaware_block_until = max(time.time() + secs, self.advanced_shardaware_block_until)

    def _get_shard_aware_endpoint(self):
        if (self.advanced_shardaware_block_until and self.advanced_shardaware_block_until < time.time()) or \
           self._session.cluster.shard_aware_options.disable_shardaware_port:
            return None

        endpoint = None
        if self._session.cluster.ssl_options and self.host.sharding_info.shard_aware_port_ssl:
            endpoint = copy.copy(self.host.endpoint)
            endpoint._port = self.host.sharding_info.shard_aware_port_ssl
        elif self.host.sharding_info.shard_aware_port:
            endpoint = copy.copy(self.host.endpoint)
            endpoint._port = self.host.sharding_info.shard_aware_port

        return endpoint

    def _open_connection_to_missing_shard(self, shard_id):
        """
        Creates a new connection, checks its shard_id and populates our shard
        aware connections if the current shard_id is missing a connection.

        The `shard_id` parameter is only here to control parallelism on
        attempts to connect. This means that if this attempt finds another
        missing shard_id, we will keep it anyway.

        NOTE: This is an optimistic implementation since we cannot control
        which shard we want to connect to from the client side and depend on
        the round-robin of the system.clients shard_id attribution.

        If we get a duplicate connection to some shard, we put it into the
        excess connection pool. The more connections a particular shard has,
        the smaller the chance that further connections will be assigned
        to that shard.
        """
        with self._lock:
            if self.is_shutdown:
                return
        shard_aware_endpoint = self._get_shard_aware_endpoint()
        log.debug("shard_aware_endpoint=%r", shard_aware_endpoint)

        if shard_aware_endpoint:
            conn = self._session.cluster.connection_factory(shard_aware_endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released,
                                                            shard_id=shard_id,
                                                            total_shards=self.host.sharding_info.shards_count)
            conn.original_endpoint = self.host.endpoint
        else:
            conn = self._session.cluster.connection_factory(self.host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)

        log.debug("Received a connection %s for shard_id=%i on host %s", id(conn), conn.features.shard_id, self.host)
        if self.is_shutdown:
            log.debug("Pool for host %s is in shutdown, closing the new connection (%s)", self.host, id(conn))
            conn.close()
            return

        if shard_aware_endpoint and shard_id != conn.features.shard_id:
            # connection didn't land on expected shared
            # assuming behind a NAT, disabling advanced shard aware for a while
            self.disable_advanced_shard_aware(10 * 60)

        old_conn = self._connections.get(conn.features.shard_id)
        if old_conn is None or old_conn.orphaned_threshold_reached:
            log.debug(
                "New connection (%s) created to shard_id=%i on host %s",
                id(conn),
                conn.features.shard_id,
                self.host
            )
            old_conn = None
            with self._lock:
                if self.is_shutdown:
                    conn.close()
                    return
                if conn.features.shard_id in self._connections.keys():
                    # Move the current connection to the trash and use the new one from now on
                    old_conn = self._connections[conn.features.shard_id]
                    log.debug(
                        "Replacing overloaded connection (%s) with (%s) for shard %i for host %s",
                        id(old_conn),
                        id(conn),
                        conn.features.shard_id,
                        self.host
                    )
                if self._keyspace:
                    conn.set_keyspace_blocking(self._keyspace)

                self._connections[conn.features.shard_id] = conn
            if old_conn is not None:
                remaining = old_conn.in_flight - len(old_conn.orphaned_request_ids)
                if remaining == 0:
                    log.debug(
                        "Immediately closing the old connection (%s) for shard %i on host %s",
                        id(old_conn),
                        old_conn.features.shard_id,
                        self.host
                    )
                    old_conn.close()
                else:
                    log.debug(
                        "Moving the connection (%s) for shard %i to trash on host %s, %i requests remaining",
                        id(old_conn),
                        old_conn.features.shard_id,
                        self.host,
                        remaining,
                    )
                    with self._lock:
                        if self.is_shutdown:
                            old_conn.close()
                        else:
                            self._trash.add(old_conn)
            num_missing_or_needing_replacement = self.num_missing_or_needing_replacement
            log.debug(
                "Connected to %s/%i shards on host %s (%i missing or needs replacement)",
                len(self._connections.keys()),
                self.host.sharding_info.shards_count,
                self.host,
                num_missing_or_needing_replacement
            )
            if num_missing_or_needing_replacement == 0:
                log.debug(
                    "All shards of host %s have at least one connection, closing %i excess connections",
                    self.host,
                    len(self._excess_connections)
                )
                self._close_excess_connections()
        elif self.host.sharding_info.shards_count == len(self._connections.keys()) and self.num_missing_or_needing_replacement == 0:
            log.debug(
                "All shards are already covered, closing newly opened excess connection %s for host %s",
                id(self),
                self.host
            )
            conn.close()
        else:
            if len(self._excess_connections) >= self._excess_connection_limit:
                log.debug(
                    "After connection %s is created excess connection pool size limit (%i) reached for host %s, closing all %i of them",
                    id(conn),
                    self._excess_connection_limit,
                    self.host,
                    len(self._excess_connections)
                )
                self._close_excess_connections()

            log.debug(
                "Putting a connection %s to shard %i to the excess pool of host %s",
                id(conn),
                conn.features.shard_id,
                self.host
            )
            close_connection = False
            with self._lock:
                if self.is_shutdown:
                    close_connection = True
                else:
                    self._excess_connections.add(conn)
            if close_connection:
                conn.close()
        self._connecting.discard(shard_id)

    def _open_connections_for_all_shards(self, skip_shard_id=None):
        """
        Loop over all the shards and try to open a connection to each one.
        """
        with self._lock:
            if self.is_shutdown:
                return

            for shard_id in range(self.host.sharding_info.shards_count):
                if skip_shard_id is not None and skip_shard_id == shard_id:
                    continue
                future = self._session.submit(self._open_connection_to_missing_shard, shard_id)
                if isinstance(future, Future):
                    self._connecting.add(shard_id)
                    self._shard_connections_futures.append(future)

        trash_conns = None
        with self._lock:
            if self._trash:
                trash_conns = self._trash
                self._trash = set()

        if trash_conns is not None:
            for conn in self._trash:
                conn.close()

    def _set_keyspace_for_all_conns(self, keyspace, callback):
        """
        Asynchronously sets the keyspace for all connections.  When all
        connections have been set, `callback` will be called with two
        arguments: this pool, and a list of any errors that occurred.
        """
        remaining_callbacks = set(self._connections.values())
        remaining_callbacks_lock = Lock()
        errors = []

        if not remaining_callbacks:
            callback(self, errors)
            return

        def connection_finished_setting_keyspace(conn, error):
            self.return_connection(conn)
            with remaining_callbacks_lock:
                remaining_callbacks.remove(conn)
            if error:
                errors.append(error)

            if not remaining_callbacks:
                callback(self, errors)

        self._keyspace = keyspace
        for conn in list(self._connections.values()):
            conn.set_keyspace_async(keyspace, connection_finished_setting_keyspace)

    def get_connections(self):
        connections = self._connections
        return list(connections.values()) if connections else []

    def get_state(self):
        in_flights = [c.in_flight for c in list(self._connections.values())]
        orphan_requests = [c.orphaned_request_ids for c in list(self._connections.values())]
        return {'shutdown': self.is_shutdown, 'open_count': self.open_count, \
                'in_flights': in_flights, 'orphan_requests': orphan_requests}

    @property
    def num_missing_or_needing_replacement(self):
        return self.host.sharding_info.shards_count \
            - sum(1 for c in self._connections.values() if not c.orphaned_threshold_reached)

    @property
    def open_count(self):
        return sum([1 if c and not (c.is_closed or c.is_defunct) else 0 for c in list(self._connections.values())])

    @property
    def _excess_connection_limit(self):
        return self.host.sharding_info.shards_count * self.max_excess_connections_per_shard_multiplier


_MAX_SIMULTANEOUS_CREATION = 1
_MIN_TRASH_INTERVAL = 10


class HostConnectionPool(object):
    """
    Used to pool connections to a host for v1 and v2 native protocol.
    """

    host = None
    host_distance = None

    is_shutdown = False
    open_count = 0
    _scheduled_for_creation = 0
    _next_trash_allowed_at = 0
    _keyspace = None

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance

        self._session = weakref.proxy(session)
        self._lock = RLock()
        self._conn_available_condition = Condition()

        log.debug("Initializing new connection pool for host %s", self.host)
        core_conns = session.cluster.get_core_connections_per_host(host_distance)
        self._connections = [session.cluster.connection_factory(host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
                             for i in range(core_conns)]

        self._keyspace = session.keyspace
        if self._keyspace:
            for conn in self._connections:
                conn.set_keyspace_blocking(self._keyspace)

        self._trash = set()
        self._next_trash_allowed_at = time.time()
        self.open_count = core_conns
        log.debug("Finished initializing new connection pool for host %s", self.host)

    def borrow_connection(self, timeout, routing_key=None):
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
            request_id = None
            # to avoid another thread closing this connection while
            # trashing it (through the return_connection process), hold
            # the connection lock from this point until we've incremented
            # its in_flight count
            need_to_wait = False
            with least_busy.lock:
                if least_busy.in_flight < least_busy.max_request_id:
                    least_busy.in_flight += 1
                    request_id = least_busy.get_request_id()
                else:
                    # once we release the lock, wait for another connection
                    need_to_wait = True

            if need_to_wait:
                # wait_for_conn will increment in_flight on the conn
                least_busy, request_id = self._wait_for_conn(timeout)

            # if we have too many requests on this connection but we still
            # have space to open a new connection against this host, go ahead
            # and schedule the creation of a new connection
            if least_busy.in_flight >= max_reqs and len(self._connections) < max_conns:
                self._maybe_spawn_new_connection()

            return least_busy, request_id

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
            log.warning("Failed to create new connection to %s: %s", self.host, exc)
        except Exception:
            log.exception("Unexpectedly failed to create new connection")
        finally:
            with self._lock:
                self._scheduled_for_creation -= 1

    def _add_conn_if_under_max(self):
        max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)
        with self._lock:
            if self.is_shutdown:
                return True

            if self.open_count >= max_conns:
                return True

            self.open_count += 1

        log.debug("Going to open new connection to host %s", self.host)
        try:
            conn = self._session.cluster.connection_factory(self.host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
            if self._keyspace:
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
            log.warning("Failed to add new connection to pool for host %s: %s", self.host, exc)
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
                    if least_busy.in_flight < least_busy.max_request_id:
                        least_busy.in_flight += 1
                        return least_busy, least_busy.get_request_id()

            remaining = timeout - (time.time() - start)

        raise NoConnectionsAvailable()

    def return_connection(self, connection, stream_was_orphaned=False):
        with connection.lock:
            if not stream_was_orphaned:
                connection.in_flight -= 1
            in_flight = connection.in_flight

        if connection.is_defunct or connection.is_closed:
            if not connection.signaled_error:
                log.debug("Defunct or closed connection (%s) returned to pool, potentially "
                          "marking host %s as down", id(connection), self.host)
                is_down = self._session.cluster.signal_connection_failure(
                    self.host, connection.last_error, is_host_addition=False)
                connection.signaled_error = True
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

    def on_orphaned_stream_released(self):
        """
        Called when a response for an orphaned stream (timed out on the client
        side) was received.
        """
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
            connection.close()
            self._session.submit(self._retrying_replace)
        else:
            log.debug("Closing connection (%s) to %s", id(connection), self.host)
            connection.close()

    def _retrying_replace(self):
        replaced = False
        try:
            replaced = self._add_conn_if_under_max()
        except Exception:
            log.exception("Failed replacing connection to %s", self.host)
        if not replaced:
            log.debug("Failed replacing connection to %s. Retrying.", self.host)
            self._session.submit(self._retrying_replace)

    def shutdown(self):
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True

        self._signal_all_available_conn()

        connections_to_close = []
        with self._lock:
            connections_to_close.extend(self._connections)
            self.open_count -= len(self._connections)
            self._connections.clear()
            connections_to_close.extend(self._trash)
            self._trash.clear()

        for conn in connections_to_close:
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
            self.return_connection(conn)
            remaining_callbacks.remove(conn)
            if error:
                errors.append(error)

            if not remaining_callbacks:
                callback(self, errors)

        self._keyspace = keyspace
        for conn in self._connections:
            conn.set_keyspace_async(keyspace, connection_finished_setting_keyspace)

    def get_connections(self):
        return self._connections

    def get_state(self):
        in_flights = [c.in_flight for c in self._connections]
        orphan_requests = [c.orphaned_request_ids for c in self._connections]
        return {'shutdown': self.is_shutdown, 'open_count': self.open_count, \
            'in_flights': in_flights, 'orphan_requests': orphan_requests}
