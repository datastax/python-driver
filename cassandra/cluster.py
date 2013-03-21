import time
from threading import Lock, RLock, Thread
import Queue

from futures import ThreadPoolExecutor

from connection import Connection
from decoder import ConsistencyLevel, QueryMessage
from metadata import Metadata
from policies import RoundRobinPolicy, SimpleConvictionPolicy, ExponentialReconnectionPolicy, HostDistance
from query import SimpleStatement
from pool import ConnectionException, BusyConnectionException

# TODO: we might want to make this configurable
MAX_SCHEMA_AGREEMENT_WAIT_MS = 10000

class Session(object):

    def __init__(self, cluster, hosts):
        self.cluster = cluster
        self.hosts = hosts

        self._lock = RLock()
        self._is_shutdown = False
        self._pools = {}
        self._load_balancer = RoundRobinPolicy()

    def execute(self, query):
        if isinstance(query, basestring):
            query = SimpleStatement(query)

    def execute_async(self, query):
        if isinstance(query, basestring):
            query = SimpleStatement(query)

        qmsg = QueryMessage(query=query.query, consistency_level=query.consistency_level)
        return self._execute_query(qmsg, query)

    def prepare(self, query):
        pass

    def shutdown(self):
        self.cluster.shutdown()

    def _execute_query(self, message, query):
        if query.tracing_enabled:
            # TODO enable tracing on the message
            pass

        errors = {}
        query_plan = self._load_balancer.make_query_plan(query)
        for host in query_plan:
            try:
                result = self._query(host)
                if result:
                    return
            except Exception, exc:
                errors[host] = exc

    def _query(self, host, query):
        pool = self._pools.get(host)
        if not pool or pool.is_shutdown:
            return False

DEFAULT_MIN_REQUESTS = 25
DEFAULT_MAX_REQUESTS = 100

DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST = 2
DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST = 8

DEFAULT_MIN_CONNECTIONS_PER_REMOTE_HOST = 1
DEFAULT_MAX_CONNECTIONS_PER_REMOTE_HOST = 2

class _Scheduler(object):

    def __init__(self, executor):
        self._scheduled = Queue.PriorityQueue()
        self._executor = executor

        t = Thread(target=self.run, name="Task Scheduler")
        t.daemon = True
        t.start()

    def schedule(self, delay, fn, *args, **kwargs):
        run_at = time.time() + delay
        self._scheduled.put_nowait((run_at, (fn, args, kwargs)))

    def run(self):
        while True:
            try:
                while True:
                    run_at, task = self._scheduled.get(block=True, timeout=None)
                    if run_at <= time.time():
                        fn, args, kwargs = task
                        self._executor.submit(fn, *args, **kwargs)
                    else:
                        self._scheduled.put_nowait((run_at, task))
                        break
            except Queue.empty:
                pass

            time.sleep(0.1)


class Cluster(object):

    port = 9042

    auth_provider = None

    load_balancing_policy = None
    reconnecting_policy = None
    retry_policy = None

    compression = None
    metrics_enabled = False
    socket_options = None

    conviction_policy_factory = SimpleConvictionPolicy

    def __init__(self, contact_points):
        self.contact_points = contact_points
        self.sessions = set()
        self.metadata = Metadata(self)

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

        # TODO real factory based on config
        self._connection_factory = Connection

        # TODO make the pool size configurable somewhere
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.scheduler = _Scheduler(self.executor)

        self._is_shutdown = False
        self._lock = Lock()

        self._control_connection = ControlConnection(self, self.metadata)
        try:
            self._control_connection.connect()
        except:
            self.shutdown()
            raise

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
            self.ensure_pool_sizing()

    def get_max_connections_per_host(self, host_distance):
        return self._max_connections_per_host[host_distance]

    def set_max_connections_per_host(self, host_distance, max_connections):
        self._max_connections_per_host[host_distance] = max_connections

    def connect(self, keyspace=None):
        # TODO set keyspace if not None
        return self._new_session()

    def shutdown(self):
        with self._lock:
            if self._is_shutdown:
                return
            else:
                self._is_shutdown = True

        self._control_connection.shutdown()

        for session in self.sessions:
            session.shutdown()

    def _new_session(self):
        session = Session(self, self.metadata.all_hosts())
        self.sessions.add(session)
        return session

    def on_up(self, host):
        pass

    def on_down(self, host):
        pass

    def on_add(self, host):
        self.prepare_all_queries(host)
        self._control_connection.on_add(host)
        for session in self.sessions:  # TODO need to copy/lock?
            session.on_add(host)

    def on_remove(self, host):
        self._control_connection.on_remove(host)
        for session in self.sessions:
            session.on_remove(host)

    def add_host(self, address, signal):
        new_host = self.metadata.add_host(address)
        if new_host and signal:
            self.on_add(new_host)
        return new_host

    def remove_host(self, host):
        if host and self.metdata.remove_host(host):
            self.on_remove(host)

class NoHostAvailable(Exception):
    pass


class ControlConnection(object):

    _SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces"
    _SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies"
    _SELECT_COLUMNS = "SELECT * FROM system.schema_columns"

    _SELECT_PEERS = "SELECT peer, data_center, rack, tokens, rpc_address FROM system.peers"
    _SELECT_LOCAL = "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'"

    _SELECT_SCHEMA_PEERS = "SELECT rpc_address, schema_version FROM system.peers"
    _SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'"

    def __init__(self, cluster, metadata):
        self._cluster = cluster
        self._balancing_policy = RoundRobinPolicy()
        self._balancing_policy.populate(cluster, metadata.all_hosts())
        self._reconnection_policy = ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000)
        self._connection = None
        self._lock = RLock()

        self._is_shutdown = False

    def connect(self):
        if self._is_shutdown:
            return

        self._set_new_connection(self._reconnect_internal())

    def _set_new_connection(self, conn):
        with self._lock:
            old = self._connection
            self._connection = conn

        if old and not old.is_closed():  # TODO is_closed() may not exist
            old.close()

    def _reconnect_internal(self):
        errors = {}
        for host in self._balancing_policy:
            try:
                return self._try_connect(host)
            except ConnectionException, exc:
                errors[host.address] = exc
                host.monitor.signal_connection_failure(exc)
            except Exception, exc:
                errors[host.address] = exc

        raise NoHostAvailable("Unable to connect to any servers", errors)

    def _try_connect(self, host):
        connection = self._cluster.connection_factory.open(host)
        connection.register_watchers({
            "TOPOLOGY_CHANGE": self._handle_topology_change,
            "STATUS_CHANGE": self._handle_status_change,
            "SCHEMA_CHANGE": self._handle_schema_change
        })

        self.refresh_node_list_and_token_map()
        self.refresh_schema()
        return connection

    def shutdown(self):
        self._is_shutdown = True
        with self._lock:
            if self._connection:
                self._connection.close()

    def refresh_schema(self, keyspace=None, table=None):
        where_clause = ""
        if keyspace:
            where_clause = " WHERE keyspace_name = '%s'" % (keyspace,)
            if table:
                where_clause += " AND columnfamily_name = '%s'" % (table,)

        cl = ConsistencyLevel.ONE
        if table:
            ks_query = None
        else:
            ks_query = QueryMessage(query=self.SELECT_KEYSPACES + where_clause, consistency_level=cl)
        cf_query = QueryMessage(query=self.SELECT_COLUMN_FAMILIES + where_clause, consistency_level=cl)
        col_query = QueryMessage(query=self.SELECT_COLUMNS + where_clause, consistency_level=cl)

        if ks_query:
            ks_result, cf_result, col_result = self._connection.wait_for_requests(ks_query, cf_query, col_query)
        else:
            ks_result = None
            cf_result, col_result = self._connection.wait_for_requests(cf_query, col_query)

        self._cluster.metadata.rebuild_schema(keyspace, table, ks_result, cf_result, col_result)

    def refresh_node_list_and_token_map(self):
        conn = self._connection
        if not conn:
            return

        cl = ConsistencyLevel.ONE
        peers_query = QueryMessage(query=self.SELECT_PEERS, consistency_level=cl)
        local_query = QueryMessage(query=self.SELECT_LOCAL, consistency_level=cl)
        try:
            peers_result, local_result = conn.wait_for_requests(peers_query, local_query)
        except (ConnectionException, BusyConnectionException):
            self.reconnect()

        partitioner = None
        token_map = {}

        if local_result and local_result.rows:  # TODO: probably just check local_result.rows
            local_row = local_result.as_dicts()[0]
            cluster_name = local_row["cluster_name"]
            self._cluster.metadata.cluster_name = cluster_name

            host = self._cluster.metadata.get_host(conn.host)
            if host:
                host.set_location_info(local_row["data_center"], local_row["rack"])

            partitioner = local_row.get("partitioner")
            tokens = local_row.get("tokens")
            if partitioner and tokens:
                token_map[host] = tokens

        found_hosts = set()

        for row in peers_result.as_dicts():
            addr = row.get("rpc_address")
            if not addr:
                addr = row.get("peer")
            elif addr == "0.0.0.0":  # TODO handle ipv6 equivalent
                addr = row.get("peer")

            found_hosts.add(addr)

            host = self._cluster.metadata.getHost(addr)
            if host is None:
                host = self._cluster.addHost(addr, True)
            host.set_location_info(row.get("data_center"), row.get("rack"))

            tokens = row.get("tokens")
            if partitioner and tokens:
                token_map[host] = tokens

        for old_host in self._cluster.metadata.all_hosts():
            if old_host.address != conn.address and \
                    old_host.address not in found_hosts:
                self._cluster.remove_host(old_host)

        if partitioner:
            self._cluster.metadata.rebuild_token_map(partitioner, token_map)

    def _handle_topology_change(self, event):
        # TODO schedule on executor
        change_type = event["change_type"]
        addr, port = event["address"]
        if change_type == "NEW_NODE":
            # TODO check Host constructor
            self.add_host(addr)
        elif change_type == "REMOVED_NODE":
            self.remove_host(self._cluster.metadata.get_host(addr))
        elif change_type == "MOVED_NODE":
            self.refresh_node_list_and_token_map()

    def _handle_status_change(self, event):
        # TODO handled async in Cluster.java
        pass

    def _handle_schema_change(self, event):
        # TODO handled async in Cluster.java
        pass

    def wait_for_schema_agreement(self):
        # TODO is returning True/False the best option for this? Potentially raise Exception?
        start = time.time()
        elapsed = 0
        cl = ConsistencyLevel.ONE
        while elapsed < MAX_SCHEMA_AGREEMENT_WAIT_MS:
            peers_query = QueryMessage(query=self.SELECT_SCHEMA_PEERS, consistency_level=cl)
            local_query = QueryMessage(query=self.SELECT_SCHEMA_LOCAL, consistency_level=cl)
            peers_result, local_result = self._connection.wait_for_requests(peers_query, local_query)

            versions = set()
            if local_result and local_result.rows:
                local_row = local_result.as_dicts()[0]
                if local_row.get("schema_version"):
                    versions.add(local_row.get("schema_version"))

            for row in peers_result.as_dicts():
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

            time.sleep(0.2)
            elapsed = time.time() - start

        return False

    @property
    def is_open(self):
        conn = self._connection
        return bool(conn and conn.is_open)

    def on_up(self, host):
        self._balancing_policy.on_up(host)

    def on_down(self, host):
        self._balancing_policy.on_down(host)

        conn = self._connection
        if conn and conn.address == host.address:  # TODO and reconnection_attempt is None
            self.reconnect()

    def on_add(self, host):
        self._balancing_policy.on_add(host)
        self.refresh_node_list_and_token_map()

    def on_remove(self, host):
        self._balancing_policy.on_remove(host)
        self.refresh_node_list_and_token_map()
