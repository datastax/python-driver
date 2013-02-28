from threading import RLock

from policies import RoundRobinPolicy, SimpleConvictionPolicy, ExponentialReconnectionPolicy
from connection import Connection

class Session(object):

    def __init__(self, cluster, hosts):
        self.cluster = cluster
        self.hosts = hosts

        self._lock = RLock()
        self._is_shutdown = False
        self._pools = {}
        self._load_balancer = RoundRobinPolicy()

    def execute(self, query):
        pass

    def execute_async(self, query):
        pass

    def prepare(self, query):
        pass

    def shutdown(self):
        pass


class Cluster(object):

    port = 9042

    auth_provider = None

    load_balancing_policy = None
    reconnecting_policy = None
    retry_policy = None

    compression = None
    metrics_enabled = False
    pooling_options = None
    socket_options = None

    def __init__(self, contact_points):
        self.contact_points = contact_points
        self.sessions = set()
        self.metadata = None
        self.conviction_policy_factory = SimpleConvictionPolicy

        self._is_shutdown = False

    def connect(keyspace=None):
        return Session()

    def shutdown():
        pass


class NoHostAvailable(Exception):
    pass


class ControlConnection(object):

    def __init__(self, cluster, metadata):
        self._cluster = cluster
        self._balancing_policy = RoundRobinPolicy()
        self._balancing_policy.populate(cluster, metadata.hosts)
        self._reconnection_policy = ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000)
        self._connection = None

        self._is_shutdown = False

    def connect(self):
        if self._is_shutdown:
            return

    def _reconnect(self):
        errors = {}
        for host in self._balancing_policy:
            try:
                return self._connect_to(host)
            except Exception, exc:
                # TODO logging, catch particular exception types
                errors[host] = exc
                pass

        raise NoHostAvailable("Unable to connect to any servers", errors)

    def _connect_to(self, host):
        # TODO create with cluster connection factory
        # connection = self._cluster.connection_factory.open(host)
        connection = Connection(host)

    def shutdown(self):
        self._is_shutdown = True
        if self._connection:
            self._connection.close()

    def refresh_schema(self, keyspace=None, table=None):
        pass

    def refresh_node_list_and_token_map(self):
        pass
