# Copyright 2026 ScyllaDB, Inc.
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
Comprehensive integration tests for Client Routes (Private Link) support.

Includes:
- TCP proxy and NLB emulator for simulating private link infrastructure
- Tests verifying all connections go exclusively through the proxy
- Tests for dynamic route updates and topology changes
- Tests for query_routes filtering
"""

import logging
import os
import select
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

import json as _json
import urllib.request

from cassandra.cluster import Cluster
from cassandra.client_routes import ClientRoutesConfig, ClientRouteProxy
from cassandra.connection import ClientRoutesEndPoint, ConnectionException
from cassandra.policies import DynamicWhiteListRoundRobinPolicy, RoundRobinPolicy
from tests.integration import (
    TestCluster,
    get_cluster,
    get_node,
    use_cluster,
    wait_for_node_socket,
    skip_scylla_version_lt,
)
from tests.util import wait_until_not_raised

log = logging.getLogger(__name__)


class ProxyOnlyReachableConnection(Cluster.connection_class):
    """
    Simulates a private-link client that can reach only the proxy endpoint.

    The CCM node addresses are reachable from the local test runner, which means
    the existing client-routes tests cannot reproduce bugs that only appear when
    direct node IPs are private. This connection class rejects those direct node
    addresses while still allowing the NLB address.
    """

    @classmethod
    def factory(cls, endpoint, timeout, host_conn=None, *args, **kwargs):
        address, _ = endpoint.resolve()
        if address.startswith("127.0.0."):
            raise ConnectionException(
                "Simulated private node address %s is unreachable from the client" % address,
                endpoint=endpoint,
            )
        return super().factory(endpoint, timeout, host_conn=host_conn, *args, **kwargs)


class TcpProxy:
    """
    A simple TCP proxy that forwards connections from a local listen port
    to a target (host, port).  Tracks active connections so tests can
    verify that traffic flows through the proxy.
    """

    BUF_SIZE = 65536

    def __init__(self, listen_host, listen_port, target_host, target_port):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port

        self._server_sock = None
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        self._connections = set()
        self.total_connections = 0

    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.listen_host, self.listen_port))
        self.listen_port = self._server_sock.getsockname()[1]
        self._server_sock.listen(128)
        self._server_sock.setblocking(False)
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="proxy-%s:%d" % (self.listen_host, self.listen_port))
        self._thread.start()
        log.info("TcpProxy started %s:%d -> %s:%d",
                 self.listen_host, self.listen_port,
                 self.target_host, self.target_port)

    def stop(self):
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        with self._lock:
            for csock, tsock in list(self._connections):
                self._close_pair(csock, tsock)
            self._connections.clear()
        if self._thread:
            self._thread.join(timeout=5)
        log.info("TcpProxy stopped %s:%d", self.listen_host, self.listen_port)

    @property
    def active_connections(self):
        with self._lock:
            return len(self._connections)

    def retarget(self, new_host, new_port):
        """Change the backend target for new connections (existing ones keep the old target)."""
        self.target_host = new_host
        self.target_port = new_port
        log.info("TcpProxy %s:%d retargeted to %s:%d",
                 self.listen_host, self.listen_port, new_host, new_port)

    def drop_connections(self):
        """Forcibly close all active connections."""
        with self._lock:
            for csock, tsock in list(self._connections):
                self._close_pair(csock, tsock)
            self._connections.clear()
        log.info("TcpProxy %s:%d dropped all connections", self.listen_host, self.listen_port)

    def _run(self):
        while self._running:
            try:
                readable, _, _ = select.select([self._server_sock], [], [], 0.2)
            except (ValueError, OSError):
                break
            for sock in readable:
                if sock is self._server_sock:
                    try:
                        client_sock, _ = self._server_sock.accept()
                    except OSError:
                        continue
                    self._handle_new_connection(client_sock)

    def _handle_new_connection(self, client_sock, target_host=None, target_port=None):
        target_host = target_host or self.target_host
        target_port = target_port or self.target_port
        try:
            target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_sock.connect((target_host, target_port))
        except Exception as e:
            log.warning("TcpProxy %s:%d failed to connect to target %s:%d: %s",
                        self.listen_host, self.listen_port,
                        target_host, target_port, e)
            client_sock.close()
            return

        with self._lock:
            self._connections.add((client_sock, target_sock))
            self.total_connections += 1

        t = threading.Thread(target=self._forward_loop,
                             args=(client_sock, target_sock),
                             daemon=True)
        t.start()

    def _forward_loop(self, client_sock, target_sock):
        try:
            while self._running:
                readable, _, _ = select.select([client_sock, target_sock], [], [], 0.5)
                for sock in readable:
                    data = sock.recv(self.BUF_SIZE)
                    if not data:
                        return
                    if sock is client_sock:
                        target_sock.sendall(data)
                    else:
                        client_sock.sendall(data)
        except (OSError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            with self._lock:
                self._connections.discard((client_sock, target_sock))
            self._close_pair(client_sock, target_sock)

    @staticmethod
    def _close_pair(csock, tsock):
        for s in (csock, tsock):
            try:
                s.close()
            except Exception:
                pass


class NLBEmulator:
    """
    Emulates a Network Load Balancer for a CCM cluster.

    Provides:
    - One *discovery port* (round-robin across all live nodes, used as the
      driver's ``contact_points``).
    - One *per-node port* for each node (dedicated proxy to that node's
      native transport port).

    All proxies listen on ``LISTEN_HOST`` (127.254.254.101), an address
    outside the CCM node range, simulating a real NLB endpoint.

    Port layout (all ports are OS-assigned by default):
        LISTEN_HOST:discovery_port        ->  round-robin to all live nodes
        LISTEN_HOST:<os-assigned>         ->  node1 (127.0.0.1:9042)
        LISTEN_HOST:<os-assigned>         ->  node2 (127.0.0.2:9042)
        ...

    Automatically creates/removes per-node proxies when nodes are
    added/removed so CCM cluster operations are reflected seamlessly.
    """

    LISTEN_HOST = "127.254.254.101"

    def __init__(self, discovery_port=0,
                 per_node_base=0,
                 native_port=9042,
                 node_addresses=None):
        self.discovery_port = discovery_port
        self.per_node_base = per_node_base
        self.native_port = native_port
        self._deferred_node_addresses = node_addresses

        self._node_proxies = {}
        self._discovery_proxy = None
        self._rr_index = 0
        self._lock = threading.Lock()
        self._running = False

    def start(self, node_addresses):
        """
        Start the NLB with an initial set of node addresses.

        :param node_addresses: dict of node_id -> ip_address, e.g.
                               {1: "127.0.0.1", 2: "127.0.0.2"}
        """
        self._running = True
        try:
            for node_id, addr in node_addresses.items():
                self._add_node_proxy(node_id, addr)

            first_addr = list(node_addresses.values())[0]
            self._discovery_proxy = TcpProxy(
                self.LISTEN_HOST, self.discovery_port,
                first_addr, self.native_port,
            )
            self._discovery_proxy.start()
            self.discovery_port = self._discovery_proxy.listen_port
        except Exception:
            self.stop()
            raise
        original_handler = self._discovery_proxy._handle_new_connection

        def rr_handler(client_sock):
            addrs = self._live_addresses()
            if not addrs:
                client_sock.close()
                return
            idx = self._rr_index % len(addrs)
            self._rr_index += 1
            addr = addrs[idx]
            original_handler(client_sock, target_host=addr, target_port=self.native_port)

        self._discovery_proxy._handle_new_connection = rr_handler

        log.info("NLB started: discovery=%s:%d, %d node proxies",
                 self.LISTEN_HOST, self.discovery_port, len(self._node_proxies))
        return self

    def __enter__(self):
        if not self._running and self._deferred_node_addresses is not None:
            self.start(self._deferred_node_addresses)
        return self

    def __exit__(self, *args):
        self.stop()

    def stop(self):
        self._running = False
        if self._discovery_proxy:
            self._discovery_proxy.stop()
        for proxy in self._node_proxies.values():
            proxy.stop()
        self._node_proxies.clear()
        log.info("NLB stopped")

    def add_node(self, node_id, addr):
        self._add_node_proxy(node_id, addr)

    def remove_node(self, node_id):
        with self._lock:
            proxy = self._node_proxies.pop(node_id, None)
        if proxy:
            proxy.stop()
            log.info("NLB removed node %d", node_id)

    def node_port(self, node_id):
        proxy = self._node_proxies.get(node_id)
        if proxy:
            return proxy.listen_port
        return self.per_node_base + node_id

    def get_node_proxy(self, node_id):
        return self._node_proxies.get(node_id)

    def total_proxy_connections(self):
        return sum(p.total_connections for p in self._node_proxies.values())

    def active_proxy_connections(self):
        return sum(p.active_connections for p in self._node_proxies.values())

    def drop_all_connections(self):
        for proxy in self._node_proxies.values():
            proxy.drop_connections()
        if self._discovery_proxy:
            self._discovery_proxy.drop_connections()

    def _add_node_proxy(self, node_id, addr):
        port = 0
        proxy = TcpProxy(self.LISTEN_HOST, port, addr, self.native_port)
        proxy.start()
        with self._lock:
            self._node_proxies[node_id] = proxy
        log.info("NLB added node %d: %s:%d -> %s:%d",
                 node_id, self.LISTEN_HOST, port, addr, self.native_port)

    def _live_addresses(self):
        """IPs of nodes with active proxies."""
        return [p.target_host for p in self._node_proxies.values()]

def post_client_routes(contact_point, routes):
    """
    Post client routes to Scylla's REST API.

    :param contact_point: IP/hostname of a Scylla node (e.g. "127.0.0.1")
    :param routes: List of route dicts with keys: connection_id, host_id, address, port
                   and optionally tls_port
    """
    payload = []
    for route in routes:
        entry = {
            "connection_id": str(route["connection_id"]),
            "host_id": str(route["host_id"]),
            "address": route["address"],
            "port": route["port"],
        }
        if route.get("tls_port") is not None:
            entry["tls_port"] = route["tls_port"]
        payload.append(entry)

    url = "http://%s:10000/v2/client-routes" % contact_point
    log.info("Posting %d routes to %s", len(payload), url)
    data = _json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    response = urllib.request.urlopen(req)
    log.info("Routes posted successfully (status %d)", response.status)


def get_host_ids_from_cluster(session):
    """
    Build a mapping of rpc_address -> host_id for all nodes in the cluster.

    Uses the driver's metadata rather than querying system.local / system.peers
    directly, because those queries can be routed to different coordinators
    (system.local returns the coordinator's own info while system.peers omits
    the coordinator), leading to a node being missing from the map.
    """
    host_id_map = {}
    for host in session.cluster.metadata.all_hosts():
        host_id_map[host.address] = host.host_id
    return host_id_map


def build_routes_for_nlb(connection_id, host_id_map, nlb):
    """
    Build routes that direct each host_id through the NLB per-node proxy.

    :param connection_id: Connection ID string
    :param host_id_map: dict ip -> uuid host_id (from get_host_ids_from_cluster)
    :param nlb: NLBEmulator instance
    :return: list of route dicts
    """
    routes = []
    for ip, host_id in host_id_map.items():
        node_id = int(ip.split(".")[-1])
        port = nlb.node_port(node_id)
        routes.append({
            "connection_id": connection_id,
            "host_id": host_id,
            "address": NLBEmulator.LISTEN_HOST,
            "port": port,
        })
    return routes


def post_routes_for_nlb(contact_point, connection_id, host_id_map, nlb):
    """Build routes for the NLB and POST them via the REST API."""
    routes = build_routes_for_nlb(connection_id, host_id_map, nlb)
    post_client_routes(contact_point, routes)
    return routes

def wait_for_routes_visible(session, connection_id, expected_count, timeout=10, poll_interval=0.1):
    """
    Poll system.client_routes on **every** node until each one sees at
    least *expected_count* rows for *connection_id*.

    ``system.client_routes`` is a node-local table, so routes posted via
    the REST API to one node are not guaranteed to be visible on the
    others at the same time.  This helper ensures they have propagated
    everywhere before the test proceeds.

    :param session: an active driver Session (direct, not through NLB)
    :param connection_id: the connection_id string to filter on
    :param expected_count: how many rows we expect to see per node
    :param timeout: maximum seconds to wait
    :param poll_interval: seconds between polls
    """
    all_hosts = list(session.cluster.metadata.all_hosts())
    deadline = time.time() + timeout
    while True:
        pending_hosts = []
        for host in all_hosts:
            rows = list(session.execute(
                "SELECT * FROM system.client_routes WHERE connection_id = %s",
                (connection_id,),
                host=host,
            ))
            if len(rows) < expected_count:
                pending_hosts.append((host, len(rows)))
        if not pending_hosts:
            return
        if time.time() >= deadline:
            details = ", ".join(
                "%s: %d" % (h.address, count) for h, count in pending_hosts
            )
            raise RuntimeError(
                "Timed out waiting for %d routes (connection_id=%s) to appear "
                "in system.client_routes on all nodes; pending: %s"
                % (expected_count, connection_id, details)
            )
        time.sleep(poll_interval)


def node_id_from_ip(ip):
    """Extract node_id from an IP like '127.0.0.3' -> 3."""
    return int(ip.split(".")[-1])


def assert_routes_via_nlb(test, cluster, nlb, expected_node_ids):
    """
    Assert that every host in *expected_node_ids* has its endpoint
    resolving through the NLB (correct address and per-node port).
    """
    nlb_listen_host = NLBEmulator.LISTEN_HOST
    expected_node_ids = set(expected_node_ids)

    seen_node_ids = set()
    for host in cluster.metadata.all_hosts():
        ep = host.endpoint
        if not isinstance(ep, ClientRoutesEndPoint):
            continue
        node_id = node_id_from_ip(ep.address)
        if node_id not in expected_node_ids:
            continue
        resolved_addr, resolved_port = ep.resolve()
        test.assertEqual(
            resolved_addr, nlb_listen_host,
            "Node %d endpoint should resolve to NLB address %s, got %s"
            % (node_id, nlb_listen_host, resolved_addr),
        )
        test.assertEqual(
            resolved_port, nlb.node_port(node_id),
            "Node %d endpoint should resolve to NLB port %d, got %d"
            % (node_id, nlb.node_port(node_id), resolved_port),
        )
        seen_node_ids.add(node_id)
    test.assertEqual(
        seen_node_ids, expected_node_ids,
        "Not all expected nodes found in metadata endpoints",
    )


def assert_routes_direct(test, cluster, expected_node_ids, direct_port=9042):
    """
    Assert that every host in *expected_node_ids* has its endpoint
    resolving to the node's own IP on *direct_port*.
    """
    expected_node_ids = set(expected_node_ids)

    for host in cluster.metadata.all_hosts():
        ep = host.endpoint
        if not isinstance(ep, ClientRoutesEndPoint):
            continue
        node_id = node_id_from_ip(ep.address)
        if node_id not in expected_node_ids:
            continue
        resolved_addr, resolved_port = ep.resolve()
        expected_ip = "127.0.0.%d" % node_id
        test.assertEqual(
            resolved_addr, expected_ip,
            "Node %d endpoint should resolve to direct address %s, got %s"
            % (node_id, expected_ip, resolved_addr),
        )
        test.assertEqual(
            resolved_port, direct_port,
            "Node %d endpoint should resolve to direct port %d, got %d"
            % (node_id, direct_port, resolved_port),
        )


_saved_scylla_ext_opts = None


def setup_module():
    global _saved_scylla_ext_opts
    _saved_scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS')
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
    use_cluster('shared_aware', [3], start=True)


def teardown_module():
    if _saved_scylla_ext_opts is None:
        os.environ.pop('SCYLLA_EXT_OPTS', None)
    else:
        os.environ['SCYLLA_EXT_OPTS'] = _saved_scylla_ext_opts


class TestProxyConnectivityWithoutClientRoutes(unittest.TestCase):
    """
    Reproducer for connecting through a generic proxy when node addresses are
    not reachable from the client.

    The initial control connection can reach the cluster through the proxy, but
    the driver later tries to open pools to the discovered node addresses
    directly. In a proxy-only environment that makes connect/query fail.
    """

    @classmethod
    def setUpClass(cls):
        cls.node_addrs = {
            1: "127.0.0.1",
            2: "127.0.0.2",
            3: "127.0.0.3",
        }
        cls.proxy_node_id = 1
        cls.nlb = NLBEmulator()
        cls.nlb.start(cls.node_addrs)

    @classmethod
    def tearDownClass(cls):
        cls.nlb.stop()

    def _make_proxy_cluster(self):
        return Cluster(
            contact_points=[NLBEmulator.LISTEN_HOST],
            port=self.nlb.node_port(self.proxy_node_id),
            connection_class=ProxyOnlyReachableConnection,
            load_balancing_policy=DynamicWhiteListRoundRobinPolicy(),
        )

    def test_dynamic_whitelist_session_succeeds_when_only_proxy_is_reachable(self):
        cluster = self._make_proxy_cluster()
        self.addCleanup(cluster.shutdown)

        session = cluster.connect()
        row = session.execute(
            "SELECT release_version FROM system.local WHERE key='local'"
        ).one()

        self.assertIsNotNone(row)
        pool_state = session.get_pool_state()
        self.assertEqual(len(pool_state), 1)

        session_host = next(iter(pool_state))
        self.assertEqual(session_host.endpoint.address, NLBEmulator.LISTEN_HOST)
        self.assertEqual(session_host.endpoint.port, self.nlb.node_port(self.proxy_node_id))

@skip_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                        scylla_version="2026.1.0")
class TestGetHostPortMapping(unittest.TestCase):
    """
    Test _query_all_routes_for_connections and _query_routes_for_change_event
    methods with different filtering scenarios.
    """

    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster(client_routes_config=ClientRoutesConfig(
            proxies=[ClientRouteProxy("conn_id", "127.0.0.1")]))
        cls.session = cls.cluster.connect()

        cls.host_ids = [uuid.uuid4() for _ in range(3)]
        cls.connection_ids = [str(uuid.uuid4()) for _ in range(3)]
        cls.expected = []

        for idx, host_id in enumerate(cls.host_ids):
            ip = f"127.0.0.{idx + 1}"
            for connection_id in cls.connection_ids:
                cls.expected.append({
                    'connection_id': connection_id,
                    'host_id': host_id,
                    'address': ip,
                    'port': 9042,
                    'tls_port': 9142,
                })

        cls._sort_routes(cls.expected)
        post_client_routes(cls.cluster.contact_points[0], cls.expected)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()

    @staticmethod
    def _sort_routes(routes):
        routes.sort(key=lambda r: (str(r['connection_id']), str(r['host_id'])))

    def _routes_to_dicts(self, routes):
        """Convert _Route objects to comparable dicts, adjusting port for ssl_enabled."""
        return [
            {
                'connection_id': route.connection_id,
                'host_id': route.host_id,
                'address': route.address,
                'port': route.port,
            }
            for route in routes
        ]

    def _expected_dicts(self, expected):
        """Build expected dicts with tls_port or port based on ssl_enabled."""
        port_key = 'tls_port' if self.cluster._client_routes_handler.ssl_enabled else 'port'
        return [
            {
                'connection_id': e['connection_id'],
                'host_id': e['host_id'],
                'address': e['address'],
                'port': e[port_key],
            }
            for e in expected
        ]

    def test_get_all_routes_for_all_connections(self):
        """Querying all connection IDs returns every route."""
        cc = self.cluster.control_connection
        routes = self.cluster._client_routes_handler._query_all_routes_for_connections(
            cc._connection, cc._timeout, self.connection_ids,
        )
        got = self._routes_to_dicts(routes)
        self._sort_routes(got)
        expected = self._expected_dicts(self.expected)
        self._sort_routes(expected)
        self.assertEqual(got, expected)

    def test_get_routes_for_single_connection(self):
        """Querying a single connection ID returns only its routes."""
        cc = self.cluster.control_connection
        routes = self.cluster._client_routes_handler._query_all_routes_for_connections(
            cc._connection, cc._timeout, [self.connection_ids[0]],
        )
        got = self._routes_to_dicts(routes)
        self._sort_routes(got)
        filtered = [r for r in self.expected
                    if r['connection_id'] == self.connection_ids[0]]
        expected = self._expected_dicts(filtered)
        self._sort_routes(expected)
        self.assertEqual(got, expected)

    def test_get_routes_for_change_event_all_pairs(self):
        """Querying all (connection_id, host_id) pairs returns every route."""
        cc = self.cluster.control_connection
        pairs = [(r['connection_id'], r['host_id']) for r in self.expected]
        routes = self.cluster._client_routes_handler._query_routes_for_change_event(
            cc._connection, cc._timeout, pairs,
        )
        got = self._routes_to_dicts(routes)
        self._sort_routes(got)
        expected = self._expected_dicts(self.expected)
        self._sort_routes(expected)
        self.assertEqual(got, expected)

    def test_get_routes_for_change_event_single_pair(self):
        """Querying a single (connection_id, host_id) pair returns one route."""
        cc = self.cluster.control_connection
        target_conn_id = self.connection_ids[0]
        target_host_id = self.host_ids[0]
        routes = self.cluster._client_routes_handler._query_routes_for_change_event(
            cc._connection, cc._timeout, [(target_conn_id, target_host_id)],
        )
        got = self._routes_to_dicts(routes)
        self._sort_routes(got)
        filtered = [r for r in self.expected
                    if r['connection_id'] == target_conn_id
                    and r['host_id'] == target_host_id]
        expected = self._expected_dicts(filtered)
        self._sort_routes(expected)
        self.assertEqual(got, expected)

@skip_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                        scylla_version="2026.1.0")
class TestPrivateLinkConnectivity(unittest.TestCase):
    """
    Verifies the driver connects to all cluster nodes exclusively through
    the NLB proxy, never directly.

    Setup:
    1. Start a 3-node CCM cluster (done by setup_module).
    2. Start an NLB emulator with per-node proxies.
    3. Use a direct session to read host_ids, then POST client routes
       pointing each host_id at the NLB proxy port.
    4. Create a client-routes-enabled session using the NLB discovery
       port as the contact point.
    5. Verify all driver connections go through proxy ports.
    """

    @classmethod
    def setUpClass(cls):
        cls.direct_cluster = TestCluster()
        cls.direct_session = cls.direct_cluster.connect()
        cls.host_id_map = get_host_ids_from_cluster(cls.direct_session)
        log.info("Host ID map: %s", cls.host_id_map)

        cls.node_addrs = {}
        for ip in cls.host_id_map:
            node_id = int(ip.split(".")[-1])
            cls.node_addrs[node_id] = ip

        cls.nlb = NLBEmulator()
        cls.nlb.start(cls.node_addrs)

        cls.connection_id = str(uuid.uuid4())
        post_routes_for_nlb("127.0.0.1", cls.connection_id, cls.host_id_map, cls.nlb)
        wait_for_routes_visible(cls.direct_session, cls.connection_id, len(cls.host_id_map))

    @classmethod
    def tearDownClass(cls):
        cls.direct_cluster.shutdown()
        cls.nlb.stop()

    def _make_client_routes_cluster(self, **extra_kwargs):
        """Create a Cluster configured with client-routes pointing at the NLB."""
        return Cluster(
            contact_points=[NLBEmulator.LISTEN_HOST],
            port=self.nlb.discovery_port,
            client_routes_config=ClientRoutesConfig(
                proxies=[ClientRouteProxy(self.connection_id, NLBEmulator.LISTEN_HOST)],
            ),
            load_balancing_policy=RoundRobinPolicy(),
            **extra_kwargs,
        )

    def test_all_connections_through_proxy(self):
        """Every pool connection must go through the NLB proxy, not directly."""
        with self._make_client_routes_cluster() as cluster:
            session = cluster.connect(wait_for_all_pools=True)

            for _ in range(50):
                session.execute("SELECT key FROM system.local")

            pool_state = session.get_pool_state()
            self.assertEqual(len(pool_state), len(self.node_addrs),
                             "Driver should have pools for all nodes")

            for host, state in pool_state.items():
                node_id = node_id_from_ip(host.address)
                proxy = self.nlb.get_node_proxy(node_id)
                self.assertIsNotNone(proxy, f"No proxy for node {node_id}")
                open_count = state['open_count']
                self.assertGreaterEqual(
                    proxy.total_connections, open_count,
                    f"Node {node_id} proxy saw {proxy.total_connections} "
                    f"connections but pool has {open_count} open — "
                    f"some connections bypassed the proxy")

            assert_routes_via_nlb(self, cluster, self.nlb,
                                  self.node_addrs.keys())

    def test_queries_succeed_through_proxy(self):
        """Queries should work normally through the proxy."""
        with self._make_client_routes_cluster() as cluster:
            session = cluster.connect()
            session.execute(
                "CREATE KEYSPACE IF NOT EXISTS test_cr_ks "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
            )
            session.execute(
                "CREATE TABLE IF NOT EXISTS test_cr_ks.t (k int PRIMARY KEY, v text)"
            )
            session.execute("INSERT INTO test_cr_ks.t (k, v) VALUES (1, 'hello')")
            row = session.execute("SELECT v FROM test_cr_ks.t WHERE k = 1").one()
            self.assertEqual(row.v, "hello")

            assert_routes_via_nlb(self, cluster, self.nlb,
                                  self.node_addrs.keys())

    def test_connection_recovery_after_proxy_drop(self):
        """
        After the proxy drops all connections, the driver should reconnect
        (still through the proxy).
        """
        with self._make_client_routes_cluster() as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            session.execute("SELECT key FROM system.local")

            assert_routes_via_nlb(self, cluster, self.nlb,
                                  self.node_addrs.keys())

            self.nlb.drop_all_connections()

            def query_ok():
                session.execute("SELECT key FROM system.local")

            wait_until_not_raised(query_ok, 1, 30)

            assert_routes_via_nlb(self, cluster, self.nlb,
                                  self.node_addrs.keys())

@skip_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                        scylla_version="2026.1.0")
class TestDynamicRouteUpdates(unittest.TestCase):
    """
    Verify that when routes are updated (e.g. port changes), the driver
    picks up the new routes and reconnects through the new proxy ports
    after existing connections are dropped.
    """

    @classmethod
    def setUpClass(cls):
        cls.direct_cluster = TestCluster()
        cls.direct_session = cls.direct_cluster.connect()
        cls.host_id_map = get_host_ids_from_cluster(cls.direct_session)

        cls.node_addrs = {}
        for ip in cls.host_id_map:
            node_id = int(ip.split(".")[-1])
            cls.node_addrs[node_id] = ip

        cls.connection_id = str(uuid.uuid4())

    @classmethod
    def tearDownClass(cls):
        cls.direct_cluster.shutdown()

    def test_route_update_causes_reconnect_to_new_port(self):
        """
        1. Start NLB v1, post routes -> driver connects through v1 ports.
        2. Start NLB v2 on different ports, post new routes.
        3. Drop v1 connections.
        4. Driver should reconnect through v2 ports.
        """
        with NLBEmulator(
            node_addresses=self.node_addrs,
        ) as nlb_v1, NLBEmulator(
            node_addresses=self.node_addrs,
        ) as nlb_v2:
            post_routes_for_nlb("127.0.0.1", self.connection_id,
                                self.host_id_map, nlb_v1)
            wait_for_routes_visible(self.direct_session, self.connection_id, len(self.host_id_map))

            with Cluster(
                contact_points=[NLBEmulator.LISTEN_HOST],
                port=nlb_v1.discovery_port,
                client_routes_config=ClientRoutesConfig(
                    proxies=[ClientRouteProxy(self.connection_id, NLBEmulator.LISTEN_HOST)],
                ),
                load_balancing_policy=RoundRobinPolicy(),
            ) as cluster:
                session = cluster.connect(wait_for_all_pools=True)
                session.execute("SELECT key FROM system.local")

                for node_id in self.node_addrs:
                    self.assertGreater(
                        nlb_v1.get_node_proxy(node_id).total_connections, 0)
                assert_routes_via_nlb(self, cluster, nlb_v1,
                                      self.node_addrs.keys())

                post_routes_for_nlb("127.0.0.1", self.connection_id,
                                    self.host_id_map, nlb_v2)
                time.sleep(2)  # let CLIENT_ROUTES_CHANGE propagate

                # Stop v1 per-node proxies entirely so v1 ports become
                # unreachable, forcing the driver to reconnect through v2.
                # (Merely dropping connections is insufficient because v1
                # proxies would still accept new connections before the
                # route update propagates.)
                for node_id in list(self.node_addrs.keys()):
                    nlb_v1.remove_node(node_id)

                def all_nodes_via_v2():
                    session.execute("SELECT key FROM system.local")
                    for nid in self.node_addrs:
                        assert nlb_v2.get_node_proxy(nid).total_connections > 0, \
                            "NLB v2 node %d proxy has no connections yet" % nid

                wait_until_not_raised(all_nodes_via_v2, 1, 30)

                assert_routes_via_nlb(self, cluster, nlb_v2,
                                      self.node_addrs.keys())


def _generate_ssl_certs(cert_dir, node_ips):
    """
    Generate test SSL certificates with SANs covering the given node IPs.

    File names follow CCM's ``ScyllaCluster.enable_ssl()`` convention so the
    resulting directory can be passed directly to ``enable_ssl(cert_dir, ...)``.

    Creates:
      - ca.key / ca.crt: self-signed CA
      - ccm_node.key / ccm_node.pem: server cert signed by CA with SANs for all node_ips

    :param cert_dir: directory to write files into (must exist)
    :param node_ips: list of IP strings to include as SANs (e.g. ["127.0.0.1", "127.0.0.2"])
    """
    if shutil.which("openssl") is None:
        raise unittest.SkipTest("openssl not found on PATH; skipping SSL cert generation")

    san_cnf = os.path.join(cert_dir, "san.cnf")
    san_value = ",".join("IP:%s" % ip for ip in node_ips)
    with open(san_cnf, "w") as f:
        f.write("subjectAltName=%s\n" % san_value)

    def _run(cmd):
        result = subprocess.run(cmd, cwd=cert_dir, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("Command failed: %s\n%s" % (" ".join(cmd), result.stderr))

    _run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
          "-keyout", "ca.key", "-out", "ca.crt",
          "-days", "1", "-nodes", "-subj", "/CN=Test CA"])

    _run(["openssl", "req", "-newkey", "rsa:2048",
          "-keyout", "ccm_node.key", "-out", "ccm_node.csr",
          "-nodes", "-subj", "/CN=Test Server"])

    _run(["openssl", "x509", "-req",
          "-in", "ccm_node.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
          "-CAcreateserial", "-out", "ccm_node.pem",
          "-days", "1", "-extfile", "san.cnf"])

    log.info("Generated SSL certs in %s with SANs: %s", cert_dir, san_value)


@skip_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                        scylla_version="2026.1.0")
class TestMixedDirectAndNlbConnections(unittest.TestCase):
    """
    Verify the cluster works when some nodes are accessed through the NLB
    proxy and others are accessed directly (no route posted, falls back
    to the default endpoint).
    """

    @classmethod
    def setUpClass(cls):
        cls.direct_cluster = TestCluster()
        cls.direct_session = cls.direct_cluster.connect()
        cls.host_id_map = get_host_ids_from_cluster(cls.direct_session)

        cls.node_addrs = {}
        for ip in cls.host_id_map:
            node_id = int(ip.split(".")[-1])
            cls.node_addrs[node_id] = ip

        cls.connection_id = str(uuid.uuid4())

    @classmethod
    def tearDownClass(cls):
        cls.direct_cluster.shutdown()

    def test_mixed_direct_and_nlb_connections(self):
        """
        Post routes for only a subset of nodes (through NLB proxy).
        Remaining nodes have no route and fall back to direct connections.
        Queries should work through both paths.
        """
        proxied_node_id = min(self.node_addrs.keys())
        proxied_ip = self.node_addrs[proxied_node_id]

        with NLBEmulator(
            node_addresses={proxied_node_id: proxied_ip},
        ) as nlb:
            proxied_host_id = self.host_id_map[proxied_ip]
            routes = [{
                "connection_id": self.connection_id,
                "host_id": proxied_host_id,
                "address": NLBEmulator.LISTEN_HOST,
                "port": nlb.node_port(proxied_node_id),
            }]
            post_client_routes("127.0.0.1", routes)
            time.sleep(1)

            with Cluster(
                contact_points=["127.0.0.1"],
                client_routes_config=ClientRoutesConfig(
                    proxies=[ClientRouteProxy(self.connection_id, NLBEmulator.LISTEN_HOST)],
                ),
                load_balancing_policy=RoundRobinPolicy(),
            ) as cluster:
                session = cluster.connect(wait_for_all_pools=True)

                for _ in range(50):
                    session.execute("SELECT key FROM system.local")

                assert_routes_via_nlb(self, cluster, nlb,
                                      [proxied_node_id])

                direct_node_ids = set(self.node_addrs.keys()) - {proxied_node_id}
                assert_routes_direct(self, cluster, direct_node_ids)

                proxy = nlb.get_node_proxy(proxied_node_id)
                self.assertGreater(proxy.total_connections, 0,
                                   "Proxied node should have connections through NLB")


@skip_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                        scylla_version="2026.1.0")
class TestSslThroughNlb(unittest.TestCase):
    """
    Verify SSL with check_hostname=False works through the NLB proxy.

    When using client routes, connections go through NLB proxies whose
    addresses won't match server certificates, so hostname verification
    must be disabled.  Certificate chain validation (verify_mode=CERT_REQUIRED)
    is still active — only hostname matching is skipped.

    The driver raises ValueError at Cluster init time if check_hostname=True
    is used with client_routes_config.
    """

    @classmethod
    def setUpClass(cls):
        cls.direct_cluster = TestCluster()
        cls.direct_session = cls.direct_cluster.connect()
        cls.host_id_map = get_host_ids_from_cluster(cls.direct_session)
        cls.direct_cluster.shutdown()

        cls.node_addrs = {}
        for ip in cls.host_id_map:
            node_id = int(ip.split(".")[-1])
            cls.node_addrs[node_id] = ip

        cls.connection_id = str(uuid.uuid4())

        cls.cert_dir = tempfile.mkdtemp(prefix="client-routes-ssl-")
        cert_ips = list(cls.node_addrs.values())
        _generate_ssl_certs(cls.cert_dir, cert_ips)

        cls.ccm_cluster = get_cluster()
        cls.ccm_cluster.stop()
        cls.ccm_cluster.set_configuration_options({
            'client_encryption_options': {
                'enabled': True,
                'certificate': os.path.join(cls.cert_dir, "ccm_node.pem"),
                'keyfile': os.path.join(cls.cert_dir, "ccm_node.key"),
            }
        })
        cls.ccm_cluster.start(wait_for_binary_proto=True)

    @classmethod
    def tearDownClass(cls):
        cls.ccm_cluster.stop()
        cls.ccm_cluster.set_configuration_options({
            'client_encryption_options': {
                'enabled': False,
            }
        })
        cls.ccm_cluster.start(wait_for_binary_proto=True)

        shutil.rmtree(cls.cert_dir, ignore_errors=True)

    def test_ssl_without_hostname_verification_through_nlb(self):
        """
        Connect through NLB with SSL but check_hostname=False.

        When using client routes, connections go through NLB proxies
        whose addresses won't match server certificates, so hostname
        verification must be disabled.  Certificate chain validation
        (verify_mode=CERT_REQUIRED) is still active.
        """
        with NLBEmulator(
            node_addresses=self.node_addrs,
        ) as nlb:
            routes = build_routes_for_nlb(
                self.connection_id, self.host_id_map, nlb,
            )
            for route in routes:
                route["tls_port"] = route["port"]
            post_client_routes("127.0.0.1", routes)

            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_ctx.check_hostname = False
            ssl_ctx.load_verify_locations(os.path.join(self.cert_dir, 'ca.crt'))

            self.assertFalse(ssl_ctx.check_hostname,
                             "check_hostname must be False for this test")
            self.assertEqual(ssl_ctx.verify_mode, ssl.CERT_REQUIRED,
                             "verify_mode must be CERT_REQUIRED")

            def routes_visible():
                with TestCluster(
                    contact_points=["127.0.0.1"],
                    ssl_context=ssl_ctx, connect_timeout=30,
                ) as c:
                    session = c.connect()
                    rs = session.execute(
                        "SELECT * FROM system.client_routes "
                        "WHERE connection_id = %s ALLOW FILTERING",
                        (self.connection_id,)
                    )
                    return len(list(rs)) >= len(self.host_id_map)

            wait_until_not_raised(
                lambda: self.assertTrue(routes_visible()),
                1, 30,
            )

            with Cluster(
                contact_points=[NLBEmulator.LISTEN_HOST],
                port=nlb.discovery_port,
                ssl_context=ssl_ctx,
                client_routes_config=ClientRoutesConfig(
                    proxies=[ClientRouteProxy(self.connection_id, NLBEmulator.LISTEN_HOST)],
                ),
                load_balancing_policy=RoundRobinPolicy(),
            ) as cluster:
                session = cluster.connect(wait_for_all_pools=True)

                for _ in range(20):
                    row = session.execute(
                        "SELECT release_version FROM system.local"
                    ).one()
                    self.assertIsNotNone(row)

                assert_routes_via_nlb(self, cluster, nlb,
                                      self.node_addrs.keys())

    def test_ssl_with_hostname_verification_raises_error(self):
        """
        Verify that Cluster raises ValueError when client_routes_config
        is used with SSL hostname verification enabled.
        """
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.load_verify_locations(os.path.join(self.cert_dir, 'ca.crt'))
        self.assertTrue(ssl_ctx.check_hostname)

        with self.assertRaises(ValueError) as cm:
            Cluster(
                contact_points=[NLBEmulator.LISTEN_HOST],
                ssl_context=ssl_ctx,
                client_routes_config=ClientRoutesConfig(
                    proxies=[ClientRouteProxy("test-id", NLBEmulator.LISTEN_HOST)],
                ),
            )
        self.assertIn("check_hostname", str(cm.exception))

@skip_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                        scylla_version="2026.1.0")
class TestFullNodeReplacementThroughNlb(unittest.TestCase):
    """
    End-to-end test: creates a session through an NLB proxy with client routes,
    scales the cluster up, then decommissions original nodes, verifying the
    session survives the full node replacement.

    This test is destructive — it modifies the CCM cluster topology by
    bootstrapping new nodes and decommissioning original ones.  It uses
    its own CCM cluster so it cannot interfere with other tests.
    """

    @classmethod
    def setUpClass(cls):
        cls._saved_scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS')
        os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
        use_cluster('test_client_routes_replacement', [3], start=True)

        cls.direct_cluster = TestCluster()
        cls.direct_session = cls.direct_cluster.connect()
        cls.host_id_map = get_host_ids_from_cluster(cls.direct_session)

        cls.node_addrs = {}
        for ip in cls.host_id_map:
            node_id = int(ip.split(".")[-1])
            cls.node_addrs[node_id] = ip

        cls.connection_id = str(uuid.uuid4())

    @classmethod
    def tearDownClass(cls):
        cls.direct_cluster.shutdown()
        if cls._saved_scylla_ext_opts is None:
            os.environ.pop('SCYLLA_EXT_OPTS', None)
        else:
            os.environ['SCYLLA_EXT_OPTS'] = cls._saved_scylla_ext_opts

    def test_should_survive_full_node_replacement_through_nlb(self):
        """
        1. Start with 3 nodes behind the NLB
        2. Bootstrap 2 new nodes, add to NLB, update routes
        3. Decommission the original 3 nodes one-by-one, updating NLB/routes
        4. Verify the session survives with only new nodes
        """
        original_node_ids = sorted(self.node_addrs.keys())
        with NLBEmulator(
            node_addresses=self.node_addrs,
        ) as nlb:
            # ---- Stage 1: Set up NLB for initial nodes ----
            log.info("Stage 1: Setting up NLB for %d initial nodes", len(original_node_ids))

            post_routes_for_nlb("127.0.0.1", self.connection_id, self.host_id_map, nlb)
            wait_for_routes_visible(self.direct_session, self.connection_id, len(self.host_id_map))

            # ---- Stage 2: Create session through NLB ----
            log.info("Stage 2: Creating session through NLB")
            with Cluster(
                contact_points=[NLBEmulator.LISTEN_HOST],
                port=nlb.discovery_port,
                client_routes_config=ClientRoutesConfig(
                    proxies=[ClientRouteProxy(self.connection_id, NLBEmulator.LISTEN_HOST)],
                ),
                load_balancing_policy=RoundRobinPolicy(),
            ) as cluster:
                session = cluster.connect(wait_for_all_pools=True)
                self._assert_query_works(session)

                handler = cluster._client_routes_handler
                self.assertIsNotNone(handler)

                assert_routes_via_nlb(self, cluster, nlb,
                                         original_node_ids)
                log.info("Stage 2: Session created, all %d nodes via NLB",
                         len(original_node_ids))

                # ---- Stage 3: Bootstrap new nodes ----
                new_node_ids = [max(original_node_ids) + 1, max(original_node_ids) + 2]
                log.info("Stage 3: Adding nodes %s", new_node_ids)
                ccm_cluster = get_cluster()

                for node_id in new_node_ids:
                    self._bootstrap_node(ccm_cluster, node_id, data_center='dc1')

                expected_total = len(original_node_ids) + len(new_node_ids)
                self._wait_for_condition(
                    lambda: len(cluster.metadata.all_hosts()) >= expected_total,
                    timeout_seconds=60,
                    description="%d nodes in metadata" % expected_total,
                )

                for node_id in new_node_ids:
                    nlb.add_node(node_id, "127.0.0.%d" % node_id)

                all_host_ids = get_host_ids_from_cluster(session)
                log.info("All host IDs after expansion: %s", all_host_ids)
                post_routes_for_nlb("127.0.0.1", self.connection_id, all_host_ids, nlb)

                handler.initialize(
                    cluster.control_connection._connection,
                    cluster.control_connection._timeout)

                self._wait_for_condition(
                    lambda: sum(1 for h in cluster.metadata.all_hosts() if h.is_up) >= expected_total,
                    timeout_seconds=60,
                    description="all %d nodes up" % expected_total,
                )

                self._assert_query_works(session)

                all_node_ids = set(original_node_ids) | set(new_node_ids)
                assert_routes_via_nlb(self, cluster, nlb, all_node_ids)
                log.info("Stage 3: All %d nodes via NLB after expansion",
                         len(all_node_ids))

                # ---- Stage 4: Decommission original nodes ----
                log.info("Stage 4: Decommissioning original nodes %s", original_node_ids)

                remaining_node_ids = set(all_node_ids)
                remaining_host_ids = dict(all_host_ids)
                for node_id in original_node_ids:
                    log.info("Decommissioning node %d", node_id)
                    get_node(node_id).decommission()
                    nlb.remove_node(node_id)
                    remaining_node_ids.discard(node_id)

                    ip = "127.0.0.%d" % node_id
                    remaining_host_ids.pop(ip, None)

                    surviving_ips = list(remaining_host_ids.keys())
                    if surviving_ips:
                        post_routes_for_nlb(
                            surviving_ips[0], self.connection_id,
                            remaining_host_ids, nlb,
                        )

                    expected_remaining = expected_total - (original_node_ids.index(node_id) + 1)
                    self._wait_for_condition(
                        lambda er=expected_remaining: (
                            len(cluster.metadata.all_hosts()) <= er
                            and self._query_succeeds(session)
                        ),
                        timeout_seconds=60,
                        description="node %d decommissioned" % node_id,
                    )

                    # Reload routes after the control connection has
                    # re-established itself (the decommission may have
                    # killed the old control connection).
                    handler.initialize(
                        cluster.control_connection._connection,
                        cluster.control_connection._timeout)

                    assert_routes_via_nlb(self, cluster, nlb,
                                             remaining_node_ids)
                    log.info("Node %d decommissioned, %d nodes still via NLB",
                             node_id, len(remaining_node_ids))

                # ---- Stage 5: Verify with only new nodes ----
                log.info("Stage 5: Verifying session works with only new nodes %s", new_node_ids)
                self._assert_query_works(session)

                hosts = cluster.metadata.all_hosts()
                self.assertEqual(
                    len(hosts), len(new_node_ids),
                    "Expected %d hosts, got %d" % (len(new_node_ids), len(hosts))
                )

                for _ in range(10):
                    self._assert_query_works(session)

                assert_routes_via_nlb(self, cluster, nlb, new_node_ids)
                log.info("PASS: Full node replacement, all %d new nodes via NLB",
                         len(new_node_ids))

    def _assert_query_works(self, session):
        rs = session.execute("SELECT release_version FROM system.local WHERE key='local'")
        row = rs.one()
        self.assertIsNotNone(row, "Query via NLB should return a result")

    def _query_succeeds(self, session):
        try:
            self._assert_query_works(session)
            return True
        except Exception:
            return False

    def _bootstrap_node(self, ccm_cluster, node_id, data_center=None, rack=None):
        node_type = type(next(iter(ccm_cluster.nodes.values())))
        ip = "127.0.0.%d" % node_id
        node_instance = node_type(
            'node%s' % node_id,
            ccm_cluster,
            auto_bootstrap=True,
            thrift_interface=(ip, 9160),
            storage_interface=(ip, 7000),
            binary_interface=(ip, 9042),
            jmx_port=str(7000 + 100 * node_id),
            remote_debug_port=0,
            initial_token=None,
        )
        # CCM requires explicit data_center/rack when adding a node so that
        # cassandra-rackdc.properties is written correctly.  Without this the
        # snitch fails to parse the empty properties file and the node crashes
        # on startup.
        ccm_cluster.add(node_instance, is_seed=False,
                        data_center=data_center, rack=rack)
        node_instance.start(wait_for_binary_proto=True, wait_other_notice=True)
        wait_for_node_socket(node_instance, 120)
        log.info("Node %d bootstrapped successfully", node_id)

    @staticmethod
    def _wait_for_condition(predicate, timeout_seconds, poll_interval=2, description="condition"):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(poll_interval)
        raise AssertionError(
            "Timed out waiting for %s after %d seconds" % (description, timeout_seconds)
        )
