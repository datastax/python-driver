# Copyright 2013-2017 DataStax, Inc.
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

from __future__ import print_function
import logging
import time

from collections import defaultdict
from ccmlib.node import Node

from cassandra.query import named_tuple_factory

from tests.integration import get_node, get_cluster, wait_for_node_socket

IP_FORMAT = '127.0.0.%s'

log = logging.getLogger(__name__)


class CoordinatorStats():

    def __init__(self):
        self.coordinator_counts = defaultdict(int)

    def add_coordinator(self, future):
        future.result()
        coordinator = future._current_host.address
        self.coordinator_counts[coordinator] += 1

        if future._errors:
            log.error('future._errors: %s', future._errors)

    def reset_counts(self):
        self.coordinator_counts = defaultdict(int)

    def get_query_count(self, node):
        ip = '127.0.0.%d' % node
        return self.coordinator_counts[ip]

    def assert_query_count_equals(self, testcase, node, expected):
        ip = '127.0.0.%d' % node
        if self.get_query_count(node) != expected:
            testcase.fail('Expected %d queries to %s, but got %d. Query counts: %s' % (
                expected, ip, self.coordinator_counts[ip], dict(self.coordinator_counts)))


def create_schema(cluster, session, keyspace, simple_strategy=True,
                  replication_factor=1, replication_strategy=None):
    row_factory = session.row_factory
    session.row_factory = named_tuple_factory

    if keyspace in cluster.metadata.keyspaces.keys():
        session.execute('DROP KEYSPACE %s' % keyspace, timeout=20)

    if simple_strategy:
        ddl = "CREATE KEYSPACE %s WITH replication" \
              " = {'class': 'SimpleStrategy', 'replication_factor': '%s'}"
        session.execute(ddl % (keyspace, replication_factor), timeout=10)
    else:
        if not replication_strategy:
            raise Exception('replication_strategy is not set')

        ddl = "CREATE KEYSPACE %s" \
              " WITH replication = { 'class' : 'NetworkTopologyStrategy', %s }"
        session.execute(ddl % (keyspace, str(replication_strategy)[1:-1]), timeout=10)

    ddl = 'CREATE TABLE %s.cf (k int PRIMARY KEY, i int)'
    session.execute(ddl % keyspace, timeout=10)
    session.execute('USE %s' % keyspace)

    session.row_factory = row_factory


def start(node):
    get_node(node).start()


def stop(node):
    get_node(node).stop()


def force_stop(node):
    log.debug("Forcing stop of node %s", node)
    get_node(node).stop(wait=False, gently=False)
    log.debug("Node %s was stopped", node)


def decommission(node):
    get_node(node).decommission()
    get_node(node).stop()


def bootstrap(node, data_center=None, token=None):
    node_instance = Node('node%s' % node,
                         get_cluster(),
                         auto_bootstrap=False,
                         thrift_interface=(IP_FORMAT % node, 9160),
                         storage_interface=(IP_FORMAT % node, 7000),
                         binary_interface=(IP_FORMAT % node, 9042),
                         jmx_port=str(7000 + 100 * node),
                         remote_debug_port=0,
                         initial_token=token if token else node * 10)
    get_cluster().add(node_instance, is_seed=False, data_center=data_center)

    try:
        start(node)
    except:
        # Try only twice
        try:
            start(node)
        except:
            log.error('Added node failed to start twice.')


def ring(node):
    get_node(node).nodetool('ring')


def wait_for_up(cluster, node):
    tries = 0
    addr = IP_FORMAT % node
    while tries < 100:
        host = cluster.metadata.get_host(addr)
        if host and host.is_up:
            wait_for_node_socket(get_node(node), 60)
            log.debug("Done waiting for node %s to be up", node)
            return
        else:
            log.debug("Host is still marked down, waiting")
            tries += 1
            time.sleep(1)

    # todo: don't mix string interpolation methods in the same package
    raise RuntimeError("Host {0} is not up after {1} attempts".format(addr, tries))


def wait_for_down(cluster, node):
    log.debug("Waiting for node %s to be down", node)
    tries = 0
    addr = IP_FORMAT % node
    while tries < 100:
        host = cluster.metadata.get_host(IP_FORMAT % node)
        if not host or not host.is_up:
            log.debug("Done waiting for node %s to be down", node)
            return
        else:
            log.debug("Host is still marked up, waiting")
            tries += 1
            time.sleep(1)

    raise RuntimeError("Host {0} is not down after {1} attempts".format(addr, tries))
