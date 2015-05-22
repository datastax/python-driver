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

import time
import traceback

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import logging
log = logging.getLogger(__name__)

import os
from threading import Event
import six
from subprocess import call

from itertools import groupby

from cassandra.cluster import Cluster

try:
    from ccmlib.cluster import Cluster as CCMCluster
    from ccmlib.cluster_factory import ClusterFactory as CCMClusterFactory
    from ccmlib import common
except ImportError as e:
    CCMClusterFactory = None

CLUSTER_NAME = 'test_cluster'
SINGLE_NODE_CLUSTER_NAME = 'single_node'
MULTIDC_CLUSTER_NAME = 'multidc_test_cluster'

CCM_CLUSTER = None

path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ccm')
if not os.path.exists(path):
    os.mkdir(path)

cass_version = None
cql_version = None


def get_server_versions():
    """
    Probe system.local table to determine Cassandra and CQL version.
    Returns a tuple of (cassandra_version, cql_version).
    """
    global cass_version, cql_version

    if cass_version is not None:
        return (cass_version, cql_version)

    c = Cluster(protocol_version=1)
    s = c.connect()
    row = s.execute('SELECT cql_version, release_version FROM system.local')[0]

    cass_version = _tuple_version(row.release_version)
    cql_version = _tuple_version(row.cql_version)

    c.shutdown()

    return (cass_version, cql_version)


def _tuple_version(version_string):
    if '-' in version_string:
        version_string = version_string[:version_string.index('-')]

    return tuple([int(p) for p in version_string.split('.')])


USE_CASS_EXTERNAL = bool(os.getenv('USE_CASS_EXTERNAL', False))

default_cassandra_version = '2.1.3'

if USE_CASS_EXTERNAL:
    if CCMClusterFactory:
        # see if the external instance is running in ccm
        path = common.get_default_path()
        name = common.current_cluster_name(path)
        CCM_CLUSTER = CCMClusterFactory.load(common.get_default_path(), name)
        CCM_CLUSTER.start(wait_for_binary_proto=True, wait_other_notice=True)

    # Not sure what's going on, but the server version query
    # hangs in python3. This appears to be related to running inside of
    # nosetests, and only for this query that would run while loading the
    # module.
    # This is a hack to make it run with default cassandra version for PY3.
    # Not happy with it, but need to move on for now.
    if not six.PY3:
        cass_ver, _ = get_server_versions()
        default_cassandra_version = '.'.join('%d' % i for i in cass_ver)
    else:
        if not os.getenv('CASSANDRA_VERSION'):
            log.warning("Using default C* version %s because external server cannot be queried" % default_cassandra_version)


CASSANDRA_DIR = os.getenv('CASSANDRA_DIR', None)
CASSANDRA_VERSION = os.getenv('CASSANDRA_VERSION', default_cassandra_version)

CCM_KWARGS = {}
if CASSANDRA_DIR:
    log.info("Using Cassandra dir: %s", CASSANDRA_DIR)
    CCM_KWARGS['install_dir'] = CASSANDRA_DIR
else:
    log.info('Using Cassandra version: %s', CASSANDRA_VERSION)
    CCM_KWARGS['version'] = CASSANDRA_VERSION

if CASSANDRA_VERSION >= '2.2':
    default_protocol_version = 4
elif CASSANDRA_VERSION >= '2.1':
    default_protocol_version = 3
elif CASSANDRA_VERSION >= '2.0':
    default_protocol_version = 2
else:
    default_protocol_version = 1

PROTOCOL_VERSION = int(os.getenv('PROTOCOL_VERSION', default_protocol_version))


def get_cluster():
    return CCM_CLUSTER


def get_node(node_id):
    return CCM_CLUSTER.nodes['node%s' % node_id]


def use_multidc(dc_list):
    use_cluster(MULTIDC_CLUSTER_NAME, dc_list, start=True)


def use_singledc(start=True):
    use_cluster(CLUSTER_NAME, [3], start=start)


def use_single_node(start=True):
    use_cluster(SINGLE_NODE_CLUSTER_NAME, [1], start=start)


def remove_cluster():
    if USE_CASS_EXTERNAL:
        return

    global CCM_CLUSTER
    if CCM_CLUSTER:
        log.debug("removing cluster %s", CCM_CLUSTER.name)
        CCM_CLUSTER.remove()
        CCM_CLUSTER = None


def is_current_cluster(cluster_name, node_counts):
    global CCM_CLUSTER
    if CCM_CLUSTER and CCM_CLUSTER.name == cluster_name:
        if [len(list(nodes)) for dc, nodes in
                groupby(CCM_CLUSTER.nodelist(), lambda n: n.data_center)] == node_counts:
            return True
    return False


def use_cluster(cluster_name, nodes, ipformat=None, start=True):
    global CCM_CLUSTER
    if USE_CASS_EXTERNAL:
        if CCM_CLUSTER:
            log.debug("Using external ccm cluster %s", CCM_CLUSTER.name)
        else:
            log.debug("Using unnamed external cluster")
        return

    if is_current_cluster(cluster_name, nodes):
        log.debug("Using existing cluster %s", cluster_name)
        return

    if CCM_CLUSTER:
        log.debug("Stopping cluster %s", CCM_CLUSTER.name)
        CCM_CLUSTER.stop()

    try:
        try:
            cluster = CCMClusterFactory.load(path, cluster_name)
            log.debug("Found existing ccm %s cluster; clearing", cluster_name)
            cluster.clear()
            cluster.set_install_dir(**CCM_KWARGS)
        except Exception:
            log.debug("Creating new ccm %s cluster with %s", cluster_name, CCM_KWARGS)
            cluster = CCMCluster(path, cluster_name, **CCM_KWARGS)
            cluster.set_configuration_options({'start_native_transport': True})
            if CASSANDRA_VERSION >= '2.2':
                cluster.set_configuration_options({'enable_user_defined_functions': True})
            common.switch_cluster(path, cluster_name)
            cluster.populate(nodes, ipformat=ipformat)

        jvm_args = []
        # This will enable the Mirroring query handler which will echo our custom payload k,v pairs back
        if PROTOCOL_VERSION >= 4:
            jvm_args = [" -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler"]

        if start:
            log.debug("Starting ccm %s cluster", cluster_name)
            cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=jvm_args)
            setup_keyspace(ipformat=ipformat)

        CCM_CLUSTER = cluster
    except Exception:
        log.exception("Failed to start ccm cluster. Removing cluster.")
        remove_cluster()
        call(["pkill", "-9", "-f", ".ccm"])
        raise


def teardown_package():
    if USE_CASS_EXTERNAL:
        return
    # when multiple modules are run explicitly, this runs between them
    # need to make sure CCM_CLUSTER is properly cleared for that case
    remove_cluster()
    for cluster_name in [CLUSTER_NAME, MULTIDC_CLUSTER_NAME]:
        try:
            cluster = CCMClusterFactory.load(path, cluster_name)
            try:
                cluster.remove()
                log.info('Removed cluster: %s' % cluster_name)
            except Exception:
                log.exception('Failed to remove cluster: %s' % cluster_name)

        except Exception:
            log.warning('Did not find cluster: %s' % cluster_name)


def setup_keyspace(ipformat=None):
    # wait for nodes to startup
    time.sleep(10)

    if not ipformat:
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
    else:
        cluster = Cluster(contact_points=["::1"], protocol_version=PROTOCOL_VERSION)
    session = cluster.connect()

    try:
        results = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
        existing_keyspaces = [row[0] for row in results]
        for ksname in ('test1rf', 'test2rf', 'test3rf'):
            if ksname in existing_keyspaces:
                session.execute("DROP KEYSPACE %s" % ksname)

        ddl = '''
            CREATE KEYSPACE test3rf
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}'''
        session.execute(ddl)

        ddl = '''
            CREATE KEYSPACE test2rf
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}'''
        session.execute(ddl)

        ddl = '''
            CREATE KEYSPACE test1rf
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}'''
        session.execute(ddl)

        ddl = '''
            CREATE TABLE test3rf.test (
                k int PRIMARY KEY,
                v int )'''
        session.execute(ddl)

    except Exception:
        traceback.print_exc()
        raise
    finally:
        cluster.shutdown()


class UpDownWaiter(object):

    def __init__(self, host):
        self.down_event = Event()
        self.up_event = Event()
        host.monitor.register(self)

    def on_up(self, host):
        self.up_event.set()

    def on_down(self, host):
        self.down_event.set()

    def wait_for_down(self):
        self.down_event.wait()

    def wait_for_up(self):
        self.up_event.wait()
