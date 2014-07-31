# Copyright 2013-2014 DataStax, Inc.
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
    import unittest # noqa

import logging
log = logging.getLogger(__name__)

import os
from threading import Event

from cassandra.cluster import Cluster

try:
    from ccmlib.cluster import Cluster as CCMCluster
    from ccmlib import common
except ImportError as e:
    raise unittest.SkipTest('ccm is a dependency for integration tests:', e)

CLUSTER_NAME = 'test_cluster'
MULTIDC_CLUSTER_NAME = 'multidc_test_cluster'

CCM_CLUSTER = None

CASSANDRA_DIR = os.getenv('CASSANDRA_DIR', None)
CASSANDRA_HOME = os.getenv('CASSANDRA_HOME', None)
CASSANDRA_VERSION = os.getenv('CASSANDRA_VERSION', '2.0.9')

if CASSANDRA_VERSION.startswith('1'):
    default_protocol_version = 1
else:
    default_protocol_version = 2
PROTOCOL_VERSION = int(os.getenv('PROTOCOL_VERSION', default_protocol_version))

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

    c = Cluster(protocol_version=PROTOCOL_VERSION)
    s = c.connect()
    s.set_keyspace('system')
    row = s.execute('SELECT cql_version, release_version FROM local')[0]

    cass_version = _tuple_version(row.release_version)
    cql_version = _tuple_version(row.cql_version)

    c.shutdown()

    return (cass_version, cql_version)


def _tuple_version(version_string):
    if '-' in version_string:
        version_string = version_string[:version_string.index('-')]

    return tuple([int(p) for p in version_string.split('.')])


def get_cluster():
    return CCM_CLUSTER


def get_node(node_id):
    return CCM_CLUSTER.nodes['node%s' % node_id]


def setup_package():
    if CASSANDRA_DIR:
        log.info("Using Cassandra dir: %s", CASSANDRA_DIR)
    elif CASSANDRA_HOME:
        log.info("Using Cassandra home: %s", CASSANDRA_HOME)
    else:
        log.info('Using Cassandra version: %s', CASSANDRA_VERSION)
    try:
        try:
            cluster = CCMCluster.load(path, CLUSTER_NAME)
            log.debug("Found existing ccm test cluster, clearing")
            cluster.clear()
            if CASSANDRA_DIR:
                cluster.set_cassandra_dir(cassandra_dir=CASSANDRA_DIR)
            else:
                cluster.set_cassandra_dir(cassandra_version=CASSANDRA_VERSION)
        except Exception:
            if CASSANDRA_DIR:
                log.debug("Creating new ccm test cluster with cassandra dir %s", CASSANDRA_DIR)
                cluster = CCMCluster(path, CLUSTER_NAME, cassandra_dir=CASSANDRA_DIR)
            else:
                log.debug("Creating new ccm test cluster with version %s", CASSANDRA_VERSION)
                cluster = CCMCluster(path, CLUSTER_NAME, cassandra_version=CASSANDRA_VERSION)
            cluster.set_configuration_options({'start_native_transport': True})
            common.switch_cluster(path, CLUSTER_NAME)
            cluster.populate(3)

        log.debug("Starting ccm test cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
    except Exception:
        log.exception("Failed to start ccm cluster:")
        raise

    global CCM_CLUSTER
    CCM_CLUSTER = cluster
    setup_test_keyspace()


def clear_and_use_multidc(dc_list):
    teardown_package()
    try:
        try:
            cluster = CCMCluster.load(path, MULTIDC_CLUSTER_NAME)
            log.debug("Found existing ccm test multi-dc cluster, clearing")
            cluster.clear()
        except Exception:
            log.debug("Creating new ccm test multi-dc cluster")
            if CASSANDRA_DIR:
                cluster = CCMCluster(path, MULTIDC_CLUSTER_NAME, cassandra_dir=CASSANDRA_DIR)
            else:
                cluster = CCMCluster(path, MULTIDC_CLUSTER_NAME, cassandra_version=CASSANDRA_VERSION)
            cluster.set_configuration_options({'start_native_transport': True})
            common.switch_cluster(path, MULTIDC_CLUSTER_NAME)
            cluster.populate(dc_list)

        log.debug("Starting ccm test cluster")
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
    except Exception:
        log.exception("Failed to start ccm cluster:")
        raise

    global CCM_CLUSTER
    CCM_CLUSTER = cluster
    setup_test_keyspace()
    log.debug("Switched to multidc cluster")


def clear_and_use_singledc():
    teardown_package()

    setup_package()
    log.debug("Switched to singledc cluster")


def setup_test_keyspace():
    # wait for nodes to startup
    time.sleep(10)

    cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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


def teardown_package():
    for cluster_name in [CLUSTER_NAME, MULTIDC_CLUSTER_NAME]:
        try:
            cluster = CCMCluster.load(path, cluster_name)

            try:
                cluster.clear()
                cluster.remove()
                log.info('Cleared cluster: %s' % cluster_name)
            except Exception:
                log.exception('Failed to clear cluster: %s' % cluster_name)

        except Exception:
            log.warn('Did not find cluster: %s' % cluster_name)


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
