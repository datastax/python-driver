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
CCM_CLUSTER = None

path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ccm')
if not os.path.exists(path):
    os.mkdir(path)


def get_server_versions():
    """
    Probe system.local table to determine Cassandra and CQL version.
    Returns a tuple of (cassandra_version, cql_version).
    """
    c = Cluster()
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
    try:
        try:
            cluster = CCMCluster.load(path, CLUSTER_NAME)
            log.debug("Found existing ccm test cluster, clearing")
            cluster.clear()
        except Exception:
            log.debug("Creating new ccm test cluster")
            cluster = CCMCluster(path, CLUSTER_NAME, cassandra_version='1.2.9')
            cluster.set_configuration_options({'start_native_transport': True})
            common.switch_cluster(path, CLUSTER_NAME)
            cluster.populate(3)

        log.debug("Starting ccm test cluster")
        cluster.start(wait_for_binary_proto=True)
    except Exception:
        log.exception("Failed to start ccm cluster:")
        raise

    global CCM_CLUSTER
    CCM_CLUSTER = cluster
    setup_test_keyspace()


def setup_test_keyspace():
    cluster = Cluster()
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
    finally:
        cluster.shutdown()


def teardown_package():
    if CCM_CLUSTER:
        try:
            CCM_CLUSTER.clear()
        except Exception:
            log.exception("Failed to clear cluster")


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
