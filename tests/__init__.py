import logging
import unittest

from cassandra.cluster import Cluster
from cassandra.connection import _loop
from cassandra.policies import HostDistance

log = logging.getLogger()
log.setLevel('DEBUG')
log.addHandler(logging.StreamHandler())

existing_keyspaces = None

def setup_package():
    try:
        cluster = Cluster()
        cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        cluster.set_max_connections_per_host(HostDistance.LOCAL, 1)
        session = cluster.connect()
    except Exception, exc:
        log.error('Failed to connect to cluster:')
        log.error(exc)
        raise unittest.SkipTest('Failed to connect to cluster: %r' % exc)

    try:
        global existing_keyspaces
        results = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
        existing_keyspaces = set([row.values()[0] for row in results])
    finally:
        try:
            cluster.shutdown()
        except Exception, exc:
            log.error('Failed to connect to cluster:')
            log.error(exc)
            raise unittest.SkipTest('Failed to connect to cluster: %r' % exc)


def teardown_package():
    try:
        cluster = Cluster()
        cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        cluster.set_max_connections_per_host(HostDistance.LOCAL, 1)
        session = cluster.connect()
    except Exception, exc:
        log.error('Failed to connect to cluster:')
        log.error(exc)
        raise unittest.SkipTest('Failed to connect to cluster: %r' % exc)

    try:
        if existing_keyspaces:
            results = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
            current_keyspaces = set([row.values()[0] for row in results])
            for keyspace in current_keyspaces - existing_keyspaces:
                session.execute("DROP KEYSPACE %s" % (keyspace,))

    finally:
        try:
            cluster.shutdown()
            _loop.stop()
        except Exception, exc:
            log.error('Failed to connect to cluster:')
            log.error(exc)
