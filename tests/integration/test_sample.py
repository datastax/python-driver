


# Runs with:
# nosetests --nocapture --verbosity=3 tests.integration.long.test_consistency:ConsistencyTests.test_rfthree_tokenaware


import struct

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

def create_schema(session, keyspace, replication_class='SimpleStrategy',
                  replication_factor=1):
    results = session.execute(
        'SELECT keyspace_name FROM system.schema_keyspaces')
    existing_keyspaces = [row[0] for row in results]
    if keyspace in existing_keyspaces:
        session.execute('DROP KEYSPACE %s' % keyspace)
    if replication_class == 'SimpleStrategy':
        ddl = "\n            CREATE KEYSPACE %s\n            WITH replication" \
              " = {'class': 'SimpleStrategy', 'replication_factor': '%s'}"
        session.execute(ddl % (keyspace, replication_factor))
        ddl = '\n            CREATE TABLE %s.cf (\n            k int PRIMARY ' \
              'KEY,\n            i int)\n        '
        session.execute(ddl % keyspace)


def add_coordinator(future):
    global coordinators
    coordinator = future._current_host.address
    if coordinator in coordinators:
        coordinators[coordinator] += 1
    else:
        coordinators[coordinator] = 1
    if future._errors:
        print 'future._errors', future._errors
    future.result()

def get_queried(node):
    ip = '127.0.0.%s' % node
    if not ip in coordinators:
        return 0
    return coordinators[ip]


def init(session, keyspace, n, consistency_level=ConsistencyLevel.ONE):
    reset_coordinators()
    session.execute('USE %s' % keyspace)
    for i in range(n):
        ss = SimpleStatement('INSERT INTO %s(k, i) VALUES (0, 0)' % 'cf',
                             consistency_level=consistency_level)
        session.execute(ss)


def query(session, keyspace, n, consistency_level=ConsistencyLevel.ONE):
    routing_key = struct.pack('>i', 0)
    for i in range(n):
        ss = SimpleStatement('SELECT * FROM %s WHERE k = 0' % 'cf',
                             consistency_level=consistency_level,
                             routing_key=routing_key)
        add_coordinator(session.execute_async(ss))

def reset_coordinators():
    global coordinators
    coordinators = {}

class SampleTest(unittest.TestCase):
    def test_one(self):

        keyspace = 'test_rfthree_tokenaware'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()

        # import time
        # time.sleep(20)

        # create_schema(session, keyspace, replication_factor=1)
        create_schema(session, keyspace, replication_factor=3)
        init(session, keyspace, 12)
        query(session, keyspace, 12)

        for i in range(500):
            query(session, keyspace, 12)
            print get_queried(1)
            print get_queried(2)
            print get_queried(3)
            print
