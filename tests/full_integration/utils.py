import struct
import time

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from tests.full_integration import get_node


coordinators = {}


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


def reset_coordinators():
    global coordinators
    coordinators = {}


def assert_queried(node, n):
    ip = '127.0.0.%s' % node
    print coordinators
    if ip in coordinators:
        if coordinators[ip] == n:
            return
        raise RuntimeError(
            'IP: %s. Expected: %s. Received: %s.' % (ip, n, coordinators[ip]))
    else:
        if n == 0:
            return
        raise RuntimeError('IP: %s. Expected: %s. Received: %s.' % (ip, n, 0))


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


def start(node):
    get_node(node).start()


def stop(node):
    get_node(node).stop()


def force_stop(node):
    get_node(node).stop(wait=False, gently=False)


def wait_for_up(cluster, node):
    while True:
        host = cluster.metadata.get_host('127.0.0.%s' % node)
        if host and host.monitor.is_up:
            # BUG: This shouldn't be needed.
            # Ideally, host.monitor.is_up would be enough?
            # If not, what should I be using?
            time.sleep(25)
            return


def wait_for_down(cluster, node):
    while True:
        host = cluster.metadata.get_host('127.0.0.%s' % node)
        if not host or not host.monitor.is_up:
            # BUG: This shouldn't be needed.
            # Ideally, host.monitor.is_up would be enough?
            # If not, what should I be using?
            time.sleep(25)
            return
