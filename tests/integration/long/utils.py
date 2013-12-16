import struct
import time

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from tests.integration import get_node


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


def get_queried(node):
    ip = '127.0.0.%s' % node
    if not ip in coordinators:
        return 0
    return coordinators[ip]


def assert_queried(node, n):
    ip = '127.0.0.%s' % node
    if ip in coordinators:
        if coordinators[ip] == n:
            return
        raise RuntimeError(
            'IP: %s. Expected: %s. Received: %s. Full detail: %s.' % (ip, n, coordinators[ip], coordinators))
    else:
        if n == 0:
            return
        raise RuntimeError('IP: %s. Expected: %s. Received: %s. Full detail: %s.' % (ip, n, 0, coordinators))


def create_schema(session, keyspace, replication_class='SS',
                  replication_factor=1, replication_strategy=None):

    results = session.execute(
        'SELECT keyspace_name FROM system.schema_keyspaces')
    existing_keyspaces = [row[0] for row in results]
    if keyspace in existing_keyspaces:
        session.execute('DROP KEYSPACE %s' % keyspace)

    if replication_class == 'SS':
        ddl = "CREATE KEYSPACE %s WITH replication" \
              " = {'class': 'SimpleStrategy', 'replication_factor': '%s'}"
        session.execute(ddl % (keyspace, replication_factor))
    elif replication_class == 'NTS':
        if not replication_strategy:
            raise Exception('replication_strategy is not set')

        ddl = "CREATE KEYSPACE %s" \
              " WITH replication = { 'class' : 'NetworkTopologyStrategy', %s }"
        session.execute(ddl % (keyspace, str(replication_strategy)[1:-1]))

    ddl = 'CREATE TABLE %s.cf (k int PRIMARY KEY, i int)'
    session.execute(ddl % keyspace)
    session.execute('USE %s' % keyspace)

    # BUG: probably related to PYTHON-39
    time.sleep(5)


def init(session, keyspace, n, consistency_level=ConsistencyLevel.ONE):
    reset_coordinators()
    # BUG: PYTHON-38
    # session.execute('USE %s' % keyspace)
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

def ring(node):
    print 'From node%s:' % node
    get_node(node).nodetool('ring')


def wait_for_up(cluster, node):
    while True:
        host = cluster.metadata.get_host('127.0.0.%s' % node)
        if host and host.is_up:
            # BUG: shouldn't have to, but we do
            time.sleep(5)
            return


def wait_for_down(cluster, node):
    while True:
        host = cluster.metadata.get_host('127.0.0.%s' % node)
        if not host or not host.is_up:
            # BUG: shouldn't have to, but we do
            time.sleep(5)
            return
