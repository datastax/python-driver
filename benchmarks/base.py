import logging
import os.path
import sys
import time
dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dirname)
sys.path.append(os.path.join(dirname, '..'))

from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.query import SimpleStatement

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

supported_reactors = [AsyncoreConnection]
try:
    from cassandra.io.pyevreactor import PyevConnection
    supported_reactors.append(PyevConnection)
except ImportError, exc:
    log.warning("Not benchmarking pyev reactor: %s" % (exc,))

KEYSPACE = "testkeyspace"
TABLE = "testtable"
NUM_QUERIES = 10000

def setup():

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        log.debug("dropping existing keyspace...")
        session.execute("DROP KEYSPACE " + KEYSPACE)

    log.debug("Creating keyspace...")
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    log.debug("Setting keyspace...")
    session.set_keyspace(KEYSPACE)

    log.debug("Creating table...")
    session.execute("""
        CREATE TABLE %s (
            thekey text,
            col1 text,
            col2 text,
            PRIMARY KEY (thekey, col1)
        )
        """ % TABLE)

def teardown():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("DROP KEYSPACE " + KEYSPACE)


def benchmark(run_fn):
    for conn_class in supported_reactors:
        setup()
        log.info("==== %s ====" % (conn_class.__name__,))

        cluster = Cluster(['127.0.0.1'])
        cluster.connection_class = conn_class
        session = cluster.connect(KEYSPACE)

        log.debug("Sleeping for two seconds...")
        time.sleep(2.0)

        query = SimpleStatement("""
            INSERT INTO {table} (thekey, col1, col2)
            VALUES (%(key)s, %(a)s, %(b)s)
            """.format(table=TABLE))
        values = {'key': 'key', 'a': 'a', 'b': 'b'}

        log.debug("Beginning inserts...")
        start = time.time()
        try:
            run_fn(session, query, values, NUM_QUERIES)
            end = time.time()
        finally:
            teardown()

        total = end - start
        log.info("Total time: %0.2fs" % total)
        log.info("Average throughput: %0.2f/sec" % (NUM_QUERIES / total))
