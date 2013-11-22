import logging
import os.path
import sys
import time
from optparse import OptionParser
from greplin import scales

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

have_libev = False
supported_reactors = [AsyncoreConnection]
try:
    from cassandra.io.libevreactor import LibevConnection
    have_libev = True
    supported_reactors.append(LibevConnection)
except ImportError, exc:
    log.warning("Not benchmarking libev reactor: %s" % (exc,))

KEYSPACE = "testkeyspace"
TABLE = "testtable"

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
    options, args = parse_options()
    for conn_class in options.supported_reactors:
        setup()
        log.info("==== %s ====" % (conn_class.__name__,))

        cluster = Cluster(options.hosts, metrics_enabled=options.enable_metrics)
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
            run_fn(session, query, values, options.num_ops)
            end = time.time()
        finally:
            teardown()

        total = end - start
        log.info("Total time: %0.2fs" % total)
        log.info("Average throughput: %0.2f/sec" % (options.num_ops / total))
        if options.enable_metrics:
            stats = scales.getStats()['cassandra']
            log.info("Connection errors: %d", stats['connection_errors'])
            log.info("Write timeouts: %d", stats['write_timeouts'])
            log.info("Read timeouts: %d", stats['read_timeouts'])
            log.info("Unavailables: %d", stats['unavailables'])
            log.info("Other errors: %d", stats['other_errors'])
            log.info("Retries: %d", stats['retries'])

            request_timer = stats['request_timer']
            log.info("Request latencies:")
            log.info("  min: %0.4fs", request_timer['min'])
            log.info("  max: %0.4fs", request_timer['max'])
            log.info("  mean: %0.4fs", request_timer['mean'])
            log.info("  stddev: %0.4fs", request_timer['stddev'])
            log.info("  median: %0.4fs", request_timer['median'])
            log.info("  75th: %0.4fs", request_timer['75percentile'])
            log.info("  95th: %0.4fs", request_timer['95percentile'])
            log.info("  98th: %0.4fs", request_timer['98percentile'])
            log.info("  99th: %0.4fs", request_timer['99percentile'])
            log.info("  99.9th: %0.4fs", request_timer['999percentile'])


def parse_options():
    parser = OptionParser()
    parser.add_option('-H', '--hosts', default='127.0.0.1',
                      help='cassandra hosts to connect to (comma-separated list) [default: %default]')
    parser.add_option('-t', '--threads', type='int', default=1,
                      help='number of threads [default: %default]')
    parser.add_option('-n', '--num-ops', type='int', default=10000,
                      help='number of operations [default: %default]')
    parser.add_option('--asyncore-only', action='store_true', dest='asyncore_only',
                      help='only benchmark with asyncore connections')
    parser.add_option('--libev-only', action='store_true', dest='libev_only',
                      help='only benchmark with libev connections')
    parser.add_option('-m', '--metrics', action='store_true', dest='enable_metrics',
                      help='enable and print metrics for operations')
    options, args = parser.parse_args()

    options.hosts = options.hosts.split(',')

    if options.libev_only:
        if not have_libev:
            log.error("libev is not available")
            sys.exit(1)
        options.supported_reactors = [LibevConnection]
    elif options.asyncore_only:
        options.supported_reactors = [AsyncoreConnection]
    else:
        options.supported_reactors = supported_reactors

    return options, args
