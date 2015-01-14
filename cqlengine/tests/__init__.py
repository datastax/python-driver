import os


from cqlengine import connection
from cqlengine.management import create_keyspace


def setup_package():
    try:
        CASSANDRA_VERSION = int(os.environ["CASSANDRA_VERSION"])
    except:
        print("CASSANDRA_VERSION must be set as an environment variable. "
              "One of (12, 20, 21)")
        raise

    if os.environ.get('CASSANDRA_TEST_HOST'):
        CASSANDRA_TEST_HOST = os.environ['CASSANDRA_TEST_HOST']
    else:
        CASSANDRA_TEST_HOST = 'localhost'

    if CASSANDRA_VERSION < 20:
        protocol_version = 1
    else:
        protocol_version = 2

    connection.setup([CASSANDRA_TEST_HOST],
                      protocol_version=protocol_version,
                      default_keyspace='cqlengine_test')

    create_keyspace("cqlengine_test", replication_factor=1, strategy_class="SimpleStrategy")
