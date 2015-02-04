from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace

from tests.integration import PROTOCOL_VERSION


def setup_package():
    keyspace = 'cqlengine_test'
    connection.setup(['localhost'],
                      protocol_version=PROTOCOL_VERSION,
                      default_keyspace=keyspace)

    create_keyspace(keyspace, replication_factor=1, strategy_class="SimpleStrategy")
